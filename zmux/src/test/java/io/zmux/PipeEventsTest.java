package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The pipe events: six state kinds with a direction, and the source of a priority change. */
@Timeout(30)
class PipeEventsTest {
    private static ZmuxConfig config(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).maxPipes(3).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                .noProbation().build();
    }

    /** Reads events until one matches, keeping the ones read before it in seen. */
    private static <T extends SessionEvent> T await(ZmuxSession s, Class<T> type, Predicate<T> p, List<SessionEvent> seen) throws Exception {
        long end = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < end) {
            SessionEvent e = s.nextEvent(Duration.ofSeconds(10));
            if (e == null) continue;
            seen.add(e);
            if (type.isInstance(e) && p.test(type.cast(e))) return type.cast(e);
        }
        throw new AssertionError("no " + type.getSimpleName() + " in " + seen);
    }

    @Test void stateEventsAreSeparateKindsAndPriorityChangesNameTheirSource() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            ZmuxConfig cfg = config(loops);
            try (ZmuxListener listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg))) {
                var sf = ex.submit((Callable<ZmuxSession>) listener::accept);
                ZmuxSession c = Zmux.connect(listener.localAddress(), cfg), s = sf.get();
                ZmuxPipe added = c.addPipe(listener.localAddress(), PipeOptions.builder().receivePriority(12).build());
                long id = added.id().value();
                List<SessionEvent> seen = new ArrayList<>();
                var a = await(c, SessionEvent.PipeAdded.class, e -> e.pipe().value() == id, seen);
                assertEquals(PipeOptions.Direction.SEND_ONLY, a.direction());
                await(c, SessionEvent.PipeJoined.class, e -> e.pipe().value() == id && e.direction() == PipeOptions.Direction.SEND_ONLY, seen);

                added.awaitAttached(Duration.ofSeconds(10));
                var st = added.stats().orElseThrow();
                assertTrue(st.peerAttached());
                assertEquals(RecordMode.SEALED, st.recordMode());
                assertEquals("", st.variant());
                assertTrue(c.pipeStats().stream().filter(x -> x.pipe().value() == 0).findFirst().orElseThrow().peerAttached());

                // A receive priority set on the joined pipe reaches the server as a request: source PEER there.
                added.update(PipeUpdate.builder().receivePriority(5).build());
                List<SessionEvent> serverSeen = new ArrayList<>();
                var peer = await(s, SessionEvent.PipePriorityChanged.class, e -> e.pipe().value() == id && e.priority() == 5, serverSeen);
                assertEquals(PipeStats.SettingSource.PEER, peer.source());

                added.update(PipeUpdate.builder().sendPriority(9).build());
                var local = await(c, SessionEvent.PipePriorityChanged.class, e -> e.pipe().value() == id && e.priority() == 9, seen);
                assertEquals(PipeStats.SettingSource.LOCAL, local.source());

                added.close();
                // The pipe's end is reported by PipeClosed, or, when it was retired before this read, only by the
                // retirement summary that folds it (a clean end counts as closed): either is the report.
                var end = await(c, SessionEvent.class, e -> (e instanceof SessionEvent.PipeClosed pc && pc.pipe().value() == id)
                        || (e instanceof SessionEvent.PipesRetired pr && pr.last().stream().anyMatch(p -> p.value() == id)), seen);
                if (end instanceof SessionEvent.PipeClosed pc) assertNotNull(pc.direction());
                else assertEquals(0, ((SessionEvent.PipesRetired) end).failed(), "a clean pipe retired as failed: " + end);
                for (SessionEvent e : seen) {
                    if (e instanceof SessionEvent.PipeSuspect || e instanceof SessionEvent.PipeRecovered || e instanceof SessionEvent.PipeFailed) {
                        fail("a clean pipe reported " + e);
                    }
                }
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
        }
    }

    /**
     * A pipe retired before the application read its events is reported by the retirement summary alone: its
     * unread events (PipeClosed among them) are folded into it, and no per-pipe event follows.
     * Reading only after the pipe's removal finished makes that order certain.
     */
    @Test void aPipeRetiredBeforeItsEventsAreReadHasNoPipeClosedButTheSummary() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            ZmuxConfig cfg = config(loops);
            try (ZmuxListener listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg))) {
                var sf = ex.submit((Callable<ZmuxSession>) listener::accept);
                ZmuxSession c = Zmux.connect(listener.localAddress(), cfg), s = sf.get();
                ZmuxPipe added = c.addPipe(listener.localAddress(), PipeOptions.defaults());
                long id = added.id().value();
                added.awaitAttached(Duration.ofSeconds(10));
                added.close();
                added.awaitClosed(Duration.ofSeconds(10)); // retired: nothing of the pipe was read before
                List<SessionEvent> seen = new ArrayList<>();
                var summary = await(c, SessionEvent.PipesRetired.class, e -> e.last().stream().anyMatch(p -> p.value() == id), seen);
                assertTrue(summary.retired() >= 1 && summary.closed() >= 1, "the unread end is counted as closed: " + summary);
                for (SessionEvent e : seen) {
                    assertFalse(e instanceof SessionEvent.PipeClosed, "an unread event of a retired pipe is folded: " + seen);
                }
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
        }
    }
}
