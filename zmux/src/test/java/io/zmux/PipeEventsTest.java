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
                .probation(Duration.ZERO).build();
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
                long id = added.id();
                List<SessionEvent> seen = new ArrayList<>();
                var a = await(c, SessionEvent.PipeAdded.class, e -> e.pipe() == id, seen);
                assertEquals(PipeOptions.Direction.SEND_ONLY, a.direction());
                await(c, SessionEvent.PipeJoined.class, e -> e.pipe() == id && e.direction() == PipeOptions.Direction.SEND_ONLY, seen);

                added.awaitPeerAttached(Duration.ofSeconds(10));
                var st = added.stats().orElseThrow();
                assertTrue(st.peerAttached());
                assertEquals(RecordMode.SEALED, st.recordMode());
                assertEquals("", st.variant());
                assertTrue(c.pipeStats().stream().filter(x -> x.pipe() == 0).findFirst().orElseThrow().peerAttached());

                // A receive priority set on the joined pipe reaches the server as a request: source PEER there.
                added.setReceivePriority(5);
                List<SessionEvent> serverSeen = new ArrayList<>();
                var peer = await(s, SessionEvent.PipePriorityChanged.class, e -> e.pipe() == id && e.priority() == 5, serverSeen);
                assertEquals(PipeStats.SettingSource.PEER, peer.source());

                added.setSendPriority(9);
                var local = await(c, SessionEvent.PipePriorityChanged.class, e -> e.pipe() == id && e.priority() == 9, seen);
                assertEquals(PipeStats.SettingSource.LOCAL, local.source());

                added.close();
                var closed = await(c, SessionEvent.PipeClosed.class, e -> e.pipe() == id, seen);
                assertNotNull(closed.direction());
                for (SessionEvent e : seen) {
                    if (e instanceof SessionEvent.PipeSuspect || e instanceof SessionEvent.PipeRecovered || e instanceof SessionEvent.PipeFailed) {
                        fail("a clean pipe reported " + e);
                    }
                }
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
        }
    }
}
