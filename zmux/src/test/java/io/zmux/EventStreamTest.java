package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.core.Event;
import io.zmux.internal.core.PipeInfo;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The keyed event queue and the tracer's view of each occurrence (the event stream and tracer rules) through real
 * sessions; the queue's rules and the tracer's backlog have deterministic unit tests of their own
 * ({@code EventQueueTest}, {@code TracerBacklogTest}).
 */
@Timeout(60)
class EventStreamTest {
    private static ZmuxConfig.Builder multipath(EventLoops loops, byte[] psk) {
        return ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(PipeAuth.psk(psk).withEndToEnd()).noProbation();
    }

    /** Records the tracer's events of every session it serves, and completes a future once want holds. */
    private static final class Seen implements Tracer {
        final List<SessionEvent> events = Collections.synchronizedList(new ArrayList<>());
        final CompletableFuture<Void> enough = new CompletableFuture<>();
        private final java.util.function.Predicate<List<SessionEvent>> want;

        Seen(java.util.function.Predicate<List<SessionEvent>> want) { this.want = want; }

        @Override public void event(SessionId session, SessionEvent event) {
            events.add(event);
            synchronized (events) { if (want.test(events)) enough.complete(null); }
        }
    }

    @Test void theTracerGetsOnePipesRetiredPerRetiredPipeNotMergedWithTheStreams() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            long[] ids = new long[2];
            Seen tracer = new Seen(events -> {
                synchronized (events) { return events.stream().filter(e -> e instanceof SessionEvent.PipesRetired).count() >= 2; }
            });
            ZmuxConfig cfg = multipath(loops, new byte[32]).tracer(tracer).build();
            ZmuxConfig plain = multipath(loops, new byte[32]).build();
            try (ZmuxListener listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(plain))) {
                var accepted = ex.submit((Callable<ZmuxSession>) listener::accept);
                ZmuxSession c = Zmux.connect(listener.localAddress(), cfg), s = accepted.get();
                for (int i = 0; i < ids.length; i++) {
                    ZmuxPipe pipe = c.addPipe(listener.localAddress(), PipeOptions.defaults());
                    ids[i] = pipe.id().value();
                    pipe.awaitAttached(Duration.ofSeconds(10));
                    pipe.close();
                    pipe.awaitClosed(Duration.ofSeconds(10));
                }
                tracer.enough.get(10, TimeUnit.SECONDS);
                List<SessionEvent.PipesRetired> got;
                synchronized (tracer.events) {
                    got = tracer.events.stream().filter(SessionEvent.PipesRetired.class::isInstance).map(SessionEvent.PipesRetired.class::cast).toList();
                }
                // Each pipe is its own event, retired 1, last that pipe, however the stream merges the two.
                assertEquals(2, got.size());
                for (int i = 0; i < ids.length; i++) {
                    var r = got.get(i);
                    assertEquals(1, r.retired());
                    assertEquals(List.of(PipeId.of(ids[i])), r.last());
                    assertEquals(1, r.failed() + r.closed(), "the outcome of the one pipe: " + r);
                }
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
        }
    }

    @Test void aFailedPipeIsRetiredAsFailedInTheTracerAndACleanOneAsClosed() {
        long pipe = 7;
        var failed = Events.traceEvent(new Event(Event.Kind.PIPE_STATE, pipe, io.zmux.internal.core.Action.DIR_BIDI, PipeInfo.Out.RETIRED, null, 0, 0, 0, 0, null, null, null, false, true, 0, 0, null, null));
        var clean = Events.traceEvent(new Event(Event.Kind.PIPE_STATE, pipe, io.zmux.internal.core.Action.DIR_BIDI, PipeInfo.Out.RETIRED, null, 0, 0, 0, 0, null, null, null, false, false, 0, 0, null, null));
        assertEquals(new SessionEvent.PipesRetired(1, 1, 0, List.of(PipeId.of(pipe))), failed);
        assertEquals(new SessionEvent.PipesRetired(1, 0, 1, List.of(PipeId.of(pipe))), clean);
        for (var r : List.of((SessionEvent.PipesRetired) failed, (SessionEvent.PipesRetired) clean)) {
            assertEquals(1, r.failed() + r.closed(), "a retirement is exactly one of failed and closed");
        }
        assertNull(Events.traceEvent(new Event(Event.Kind.PIPE_STATE, pipe, io.zmux.internal.core.Action.DIR_OUT, PipeInfo.Out.CLOSED, null, 0, 0, 0, 0, null, null, null, false, false, 0, 0, null, null)),
                "the other pipe states are internal");
    }

    /**
     * A peer that fails join after join: the tracer gets each failure with count 1; the event stream holds
     * one {@link SessionEvent.JoinAuthFailed} per reason with the number of failures, after the event that
     * was already unread, which the storm does not displace.
     */
    @Test void repeatedFailedHellosAreCountedInOneEventAndDisplaceNothing() throws Exception {
        int failures = 12;
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            byte[] other = new byte[32];
            other[0] = 1;
            Seen tracer = new Seen(events -> {
                synchronized (events) { return events.stream().filter(e -> e instanceof SessionEvent.JoinAuthFailed).count() >= failures; }
            });
            var lc = ListenerConfig.builder().session(multipath(loops, new byte[32]).tracer(tracer).build())
                    .firstRecordTimeout(Duration.ofMillis(200)).build();
            try (ZmuxListener l = Zmux.listen(NetworkTest.any(), lc)) {
                var accepted = ex.submit((Callable<ZmuxSession>) l::accept);
                ZmuxSession c = Zmux.connect(l.localAddress(), multipath(loops, other).build()), s = accepted.get();
                // An unread event of another key, before the storm.
                s.pipes().getFirst().update(PipeUpdate.builder().sendPriority(7).build());
                for (int i = 0; i < failures; i++) assertThrows(java.io.IOException.class, () -> c.addPipe(l.localAddress(), PipeOptions.defaults()));
                tracer.enough.get(10, TimeUnit.SECONDS);
                synchronized (tracer.events) {
                    for (SessionEvent e : tracer.events) if (e instanceof SessionEvent.JoinAuthFailed f) assertEquals(1, f.count(), "the tracer sees each failure");
                }
                List<SessionEvent> read = new ArrayList<>();
                long counted = 0;
                while (counted < failures) {
                    SessionEvent e = s.nextEvent(Duration.ofSeconds(10));
                    read.add(e);
                    if (e instanceof SessionEvent.JoinAuthFailed f) {
                        assertEquals("auth", f.reason());
                        counted += f.count();
                    }
                }
                int priority = -1, firstFailed = -1;
                for (int i = 0; i < read.size(); i++) {
                    if (read.get(i) instanceof SessionEvent.PipePriorityChanged p && p.priority() == 7 && priority < 0) priority = i;
                    if (read.get(i) instanceof SessionEvent.JoinAuthFailed && firstFailed < 0) firstFailed = i;
                }
                assertTrue(priority >= 0 && priority < firstFailed, "the earlier event is read first, undisturbed: " + read);
                assertEquals(failures, s.stats().joinAuthFailures());
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
        }
    }
}
