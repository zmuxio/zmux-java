package io.zmux.internal.loop;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.SessionEvent;
import io.zmux.SessionId;
import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.Event;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.SessionError;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The tracer's backlog (the tracer rules): 256 events per session, established, dropped and closed outside
 * it, the loss count where the lost events stood, nothing after the end. The core's events are handed to the
 * driver one by one on its loop and the tracer is held on a latch, so every count is exact and no test waits
 * for a clock.
 */
@Timeout(30)
class TracerBacklogTest {
    private static final long WAIT = 10;

    /** A tracer that records its calls in order; established waits for the gate. */
    private static final class Recorder implements DriverHooks.TraceSink {
        final List<String> calls = Collections.synchronizedList(new ArrayList<>());
        final CountDownLatch gate = new CountDownLatch(1), closed = new CountDownLatch(1);
        final CountDownLatch events;
        final AtomicInteger views = new AtomicInteger();

        Recorder(int awaitedEvents) { events = new CountDownLatch(awaitedEvents); }

        @Override public SessionEvent view(Event e) {
            views.incrementAndGet();
            return new SessionEvent.EpochCommitted(e.code());
        }
        @Override public void established(SessionId session) {
            calls.add("established");
            try { assertTrue(gate.await(WAIT, TimeUnit.SECONDS)); } catch (InterruptedException e) { throw new AssertionError(e); }
        }
        @Override public void event(SessionId session, SessionEvent event) {
            if (event instanceof SessionEvent.EpochCommitted committed) calls.add("event " + committed.epoch());
            else calls.add("unexpected " + event);
            events.countDown();
        }
        @Override public void dropped(SessionId session, long count) { calls.add("dropped " + count); }
        @Override public void closed(SessionId session, SessionError error) {
            calls.add("closed");
            closed.countDown();
        }
    }

    private static Event occurrence(long n) {
        return new Event(Event.Kind.EPOCH_COMMITTED, 0, 0, null, null, 0, 0, n, 0, null, null, null, false, false, 0, 0, null);
    }

    private static Event bare(Event.Kind kind) {
        return new Event(kind, 0, 0, null, null, 0, 0, 0, 0, null, null, null, false, false, 0, 0, null);
    }

    private static List<String> events(long from, long to) {
        List<String> l = new ArrayList<>();
        for (long i = from; i <= to; i++) l.add("event " + i);
        return l;
    }

    private static List<String> concat(List<String> a, List<String> b) {
        List<String> l = new ArrayList<>(a);
        l.addAll(b);
        return l;
    }

    private interface Script { void run(EventLoop loop, SessionRuntime driver, Recorder tracer) throws Exception; }

    private static Recorder play(int awaitedEvents, Script script) throws Exception {
        EventLoop loop = new EventLoop("tracer-backlog", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            Recorder tracer = new Recorder(awaitedEvents);
            var hooks = new DriverHooks(tracer, null, null, null, null, null, DriverHooks.JoinPolicy.DEFAULT, null, null, null);
            SessionRuntime[] driver = new SessionRuntime[1];
            ExternalTransportTest.run(loop, () -> driver[0] = new SessionRuntime(loop, new Session(CoreConfig.defaults(), Clock.now()), hooks));
            script.run(loop, driver[0], tracer);
            return tracer;
        } finally { loop.close(); }
    }

    private static void deliver(EventLoop loop, SessionRuntime driver, Event... events) throws Exception {
        ExternalTransportTest.run(loop, () -> { for (Event e : events) driver.deliver(e); });
    }

    @Test void aBlockedTracerKeepsTheFirst256EventsAndReportsTheRestBeforeClosed() throws Exception {
        var t = play(256, (loop, driver, tracer) -> {
            deliver(loop, driver, bare(Event.Kind.ESTABLISHED));
            // Established holds the hook context: every event that follows meets a backlog that cannot drain.
            Event[] all = new Event[300];
            for (int i = 0; i < all.length; i++) all[i] = occurrence(i + 1);
            deliver(loop, driver, all);
            deliver(loop, driver, bare(Event.Kind.CLOSED));
            // What occurs after the end is traced by nobody: not even viewed.
            int viewed = tracer.views.get();
            deliver(loop, driver, occurrence(1000), bare(Event.Kind.ESTABLISHED), bare(Event.Kind.CLOSED));
            assertEquals(viewed, tracer.views.get());
            tracer.gate.countDown();
            assertTrue(tracer.closed.await(WAIT, TimeUnit.SECONDS));
        });
        assertEquals(concat(concat(List.of("established"), events(1, 256)), List.of("dropped 44", "closed")), t.calls);
    }

    @Test void applicationEventsBeforeEstablishedReachTheTracerInOrderAfterIt() throws Exception {
        var t = play(3, (loop, driver, tracer) -> {
            ExternalTransportTest.run(loop, () -> {
                driver.addApplicationEvent("a", new SessionEvent.EpochCommitted(1));
                driver.addApplicationEvent("b", new SessionEvent.EpochCommitted(2));
            });
            deliver(loop, driver, bare(Event.Kind.ESTABLISHED));
            ExternalTransportTest.run(loop, () -> driver.addApplicationEvent("c", new SessionEvent.EpochCommitted(3)));
            tracer.gate.countDown();
            assertTrue(tracer.events.await(WAIT, TimeUnit.SECONDS));
        });
        assertEquals(List.of("established", "event 1", "event 2", "event 3"), t.calls);
    }

    @Test void theLossCountStandsBeforeTheNextAcceptedEvent() throws Exception {
        var t = play(256, (loop, driver, tracer) -> {
            deliver(loop, driver, bare(Event.Kind.ESTABLISHED));
            Event[] all = new Event[259];
            for (int i = 0; i < all.length; i++) all[i] = occurrence(i + 1);
            deliver(loop, driver, all);
            tracer.gate.countDown();
            assertTrue(tracer.events.await(WAIT, TimeUnit.SECONDS), "the backlog drained");
            // The room is back: the next event is accepted, and the three lost ones are reported where they stood.
            deliver(loop, driver, occurrence(300), occurrence(301), bare(Event.Kind.CLOSED));
            assertTrue(tracer.closed.await(WAIT, TimeUnit.SECONDS));
        });
        assertEquals(concat(concat(List.of("established"), events(1, 256)), List.of("dropped 3", "event 300", "event 301", "closed")), t.calls);
    }

    @Test void aLossWithNoLaterEventIsReportedJustBeforeClosedAndNeverAfterIt() throws Exception {
        var t = play(256, (loop, driver, tracer) -> {
            deliver(loop, driver, bare(Event.Kind.ESTABLISHED));
            Event[] all = new Event[258];
            for (int i = 0; i < all.length; i++) all[i] = occurrence(i + 1);
            deliver(loop, driver, all);
            tracer.gate.countDown();
            assertTrue(tracer.events.await(WAIT, TimeUnit.SECONDS));
            deliver(loop, driver, bare(Event.Kind.CLOSED));
            assertTrue(tracer.closed.await(WAIT, TimeUnit.SECONDS));
        });
        assertEquals(concat(concat(List.of("established"), events(1, 256)), List.of("dropped 2", "closed")), t.calls);
    }

    @Test void aBacklogThatNeverFillsLosesNothingAndReportsNothingLost() throws Exception {
        var t = play(5, (loop, driver, tracer) -> {
            deliver(loop, driver, bare(Event.Kind.ESTABLISHED));
            Event[] all = new Event[5];
            for (int i = 0; i < all.length; i++) all[i] = occurrence(i + 1);
            deliver(loop, driver, all);
            tracer.gate.countDown();
            deliver(loop, driver, bare(Event.Kind.CLOSED));
            assertTrue(tracer.closed.await(WAIT, TimeUnit.SECONDS));
        });
        assertEquals(concat(concat(List.of("established"), events(1, 5)), List.of("closed")), t.calls);
    }

    /** From CLOSED on the event queue takes nothing new (the linger's retirements included); what it holds is still read. */
    @Test void afterClosedTheEventQueueTakesNothingButKeepsWhatItHolds() throws Exception {
        var t = play(0, (loop, driver, tracer) -> {
            tracer.gate.countDown();
            Object[] read = new Object[3];
            ExternalTransportTest.run(loop, () -> {
                driver.deliver(occurrence(5));
                driver.deliver(bare(Event.Kind.CLOSED));
                driver.deliver(occurrence(6));
                driver.deliver(new Event(Event.Kind.PIPE_STATE, 3, io.zmux.internal.core.Action.DIR_BIDI, io.zmux.internal.core.PipeInfo.Out.RETIRED, null, 0, 0, 0, 0, null, null, null, false, false, 0, 0, null));
                read[0] = driver.pollEvent(io.zmux.internal.core.Waiter.inert());
                read[1] = driver.pollEvent(io.zmux.internal.core.Waiter.inert());
            });
            assertEquals(5, ((Event) read[0]).code());
            assertNull(read[1], "the late occurrences never entered");
            assertTrue(tracer.closed.await(WAIT, TimeUnit.SECONDS));
        });
        assertEquals(List.of("event 5", "closed"), t.calls);
    }
}
