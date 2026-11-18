package io.zmux.internal.loop;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.core.Action;
import io.zmux.internal.core.Event;
import io.zmux.internal.core.PipeInfo;
import io.zmux.internal.core.Waiter;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class EventQueueTest {
    private static Event ev(Event.Kind k, long pipe, int dir) {
        return new Event(k, pipe, dir, null, null, 0, 0, 0, 0, null, null, null, false, false, 0, 0, null);
    }

    private static Event retire(long pipe) { return retire(pipe, false); }

    /** The retirement as the core emits it: failed is its failure flag. */
    private static Event retire(long pipe, boolean failed) {
        return new Event(Event.Kind.PIPE_STATE, pipe, Action.DIR_BIDI, PipeInfo.Out.RETIRED, null, 0, 0, 0, 0, null, null, null, false, failed, 0, 0, null);
    }

    private static final int OUT = Action.DIR_OUT, IN = Action.DIR_IN;
    private static final Event.Kind SUSPECT = Event.Kind.PIPE_SUSPECT, RECOVERED = Event.Kind.PIPE_RECOVERED,
            JOINED = Event.Kind.PIPE_JOINED, FAILED = Event.Kind.PIPE_FAILED, CLOSED = Event.Kind.PIPE_CLOSED;

    private static List<Object> drain(EventQueue q) {
        List<Object> out = new ArrayList<>();
        for (Object e; (e = q.poll(Waiter.inert())) != null; ) out.add(e);
        return out;
    }

    @Test void aFlappingPipeKeepsEachStateOnceAtItsLatestPlace() {
        var q = new EventQueue();
        for (int i = 0; i < 1000; i++) {
            q.add(ev(SUSPECT, 1, OUT));
            q.add(ev(RECOVERED, 1, OUT));
        }
        q.add(ev(FAILED, 1, OUT));
        var got = drain(q).stream().map(e -> ((Event) e).kind()).toList();
        assertEquals(List.of(SUSPECT, RECOVERED, FAILED), got);
    }

    @Test void everyRetirementFoldsIntoOneSummary() {
        var q = new EventQueue();
        q.add(ev(JOINED, 7, OUT));
        for (long p = 1; p <= 100; p++) {
            q.add(ev(JOINED, p, OUT));
            q.add(ev(p % 2 == 0 ? FAILED : CLOSED, p, OUT));
            if (p % 2 == 1) q.add(ev(CLOSED, p, IN));
            q.add(retire(p, p % 2 == 0));
        }
        q.add(ev(JOINED, 200, OUT));
        var got = drain(q);
        // Unread non-terminal events leave uncounted.
        // Pipe 7's first event was folded with the pipe; only the summary and pipe 200 remain.
        assertEquals(2, got.size());
        var s = (SessionRuntime.RetiredPipes) got.getFirst();
        assertEquals(new SessionRuntime.RetiredPipes(100, 50, 50, List.of(97L, 98L, 99L, 100L)), s);
        assertEquals(200, ((Event) got.get(1)).pipe());
    }

    @Test void aReadTerminalStateIsNotCountedAgainAndTheSummaryRestartsAfterARead() {
        var q = new EventQueue();
        q.add(ev(FAILED, 1, OUT));
        assertEquals(FAILED, ((Event) q.poll(Waiter.inert())).kind());
        q.add(retire(1, true));
        assertEquals(new SessionRuntime.RetiredPipes(1, 0, 0, List.of(1L)), q.poll(Waiter.inert()));
        q.add(retire(2, true));
        assertEquals(List.of(new SessionRuntime.RetiredPipes(1, 1, 0, List.of(2L))), drain(q));
    }

    @Test void aRetirementCountsByTheCoresFlagOnlyWhenNoTerminalStateWasRead() {
        var q = new EventQueue();
        // No terminal event ever queued: by the flag, failed.
        q.add(retire(1, true));
        // No terminal event ever queued: by the flag, closed.
        q.add(retire(2, false));
        // Terminal events unread: by the flag, not by which event remains (the unread one is the inbound end, the pipe failed).
        q.add(ev(CLOSED, 3, IN));
        q.add(retire(3, true));
        assertEquals(List.of(new SessionRuntime.RetiredPipes(3, 2, 1, List.of(1L, 2L, 3L))), drain(q));
        // Outbound FAILED read, inbound end unread: the application has read a terminal state, neither.
        q.add(ev(FAILED, 4, OUT));
        assertEquals(FAILED, ((Event) q.poll(Waiter.inert())).kind());
        q.add(ev(CLOSED, 4, IN));
        // Outbound CLOSED read, a clean pipe: neither.
        q.add(ev(CLOSED, 5, OUT));
        assertEquals(CLOSED, ((Event) q.poll(Waiter.inert())).kind());
        assertEquals(CLOSED, ((Event) q.poll(Waiter.inert())).kind());
        q.add(retire(4, true));
        q.add(retire(5, false));
        // Only a non-terminal event unread: the pipe counts by the flag.
        q.add(ev(SUSPECT, 6, OUT));
        q.add(retire(6, false));
        assertEquals(List.of(new SessionRuntime.RetiredPipes(3, 0, 1, List.of(4L, 5L, 6L))), drain(q));
    }

    @Test void theSummaryMovesToTheTailAtEveryRetirement() {
        var q = new EventQueue();
        q.add(retire(1));
        q.add(ev(JOINED, 9, OUT));
        q.add(retire(2));
        q.add(ev(JOINED, 10, OUT));
        var got = drain(q);
        assertEquals(List.of(9L, -1L, 10L), got.stream().map(e -> e instanceof Event ev ? ev.pipe() : -1L).toList(),
                "the summary stands where its latest retirement occurred");
        assertEquals(new SessionRuntime.RetiredPipes(2, 0, 2, List.of(1L, 2L)), got.get(1));
    }

    private static Event of(Event.Kind k) { return ev(k, 0, 0); }

    private static Event authFailed(String reason) {
        return new Event(Event.Kind.JOIN_AUTH_FAILED, 0, 0, null, null, 0, 0, 0, 1, null, reason, null, false, false, 0, 0, null);
    }

    private static Event tier(long priority, boolean saturated) {
        return new Event(Event.Kind.TIER_SATURATION, 0, 0, null, null, 0, 0, priority, 0, null, null, null, false, saturated, 0, 0, null);
    }

    private static Event epoch(Event.Kind k, long n) {
        return new Event(k, 0, 0, null, null, 0, 0, n, 0, null, null, null, false, false, 0, 0, null);
    }

    private static List<Event.Kind> kinds(EventQueue q) {
        return drain(q).stream().map(e -> ((Event) e).kind()).toList();
    }

    /** The event key table: the order of reading is the order of the keys' latest occurrences. */
    @Test void eventsAreReadInTheOrderOfTheirKeysLatestOccurrences() {
        var q = new EventQueue();
        q.add(of(Event.Kind.SUSPENDED));
        q.add(ev(Event.Kind.PIPE_ADDED, 4, OUT));
        q.add(of(Event.Kind.RESUMED));
        assertEquals(List.of(Event.Kind.SUSPENDED, Event.Kind.PIPE_ADDED, Event.Kind.RESUMED), kinds(q));

        // An update moves the event to the tail: it stands where its latest occurrence was.
        q.add(of(Event.Kind.SUSPENDED));
        q.add(ev(Event.Kind.PIPE_QUALITY, 4, 0));
        q.add(of(Event.Kind.RESUMED));
        q.add(of(Event.Kind.SUSPENDED));
        assertEquals(List.of(Event.Kind.PIPE_QUALITY, Event.Kind.RESUMED, Event.Kind.SUSPENDED), kinds(q),
                "Suspended and Resumed are two keys: an interruption is never swallowed by the resumption before it");
    }

    @Test void aKeptEventStaysWhereItIsAndALaterValueReplacesTheOthers() {
        var q = new EventQueue();
        q.add(ev(FAILED, 1, OUT));
        q.add(ev(Event.Kind.PIPE_QUALITY, 1, 0));
        q.add(ev(FAILED, 1, OUT)); // the same terminal key: kept, not moved
        q.add(ev(CLOSED, 1, IN));
        q.add(ev(CLOSED, 1, OUT)); // another direction, another key
        assertEquals(List.of(FAILED, Event.Kind.PIPE_QUALITY, CLOSED, CLOSED), kinds(q));

        var latest = new Event(Event.Kind.PIPE_QUALITY, 1, 0, null, null, 0, 0, 0, 0, null, null, null, false, false, 7, 9, null);
        q.add(ev(Event.Kind.PIPE_QUALITY, 1, 0));
        q.add(latest);
        assertEquals(List.of(latest), drain(q), "the latest values replace the unread event");
    }

    @Test void suspectRecoveredAndTheTerminalStatesAreKeyedByDirection() {
        var q = new EventQueue();
        q.add(ev(SUSPECT, 1, OUT));
        q.add(ev(SUSPECT, 1, IN));
        q.add(ev(RECOVERED, 1, OUT));
        q.add(ev(SUSPECT, 1, OUT));
        var got = drain(q).stream().map(e -> ((Event) e).kind() + ":" + ((Event) e).dir()).toList();
        assertEquals(List.of(SUSPECT + ":" + IN, RECOVERED + ":" + OUT, SUSPECT + ":" + OUT), got);
    }

    @Test void aResumptionRemovesTheUnreadResumeUnattendedAndAnEarlierOneStaysUntilThen() {
        var q = new EventQueue();
        q.add(of(Event.Kind.SUSPENDED));
        q.add(of(Event.Kind.RESUME_UNATTENDED));
        q.add(of(Event.Kind.RESUME_UNATTENDED));
        assertEquals(List.of(Event.Kind.SUSPENDED, Event.Kind.RESUME_UNATTENDED), kinds(q));
        q.add(of(Event.Kind.RESUME_UNATTENDED));
        q.add(of(Event.Kind.RESUMED));
        assertEquals(List.of(Event.Kind.RESUMED), kinds(q));
        // One already read cannot be taken back.
        q.add(of(Event.Kind.RESUME_UNATTENDED));
        assertEquals(Event.Kind.RESUME_UNATTENDED, ((Event) q.poll(Waiter.inert())).kind());
        q.add(of(Event.Kind.RESUMED));
        assertEquals(List.of(Event.Kind.RESUMED), kinds(q));
    }

    @Test void failedHellosCountPerReasonAndMoveToTheTail() {
        var q = new EventQueue();
        q.add(authFailed("auth"));
        q.add(of(Event.Kind.SUSPENDED));
        q.add(authFailed("kex"));
        q.add(authFailed("auth"));
        q.add(authFailed("auth"));
        var got = drain(q).stream().map(e -> (Event) e).toList();
        assertEquals(List.of(Event.Kind.SUSPENDED, Event.Kind.JOIN_AUTH_FAILED, Event.Kind.JOIN_AUTH_FAILED), got.stream().map(Event::kind).toList());
        assertEquals(List.of("kex", 1L, "auth", 3L), List.of(got.get(1).reason(), got.get(1).count(), got.get(2).reason(), got.get(2).count()));
        q.add(authFailed("auth"));
        assertEquals(1, ((Event) q.poll(Waiter.inert())).count(), "the count restarts once the event was read");
    }

    @Test void tiersAreKeyedByPriorityAndCapacityEventsByKind() {
        var q = new EventQueue();
        q.add(tier(0, true));
        q.add(tier(1, true));
        q.add(tier(0, false));
        q.add(of(Event.Kind.CAPACITY_BELOW));
        q.add(of(Event.Kind.CAPACITY_RESTORED));
        q.add(of(Event.Kind.CAPACITY_BELOW));
        var got = drain(q).stream().map(e -> (Event) e).toList();
        assertEquals(List.of(Event.Kind.TIER_SATURATION, Event.Kind.TIER_SATURATION, Event.Kind.CAPACITY_RESTORED, Event.Kind.CAPACITY_BELOW),
                got.stream().map(Event::kind).toList());
        assertEquals(List.of(1L, 0L), List.of(got.get(0).code(), got.get(1).code()), "priority 1 stands where it occurred, priority 0 at its latest state");
        assertFalse(got.get(1).flag());
    }

    @Test void epochEventsKeepTheLatestEpochOfEachKind() {
        var q = new EventQueue();
        q.add(epoch(Event.Kind.EPOCH_COMMITTED, 1));
        q.add(epoch(Event.Kind.EPOCH_DELETED, 1));
        q.add(epoch(Event.Kind.EPOCH_COMMITTED, 2));
        q.add(epoch(Event.Kind.EPOCH_COMMITTED, 3));
        var got = drain(q).stream().map(e -> (Event) e).toList();
        assertEquals(List.of(Event.Kind.EPOCH_DELETED, Event.Kind.EPOCH_COMMITTED), got.stream().map(Event::kind).toList());
        assertEquals(3, got.get(1).code());
    }

    @Test void applicationEventsAreKeyedAndMoveToTheTailToo() {
        var q = new EventQueue();
        q.addApplication("candidate-a", "first");
        q.addApplication("candidate-b", "second");
        q.addApplication("candidate-a", "third");
        assertEquals(List.of("second", "third"), drain(q));
    }

    @Test void aFrozenQueueTakesNothingNewButKeepsWhatItHolds() {
        var q = new EventQueue();
        q.add(ev(JOINED, 1, OUT));
        q.freeze();
        q.add(ev(JOINED, 2, OUT));
        q.add(retire(1));
        q.add(authFailed("auth"));
        q.addApplication("k", "late");
        assertEquals(List.of(1L), drain(q).stream().map(e -> ((Event) e).pipe()).toList());
        assertNull(q.poll(Waiter.inert()), "read to the end, then nothing");
    }

    /**
     * A hostile peer that fails Hellos without end, and pipes joined and dropped in a loop, never displace
     * another event and add no memory: the queue's size is the same after a million occurrences as after one.
     */
    @Test void repeatedFailedHellosAndPipeChurnNeverDisplaceOtherEventsAndStayBounded() {
        var q = new EventQueue();
        q.add(of(Event.Kind.SUSPENDED));
        q.add(ev(JOINED, 1, OUT));
        q.add(of(Event.Kind.GOAWAY_RECEIVED));
        q.add(authFailed("auth"));
        int bound = size(q);
        for (int i = 0; i < 1_000_000; i++) {
            q.add(authFailed(i % 2 == 0 ? "auth" : "bad_field"));
            if (i % 1000 == 0) {
                long pipe = 100L + i;
                q.add(ev(JOINED, pipe, OUT));
                q.add(ev(CLOSED, pipe, OUT));
                q.add(retire(pipe));
            }
        }
        assertEquals(bound + 2, size(q), "one more key for the second reason and one for the summary, however many failures");
        var got = drain(q);
        assertEquals(6, got.size());
        assertEquals(Event.Kind.SUSPENDED, ((Event) got.get(0)).kind());
        assertEquals(1L, ((Event) got.get(1)).pipe());
        assertEquals(Event.Kind.GOAWAY_RECEIVED, ((Event) got.get(2)).kind());
        long total = got.stream().filter(e -> e instanceof Event ev && ev.kind() == Event.Kind.JOIN_AUTH_FAILED).mapToLong(e -> ((Event) e).count()).sum();
        assertEquals(1_000_001, total, "every failure is counted");
        assertEquals(1000, got.stream().filter(e -> e instanceof SessionRuntime.RetiredPipes).mapToLong(e -> ((SessionRuntime.RetiredPipes) e).retired()).sum());
    }

    private static int size(EventQueue q) {
        return q.size();
    }
}
