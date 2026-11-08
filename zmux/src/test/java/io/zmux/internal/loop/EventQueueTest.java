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

    private static Event retire(long pipe) {
        return new Event(Event.Kind.PIPE_STATE, pipe, Action.DIR_BIDI, PipeInfo.Out.RETIRED, null, 0, 0, 0, 0, null, null, null, false, false, 0, 0, null);
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

    @Test void everyRetirementFoldsIntoOneSummaryThatKeepsItsPlace() {
        var q = new EventQueue();
        q.add(ev(JOINED, 7, OUT));
        for (long p = 1; p <= 100; p++) {
            q.add(ev(JOINED, p, OUT));
            q.add(ev(p % 2 == 0 ? FAILED : CLOSED, p, OUT));
            if (p % 2 == 1) q.add(ev(CLOSED, p, IN));
            q.add(retire(p));
        }
        q.add(ev(JOINED, 200, OUT));
        var got = drain(q);
        // Unread non-terminal events leave uncounted: dropped counts only queue-limit discards.
        // Pipe 7's first event was folded with the pipe; only the summary and pipe 200 remain.
        assertEquals(2, got.size());
        var s = (SessionRuntime.RetiredPipes) got.getFirst();
        assertEquals(new SessionRuntime.RetiredPipes(100, 50, 50, 0, List.of(97L, 98L, 99L, 100L)), s);
        assertEquals(200, ((Event) got.get(1)).pipe());
    }

    @Test void aReadTerminalStateIsNotCountedAgainAndTheSummaryRestartsAfterARead() {
        var q = new EventQueue();
        q.add(ev(FAILED, 1, OUT));
        assertEquals(FAILED, ((Event) q.poll(Waiter.inert())).kind());
        q.add(retire(1));
        assertEquals(new SessionRuntime.RetiredPipes(1, 0, 0, 0, List.of(1L)), q.poll(Waiter.inert()));
        q.add(retire(2));
        assertEquals(List.of(new SessionRuntime.RetiredPipes(1, 0, 0, 0, List.of(2L))), drain(q));
    }

    @Test void onlyTheUnreadTerminalStatesCountByDirection() {
        var q = new EventQueue();
        // Outbound FAILED read, inbound ENDED unread: only the unread one counts, as closed.
        q.add(ev(FAILED, 1, OUT));
        assertEquals(FAILED, ((Event) q.poll(Waiter.inert())).kind());
        q.add(ev(CLOSED, 1, IN));
        q.add(retire(1));
        // Outbound FAILED unread with inbound ENDED unread: failed.
        q.add(ev(CLOSED, 2, IN));
        q.add(ev(FAILED, 2, OUT));
        q.add(retire(2));
        // Only non-terminal events unread: neither, and nothing counted as dropped.
        q.add(ev(SUSPECT, 3, OUT));
        q.add(retire(3));
        assertEquals(List.of(new SessionRuntime.RetiredPipes(3, 1, 1, 0, List.of(1L, 2L, 3L))), drain(q));
    }

    @Test void theSummaryEntersOnceAtTheFirstRetirement() {
        var q = new EventQueue();
        q.add(retire(1));
        q.add(ev(JOINED, 9, OUT));
        q.add(retire(2));
        var got = drain(q);
        assertEquals(2, got.size(), "one summary, then pipe 9's event");
        assertEquals(new SessionRuntime.RetiredPipes(2, 0, 0, 0, List.of(1L, 2L)), got.getFirst());
        assertEquals(9, ((Event) got.get(1)).pipe());
    }
}
