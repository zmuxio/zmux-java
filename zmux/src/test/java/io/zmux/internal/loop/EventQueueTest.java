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
    private static Event state(long pipe, int dir, Enum<?> s) {
        return new Event(Event.Kind.PIPE_STATE, pipe, dir, s, null, 0, 0, 0, 0, null, null, null, false, false, 0, 0, null);
    }

    private static List<Object> drain(EventQueue q) {
        List<Object> out = new ArrayList<>();
        for (Object e; (e = q.poll(Waiter.inert())) != null; ) out.add(e);
        return out;
    }

    @Test void aFlappingPipeKeepsEachStateOnceAtItsLatestPlace() {
        var q = new EventQueue();
        for (int i = 0; i < 1000; i++) {
            q.add(state(1, Action.DIR_OUT, PipeInfo.Out.SUSPECT));
            q.add(state(1, Action.DIR_OUT, PipeInfo.Out.ACTIVE));
        }
        q.add(state(1, Action.DIR_OUT, PipeInfo.Out.FAILED));
        var got = drain(q).stream().map(e -> ((Event) e).state()).toList();
        assertEquals(List.of(PipeInfo.Out.SUSPECT, PipeInfo.Out.ACTIVE, PipeInfo.Out.FAILED), got);
    }

    @Test void everyRetirementFoldsIntoOneSummaryThatKeepsItsPlace() {
        var q = new EventQueue();
        q.add(state(7, Action.DIR_OUT, PipeInfo.Out.ACTIVE));
        for (long p = 1; p <= 100; p++) {
            q.add(state(p, Action.DIR_OUT, PipeInfo.Out.ACTIVE));
            q.add(state(p, Action.DIR_IN, PipeInfo.In.OPEN));
            q.add(state(p, Action.DIR_OUT, p % 2 == 0 ? PipeInfo.Out.FAILED : PipeInfo.Out.CLOSED));
            if (p % 2 == 1) q.add(state(p, Action.DIR_IN, PipeInfo.In.ENDED));
            q.add(state(p, Action.DIR_BIDI, PipeInfo.Out.RETIRED));
        }
        q.add(state(200, Action.DIR_OUT, PipeInfo.Out.ACTIVE));
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
        q.add(state(1, Action.DIR_OUT, PipeInfo.Out.FAILED));
        assertEquals(PipeInfo.Out.FAILED, ((Event) q.poll(Waiter.inert())).state());
        q.add(state(1, Action.DIR_BIDI, PipeInfo.Out.RETIRED));
        assertEquals(new SessionRuntime.RetiredPipes(1, 0, 0, 0, List.of(1L)), q.poll(Waiter.inert()));
        q.add(state(2, Action.DIR_BIDI, PipeInfo.Out.RETIRED));
        assertEquals(List.of(new SessionRuntime.RetiredPipes(1, 0, 0, 0, List.of(2L))), drain(q));
    }

    @Test void onlyTheUnreadTerminalStatesCountByDirection() {
        var q = new EventQueue();
        // Outbound FAILED read, inbound ENDED unread: only the unread one counts, as closed.
        q.add(state(1, Action.DIR_OUT, PipeInfo.Out.FAILED));
        assertEquals(PipeInfo.Out.FAILED, ((Event) q.poll(Waiter.inert())).state());
        q.add(state(1, Action.DIR_IN, PipeInfo.In.ENDED));
        q.add(state(1, Action.DIR_BIDI, PipeInfo.Out.RETIRED));
        // Outbound FAILED unread with inbound ENDED unread: failed.
        q.add(state(2, Action.DIR_IN, PipeInfo.In.ENDED));
        q.add(state(2, Action.DIR_OUT, PipeInfo.Out.FAILED));
        q.add(state(2, Action.DIR_BIDI, PipeInfo.Out.RETIRED));
        // Only non-terminal events unread: neither, and nothing counted as dropped.
        q.add(state(3, Action.DIR_OUT, PipeInfo.Out.SUSPECT));
        q.add(state(3, Action.DIR_BIDI, PipeInfo.Out.RETIRED));
        assertEquals(List.of(new SessionRuntime.RetiredPipes(3, 1, 1, 0, List.of(1L, 2L, 3L))), drain(q));
    }

    @Test void theSummaryEntersOnceAtTheFirstRetirement() {
        var q = new EventQueue();
        q.add(state(1, Action.DIR_BIDI, PipeInfo.Out.RETIRED));
        q.add(state(9, Action.DIR_OUT, PipeInfo.Out.ACTIVE));
        q.add(state(2, Action.DIR_BIDI, PipeInfo.Out.RETIRED));
        var got = drain(q);
        assertEquals(2, got.size(), "one summary, then pipe 9's event");
        assertEquals(new SessionRuntime.RetiredPipes(2, 0, 0, 0, List.of(1L, 2L)), got.getFirst());
        assertEquals(9, ((Event) got.get(1)).pipe());
    }
}
