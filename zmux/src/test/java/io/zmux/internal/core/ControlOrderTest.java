package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class ControlOrderTest {
    private static Pipe pipe(Session s, int state, long priority, long rtt) {
        Pipe p = PipeTargetTest.appendOut(s, true, state, Attach.IN_OPEN);
        Poke.set(p.mp().tier, "sendPrio", priority);
        Poke.set(p.mp().rtt, "has", true); Poke.set(p.mp().rtt, "min", rtt); Poke.set(p.mp().rtt, "smoothed", rtt);
        return p;
    }
    private static Session session() {
        Session s = PipeTargetTest.session(); s.pipes.clear(); return s;
    }
    @Test void stateTierDelayAndIdDetermineTheFirstTwoPipes() {
        Session s = session();
        Pipe joining = pipe(s, Attach.OUT_JOINING, 0, 1);
        Pipe standby = pipe(s, Attach.OUT_ACTIVE, 0, 1); Poke.set(standby.mp().tier, "sendStandby", true);
        Pipe highNumber = pipe(s, Attach.OUT_ACTIVE, (1L << 62) - 1, 1);
        Pipe slow = pipe(s, Attach.OUT_ACTIVE, 3, 20);
        Pipe fast = pipe(s, Attach.OUT_ACTIVE, 3, 10);
        Pipe tie = pipe(s, Attach.OUT_ACTIVE, 3, 10);
        assertSame(fast, s.mp().primary()); assertSame(tie, s.mp().second());
        Poke.set(fast.out(), "failed", true); Poke.set(tie.mp().health, "retired", true);
        assertSame(slow, s.mp().primary()); assertSame(highNumber, s.mp().second());
        Poke.set(slow.mp().health, "out", Attach.OUT_DRAINING);
        assertSame(highNumber, s.mp().primary()); assertSame(standby, s.mp().second());
        Poke.set(highNumber.mp().health, "out", Attach.OUT_FAILED); Poke.set(standby.mp().health, "out", Attach.OUT_CLOSED);
        assertSame(joining, s.mp().primary()); assertSame(slow, s.mp().second());
        Poke.set(slow.mp().health, "abandonSent", true); Poke.set(slow.mp().health, "abandonRecv", true);
        assertSame(joining, s.mp().primary()); assertNull(s.mp().second());
        Poke.set(joining, "out", null); assertNull(s.mp().primary()); assertNull(s.mp().second());
    }
    @Test void unknownRttAndNonCandidateRttKeepTheOriginalDelaySemantics() {
        Session s = session();
        Pipe measured = pipe(s, Attach.OUT_ACTIVE, 0, 100);
        Pipe unknown = pipe(s, Attach.OUT_ACTIVE, 0, 0);
        Poke.set(unknown.mp().rtt, "has", false); Poke.set(unknown.mp().rtt, "smoothed", 120);
        assertSame(measured, s.mp().primary()); // OWD 50 vs 60.
        Pipe retired = pipe(s, Attach.OUT_ACTIVE, 0, 10); Poke.set(retired.mp().health, "retired", true);
        assertSame(unknown, s.mp().primary()); // Retired live-out sample still enters OWD: 95 vs 60.
        Poke.set(retired.mp().health, "out", Attach.OUT_FAILED);
        assertSame(measured, s.mp().primary());
        assertEquals(50, s.mp().metrics.owd(measured)); assertEquals(60, s.mp().metrics.owd(unknown));
    }
    // Snapshot of the pre-optimization full-sort algorithm, test-only oracle.
    private static long oldOwd(Session s, Pipe p) {
        Attach a = p.mp(); if (!a.rtt.has()) return a.rtt.smoothed() / 2;
        long m = a.rtt.min();
        for (Pipe q : s.pipes.all()) if (q.mp() != null && q.out() != null && q.mp().liveOut() && q.mp().rtt.has())
            m = Math.min(m, q.mp().rtt.min());
        return a.rtt.min() - m / 2;
    }
    private static int rank(Attach a) {
        return switch (a.health.out()) {
            case Attach.OUT_ACTIVE -> 0;
            case Attach.OUT_JOINING -> 1;
            case Attach.OUT_SUSPECT -> 2;
            case Attach.OUT_DRAINING -> a.health.drainSuspect() ? 4 : 3;
            default -> 4;
        };
    }
    private static List<Pipe> oldOrder(Session s) {
        var result = new ArrayList<Pipe>();
        for (Pipe p : s.pipes.all()) if (p.mp() != null && p.out() != null && !p.out().failed() && p.mp().liveOut() && !p.mp().health.retired()
                && !(p.mp().health.out() == Attach.OUT_DRAINING && p.mp().health.abandonSent() && p.mp().health.abandonRecv())) result.add(p);
        result.sort((x,y) -> {
            int c = Integer.compare(rank(x.mp()), rank(y.mp())); if (c != 0) return c;
            c = Boolean.compare(x.mp().effStandby(), y.mp().effStandby()); if (c != 0) return c;
            c = Long.compareUnsigned(x.mp().effPrio(), y.mp().effPrio()); if (c != 0) return c;
            c = Long.compare(oldOwd(s,x), oldOwd(s,y)); return c != 0 ? c : Long.compare(x.id,y.id);
        });
        return result;
    }
    @Test void seededDifferentialQueriesReflectLiveUpdatesWithoutChangingPipeOrder() {
        var random = new Random(0x5a4d5558);
        for (int trial = 0; trial < 300; trial++) {
            Session s = session();
            int count = random.nextInt(65);
            for (int i = 0; i < count; i++) pipe(s, Attach.OUT_ACTIVE, 0, 1);
            Collections.shuffle(s.pipes.all(), random);
            var original = List.copyOf(s.pipes.all());
            for (int update = 0; update < 5; update++) {
                for (Pipe p : s.pipes.all()) {
                    Attach a = p.mp(); Poke.set(a.health, "out", random.nextInt(7));
                    Poke.set(a.health, "retired", random.nextInt(8) == 0); Poke.set(a.health, "drainSuspect", random.nextBoolean());
                    Poke.set(a.health, "abandonSent", random.nextBoolean()); Poke.set(a.health, "abandonRecv", random.nextBoolean());
                    Poke.set(a.tier, "sendStandby", random.nextBoolean()); Poke.set(a.tier, "sendPrio", random.nextLong() & ((1L << 62) - 1));
                    Poke.set(a.rtt, "has", random.nextBoolean()); Poke.set(a.rtt, "min", 1 + random.nextInt(1000));
                    Poke.set(a.rtt, "smoothed", 1 + random.nextInt(2000));
                    Poke.set(p.out(), "failed", random.nextInt(8) == 0);
                }
                var expected = oldOrder(s);
                assertSame(expected.isEmpty() ? null : expected.get(0), s.mp().primary());
                assertSame(expected.size() < 2 ? null : expected.get(1), s.mp().second());
                for (Pipe p : s.pipes.all()) assertEquals(oldOwd(s,p), s.mp().metrics.owd(p));
                assertEquals(original, s.pipes.all(), "query must not reorder the authoritative pipe list");
            }
        }
    }
}
