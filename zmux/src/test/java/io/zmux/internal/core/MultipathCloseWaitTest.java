package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * In a multipath session the peer closes on the first CONNECTION_CLOSE it reads and discards what
 * arrives afterwards, so a close without error waits while data written on any lane is unacknowledged.
 */
class MultipathCloseWaitTest {
    /** A multipath session whose bootstrap pipe has written 10 unacknowledged bytes on the given lane. */
    private static Session unacked(long lane) {
        Session s = PipeTargetTest.session();
        Poke.set(s, "feat", new Features(false, false, false, false, false, true, true, false, true, true, false, false));
        Pipe p = s.pipes.get(0);
        Pipe.OutLane ol = lane == 0 ? p.out().lanes.get(0) : new Pipe.OutLane(lane, new CarrierInfo(Framing.BYTES, false, false, false, false), true);
        if (lane != 0) p.out().lanes.add(ol);
        Poke.set(ol.tx, "pos", 10L);
        Poke.set(ol.tx, "elicitPos", 10L);
        return s;
    }

    @Test void closeWaitsForUnacknowledgedDataOnLaneOne() {
        Session s = unacked(1);
        s.close(0x10, null);
        assertTrue(s.life.waitingLanes(), "CONNECTION_CLOSE does not wait for 10 unacknowledged bytes on lane 1");
    }

    @Test void closeWaitsForUnacknowledgedDataOnLaneZero() {
        Session s = unacked(0);
        s.close(0x10, null);
        assertTrue(s.life.waitingLanes(), "CONNECTION_CLOSE does not wait for 10 unacknowledged bytes on lane 0");
    }

    /**
     * The wait is bounded by the linger timeout from the close, and CONNECTION_CLOSE's hand-off gets its own bound
     * from the wait's end, whether the wait ends early (covered) or at its bound; a caller's hand-off bound does not
     * cut the wait short.
     */
    @Test void theHandOffBoundStartsWhenTheWaitEnds() {
        long ms = Time.MILLISECOND;
        Session s = unacked(1);
        s.closeBounded(0x10, null, 100 * ms, 0);
        assertTrue(s.life.closeWait());
        long linger = s.lingerTimeout();
        assertEquals(linger, (long) Poke.get(s.life, "laneWait"), "the wait is bounded by the linger timeout");
        assertEquals(linger + Session.closeLimit, handOff(s), "nor is the hand-off cut to the caller's 100 ms");
        s.setNow(300 * ms);
        Poke.set(s.pipes.get(0).out().lanes.get(1).ack, "acked", 10L);
        s.recheckCloseWait();
        assertFalse(s.life.closeWait(), "covered");
        assertEquals(300 * ms + Session.closeLimit, handOff(s), "the hand-off is bounded from the wait's end");

        Session t = unacked(1);
        t.closeBounded(0x10, null, 100 * ms, 0);
        long end = t.lingerTimeout();
        t.handleTimeout(100 * ms);
        assertTrue(t.life.closeWait(), "the caller's 100 ms do not end the wait");
        t.handleTimeout(end);
        assertFalse(t.life.closeWait(), "the wait ends at the linger timeout");
        assertEquals(end + Session.closeLimit, handOff(t));
    }

    private static long handOff(Session s) {
        return (long) Poke.get(s.life, "handOff");
    }

    @Test void aFailedPipeIsNoLongerWaitedFor() {
        Session s = unacked(1);
        s.close(0x10, null);
        assertTrue(s.life.waitingLanes());
        s.pipes.get(0).out().fail();
        s.checkLingerDone();
        assertFalse(s.life.waitingLanes(), "the wait outlives the only pipe it waits for");
    }
}
