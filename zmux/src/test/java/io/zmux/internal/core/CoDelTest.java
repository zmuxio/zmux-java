package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** The CoDel state machine of the datagram queues (RFC 8289) and its use by a stream's receive queue. */
class CoDelTest {
    private static final long MS = Time.MILLISECOND, TARGET = 20 * MS;

    private final CoDel c = new CoDel();
    private long now = Time.SECOND;

    private boolean leave(long sojourn, boolean alone) {
        return c.drop(now - sojourn, now, TARGET, alone);
    }

    /**
     * Nothing is dropped below the target or for a moment above it; dropping starts once the sojourn has stood above
     * it for an interval, the next drop comes interval / sqrt(count) later, and it stops as soon as the delay falls.
     */
    @Test void dropsOnlyAStandingDelayAtTheControlLawsRate() {
        assertFalse(leave(10 * MS, false) || leave(19 * MS, false), "dropped below the target");
        assertFalse(leave(30 * MS, false), "dropped on the first sojourn above the target");
        now += CoDel.INTERVAL - MS;
        assertFalse(leave(30 * MS, false), "dropped before an interval above the target");
        now += MS;
        assertFalse(leave(30 * MS, true), "dropped the only datagram queued");
        assertFalse(leave(30 * MS, false), "a lone datagram did not reset the standing state");
        now += CoDel.INTERVAL;
        assertTrue(leave(30 * MS, false), "no drop after an interval above the target");
        assertFalse(leave(30 * MS, false), "dropped again before the control law allows");
        now += CoDel.INTERVAL;
        assertTrue(leave(30 * MS, false));
        assertEquals(2, c.count());
        long gap = c.dropNext() - now;
        assertTrue(Math.abs(gap - 70_710_678L) <= MS, "next drop in " + gap + ", want interval / sqrt(2)");
        assertFalse(leave(5 * MS, false) || c.dropping(), "still dropping below the target");
        c.idle();
        assertFalse(c.dropping());
        assertFalse(leave(30 * MS, false), "idle kept the standing state");
        assertFalse(new CoDel().drop(0, 10 * Time.SECOND, 0, false), "a zero target dropped");
    }

    /** A stream queue read late drops by sojourn, each counted for the stream and the session, and keeps the last datagram. */
    @Test void aStreamQueueReadLateDropsBySojourn() throws Exception {
        Session s = new Session(CoreConfig.builder().datagrams(true).datagramStreamQueue(1000).build(), Time.SECOND);
        Stream st = s.streams.openStream(true);
        for (int i = 0; i < 100; i++) {
            s.datagrams().received(st, new byte[] {1, 'x'});
            s.setNow(s.now() + MS);
        }
        s.setNow(s.now() + 200 * MS);
        int got = 0;
        while (s.datagrams().receive(st) != null) {
            got++;
            s.setNow(s.now() + 10 * MS);
        }
        long dropped = s.stats().datagramsDroppedRecv();
        assertTrue(got > 0 && got < 100, "delivered " + got);
        assertEquals(100 - got, dropped);
        assertEquals(100 - got, st.stats().datagramsDroppedRecv());
    }

    /** Turned off, the queue delivers everything however late it is read. */
    @Test void offKeepsEveryDatagram() throws Exception {
        Session s = new Session(CoreConfig.builder().datagrams(true).datagramReceiveQueue(1000).datagramQueueDelay(0).build(), Time.SECOND);
        for (int i = 0; i < 100; i++) s.datagrams().received(null, new byte[] {'x'});
        s.setNow(s.now() + 10 * Time.SECOND);
        int got = 0;
        while (s.datagrams().receive() != null) got++;
        assertEquals(100, got);
    }
}
