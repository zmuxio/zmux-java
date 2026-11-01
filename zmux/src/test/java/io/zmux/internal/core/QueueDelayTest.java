package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * queue_p (MULTIPATH.md 7.1): T_suspect and T_fail include min(1 s, unsent_ahead / R_p), where
 * unsent_ahead is Q_p when the oldest unacknowledged ack-eliciting batch was handed over,
 * without that batch: the bytes of write calls in progress plus the carrier's unsent count.
 */
class QueueDelayTest {
    private static long expected(PathHealth h, PathMetrics m, Pipe p, long ahead) {
        long queue = (long) Math.min(Time.SECOND, ahead / m.rateOf(p) * Time.SECOND);
        long floor = CoreConfig.builder().build().suspectFloor();
        return Math.max(floor, Time.mul(Time.add(m.pto(p.mp()), queue), 2));
    }

    /** Hands over one ack-eliciting batch on pipe 0 and reports it written; the peer never acknowledges. */
    private static void handOver(Session s) throws CoreException {
        s.ping(Waiter.inert());
        Batch b = new Batch();
        assertTrue(s.pollTransmit(0, 0, b, Session.DEFAULT_BATCH, 0), "a batch was due");
        s.writeDone(b, null, 0);
    }

    @Test void aSlowCarrierRaisesTheSuspicionDeadlineUpToOneSecond() throws Exception {
        Session s = CounterSemanticsTest.established();
        Pipe p = s.pipes().get(0);
        PathHealth h = s.mp().health;
        PathMetrics m = s.mp().metrics;
        assertFalse(p.mp().link.unacked());
        long idle = h.tSuspect(p);

        // A carrier that holds 200 KB unsent when the batch is handed over.
        s.unsentReported(0, 0, 200 << 10);
        handOver(s);
        assertTrue(p.mp().link.unacked());
        double rateAtHandOver = m.rateOf(p);
        assertEquals(PathHealth.queueDelay(200 << 10, rateAtHandOver), p.mp().link.oldestQueue(), "the snapshot is Q_p without the batch over R_p");
        long slow = h.tSuspect(p);
        assertEquals(expected(h, m, p, 200 << 10), slow);
        assertTrue(slow > idle, "T_suspect grows with the carrier's queue: " + slow + " <= " + idle);
        long queue = PathHealth.queueDelay(200 << 10, m.rateOf(p));
        assertEquals(Math.max(2 * Time.SECOND, Time.mul(Time.add(m.pto(p.mp()), queue), 8)), h.tFail(p));

        // The snapshot does not grow afterwards, so a pipe that stops moving is still detected.
        s.unsentReported(0, 0, 64L << 20);
        assertEquals(PathHealth.queueDelay(200 << 10, rateAtHandOver), p.mp().link.oldestQueue());
        assertEquals(slow, h.tSuspect(p));

        // Nor does it follow R_p: a rate estimate that changes later leaves queue_p as snapshotted.
        p.out().snd.re().penalize();
        assertNotEquals(rateAtHandOver, m.rateOf(p));
        assertEquals(slow, h.tSuspect(p), "R_p is taken with unsent_ahead at the hand-over");

        // A carrier that holds far more than R_p moves in a second: queue_p is capped at 1 s.
        Session t = CounterSemanticsTest.established();
        Pipe q = t.pipes().get(0);
        t.unsentReported(0, 0, 64L << 20);
        handOver(t);
        long capped = t.mp().health.tSuspect(q);
        assertEquals(Time.mul(Time.add(t.mp().metrics.pto(q.mp()), Time.SECOND), 2), capped);
        assertEquals(expected(t.mp().health, t.mp().metrics, q, 64L << 20), capped);
    }

    @Test void queueDelayIsCappedAtOneSecond() {
        assertEquals(0, PathHealth.queueDelay(0, 1e6));
        assertEquals(250 * Time.MILLISECOND, PathHealth.queueDelay(250_000, 1e6));
        assertEquals(Time.SECOND, PathHealth.queueDelay(64L << 20, 1e6));
        assertEquals(Time.SECOND, PathHealth.queueDelay(1, 0));
    }

    @Test void queueBytesCountTheWriteInProgressAndTheCarriersUnsentBytes() throws Exception {
        Session s = CounterSemanticsTest.established();
        Pipe p = s.pipes().get(0);
        PathMetrics m = s.mp().metrics;
        assertEquals(0, m.queueBytes(p));
        s.ping(Waiter.inert());
        Batch b = new Batch();
        assertTrue(s.pollTransmit(0, 0, b, Session.DEFAULT_BATCH, 0));
        assertEquals(b.bytes(), m.queueBytes(p), "a batch counts in full while its write call is in progress");
        s.unsentReported(0, 0, 1000);
        assertEquals(b.bytes() + 1000, m.queueBytes(p));
        s.writeDone(b, null, 0);
        assertEquals(1000, m.queueBytes(p), "after the call only the carrier's unsent bytes remain");
        s.unsentReported(0, 0, -1);
        assertEquals(0, m.queueBytes(p));
    }
}
