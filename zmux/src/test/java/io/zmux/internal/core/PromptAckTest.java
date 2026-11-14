package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.wire.TransportParams;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pitfalls T5 and E5: an ack-eliciting record on an inbound pipe without a PIPE_ACK during the preceding
 * zmux_max_ack_delay is acknowledged at once, a bulk transfer is not acknowledged more often for it, unreported
 * positions ride along with credit frames, and the limiter's release time stays where the hold set it.
 */
class PromptAckTest {
    private static final long MS = Time.MILLISECOND;
    private static final long DELAY = 25 * MS; // default zmux_max_ack_delay
    private final CarrierInfo info = new CarrierInfo(Framing.BYTES, false, false, false, true);
    private final Pipe.InLane lane = new Pipe.InLane(0, false, false);
    private final Pipe pipe = new Pipe(0, new Pipe.InDir(lane), new Pipe.OutDir(info, null), null, null);
    private final AckEngine engine;
    private long pos;

    PromptAckTest() {
        TransportParams none = new TransportParams();
        List<Pipe> pipes = List.of(pipe);
        Pacer pacer = new Pacer(CoreConfig.defaults(), 65536, AckEngine.peerDelay(none), pipes, null);
        engine = new AckEngine(none, none, pipes, pacer, new AckEngine.Port() {
            @Override public void activity() { }
            @Override public void rttSampled(long min) { }
            @Override public void advanced(Pipe p, Pipe.OutLane ol, boolean cum) { }
            @Override public boolean quiet() { return false; }
        });
    }

    /** A processed ack-eliciting record of n bytes; true when a PIPE_ACK is now due. */
    private boolean record(long n, long now) throws Exception {
        pos += n;
        Poke.set(lane.rx, "pos", pos);
        Poke.set(lane.rx, "posAt", now);
        return engine.onRecord(pipe, lane, n, true, false, now);
    }

    private static RecordWriter writer() {
        RecordWriter w = new RecordWriter();
        w.init(new Batch(), Framing.BYTES, 16382, 1 << 16);
        return w;
    }

    @Test void anIsolatedRecordIsAcknowledgedAtOnce() throws Exception {
        long t = Time.SECOND;
        assertTrue(record(100, t), "no PIPE_ACK sent yet");
        assertTrue(engine.snapshot(writer(), t));
        assertFalse(engine.due(t));
        long later = t + DELAY;
        assertTrue(record(100, later), "quiet for zmux_max_ack_delay");
        engine.write(writer(), later, false);
        assertFalse(engine.due(later), "the snapshot settled the prompt acknowledgement");
    }

    @Test void aBulkTransferIsNotAcknowledgedMoreOften() throws Exception {
        long t = Time.SECOND;
        assertTrue(record(1000, t));
        assertTrue(engine.snapshot(writer(), t));
        // Records every 100 us below the 64 KiB threshold: due only at the threshold, as before.
        long now = t;
        int acks = 0;
        for (int i = 0; i < 640; i++) {
            now += 100 * Time.MICROSECOND;
            if (record(1024, now)) {
                acks++;
                assertTrue(lane.ack.elicit() >= 65536, "due only at the threshold");
                assertTrue(engine.snapshot(writer(), now));
            }
        }
        assertEquals(10, acks);
    }

    @Test void unreportedPositionsRideAlongWithCredit() throws Exception {
        long t = Time.SECOND;
        record(100, t);
        engine.snapshot(writer(), t);
        record(100, t + MS);
        assertFalse(engine.due(t + MS));
        engine.write(writer(), t + MS, false);
        assertEquals(100, lane.ack.reported(), "no credit frame: nothing written");
        engine.write(writer(), t + MS, true);
        assertEquals(200, lane.ack.reported(), "written with the credit frame");
        engine.write(writer(), t + MS, true);
        assertEquals(200, lane.ack.reported());
    }

    @Test void theReleaseTimeDoesNotMoveWithEvents() throws Exception {
        Limiter.Params pp = new Limiter.Params(100 * Time.MICROSECOND, 64 << 10, DELAY, 16 << 10);
        long now = Time.SECOND;
        RateEstimator est = new RateEstimator();
        Poke.set(est, "blockedNow", true);
        est.sample(1.25e9, false, 0, false, now); // 10 Gbit/s
        Poke.set(est, "blockedNow", false);
        Limiter l = new Limiter(now);
        Poke.set(l, "mode", Limiter.CRUISE);
        Poke.set(l, "phaseSince", now);
        long srtt = 10 * MS;
        Poke.set(l, "baseD", srtt);
        Poke.set(l, "baseHas", true);
        long at = now + 30 * MS;
        long room = l.room(at, 0, est.value(), srtt, pp);
        assertTrue(room > 0);
        l.settle(at, room, est.value(), srtt, pp);
        l.refresh(at, 0, est.value(), srtt, pp);
        assertTrue(l.held());
        long release = l.release();
        assertEquals(at + Limiter.GRANULARITY, release, "no earlier than G after the hold began");
        // Events every 100 us before the release refresh the hold; the release time stays.
        for (long e = at + 100 * Time.MICROSECOND; e < release; e += 100 * Time.MICROSECOND) {
            l.refresh(e, 0, est.value(), srtt, pp);
            if (!l.held()) break;
            assertEquals(release, l.release(), "at " + (e - at));
        }
    }
}
