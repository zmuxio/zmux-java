package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.wire.TransportParams;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The PIPE_PING of a suspicion gives an RTT sample despite a later suspicion when the path delivered it at its own
 * pace: its acknowledgement came apart from that of the bytes acknowledged before it by at
 * least half the time between their sending. A path whose round trip exceeds T_suspect is learned instead of
 * suspected at every exchange; bytes released together after an outage give no sample.
 */
class SuspicionPingSampleTest {
    private static final long MS = Time.MILLISECOND;
    private final CarrierInfo info = new CarrierInfo(Framing.BYTES, false, false, false, true);
    private final Attach attach = new Attach(true, 2, 0, 0, 0, -1, Double.NaN, Attach.OUT_ACTIVE, Attach.IN_OPEN, false, 0);
    private final Pipe pipe = new Pipe(0, null, new Pipe.OutDir(info), attach, null);
    private final Pipe.OutLane lane = pipe.out().lanes.get(0);
    private final List<Long> sampled = new ArrayList<>();
    private final AckEngine engine;
    private long end;

    SuspicionPingSampleTest() {
        TransportParams none = new TransportParams();
        List<Pipe> pipes = List.of(pipe);
        Pacer pacer = new Pacer(CoreConfig.defaults(), 65536, AckEngine.peerDelay(none), null);
        engine = new AckEngine(none, none, pipes, pacer, new AckEngine.Port() {
            @Override public void activity() { }
            @Override public void rttSampled(Pipe pipe, long min) { sampled.add(min); }
            @Override public void advanced(Pipe p, Pipe.OutLane ol, boolean cum) { }
            @Override public boolean quiet() { return false; }
        });
    }

    /** The pipe became suspect at t: bytes sent until then give no RTT sample. */
    private void suspect(long t) {
        Poke.set(attach.health, "noSample", true);
        Poke.set(attach.health, "noSampleAt", t);
    }

    /** Records a batch sent at sent, the PIPE_PING of a suspicion when probe is set. */
    @SuppressWarnings("unchecked")
    private void send(long sent, boolean probe) throws Exception {
        AckEngine.Send ps = pipe.out().snd;
        long start = end;
        end += 100;
        Poke.set(ps, "so", end);
        Field samples = AckEngine.Send.class.getDeclaredField("samples");
        samples.setAccessible(true);
        ((ArrayList<Pipe.Sample>) samples.get(ps)).add(new Pipe.Sample(end, sent, 0, sent, sent, 0, false, 0, probe));
        lane.segs.add(start, end, start);
    }

    /** Everything sent is acknowledged at now. */
    private void ack(long now) {
        engine.acked(lane, end);
        engine.advanced(pipe, lane, end, 0, now);
    }

    @Test void aSuspicionsPingDeliveredAtThePathsPaceGivesASample() throws Exception {
        // A probe at 0 on a 3 s path; suspect at 1 s, the suspicion's PIPE_PING follows.
        suspect(Time.SECOND);
        send(0, false);
        ack(3 * Time.SECOND);
        assertTrue(sampled.isEmpty(), "the probe was sent before the suspicion");
        send(Time.SECOND, true);
        suspect(3500 * MS); // suspect again before the PIPE_PING's answer
        ack(4 * Time.SECOND);
        assertEquals(1, sampled.size(), "the PIPE_PING's acknowledgement came 1 s after the probe's, sent 1 s apart");
    }

    @Test void aPingReleasedRightAfterEarlierBytesGivesNoSample() throws Exception {
        suspect(Time.SECOND);
        send(0, false);
        ack(3900 * MS);
        send(Time.SECOND, true);
        suspect(3900 * MS);
        ack(4 * Time.SECOND);
        assertTrue(sampled.isEmpty(), "acknowledged 100 ms after bytes sent 1 s earlier: an outage released them");
    }

    @Test void aPingAcknowledgedTogetherWithEarlierBytesGivesNoSample() throws Exception {
        suspect(Time.SECOND);
        send(0, false);
        send(Time.SECOND, true);
        suspect(3 * Time.SECOND);
        ack(4 * Time.SECOND);
        assertTrue(sampled.isEmpty(), "one advance acknowledged the PIPE_PING with earlier bytes");
    }

    @Test void anOrdinaryBatchSentBeforeASuspicionGivesNoSample() throws Exception {
        suspect(Time.SECOND);
        send(0, false);
        ack(3 * Time.SECOND);
        send(Time.SECOND, false);
        suspect(3500 * MS);
        ack(4 * Time.SECOND);
        assertTrue(sampled.isEmpty(), "only the PIPE_PING of a suspicion is taken despite a later suspicion");
    }
}
