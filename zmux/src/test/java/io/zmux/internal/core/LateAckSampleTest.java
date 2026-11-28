package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.wire.TransportParams;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * An entry whose ACK Delay is above the peer's maximum gives an RTT sample on a pipe that never had
 * one and on a pipe without one for 1 s, and no other.
 */
class LateAckSampleTest {
    private static final long MS = Time.MILLISECOND;
    private final CarrierInfo info = new CarrierInfo(Framing.BYTES, false, false, false, true);
    private final Pipe pipe = new Pipe(0, null, new Pipe.OutDir(info), null, null);
    private final Pipe.OutLane lane = pipe.out().lanes.get(0);
    private final List<Long> sampled = new ArrayList<>();
    private final AckEngine engine;
    private long end;

    LateAckSampleTest() {
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

    @SuppressWarnings("unchecked") // parsed JSON: the vector file's schema fixes the type
    private void ack(long sent, long now, long delayMicros) throws Exception {
        AckEngine.Send ps = pipe.out().snd;
        long start = end;
        end += 100;
        Poke.set(ps, "so", end);
        Field samples = AckEngine.Send.class.getDeclaredField("samples");
        samples.setAccessible(true);
        ((ArrayList<Pipe.Sample>) samples.get(ps)).add(new Pipe.Sample(end, sent, 0, sent, sent, 0, false, 0, 0, false));
        lane.segs.add(start, end, start);
        engine.acked(lane, end);
        engine.advanced(pipe, lane, end, delayMicros, now);
    }

    @Test
    void aLateEntrySamplesOnlyAPipeWithoutARecentSample() throws Exception {
        long late = 50_000; // microseconds, above the default 25 ms maximum
        long first = 200 * MS;
        ack(100 * MS, first, late);
        assertEquals(1, sampled.size(), "a pipe that never had a sample takes the late entry");
        long soon = first + 500 * MS;
        ack(first, soon, late);
        assertEquals(1, sampled.size(), "a late entry within 1 s of a sample is not one");
        ack(soon, first + Time.SECOND, late);
        assertEquals(2, sampled.size(), "a pipe without a sample for 1 s takes the late entry");
    }
}
