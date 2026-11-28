package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.wire.TransportParams;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A rise of the propagation delay delays a lone range as much as a queue would: with nothing of the
 * pipe unacknowledged ahead it proves no queue, so its few bytes over the idle time before it do not
 * replace R_p. The same delay behind a backlog of the pipe is queue-proven.
 */
class LoneRangeRateTest {
    private static final long MS = Time.MILLISECOND;
    private final CarrierInfo info = new CarrierInfo(Framing.BYTES, false, false, false, true);
    private final Pipe pipe = new Pipe(0, null, new Pipe.OutDir(info), null, null);
    private final Pipe.OutLane lane = pipe.out().lanes.get(0);
    private final AckEngine engine;
    private long end;

    LoneRangeRateTest() {
        TransportParams none = new TransportParams();
        Pacer pacer = new Pacer(CoreConfig.defaults(), 65536, AckEngine.peerDelay(none), null);
        engine = new AckEngine(none, none, List.of(pipe), pacer, new AckEngine.Port() {
            @Override public void activity() { }
            @Override public void rttSampled(Pipe pipe, long min) { }
            @Override public void advanced(Pipe p, Pipe.OutLane ol, boolean cum) { }
            @Override public boolean quiet() { return false; }
        });
    }

    /** Acknowledges n bytes sent at sent, at now, in an application-limited bubble. */
    @SuppressWarnings("unchecked") // the field's declared type
    private void ack(long sent, long now, long n, boolean queued) throws Exception {
        AckEngine.Send ps = pipe.out().snd;
        long start = end;
        end += n;
        Poke.set(ps, "so", end);
        Field samples = AckEngine.Send.class.getDeclaredField("samples");
        samples.setAccessible(true);
        ((ArrayList<Pipe.Sample>) samples.get(ps)).add(
                new Pipe.Sample(end, sent, start, now - 100 * MS, sent, 0, true, queued, 0, false));
        Poke.set(ps, "deliveredAt", now - 100 * MS);
        lane.segs.add(start, end, start);
        engine.acked(lane, end);
        engine.advanced(pipe, lane, end, 0, now);
    }

    @Test
    void aLoneRangeAfterADelayRiseIsNotQueueProven() throws Exception {
        ack(0, 50 * MS, 3000, false); // min_rtt 50 ms
        double before = pipe.out().snd.re().value();
        long at = Time.SECOND;
        for (int i = 0; i < 20; i++) {
            ack(at, at + 400 * MS, 36, false);
            at += Time.SECOND;
        }
        assertEquals(before, pipe.out().snd.re().value(), "lone ranges on a slower path replaced R_p");
        for (int i = 0; i < 20; i++) {
            ack(at, at + 400 * MS, 3000, true);
            at += 100 * MS;
        }
        assertEquals(3e4, pipe.out().snd.re().value(), "a backlog behind the same delay is queue-proven");
    }
}
