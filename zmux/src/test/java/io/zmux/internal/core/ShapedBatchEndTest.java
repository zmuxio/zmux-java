package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.wire.FrameEncoder;
import io.zmux.carrier.VarInt;
import io.zmux.shaping.Capability;
import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.RecordView;
import io.zmux.shaping.Shaper;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A planned record opens only where the batch holds its target: a frame may fit the batch's end
 * while the PADDING to pad_to does not, and the target would be clamped (design 18.9).
 */
@Timeout(30)
class ShapedBatchEndTest {
    static final int BATCH = 1500;

    /** A session whose pipe plans records alternately of 1000 and 200 bytes. */
    static Session shaped() {
        CoreConfig.Builder c = CoreConfig.builder().role(CoreConfig.Role.INITIATOR).transportRole(CoreConfig.Role.INITIATOR);
        Session s = new Session(c.build(), 0);
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, false, true);
        s.attachBootstrap(info, info, null);
        int[] k = {0};
        s.setShaper(pipe -> new Shaper() {
            @Override public Set<Capability> capabilities() { return EnumSet.of(Capability.PLAN); }
            @Override public void plan(RecordView view, RecordPlan plan) {
                int t = k[0]++ % 2 == 0 ? 1000 : 200;
                plan.maxLength(t).padTo(t);
            }
            @Override public void onEvent(io.zmux.shaping.Event event, io.zmux.shaping.Reply reply) {}
        });
        return s;
    }

    /** Frames lengths of the batch's records. */
    static List<Integer> records(Batch b) {
        byte[] bytes = BatchFixture.flat(b);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        List<Integer> r = new ArrayList<>();
        for (int i = 0; i < bytes.length; ) {
            int size = (int) VarInt.get(bb, i, bytes.length);
            r.add(size);
            i += VarInt.lengthOf(bytes[i]) + size;
        }
        return r;
    }

    interface Tail { boolean write(RecordWriter w); }

    /**
     * Fills records of 1000 and 200 bytes with STREAM data, then tail adds a small frame near the
     * batch's end; every record of the batches hits its target and nothing is clamped.
     */
    static void check(Tail tail) {
        Session s = shaped();
        Shaping.PipeShape ps = s.pipes().shaping().of(s.pipes().get(0));
        assertNotNull(ps);
        RecordWriter w = new RecordWriter();
        for (int batch = 0; batch < 4; batch++) {
            Batch b = new Batch();
            b.reset(0, 0);
            w.init(b, Framing.BYTES, 65535, BATCH);
            w.shape(ps);
            boolean wrote = false;
            for (int rec = 0; rec < 2; rec++) {
                int room = w.room() - FrameEncoder.streamHeaderLength(4, 0, 1000, true);
                if (room <= 0) break;
                assertTrue(w.stream(4, 0, ByteBuffer.allocate(room), Pin.NONE, false));
                w.end();
                wrote = true;
            }
            wrote |= tail.write(w);
            w.end();
            List<Integer> recs = records(b);
            assertTrue(wrote && !recs.isEmpty());
            for (int n : recs) assertTrue(n == 1000 || n == 200, "a record of " + n + " bytes: " + recs);
            b.release();
        }
        assertEquals(0, s.shapingStats().clamped());
    }

    @Test void aControlFrameAtTheBatchEndWaitsForTheNextBatch() {
        check(w -> w.frame(1 + VarInt.length(100), true, bb -> FrameEncoder.maxStreams(bb, true, 100)));
    }

    @Test void streamDataAtTheBatchEndWaitsForTheNextBatch() {
        check(w -> w.stream(8, 0, ByteBuffer.allocate(20), Pin.NONE, true));
    }
}
