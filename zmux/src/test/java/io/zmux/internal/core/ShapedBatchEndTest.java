package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.VarInt;
import io.zmux.shaping.Capability;
import io.zmux.shaping.ProfileLimits;
import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.RecordView;
import io.zmux.shaping.RuntimeReason;
import io.zmux.shaping.Shaper;
import io.zmux.shaping.ShapingBudget;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A plan can pad the selected record within the remaining batch room. Baseline frames
 * that fit at the batch end still go out when the full padding target cannot fit.
 */
@Timeout(30)
class ShapedBatchEndTest {
    static final int BATCH = 1500;

    /** A 1000-byte target and enough overhead allowance to isolate clipping by batch room. */
    static Session shaped() {
        CoreConfig.Builder c = CoreConfig.builder().role(CoreConfig.Role.INITIATOR).transportRole(CoreConfig.Role.INITIATOR);
        Session s = new Session(c.build(), 0);
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, false, true);
        s.attachBootstrap(info, info);
        s.setShaper(pipe -> new Shaper() {
            @Override public Set<Capability> capabilities() { return EnumSet.of(Capability.PLAN); }
            @Override public void plan(RecordView view, RecordPlan plan) {
                plan.padTo(1000);
            }
            @Override public void onEvent(io.zmux.shaping.Event event, io.zmux.shaping.Effects effects) {}
        }, new ShapingBudget(null, new ProfileLimits(10, 0, 2000, Duration.ZERO), true));
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

    static void check(Tail tail) {
        Session s = shaped();
        Shaping.PipeShape ps = s.pipes().shaping().of(s.pipes().get(0));
        assertNotNull(ps);
        RecordWriter w = new RecordWriter();
        Batch b = new Batch();
        b.reset(0, 0);
        try {
            w.init(b, Framing.BYTES, 65535, BATCH);
            w.shape(ps);
            assertTrue(w.stream(4, 0, ByteBuffer.allocate(990), Pin.NONE, false, true));
            w.end();
            assertTrue(tail.write(w), "the baseline frame fits at the batch end");
            w.end();
            assertEquals(List.of(998, 498), records(b));
            assertEquals(1L, s.shapingStats().reasons().get(RuntimeReason.CLIPPED_ROOM));
        } finally {
            b.release();
        }
    }

    @Test void aControlFrameAtTheBatchEndLimitsPadding() {
        check(w -> w.frame(1 + VarInt.length(100), true, bb -> FrameEncoder.maxStreams(bb, true, 100)));
    }

    @Test void streamDataAtTheBatchEndLimitsPadding() {
        check(w -> w.stream(8, 0, ByteBuffer.allocate(20), Pin.NONE, true, true));
    }
}
