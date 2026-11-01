package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.wire.FrameEncoder;
import io.zmux.shaping.Capability;
import io.zmux.shaping.Event;
import io.zmux.shaping.EventKind;
import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.Reply;
import io.zmux.shaping.Shaper;
import io.zmux.shaping.ShapingLimits;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The ratio limit's startup allowance, and injections that wait for a batch with room instead of shrinking. */
@Timeout(30)
class PaddingBudgetTest {
    static Session session(ShapingLimits lim, int inject) {
        return session(lim, inject, null);
    }

    static Session session(ShapingLimits lim, int inject, java.util.function.Consumer<RecordPlan> plan) {
        CoreConfig.Builder c = CoreConfig.builder().role(CoreConfig.Role.INITIATOR).transportRole(CoreConfig.Role.INITIATOR)
                .shapingLimits(lim);
        Session s = new Session(c.build(), 0);
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, false, true);
        s.attachBootstrap(info, info, null);
        s.setShaper(pipe -> new Shaper() {
            @Override public Set<Capability> capabilities() { return plan == null ? EnumSet.noneOf(Capability.class) : EnumSet.of(Capability.PLAN); }
            @Override public void plan(io.zmux.shaping.RecordView v, RecordPlan p) { if (plan != null) plan.accept(p); }
            @Override public void onEvent(Event e, Reply r) { if (e.kind() == EventKind.START && inject > 0) r.inject(0, inject); }
        });
        return s;
    }

    @Test void ratioAllowsOneLargestRecordFromTheStart() {
        Session s = session(ShapingLimits.none().withMaxPaddingRatio(0.01), 0);
        Shaping sh = s.pipes().shaping();
        assertEquals(RecordPlan.MAX_RECORD_LENGTH - 2, sh.allowPad(RecordPlan.MAX_RECORD_LENGTH - 2));
        assertEquals(0, s.shapingStats().clamped());
        assertEquals(2, sh.allowPad(100), "beyond the allowance the ratio bounds it");
        assertEquals(1, s.shapingStats().clamped());
    }

    @Test void injectionWaitsForTheNextBatchWhole() {
        Session s = session(ShapingLimits.none(), 1200);
        Shaping sh = s.pipes().shaping();
        Shaping.PipeShape ps = sh.of(s.pipes().get(0));
        assertNotNull(ps);
        sh.timeout(Long.MAX_VALUE);
        assertTrue(Shaping.wants(s.pipes().get(0), 0));
        RecordWriter w = new RecordWriter();
        Batch b = new Batch();
        b.reset(0, 0);
        w.init(b, Framing.BYTES, 65535, 1500);
        w.shape(ps);
        assertTrue(w.stream(4, 0, ByteBuffer.allocate(1000 - FrameEncoder.streamHeaderLength(4, 0, 1000, true)), Pin.NONE, false));
        w.end();
        sh.inject(w);
        w.end();
        assertEquals(1, ShapedBatchEndTest.records(b).size(), "no PADDING record in the full batch");
        assertTrue(Shaping.deferred(ps));
        b.release();
        b = new Batch();
        b.reset(0, 0);
        w.init(b, Framing.BYTES, 65535, 1500);
        w.shape(ps);
        sh.inject(w);
        w.end();
        assertEquals(List.of(1200), ShapedBatchEndTest.records(b));
        b.release();
        assertEquals(0, s.shapingStats().clamped());
        assertEquals(1, s.shapingStats().injected());
    }

    @Test void aNegativePaddingRequestIsNone() {
        Session s = session(ShapingLimits.none().withMaxPaddingBytes(1 << 20), 0);
        assertEquals(0, s.pipes().shaping().allowPad(-5));
        assertEquals(0, s.shapingStats().paddingBytes());
        assertEquals(0, s.shapingStats().clamped());
    }

    @Test void aPauseThatWouldOverflowTheTotalIsBoundedByMaxPauseTotal() {
        long minute = 60_000_000_000L;
        Session s = session(ShapingLimits.none().withMaxPauseTotal(java.time.Duration.ofHours(1)), 0);
        Shaping sh = s.pipes().shaping();
        sh.pause(30 * minute);
        sh.pause(Long.MAX_VALUE - 10 * minute);
        assertEquals(java.time.Duration.ofHours(1), s.shapingStats().pauseTotal());
    }

    @Test void paddingTargetsBeyondARecordAreBoundedByIt() {
        List<java.util.function.Consumer<RecordPlan>> plans = List.of(
                p -> p.padTo(Integer.MAX_VALUE).padMultiple(3),
                p -> p.padMultiple(Integer.MAX_VALUE),
                p -> p.pad(Integer.MAX_VALUE),
                p -> p.padTo(300).pad(Integer.MAX_VALUE),
                p -> p.padTo(300).padMultiple(Integer.MAX_VALUE));
        for (var plan : plans) {
            Session s = session(ShapingLimits.none().withMaxPaddingBytes(1 << 20), 0, plan);
            Shaping sh = s.pipes().shaping();
            Shaping.PipeShape ps = sh.of(s.pipes().get(0));
            RecordWriter w = new RecordWriter();
            Batch b = new Batch();
            b.reset(0, 0);
            w.init(b, Framing.BYTES, 65535, 1500);
            w.shape(ps);
            w.stream(4, 0, ByteBuffer.allocate(100), Pin.NONE, false);
            w.end();
            for (int n : ShapedBatchEndTest.records(b)) assertTrue(n <= 65535, "record of " + n + " bytes");
            assertTrue(s.shapingStats().clamped() > 0);
            b.release();
        }
    }
}
