package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.wire.FrameEncoder;
import io.zmux.shaping.Capability;
import io.zmux.shaping.Effects;
import io.zmux.shaping.Event;
import io.zmux.shaping.EventKind;
import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.RecordView;
import io.zmux.shaping.Shaper;
import io.zmux.shaping.ShapingBudget;
import io.zmux.shaping.ShapingLimits;
import io.zmux.shaping.WireAccount;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** What the account refuses, what it only meters, and what counts as this endpoint's own bytes. */
@Timeout(30)
class ShapingBudgetTest {
    private static final long NOW = Time.SECOND;

    /** A shaper that asks for one injection at its start and keeps it. */
    static final class Fixed implements Shaper {
        final int inject;
        final Consumer<RecordPlan> plan;
        int injected, dropped;
        RecordView view;
        Fixed(int inject, Consumer<RecordPlan> plan) { this.inject = inject; this.plan = plan; }
        @Override public Set<Capability> capabilities() { return plan == null ? EnumSet.noneOf(Capability.class) : EnumSet.of(Capability.PLAN, Capability.BUDGET); }
        @Override public void plan(RecordView v, RecordPlan p) { view = v; plan.accept(p); }
        @Override public void onEvent(Event e, Effects r) {
            if (e.kind() == EventKind.START && inject > 0) r.inject(0, inject);
            if (e.kind() == EventKind.INJECTED) injected++;
            if (e.kind() == EventKind.INJECT_DROPPED) dropped++;
        }
    }

    static Session session(ShapingLimits lim, ShapingBudget budget, Fixed f) {
        CoreConfig.Builder c = CoreConfig.builder().role(CoreConfig.Role.INITIATOR).transportRole(CoreConfig.Role.INITIATOR)
                .shapingLimits(lim).shapingBudget(budget);
        Session s = new Session(c.build(), 0);
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, false, true);
        s.attachBootstrap(info, info, null);
        s.setShaper(pipe -> f);
        s.setNow(NOW);
        return s;
    }

    /** A batch of the writer's data on the bootstrap pipe, then a due injection. */
    private static List<Integer> write(Session s, int data) {
        Shaping sh = s.pipes().shaping();
        Shaping.PipeShape ps = sh.of(s.pipes().get(0));
        sh.timeout(NOW);
        RecordWriter w = new RecordWriter();
        Batch b = new Batch();
        b.reset(0, 0);
        w.init(b, Framing.BYTES, 65535, 65536);
        w.shape(ps);
        if (data > 0) assertTrue(w.stream(4, 0, ByteBuffer.allocate(data), Pin.NONE, false));
        sh.inject(w);
        w.end();
        List<Integer> sizes = ShapedBatchEndTest.records(b);
        b.release();
        return sizes;
    }

    private static ShapingLimits initial(long n) { return ShapingLimits.defaults().withWire(new WireAccount(n, 0, 0)); }

    @Test void paddingIsNeverRefusedAndOnlyMetered() {
        Fixed f = new Fixed(0, p -> p.padTo(1000));
        Session s = session(initial(1), ShapingBudget.NONE, f);
        assertEquals(List.of(998), write(s, 100));
        assertEquals(1, s.shapingMeters().wireLeft(), "padding takes nothing from the account");
        long frame = FrameEncoder.streamHeaderLength(4, 0, 100, true) + 100;
        assertEquals(998 - frame, s.shapingStats().wireSpent());
    }

    @Test void anInjectionTheAccountDoesNotCoverIsDroppedOnly() {
        Fixed f = new Fixed(500, null);
        Session s = session(initial(100), ShapingBudget.NONE, f);
        assertEquals(List.of(), write(s, 0));
        assertEquals(1, f.dropped);
        assertEquals(0, f.injected);
        assertEquals(100, s.shapingMeters().wireLeft());
        assertEquals(0, s.shapingStats().wireSpent());
    }

    @Test void aRecordOfPaddingIsChargedItsWholeLength() {
        Fixed f = new Fixed(500, null);
        Session s = session(initial(600), ShapingBudget.NONE, f);
        assertEquals(List.of(498), write(s, 0));
        assertEquals(1, f.injected);
        assertEquals(100, s.shapingMeters().wireLeft());
        assertEquals(500, s.shapingStats().wireSpent());
        assertEquals(1, s.shapingStats().injected());
    }

    @Test void aMergedInjectionIsMeteredOnce() {
        Fixed f = new Fixed(500, p -> { });
        Session s = session(initial(1000), ShapingBudget.NONE, f);
        write(s, 50);
        assertEquals(1, f.injected);
        assertEquals(500, s.shapingMeters().wireLeft());
        assertEquals(500, s.shapingStats().wireSpent());
    }

    @Test void aMergedInjectionTheAccountRefusesChangesNothing() {
        Fixed f = new Fixed(500, p -> { });
        Session s = session(initial(100), ShapingBudget.NONE, f);
        List<Integer> sizes = write(s, 50);
        assertEquals(1, f.dropped);
        assertEquals(1, sizes.size());
        assertTrue(sizes.getFirst() < 100, sizes.toString());
    }

    @Test void theSwitchesDropInjections() {
        for (ShapingLimits l : List.of(ShapingLimits.defaults().withNoInjection(true), ShapingLimits.defaults().withNoPadding(true))) {
            Fixed f = new Fixed(100, null);
            write(session(l, ShapingBudget.NONE, f), 0);
            assertEquals(1, f.dropped, l.toString());
        }
    }

    /** One drop at one instant; the kept injection is tried a granularity later. */
    @Test void aDroppedInjectionKeptDoesNotSpin() {
        Fixed f = new Fixed(100, null);
        Session s = session(ShapingLimits.defaults().withNoInjection(true), ShapingBudget.NONE, f);
        write(s, 0);
        write(s, 0);
        assertEquals(1, f.dropped);
        assertTrue(s.pipes().shaping().deadline() > NOW);
        s.setNow(NOW + Shaping.GRANULARITY);
        s.pipes().shaping().timeout(NOW + Shaping.GRANULARITY);
        assertTrue(Shaping.wants(s.pipes().get(0), 0));
    }

    @Test void aBudgetCannotWidenTheLimits() {
        ShapingBudget b = new ShapingBudget(new WireAccount(1000, 1000, 1000), Duration.ofSeconds(5), false);
        ShapingLimits in = ShapingLimits.defaults().withWire(new WireAccount(10, 20, 30)).withMaxDelay(Duration.ofMillis(5)).under(b);
        assertEquals(new WireAccount(10, 20, 30), in.wire());
        assertEquals(Duration.ofMillis(5), in.maxDelay());
        assertTrue(ShapingLimits.defaults().under(new ShapingBudget(WireAccount.NONE, Duration.ZERO, true)).noInjection());
    }

    @Test void accountsWithNothingInCommonAllowNoInjection() {
        ShapingLimits l = ShapingLimits.defaults().withWire(new WireAccount(10, 0, 0));
        ShapingLimits in = l.under(new ShapingBudget(new WireAccount(0, 5, 0), Duration.ZERO, false));
        assertFalse(in.wire().isSet());
        assertTrue(in.noInjection());
        assertFalse(ShapingLimits.defaults().under(ShapingBudget.NONE).noInjection(), "neither set is no bound");
    }

    @Test void aSmallBurstIsRaisedToTheLeastRecord() {
        Session s = session(ShapingLimits.defaults(), ShapingBudget.NONE, new Fixed(0, p -> { }));
        Shaping.PipeShape ps = s.pipes().shaping().of(s.pipes().get(0));
        ps.apply(new Effects().gate(0, 1_000_000, 1));
        assertEquals(1, s.shapingStats().clamped());
        assertFalse(ps.held(NOW), "a burst of 1 could never reach its quantum");
    }

    @Test void changingTheRateSettlesTheTimeHeld() {
        Session s = session(ShapingLimits.defaults(), ShapingBudget.NONE, new Fixed(0, p -> { }));
        Shaping.PipeShape ps = s.pipes().shaping().of(s.pipes().get(0));
        ps.apply(new Effects().gate(0, 1000, 100));
        ps.spent(100);
        assertTrue(ps.held(NOW));
        s.setNow(NOW + Time.MILLISECOND);
        ps.apply(new Effects().gate(0, 2000, 100));
        assertEquals(Duration.ofMillis(1), s.shapingStats().stallSpent());
    }

    @Test void delaysAreBoundedByDefault() {
        Session s = session(ShapingLimits.defaults(), ShapingBudget.NONE, new Fixed(0, p -> { }));
        Shaping sh = s.pipes().shaping();
        assertEquals(Time.SECOND, sh.pause(5 * Time.SECOND));
        assertEquals(1, s.shapingStats().clamped());
        assertEquals(Duration.ofSeconds(1), s.shapingStats().stallSpent());
        assertEquals(1_000_000, s.shapingMeters().stallSpent());
        assertEquals(Time.SECOND, sh.maxHold(s.pipes().get(0)));
    }

    @Test void noBoundLeavesAPauseAndHoldsNothing() {
        Session s = session(ShapingLimits.defaults().withMaxDelay(Duration.ofNanos(-1)), ShapingBudget.NONE, new Fixed(0, p -> { }));
        Shaping sh = s.pipes().shaping();
        assertEquals(5 * Time.SECOND, sh.pause(5 * Time.SECOND));
        assertEquals(0, s.shapingStats().clamped());
        assertEquals(0, sh.maxHold(s.pipes().get(0)));
    }

    @Test void aRateBelowTheFloorIsRaisedAndFollowsTheLimits() {
        Session s = session(ShapingLimits.defaults(), ShapingBudget.NONE, new Fixed(0, p -> { }));
        Shaping.PipeShape ps = s.pipes().shaping().of(s.pipes().get(0));
        ps.apply(new Effects().gate(0, 1, 0));
        assertEquals(1, s.shapingStats().clamped());
        s.setShapingLimits(ShapingLimits.defaults().withMaxDelay(Duration.ofNanos(-1)));
        assertEquals(1, s.shapingStats().clamped(), "no bound, no floor");
    }

    @Test void aRateAskedAboveItsBoundIsHeldAtTheBound() {
        Session s = session(ShapingLimits.defaults(), ShapingBudget.NONE, new Fixed(0, p -> { }));
        Shaping.PipeShape ps = s.pipes().shaping().of(s.pipes().get(0));
        ps.apply(new Effects().gate(0, -1L, -1L));
        assertFalse(ps.held(NOW), "the top of the unsigned range is a fast gate, not none");
        assertEquals(0, s.shapingStats().faults());
    }

    @Test void aCutCountsTheNextRecordsHead() {
        Session s = session(ShapingLimits.defaults(), ShapingBudget.NONE, new Fixed(0, p -> p.maxLength(202)));
        write(s, 200 - FrameEncoder.streamHeaderLength(4, 0, 196, true));
        assertEquals(2, s.shapingStats().wireSpent(), "the size field of a 200-byte record");
    }

    @Test void changingTheBudgetCarriesTheAccountOver() {
        Session s = session(initial(1000), ShapingBudget.NONE, new Fixed(0, null));
        s.setShapingBudget(new ShapingBudget(new WireAccount(100, 0, 0), Duration.ZERO, false));
        assertEquals(100, s.shapingMeters().wireLeft());
        s.setShapingBudget(ShapingBudget.NONE);
        assertEquals(1000, s.shapingMeters().wireLeft());
    }

    @Test void theAccountEarnsFromOwnBytesAndTime() {
        Session s = session(ShapingLimits.defaults().withWire(new WireAccount(0, 1000, 10)), ShapingBudget.NONE, new Fixed(0, null));
        assertEquals(10, s.shapingMeters().wireLeft(), "one second since the start");
        assertEquals(-1L, s.shapingMeters().stallLeft());
    }

    /** Padding, injected records and the size field of an injected record earn nothing. */
    @Test void ownBytesLeaveOutPaddingAndInjections() {
        Session s = session(ShapingLimits.defaults(), ShapingBudget.NONE, new Fixed(0, p -> { }));
        Transmitter tx = s.tx;
        RecordWriter w = new RecordWriter();
        Batch b = new Batch();
        b.reset(0, 0);
        w.init(b, Framing.BYTES, 65535, 65536);
        w.padRecord(1000);
        assertEquals(1000 + 2, w.notEarned());
        b.release();
        assertEquals(0, tx.earnBytes());
        w.copied(4, 0, 100);
        assertEquals(1002 + FrameEncoder.streamHeaderLength(4, 0, 100, true) + 100, w.notEarned());
    }

    @Test void injectionWaitsForTheNextBatchWhole() {
        Fixed f = new Fixed(1200, null);
        Session s = session(ShapingLimits.defaults(), ShapingBudget.NONE, f);
        Shaping sh = s.pipes().shaping();
        Shaping.PipeShape ps = sh.of(s.pipes().get(0));
        sh.timeout(NOW);
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
        assertEquals(0, s.shapingStats().wireSpent(), "nothing charged until it goes");
        b.release();
        b = new Batch();
        b.reset(0, 0);
        w.init(b, Framing.BYTES, 65535, 1500);
        w.shape(ps);
        sh.inject(w);
        w.end();
        assertEquals(List.of(1198), ShapedBatchEndTest.records(b));
        b.release();
        assertEquals(0, s.shapingStats().clamped());
        assertEquals(1, s.shapingStats().injected());
    }

    @Test void paddingTargetsBeyondARecordAreBoundedByIt() {
        List<Consumer<RecordPlan>> plans = List.of(
                p -> p.padTo(Integer.MAX_VALUE).padMultiple(3),
                p -> p.padMultiple(Integer.MAX_VALUE),
                p -> p.pad(Integer.MAX_VALUE),
                p -> p.padTo(300).pad(Integer.MAX_VALUE),
                p -> p.padTo(300).padMultiple(Integer.MAX_VALUE));
        for (var plan : plans) {
            Session s = session(ShapingLimits.defaults(), ShapingBudget.NONE, new Fixed(0, plan));
            Shaping.PipeShape ps = s.pipes().shaping().of(s.pipes().get(0));
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

    /** On a message carrier a record of padding holds no size field to leave out. */
    @Test void aRecordOfPaddingOnMessagesEarnsNothing() {
        RecordWriter w = new RecordWriter();
        Batch b = new Batch();
        b.reset(0, 0);
        w.init(b, Framing.RECORDS, 65535, 65536);
        w.padRecord(1000);
        assertEquals(1000, w.notEarned());
        b.release();
    }

    /** The record before it closed the batch: the injection waits, uncharged, and is not dropped. */
    @Test void anInjectionAfterAClosedBatchWaits() {
        Fixed f = new Fixed(100, p -> { });
        Session s = session(initial(1000), ShapingBudget.NONE, f);
        Shaping sh = s.pipes().shaping();
        Shaping.PipeShape ps = sh.of(s.pipes().get(0));
        sh.timeout(NOW);
        RecordWriter w = new RecordWriter();
        Batch b = new Batch();
        b.reset(0, 0);
        w.init(b, Framing.BYTES, 65535, 65536);
        w.shape(ps);
        assertTrue(w.stream(4, 0, ByteBuffer.allocate(50), Pin.NONE, false));
        w.end();
        w.close();
        sh.inject(w);
        assertTrue(Shaping.deferred(ps));
        assertEquals(0, f.dropped);
        assertEquals(1000, s.shapingMeters().wireLeft());
        b.release();
    }

    private static Session[] pair(Shaper shaper) {
        Session a = PreambleLimitTest.session(CoreConfig.Role.INITIATOR, 4096), b = PreambleLimitTest.session(CoreConfig.Role.RESPONDER, 4096);
        PreambleLimitTest.pump(a, b);
        a.setShaper(pipe -> shaper);
        return new Session[] {a, b};
    }

    /** Sent bytes and the spent bytes end to end: the ratio a machine reads. */
    @Test void wirePpmFollowsTheBatches() throws Exception {
        Session[] p = pair(new Fixed(0, x -> x.padTo(1000)));
        long earned = p[0].tx.earnBytes();
        Stream st = p[0].streams().openStream(true);
        p[0].sending().write(st, ByteBuffer.allocate(100), false);
        PreambleLimitTest.pump(p[0], p[1]);
        var m = p[0].shapingMeters();
        long own = m.sentBytes() - earned;
        assertTrue(own >= 100 && own < 200, "earned " + own);
        assertTrue(m.wireSpent() > 700, "spent " + m.wireSpent());
        assertEquals(io.zmux.shaping.ShapingMeters.wirePpm(m.wireSpent(), m.sentBytes()), m.wirePpm());
        assertTrue(m.wirePpm() > 1_000_000L);
    }

    /** A preamble above the plan's largest record goes whole, counted as clamped. */
    @Test void aPreambleAboveMaxLenGoesWholeAndIsCounted() throws Exception {
        Session[] p = pair(new Fixed(0, x -> x.maxLength(64)));
        Stream st = p[0].streams().openStream(true);
        p[0].streams().setOpenInfo(st, new byte[300]);
        p[0].sending().write(st, ByteBuffer.allocate(10), false);
        PreambleLimitTest.pump(p[0], p[1]);
        assertTrue(st.snd.sent() > 300, "sent " + st.snd.sent());
        assertTrue(p[0].shapingStats().clamped() > 0);
    }

    /** A preamble above the open gate's tokens goes whole, in debt. */
    @Test void aPreambleOverdrawsTheOpenGate() throws Exception {
        Shaper gated = new Shaper() {
            @Override public Set<Capability> capabilities() { return EnumSet.of(Capability.PLAN); }
            @Override public void plan(RecordView v, RecordPlan x) { }
            @Override public void onEvent(Event e, Effects r) {
                if (e.kind() == EventKind.START) r.gate(0, 1000, 100);
            }
        };
        Session[] p = pair(gated);
        Stream st = p[0].streams().openStream(true);
        p[0].streams().setOpenInfo(st, new byte[300]);
        p[0].sending().write(st, ByteBuffer.allocate(1), false);
        PreambleLimitTest.pump(p[0], p[1]);
        long sent = st.snd.sent();
        assertTrue(sent > 300, "sent " + sent);
        p[0].sending().write(st, ByteBuffer.allocate(50), false);
        PreambleLimitTest.pump(p[0], p[1]);
        assertEquals(sent, st.snd.sent(), "in debt: nothing more goes yet");
    }
}
