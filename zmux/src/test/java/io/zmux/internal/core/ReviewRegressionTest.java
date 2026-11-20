package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.join.Mode;
import io.zmux.internal.wire.TransportParams;
import io.zmux.shaping.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class ReviewRegressionTest {
    private static long addPipe(Session s) throws Exception {
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, false, false, false);
        return s.addPipe(Joins.JoinOptions.builder().mode(Mode.SEALED).in(info).out(info).build());
    }

    private abstract static class Planner implements Shaper {
        @Override public Set<Capability> capabilities() { return Set.of(Capability.PLAN); }
        @Override public void onEvent(io.zmux.shaping.Event e, Effects out) { }
    }

    /** A join the cap or the backoff holds has its deadline in the core, from JOINING. */
    @Test void aHeldJoinEndsAtItsDeadline() throws Exception {
        Session s = PipeTargetTest.session(CoreConfig.builder().joinTimeout(Time.SECOND));
        long id = addPipe(s);
        Poke.set(s.pipes.get(id).mp().joining, "held", true);
        s.mp().health.pipeTimers(Time.SECOND - 1, true);
        assertEquals(Session.JoinProgress.PENDING, s.joinProgress(id));
        assertTrue(s.mp().health.pipeTimers(0, false) <= Time.SECOND, "the core's timer is armed");
        s.mp().health.pipeTimers(Time.SECOND, true);
        assertEquals(Session.JoinProgress.GONE, s.joinProgress(id));
    }

    /** Being let go does not restart the clock of a held join. */
    @Test void aHeldJoinKeepsItsDeadlineOnceLetGo() throws Exception {
        Session s = PipeTargetTest.session(CoreConfig.builder().joinTimeout(Time.SECOND));
        long id = addPipe(s);
        Poke.set(s.pipes.get(id).mp().joining, "held", true);
        assertEquals("hello", s.joinPrepare(id, Time.SECOND / 2));
        s.mp().health.pipeTimers(Time.SECOND, true);
        assertEquals(Session.JoinProgress.GONE, s.joinProgress(id));
    }

    @Test void aSessionLimitViolationCountsNothing() {
        ConnFlow f = new ConnFlow(100, () -> { });
        assertTrue(f.received(60));
        assertFalse(f.received(41));
        assertEquals(60, f.received());
        assertTrue(f.received(40));
        assertEquals(100, f.received());
        assertFalse(f.received(Long.MAX_VALUE));
        assertEquals(100, f.received());
    }

    @Test void aLocatorBeyondItsLengthFieldIsAnError() throws Exception {
        TransportParams p = new TransportParams();
        p.present |= TransportParams.MULTIPATH | TransportParams.SESSION_CRYPTO;
        p.sessionCrypto = io.zmux.internal.join.SessionCrypto.of(new long[] {1}, new long[] {2});
        p.locator = new byte[255];
        byte[] b = p.toBytes();
        assertEquals(255, TransportParams.decode(ByteBuffer.wrap(b), 0, b.length).locator.length);
        p.locator = new byte[256];
        assertThrows(IllegalArgumentException.class, p::toBytes);
    }

    /** An answer that repeats a pause a limit shortened counts it once. */
    @Test void aRepeatedPauseCountsOnce() {
        Session s = HeldControlTest.session(CoreConfig.Role.INITIATOR);
        s.setShapingLimits(ShapingLimits.defaults().withMaxDelay(Duration.ofMillis(10)));
        s.setShaper(pipe -> new Planner() { @Override public void plan(RecordView v, RecordPlan p) { } });
        var ps = s.pipes.get(0).shape();
        long now = Time.SECOND;
        s.setNow(now);
        Effects e = new Effects().gate(now + Time.SECOND, 0, 0);
        ps.apply(e);
        for (int i = 0; i < 5; i++) {
            s.setNow(now += Time.MILLISECOND);
            ps.apply(e);
        }
        assertEquals(Duration.ofMillis(10), s.shapingStats().stallSpent());
        s.setNow(now + Time.MILLISECOND);
        ps.apply(new Effects().gate(Time.SECOND + Time.SECOND + 5 * Time.MILLISECOND, 0, 0));
        assertEquals(Duration.ofMillis(15), s.shapingStats().stallSpent());
    }

    /** A record planned in the middle of a write and delayed, opening a later write, carries that write's overhead. */
    @Test void aDelayedRecordFitsTheWriteItOpens() throws Exception {
        int over = 7, target = 300;
        Session a = withOverhead(CoreConfig.Role.INITIATOR, over);
        a.setShaper(pipe -> new Planner() {
            @Override public void plan(RecordView v, RecordPlan p) { p.padTo(target).delayNanos(5 * Time.MILLISECOND); }
        });
        var ps = a.pipes.get(0).shape();
        RecordWriter w = new RecordWriter();
        w.init(new Batch(), Framing.BYTES, 16382, 1 << 16);
        Poke.set(w, "recs", 1); // planned behind a record of its write
        assertTrue(ps.prep(w));
        assertEquals(target, WireLength.wireOf(ps.target(100), true, 0));
        Poke.set(w, "recs", 0); // it opens the next write
        assertTrue(ps.prep(w));
        assertEquals(target, WireLength.wireOf(ps.target(100), true, over));
        assertEquals(target, WireLength.wireOf(ps.target(100), true, over), "and stays so");
    }

    private static Session withOverhead(CoreConfig.Role role, int over) {
        Session s = new Session(CoreConfig.builder().role(role).transportRole(role).build(), 0);
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, false, false, false, null, null, "", over, "", 0, false);
        s.attachBootstrap(info, info, null);
        return s;
    }
}
