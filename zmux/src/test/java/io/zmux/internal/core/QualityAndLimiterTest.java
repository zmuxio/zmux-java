package io.zmux.internal.core;

import io.zmux.internal.loop.MemoryBudget;

import static org.junit.jupiter.api.Assertions.*;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

class QualityAndLimiterTest {
    @Test void qualityBandCoalescesAndZeroCapacityFloorIsDisabled() throws Exception {
        Session session = new Session(CoreConfig.defaults(), 0);
        CarrierInfo info = new CarrierInfo(Framing.BYTES, false, false, false, false);
        session.pipes().attachBootstrap(info, info, null);
        session.setCapacityFloor(0, 0);
        assertNull(session.outbox().pollEvent());
        session.setQualityBand(0, 10, 0, 100);
        Pipe pipe = session.pipes.get(0);
        Poke.set(pipe.out().snd.rtt(), "has", true); Poke.set(pipe.out().snd.rtt(), "smoothed", 100);
        session.quality().sample(pipe);
        Poke.set(session, "now", 10); Poke.set(pipe.out().snd.rtt(), "smoothed", 120); session.quality().sample(pipe);
        Poke.set(session, "now", 20); Poke.set(pipe.out().snd.rtt(), "smoothed", 150); session.quality().sample(pipe);
        assertNull(session.outbox().pollEvent());
        session.quality().timers(100, true);
        Event event = session.outbox().pollEvent();
        assertEquals(Event.Kind.PIPE_QUALITY, event.kind()); assertEquals(150, event.rtt());
        assertNull(session.outbox().pollEvent());
    }

    @Test void delayedWindowPermissionCannotSpendAnExhaustedBudget() {
        CoreConfig config = CoreConfig.builder().windowPredicate(true).budget(new MemoryBudget(100, null)).build();
        Session session = new Session(config, 0);
        long original = session.conn.window();
        session.tuner.growConn(original + 50);
        long proposal = session.tuner().proposal();
        assertEquals(original + 50, proposal);
        config.budget().add(100);
        session.tuner().applyProposal(proposal, true);
        assertEquals(original, session.conn.window());
    }
    private static Event capacityEvent(Session session) {
        for (Event event; (event = session.outbox().pollEvent()) != null;) {
            if (event.kind() == Event.Kind.CAPACITY_BELOW || event.kind() == Event.Kind.CAPACITY_RESTORED) return event;
        }
        return null;
    }

    private static void measured(Pipe pipe, double rate) {
        Poke.set(pipe.out().snd.re(), "blockedNow", true);
        pipe.out().snd.re().sample(rate, false, 0, 0);
        Poke.set(pipe.out().snd.re(), "blockedNow", false);
    }

    private static Pipe extra(Session session) {
        return PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_OPEN);
    }

    @Test void capacityCountsAcceptedApplicationLimitedSamplesWithoutEndingPlacement() throws Exception {
        Session session = new Session(CoreConfig.defaults(), 0);
        var info = new CarrierInfo(Framing.BYTES, false, false, false, false);
        session.pipes().attachBootstrap(info, info, null);
        session.setCapacityFloor(1_500_000, 1.01);
        assertEquals(0, capacityEvent(session).rate());
        var rate = session.pipes.get(0).out().snd.re();
        rate.sample(2_000_000, true, 0, 1);
        assertFalse(rate.measured(), "an application-limited sample must not end placement probing");
        session.quality().sample(session.pipes.get(0));
        var restored = capacityEvent(session);
        assertNotNull(restored); assertEquals(Event.Kind.CAPACITY_RESTORED, restored.kind());
        assertEquals(2_000_000, restored.rate());
        assertNull(capacityEvent(session));
    }

    @Test void capacityDoesNotCountTheSyntheticInitialRateAfterARejectedSample() throws Exception {
        Session session = new Session(CoreConfig.defaults(), 0);
        var info = new CarrierInfo(Framing.BYTES, false, false, false, false);
        session.pipes().attachBootstrap(info, info, null);
        session.pipes.get(0).out().snd.re().sample(500_000, true, 0, 1);
        session.setCapacityFloor(100_000, 1.01);
        assertEquals(0, capacityEvent(session).rate());
    }

    @Test void capacityUsesSettledPathsAndUpdatesWhenProbationExpiresOrIsDisabled() throws Exception {
        Session session = PipeTargetTest.session();
        Pipe stable = session.pipes.get(0), probation = extra(session);
        measured(stable, 2_000_000); measured(probation, 3_000_000);
        Poke.set(probation.mp().prob, "probOn", true); Poke.set(probation.mp().tier, "probOpt", 10 * Time.MILLISECOND);
        session.setCapacityFloor(4_000_000, 1.01);
        Event initial = capacityEvent(session);
        assertNotNull(initial); assertEquals(2_000_000, initial.rate());
        Poke.set(session, "now", 10 * Time.MILLISECOND);
        session.mp().probation.probationTimers(session.now(), true);
        Event restored = capacityEvent(session);
        assertNotNull(restored); assertEquals(Event.Kind.CAPACITY_RESTORED, restored.kind()); assertEquals(5_000_000, restored.rate());
        Poke.set(probation.mp().prob, "probOn", true);
        session.quality().checkFloor(); assertEquals(2_000_000, capacityEvent(session).rate());
        var options = Tiers.PipeSettings.builder().probation(0L).build();
        session.mp().tiers.setPipe(probation.id, options);
        assertEquals(Event.Kind.CAPACITY_RESTORED, capacityEvent(session).kind());
    }

    @Test void expiredProbationRemainsAnImmediateDeadline() throws Exception {
        Session session = PipeTargetTest.session();
        Pipe pipe = extra(session);
        Poke.set(pipe.mp().prob, "probOn", true); Poke.set(pipe.mp().tier, "probOpt", 10 * Time.MILLISECOND);
        Poke.set(session, "now", 20 * Time.MILLISECOND);
        long deadline = session.mp().probation.probationTimers(session.now(), false);
        assertTrue(deadline <= session.now(), "expired probation must still schedule immediate processing");
        assertTrue(pipe.mp().prob.probOn(), "deadline queries must not mutate probation");
        session.mp().probation.probationTimers(session.now(), true);
        assertFalse(pipe.mp().prob.probOn());
    }

    @Test void capacityRevalidationUsesTheSameFallbackWithoutIncludingStandby() throws Exception {
        Session session = PipeTargetTest.session();
        Pipe stable = session.pipes.get(0), revalidating = extra(session);
        measured(stable, 2_000_000); measured(revalidating, 3_000_000);
        session.setCapacityFloor(4_000_000, 1.01);
        assertNull(capacityEvent(session));
        session.mp().probation.revalidate(revalidating.id, false);
        Event below = capacityEvent(session);
        assertNotNull(below); assertEquals(2_000_000, below.rate());
        session.mp().probation.endRevalidation(revalidating);
        assertEquals(Event.Kind.CAPACITY_RESTORED, capacityEvent(session).kind());
        Poke.set(stable.mp().tier, "sendStandby", true);
        Poke.set(revalidating.mp().prob, "reval", true);
        session.quality().checkFloor();
        assertEquals(0, capacityEvent(session).rate(), "a settled standby suppresses probation fallback but never adds to the floor");
        Poke.set(stable.mp().prob, "reval", true);
        session.quality().checkFloor();
        // Both paths are revalidating: the normal last-path fallback admits the non-standby one.
        session.setCapacityFloor(2_500_000, 1.01);
        assertNull(capacityEvent(session));
    }

    @Test void drainingARevalidatingPipeNeverReportsIntermediateRestoration() throws Exception {
        Session session = PipeTargetTest.session();
        measured(session.pipes.get(0), 2_000_000);
        Pipe draining = extra(session); measured(draining, 3_000_000); Poke.set(draining.mp().prob, "reval", true);
        session.setCapacityFloor(4_000_000, 1.01);
        assertEquals(2_000_000, capacityEvent(session).rate());
        session.mp().health.startDrain(draining);
        assertNull(capacityEvent(session), "draining cannot temporarily restore a path to the usable set");
    }

    @Test void penaltyEntryAndFinalAcknowledgementUpdateCapacityWithoutLaterTraffic() throws Exception {
        Session session = PipeTargetTest.session();
        measured(session.pipes.get(0), 1_000_000);
        Pipe penalized = extra(session); measured(penalized, 4_000_000);
        session.setCapacityFloor(2_500_000, 1.01);
        assertNull(capacityEvent(session));
        Poke.set(penalized.out().snd, "so", 100L);
        session.mp().retention.penalize(penalized);
        Event below = capacityEvent(session);
        assertNotNull(below); assertEquals(1_000_000, below.rate());
        Poke.set(penalized.out().snd, "cum", 99L); session.mp().retention.penaltyAcked(penalized);
        assertNull(capacityEvent(session));
        Poke.set(penalized.out().snd, "cum", 100L); session.mp().retention.penaltyAcked(penalized);
        Event restored = capacityEvent(session);
        assertNotNull(restored); assertEquals(Event.Kind.CAPACITY_RESTORED, restored.kind());
        assertEquals(3_000_000, restored.rate(), "the real prior observation remains valid at its penalized rate");
    }
}
