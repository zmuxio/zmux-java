package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.DirectionHealth;
import io.zmux.PipeState;
import io.zmux.internal.loop.SealedTransport;
import org.junit.jupiter.api.Test;

/**
 * A negative dead timeout turns stall-based pipe failure off: a pipe whose acknowledgements stall far beyond any
 * derived T_dead turns suspect but never fails. A carrier that reports its own failure has it off by default; the
 * pipe's own dead timeout overrides the carrier and the session.
 */
class StallDeathOffTest {
    private static final long S = Time.SECOND, HOUR = 3600 * S;

    /** A multipath session whose bootstrap pipe has had unacknowledged bytes since 1 s. */
    private static Session stalled(long sessionDead, boolean ownFailure, long pipeDead) {
        return stalled(CoreConfig.builder().deadTimeout(sessionDead).resume(60 * S), ownFailure, pipeDead);
    }

    private static Session stalled(CoreConfig.Builder cfg, boolean ownFailure, long pipeDead) {
        Session s = PipeTargetTest.session(cfg);
        Pipe p = s.pipes.get(0);
        Attach a = p.mp();
        a.ownFailure(ownFailure);
        a.liveness(0, pipeDead, -1, Double.NaN);
        Poke.set(a.link, "unacked", true);
        Poke.set(a.link, "oldestAt", S);
        Poke.set(a.link, "liveAt", S);
        Poke.set(a.link, "lastAdvance", S);
        Poke.set(a.link, "lastRecord", S);
        return s;
    }

    /** Applies every pipe deadline up to end. */
    private static void runTo(Session s, long end) {
        for (int i = 0; i < 10_000; i++) {
            long t = s.mp().health.pipeTimers(end, false);
            if (t > end) { s.mp().health.pipeTimers(end, true); return; }
            s.mp().health.pipeTimers(Math.max(t, 0), true);
        }
        fail("the timers do not settle");
    }

    private static void check(String name, long sessionDead, boolean ownFailure, long pipeDead, boolean fails) {
        Session s = stalled(sessionDead, ownFailure, pipeDead);
        Pipe p = s.pipes.get(0);
        runTo(s, 5 * S);
        assertEquals(Attach.OUT_SUSPECT, p.mp().health.out(), name + ": suspect after 4 s of stall");
        runTo(s, HOUR);
        assertEquals(fails, p.mp().health.out() == Attach.OUT_FAILED, name + ": failed after an hour of stall");
        if (!fails) assertEquals(Time.NEVER, s.mp().health.pipeTimers(HOUR, false) < HOUR ? 0 : Time.NEVER, name + ": no deadline is due");
    }

    @Test void aDeadTimeoutBelowTheSuspicionFailsTheLastPipeInTheSamePass() {
        var session = stalled(CoreConfig.builder().suspectFloor(2 * S).deadTimeout(S).resume(0), false, 0);
        var pipe = session.pipes.get(0);
        var health = session.mp().health;
        long after = health.tSuspect(pipe);
        assertEquals(after, health.tDead(pipe));
        long cut = pipe.mp().tref() + after;
        health.pipeTimers(cut - 1, true);
        assertEquals(Attach.OUT_ACTIVE, pipe.mp().out());
        health.pipeTimers(cut, true);
        assertEquals(Attach.OUT_FAILED, pipe.mp().out());
        assertEquals(SessionError.Kind.PIPES_LOST, session.error().kind());
    }

    @Test void theDefaultFailsAStalledPipe() { check("default", 0, false, 0, true); }

    @Test void aPipesOwnOffNeverFailsIt() { check("pipe off", 0, false, -1, false); }

    @Test void theSessionsOffNeverFailsIt() { check("session off", -1, false, 0, false); }

    @Test void aCarrierThatReportsItsOwnFailureHasItOff() { check("carrier", 0, true, 0, false); }

    @Test void thePipesOwnDeadTimeoutOverridesTheCarrier() { check("carrier, pipe explicit", 0, true, 20 * S, true); }

    @Test void thePipesOwnDeadTimeoutOverridesTheSessionsOff() { check("session off, pipe explicit", -1, false, 20 * S, true); }

    @Test void aHugeDeadTimeoutIsSafe() { check("huge", 0, false, Long.MAX_VALUE, false); }

    @Test void aHeldRevalidationWriteBecomesSuspectWithoutAnAcknowledgement() throws Exception {
        Session s = CounterSemanticsTest.established();
        Pipe replacement = PipeTargetTest.appendOut(s, false, Attach.OUT_ACTIVE, Attach.IN_OPEN);
        s.mp().tiers.setPipe(replacement.id, Tiers.PipeSettings.builder().probation(100 * Time.MILLISECOND).build());
        s.setNow(S);
        s.mp().probation.startProbation(replacement, false);
        s.revalidatePipes(replacement.id, false);

        // The carrier takes the probe batch but withholds its completion, like a held writer.
        Batch batch = new Batch();
        try {
            assertTrue(s.pollTransmit(replacement.id, 0, batch, Session.DEFAULT_BATCH, S));
            assertFalse(replacement.mp().prob.revalPing(), "the held batch contains the revalidation probe");
            long suspectAt = S + s.mp().health.tSuspect(replacement);
            s.handleTimeout(suspectAt - 1);
            var before = s.pipeStats(replacement.id, s.now(), s.now());
            assertFalse(before.inProbation(), before::toString);
            assertTrue(before.revalidating(), before::toString);
            assertEquals(PipeState.Outbound.ACTIVE, before.outbound(), before::toString);

            s.handleTimeout(suspectAt);
            var after = s.pipeStats(replacement.id, s.now(), s.now());
            assertFalse(after.inProbation(), after::toString);
            assertFalse(after.revalidating(), after::toString);
            assertEquals(PipeState.Outbound.SUSPECT, after.outbound(), after::toString);
            assertEquals(DirectionHealth.Reason.ACK_STALL, s.pipeHealth(replacement.id).send().reason(),
                    "the write and cumulative ACK deadlines tie");
            assertEquals(before.bytesAcked(), after.bytesAcked());
        } finally {
            batch.release();
        }
    }

    /** A pipe that turned suspect under off keeps it when the settings change while it is suspect. */
    @Test void aSuspectPipeKeepsTheOffItTurnedSuspectUnder() {
        Session s = stalled(0, false, -1);
        Pipe p = s.pipes.get(0);
        runTo(s, 5 * S);
        assertEquals(Attach.OUT_SUSPECT, p.mp().health.out());
        s.mp().health.keepSuspect(p);
        p.mp().liveness(0, 0, -1, Double.NaN);
        runTo(s, HOUR);
        assertEquals(Attach.OUT_SUSPECT, p.mp().health.out(), "the kept off still holds");
    }

    @Test void sealedRecordsKeepTheCarriersReport() {
        CarrierInfo c = new CarrierInfo(Framing.BYTES, false, false, false, false).withOwnFailure(true);
        assertTrue(SealedTransport.sealedInfo(c).ownFailure());
        assertTrue(c.withKind(null).withLabel("x").withVariant("v").withMaxRecord(0).withWriteOverhead(1).ownFailure());
        assertFalse(new CarrierInfo(Framing.BYTES, false, false, false, false).ownFailure());
    }
}
