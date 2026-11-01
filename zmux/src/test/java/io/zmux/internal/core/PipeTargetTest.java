package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class PipeTargetTest {
    static Session session() {
        return session(CoreConfig.builder());
    }

    static Session session(CoreConfig.Builder b) {
        CoreConfig config = b.maxPipes(2).headroom(8).build();
        Session s = new Session(config, 0); var info = new CarrierInfo(Framing.BYTES, false, false, false, false);
        s.pipes().attachBootstrap(info, info, null); Poke.set(s, "peerSeen", true); Poke.set(s, "state", SessionState.ACTIVE);
        Poke.set(s, "path", s.multiPath()); return s;
    }
    static Pipe append(Session s, boolean local, int out, int in) {
        return add(s, local, out, in, null);
    }
    /** A pipe with an outbound direction (lane 0 only). */
    static Pipe appendOut(Session s, boolean local, int out, int in) {
        return add(s, local, out, in, new Pipe.OutDir(new CarrierInfo(Framing.BYTES, false, false, false, false), null));
    }
    private static Pipe add(Session s, boolean local, int out, int in, Pipe.OutDir dir) {
        Pipe p = new Pipe(s.pipes.all().size() * 2L, null, dir,
                new Attach(local, out == Attach.OUT_NONE ? 1 : 2, 0, false, 0, 0, -1, Double.NaN, out, in, false, 0), null);
        s.pipes.add(p); return p;
    }
    @Test void readinessUsesTheLocalTargetRatherThanFailureHeadroom() {
        Session s = session(); assertTrue(s.joinReady());
        Pipe p = append(s, true, Attach.OUT_ACTIVE, Attach.IN_OPEN);
        assertTrue(s.mp().joins.joinAllowed(), "hard headroom still permits an explicit join");
        assertFalse(s.joinReady(), "maintenance target is already full");
        Poke.set(p.mp().health, "out", Attach.OUT_DRAINING); assertFalse(s.joinReady());
        Poke.set(p.mp().health, "out", Attach.OUT_FAILED); assertTrue(s.joinReady());
        Poke.set(s.mp().joins, "backoffUntil", 10); assertFalse(s.joinReady()); Poke.set(s.mp().joins, "backoffUntil", 0);
        for (int i = 2; i < s.mp().joins.ownCap; i++) append(s, true, Attach.OUT_FAILED, Attach.IN_ENDED);
        assertFalse(s.joinReady(), "failed state cannot exceed physical headroom");
    }
    @Test void peerJoinsDoNotConsumeOurTargetAndEndedReceiveOnlyPipesDoNotBlockRecovery() {
        Session s = session(); append(s, false, Attach.OUT_ACTIVE, Attach.IN_OPEN); assertTrue(s.joinReady());
        Pipe p = append(s, true, Attach.OUT_NONE, Attach.IN_OPEN); assertFalse(s.joinReady());
        Poke.set(p.mp().health, "in", Attach.IN_ENDED); assertTrue(s.joinReady());
        Poke.set(s, "state", SessionState.CLOSING); assertFalse(s.joinReady());
    }
    @Test void suspendedRecoveryCanReclaimPhysicalHeadroomWithoutQuerySideEffects() {
        Session s = session(); Poke.set(s.pipes.all().getFirst().mp().health, "out", Attach.OUT_FAILED); Poke.set(s.pipes.all().getFirst().mp().health, "in", Attach.IN_ENDED);
        for (int i = 1; i < s.mp().joins.ownCap; i++) append(s, true, Attach.OUT_FAILED, Attach.IN_ENDED);
        assertFalse(s.joinReady());
        Poke.set(s, "state", SessionState.SUSPENDED);
        assertTrue(s.joinReady(), "AddPipe can retire the oldest failed attachment during resume");
        assertEquals(s.mp().joins.ownCap, s.mp().joins.ownAttachments(), "query must not retire anything");
        Poke.set(s.mp().joins, "backoffUntil", 10); assertFalse(s.joinReady());
    }
}
