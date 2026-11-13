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
        Poke.set(s, "path", s.multiPath()); Poke.set(s.mp().joins, "keysPending", false); return s;
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
                new Attach(local, out == Attach.OUT_NONE ? 1 : 2, 0, 0, 0, -1, Double.NaN, out, in, false, 0), null);
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
    private static Pipe failed(Session s, long attachedAt, boolean acked) {
        Pipe p = append(s, true, Attach.OUT_FAILED, Attach.IN_ENDED);
        Poke.set(p.mp().joining, "attachedAt", attachedAt); Poke.set(p.mp().health, "abandonAcked", acked); return p;
    }
    /** A local attachment with the given directions. */
    private static Pipe directed(Session s, boolean in, int inState, boolean out, int outState) {
        Pipe p = new Pipe(s.pipes.all().size() * 2L, in ? new Pipe.InDir(new Pipe.InLane(0, false, false)) : null,
                out ? new Pipe.OutDir(new CarrierInfo(Framing.BYTES, false, false, false, false), null) : null,
                new Attach(true, in ? (out ? 2 : 1) : 0, 0, 0, 0, -1, Double.NaN, outState, inState, false, 0), null);
        s.pipes.add(p); return p;
    }
    private static Pipe held(Session s, boolean in, boolean out) {
        Pipe p = directed(s, in, in ? Attach.IN_JOINING : Attach.IN_NONE, out, out ? Attach.OUT_JOINING : Attach.OUT_NONE);
        Poke.set(p.mp().joining, "held", true); return p;
    }
    @Test void capDisplacesAcknowledgedFailuresThenPendingOnesThenTheStalestLiveAttachment() {
        Session s = session(); Pipe boot = s.pipes.all().getFirst();
        Poke.set(boot.mp().health, "out", Attach.OUT_FAILED); Poke.set(boot.mp().health, "in", Attach.IN_ENDED);
        Poke.set(boot.mp().health, "abandonAcked", false); Poke.set(boot.mp().joining, "attachedAt", 1L);
        Pipe acked = failed(s, 5, true), older = failed(s, 3, true), pending = failed(s, 2, false);
        Poke.set(s, "state", SessionState.SUSPENDED);
        Joins j = s.mp().joins;
        assertSame(older, j.roomCandidate(true, true, true), "the oldest acknowledged failure goes first, though pending ones are older");
        assertTrue(j.makeRoom(true, true, true)); assertTrue(older.mp().health.retired());
        assertSame(acked, j.roomCandidate(true, false, false), "failed attachments are retired whatever the join provides");
        assertTrue(j.makeRoom(true, true, true));
        assertSame(boot, j.roomCandidate(true, true, true), "then the oldest failure whose PIPE_ABANDON is pending");
        assertTrue(j.makeRoom(true, true, true)); assertTrue(j.makeRoom(true, true, true)); assertTrue(pending.mp().health.retired());
        Pipe stale = directed(s, true, Attach.IN_OPEN, false, Attach.OUT_NONE), fresh = directed(s, true, Attach.IN_OPEN, false, Attach.OUT_NONE);
        Poke.set(stale.mp().link, "lastRecord", 7L); Poke.set(fresh.mp().link, "lastRecord", 9L);
        assertNull(j.roomCandidate(true, true, false), "a live attachment goes only for a missing direction");
        assertSame(stale, j.roomCandidate(true, false, true), "the live attachment with the oldest last progress");
        assertNull(j.roomCandidate(false, true, true), "the peer's attachments are counted apart");
    }
    @Test void displacementDropsTheQueuedPipeAbandonButStatelessIdsStillGetOne() {
        Session s = session(); Pipe boot = s.pipes.all().getFirst();
        Poke.set(boot.mp().health, "out", Attach.OUT_FAILED); Poke.set(boot.mp().health, "in", Attach.IN_ENDED);
        Poke.set(boot.mp().joining, "attachedAt", 5L);
        Joins j = s.mp().joins;
        Pipe queued = failed(s, 1, false);
        Poke.set(queued.mp().health, "abandonQueued", true); Poke.set(queued.mp().health, "abandonCode", io.zmux.internal.wire.TransportError.PIPE_FAILED);
        Poke.set(s, "state", SessionState.SUSPENDED);
        assertSame(queued, j.roomCandidate(true, true, true));
        assertTrue(j.makeRoom(true, true, true)); assertTrue(queued.mp().health.retired());
        assertFalse(queued.mp().health.abandonQueued(), "the queued, unsent PIPE_ABANDON is dropped");
        assertFalse(j.owesAbandons(), "nor is it kept owed by ID");
        long peerId = 1; // peer parity, never seen: the reply to its PIPE_ABANDON is still sent
        assertDoesNotThrow(() -> j.abandonUnknown(peerId));
        assertTrue(j.owesAbandons());
    }
    @Test void heldJoinsAreReleasedOneByOneWhenTheyFillAMissingDirection() {
        Session s = session(); Pipe boot = s.pipes.all().getFirst();
        Poke.set(boot.mp().health, "in", Attach.IN_ENDED); // outbound live, inbound missing
        Joins j = s.mp().joins;
        for (int i = 1; i < j.ownCap; i++) failed(s, i, i % 2 == 0);
        Pipe first = held(s, false, true), second = held(s, true, false);
        assertFalse(j.joinAllowedFor(second), "not suspended: the cap holds every join");
        Poke.set(s, "state", SessionState.SUSPENDED);
        assertFalse(j.joinAllowedFor(first), "a join for a direction already live keeps waiting");
        assertTrue(j.joinAllowedFor(second), "a later join for the missing direction goes before it");
        assertFalse(second.mp().joining.held()); assertTrue(first.mp().joining.held());
        assertEquals(j.ownCap, s.pipes.all().stream().filter(p -> p != first && !p.mp().health.retired()).count(), "one failure was displaced");
        assertFalse(j.joinAllowedFor(first));
    }
    @Test void aHeldJoinBeforeTheKeysDisplacesNothing() throws Exception {
        Session s = session(); Pipe boot = s.pipes.all().getFirst();
        Poke.set(boot.mp().health, "in", Attach.IN_ENDED);
        Joins j = s.mp().joins;
        for (int i = 1; i < j.ownCap; i++) failed(s, i, true);
        Pipe join = held(s, true, false);
        Poke.set(join.mp(), "handshaking", true);
        Poke.set(s, "state", SessionState.SUSPENDED);
        Poke.set(j, "keysPending", true);
        assertNull(j.joinPrepare(join, 0), "held until the keys arrive");
        assertEquals(j.ownCap, j.ownAttachments() - 1, "no attachment was retired for a join that cannot go");
        assertTrue(join.mp().joining.held());
        Poke.set(j, "keysPending", false);
        assertEquals("hello", j.joinPrepare(join, 0));
        assertFalse(join.mp().joining.held());
    }
    @Test void readinessNeedsADisplaceableAttachmentOnlyAtTheCap() {
        Session s = session(); Joins j = s.mp().joins;
        for (int i = 1; i < j.ownCap - 1; i++) failed(s, i, false);
        assertTrue(s.joinReady(), "below the cap no displacement is needed, suspended or not");
        failed(s, j.ownCap, true);
        assertFalse(s.joinReady(), "at the cap while active");
        Poke.set(s, "state", SessionState.SUSPENDED);
        assertFalse(s.joinReady(), "every direction is live: no join may displace");
        Poke.set(s.pipes.all().getFirst().mp().health, "in", Attach.IN_ENDED);
        assertTrue(s.joinReady(), "a join for the missing inbound direction may displace a failure");
        assertEquals(j.ownCap, j.ownAttachments(), "query must not retire anything");
    }
}
