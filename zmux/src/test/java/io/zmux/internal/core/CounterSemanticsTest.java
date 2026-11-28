package io.zmux.internal.core;

import io.zmux.CapacityLimits;
import io.zmux.internal.buffer.ReadBuf;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.internal.wire.TransportParams;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/** penalties, bytes_opportunistic and pings_answered follow what the core does. */
class CounterSemanticsTest {
    static SessionStats.Snapshot snap(Session s) { return s.stats().snapshot(0, null); }

    @Test void unexpectedInputEofRetainsItsReasonAndCountsAsAMemberFault() throws Exception {
        var session = established();
        PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_OPEN);
        var bootstrap = session.pipes().get(0);
        session.bindBootstrapCandidate("initial", new SessionCapacity.CandidateIdentity(
                "initial", bootstrap.carrier(), bootstrap.variant(), java.util.List.of(io.zmux.Directions.BOTH)));
        session.pipeHealth(0);
        var changes = new java.util.ArrayList<SessionCapacity.MemberHealth>();
        session.memberFeedback(changes::add);
        var eof = new java.io.EOFException("input ended");
        session.pipeEnded(0, 0, false, false, eof, Time.MICROSECOND);
        var health = session.pipeHealth(0);
        assertEquals(io.zmux.DirectionHealth.Reason.UNEXPECTED_EOF, health.receive().reason());
        assertSame(eof, health.failure());
        assertEquals(1, changes.stream().filter(SessionCapacity.MemberHealth::failed).count());
    }

    @Test void arrivalEstimateKeepsTheCarrierQueueNonnegativeWithAWriteInFlight() throws Exception {
        var session = established();
        var stream = session.streams.openStream(true);
        assertEquals(128, session.send.write(stream, ByteBuffer.allocate(128), false));
        var batch = new Batch();
        assertTrue(session.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, session.now()));
        try {
            session.unsentReported(0, 0, Long.MAX_VALUE);
            assertEquals(Long.MAX_VALUE, session.mp().metrics.queueBytes(session.pipes().get(0)));
        } finally {
            session.writeDone(batch, null, session.now());
        }
    }

    @Test void aPenaltyCountsOncePerEpisode() {
        Session s = PipeTargetTest.session();
        Pipe p = PipeTargetTest.appendOut(s, true, Attach.OUT_ACTIVE, Attach.IN_OPEN);
        Poke.set(p.out().snd, "so", 100L);
        s.mp().retention.penalize(p);
        s.mp().retention.penalize(p); // the same episode
        assertEquals(1, snap(s).penalties());
        Poke.set(p.out().snd, "cum", 100L);
        s.mp().retention.penaltyAcked(p); // the episode ends
        s.mp().retention.penalize(p);
        assertEquals(2, snap(s).penalties());
    }

    /** An established multipath session whose peer reads nothing more: what it sends stays unacknowledged. */
    static Session established() throws Exception {
        return established(ignored -> {});
    }

    static Session established(java.util.function.Consumer<CoreConfig.Builder> configure) throws Exception {
        Session[] ends = new Session[2];
        for (int i = 0; i < 2; i++) {
            CoreConfig.Role role = i == 0 ? CoreConfig.Role.INITIATOR : CoreConfig.Role.RESPONDER;
            CoreConfig.Builder c = CoreConfig.builder().role(role).transportRole(role).sessionPolicy(new CoreSessionPolicy(new CapacityLimits(2, 8)));
            c.local().present |= TransportParams.MULTIPATH;
            c.local().secretFlags |= TransportParams.SECRET_PSK;
            c.local().present |= TransportParams.SESSION_CRYPTO;
            c.local().sessionCrypto = io.zmux.internal.join.SessionCrypto.supported();
            c.psk(new byte[32]).declaredEndToEnd(true);
            configure.accept(c);
            ends[i] = new Session(c.build(), 0);
            CarrierInfo info = new CarrierInfo(Framing.BYTES, false, true, false, true).withKind(io.zmux.CarrierKind.STREAMING);
            ends[i].attachBootstrap(info, info);
        }
        Batch batch = new Batch();
        for (int round = 0; round < 20; round++) {
            for (int i = 0; i < 2; i++) {
                Session from = ends[i], to = ends[1 - i];
                while (from.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, 0)) {
                    byte[] bytes = BatchFixture.flat(batch);
                    from.writeDone(batch, null, 0);
                    ByteBuffer bb = ByteBuffer.wrap(bytes);
                    to.receive(0, 0, new ReadBuf(bb, null), bb, 0, bytes.length, 0);
                }
            }
        }
        assertNotNull(ends[0].mp(), "multipath negotiated");
        return ends[0];
    }

    @Test void aPingIsAnsweredOnceItsRecordIsAcknowledgedEachOnce() throws Exception {
        Session s = established();
        Pipe p = s.pipes.get(0);
        Pipe.OutLane lane0 = p.out().lanes.getFirst();
        PathHealth h = s.mp().health;
        long base = p.mp().health.pingsAnswered();
        h.pingRecorded(p, 100);
        h.pingRecorded(p, 200);
        Poke.set(lane0.ack, "acked", 99L); h.laneAcked(p, lane0);
        assertEquals(base, p.mp().health.pingsAnswered(), "short of the end of the record");
        Poke.set(lane0.ack, "acked", 100L); h.laneAcked(p, lane0);
        assertEquals(base + 1, p.mp().health.pingsAnswered());
        h.laneAcked(p, lane0);
        assertEquals(base + 1, p.mp().health.pingsAnswered(), "each once");
        Poke.set(lane0.ack, "acked", 500L); h.laneAcked(p, lane0);
        assertEquals(base + 2, p.mp().health.pingsAnswered(), "two outstanding pings, both answered");
    }

    @Test void theNoEffectBudgetGaugeStaysWithinZeroAndItsCapacity() throws Exception {
        Session s = established();
        KeepAlive ka = s.keepAlive();
        assertEquals(KeepAlive.BUDGET_CAP, ka.budgetTokens());
        for (long i = 0; i <= KeepAlive.BUDGET_CAP; i++) ka.charge(0); // the last charge goes below 0
        assertEquals(0, ka.budgetTokens(), "the charge that closes the session leaves the gauge at 0");
    }

    /** New bytes of a first batch are sent: session credit counts them once, and they go again as reinjected data. */
    @Test void aFirstBatchCountsItsNewBytesOnce() throws Exception {
        Session s = established();
        Stream st = s.streams.openStream(true);
        s.sending().write(st, ByteBuffer.allocate(6000), false);
        long sessionBefore = s.conn.sent(), retainedBefore = s.path().retained();
        byte[] first = s.sending().firstBatch(4000, s.tx::canSendNow, s.path()).frames();
        assertNotNull(first);
        long carried = st.snd.sent();
        assertTrue(carried > 0 && carried < 6000, "carried " + carried);
        assertEquals(sessionBefore + carried, s.conn.sent());
        assertEquals(retainedBefore + carried, s.path().retained());
        s.sending().firstBatch(4000, s.tx::canSendNow, s.path());
        assertEquals(carried, st.snd.sent(), "the same bytes again are not new");
        assertEquals(sessionBefore + carried, s.conn.sent());
        Batch b = new Batch();
        java.util.List<HeldControlTest.Sent> sent = new java.util.ArrayList<>();
        for (int i = 0; i < 8 && s.pollTransmit(0, 0, b, Session.DEFAULT_BATCH, 0); i++) {
            HeldControlTest.decode(BatchFixture.flat(b), sent);
            s.writeDone(b, null, 0);
        }
        assertTrue(sent.stream().anyMatch(f -> f.off() == 0 && f.len() >= carried), "what it carried goes again: " + sent);
    }
}
