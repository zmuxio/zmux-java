package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.CapacityLimits;
import io.zmux.DirectionHealth.ProofKind;
import io.zmux.Directions;
import io.zmux.carrier.HandshakeShaper.UnitPlan;
import io.zmux.internal.join.Framing;
import io.zmux.internal.join.Group;
import io.zmux.internal.join.Hello;
import io.zmux.internal.join.HelloPayload;
import io.zmux.internal.join.JoinPipe;
import io.zmux.internal.join.Joiner;
import io.zmux.internal.join.Mode;
import io.zmux.internal.join.Suite;
import io.zmux.testutil.Json;
import io.zmux.testutil.SpecFixtures;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NextBatchHTest {
    private static TraceRunner replay(String trace, int through) throws Exception {
        var root = SpecFixtures.root().resolve("fixtures");
        var profiles = Json.object(Files.readString(root.resolve("profiles.json")));
        var lines = SpecFixtures.ndjson("traces/" + trace);
        var runner = new TraceRunner(lines.getFirst(), profiles);
        for (int i = 1; i <= through; i++) runner.step(lines.get(i));
        return runner;
    }

    /** The shared trace through the answer, with the answer still unacknowledged. */
    private static TraceRunner answerer() throws Exception {
        var runner = replay("multipath/epoch/answerer_switches_on_peer_join.ndjson", 12);
        var limits = runner.s.sessionPolicy().limits();
        runner.s.setSessionPolicy(new CoreSessionPolicy(1, 0,
                new CapacityLimits(limits.steadyAttachments(), limits.joinHeadroom(), limits.localCarriers(), 1)));
        assertEquals(0, runner.s.epoch());
        return runner;
    }

    private static Joins.PeerJoin authenticated(TraceRunner runner, int epoch, long id) throws Exception {
        return prepare(runner, epoch, id).join();
    }

    private record Peer(Joiner joiner, Joins.PeerJoin join, byte[] hello) {}

    private static Peer prepare(TraceRunner runner, int epoch, long id) throws Exception {
        var locator = epoch == 0 ? runner.s.locator() : TraceFrames.hex("4c4f43414c2d31");
        var joiner = Joiner.create(Suite.AES, Mode.TRUSTED, Framing.STREAM, locator,
                runner.joinPsk(true, epoch), new byte[0], Group.X25519_MLKEM768, epoch & 1);
        var wire = joiner.writeHello(new HelloPayload(new JoinPipe(id, 0, 0, 0, false), null, 0));
        var opened = runner.s.openHello(Hello.parseExact(wire, 0, wire.length, Framing.STREAM), Framing.STREAM, new byte[0]);
        assertEquals(Joins.JoinResult.ACCEPTED, opened.result());
        return new Peer(joiner, opened.join(), wire);
    }

    private static Joins.Claimed claim(Session session, Joins.PeerJoin join) {
        return claim(session, join, Time.NEVER);
    }

    private static Joins.Claimed claim(Session session, Joins.PeerJoin join, long deadline) {
        var info = new CarrierInfo(io.zmux.internal.core.Framing.BYTES, false, true, false, false);
        var carrier = new Joins.JoinCarrier(true, true, info, info, false);
        return session.claimHello(join, carrier, UnitPlan.NONE, 0, 1000, deadline);
    }

    @Test void rejectedAuthorizationDoesNotAdvanceEpochOrClaimPipeId() throws Exception {
        var runner = answerer();
        authenticated(runner, 1, 4); // The authorization denies this request, so no claim follows.
        assertEquals(0, runner.s.epoch());
        assertNull(runner.s.pipes().get(4));
        assertEquals(Joins.JoinResult.ACCEPTED, claim(runner.s, authenticated(runner, 1, 4)).result());
        assertEquals(1, runner.s.epoch());
    }

    @Test void generationChangeBeforeClaimHasNoProtocolEffects() throws Exception {
        var runner = answerer();
        var old = authenticated(runner, 1, 4);
        switchGeneration(runner.s);
        var rejected = claim(runner.s, old);
        assertEquals(Joins.JoinResult.CLOSE, rejected.result());
        assertEquals("authorization_changed", rejected.reason());
        assertEquals(0, runner.s.epoch());

        var fresh = authenticated(runner, 1, 4);
        assertEquals(1, fresh.authGeneration());
        assertEquals(Joins.JoinResult.ACCEPTED, claim(runner.s, fresh).result());
        assertEquals(1, runner.s.epoch());
    }

    @Test void generationChangeAfterReplyPreservesClaimAndRejectsConfirmation() throws Exception {
        var runner = answerer();
        var pending = prepare(runner, 1, 4);
        var reply = claim(runner.s, pending.join());
        assertEquals(Joins.JoinResult.ACCEPTED, reply.result());
        assertEquals(1, runner.s.epoch());

        switchGeneration(runner.s);
        readConfirmation(pending, reply);
        var rejected = runner.s.confirmJoin(pending.join());
        assertEquals(Joins.JoinResult.CLOSE, rejected.result());
        assertEquals("authorization_changed", rejected.reason());
        assertNull(runner.s.pipes().get(4));
        assertEquals(1, runner.s.epoch());

        byte[] hello = pending.hello();
        var replay = runner.s.openHello(Hello.parseExact(hello, 0, hello.length, Framing.STREAM), Framing.STREAM, new byte[0]);
        assertEquals(Joins.JoinResult.CLOSE, replay.result());

        var fresh = prepare(runner, 1, 6);
        var freshReply = claim(runner.s, fresh.join());
        assertEquals(Joins.JoinResult.ACCEPTED, freshReply.result());
        assertNull(freshReply.evicted());
        readConfirmation(fresh, freshReply);
        assertEquals(Joins.JoinResult.ACCEPTED, runner.s.confirmJoin(fresh.join()).result());
        assertNotNull(runner.s.pipes().get(6));
    }

    @Test void confirmationAtTheDeadlineReleasesThePendingSlot() throws Exception {
        var runner = answerer();
        var pending = prepare(runner, 1, 4);
        long deadline = runner.s.now() + Time.SECOND;
        var reply = claim(runner.s, pending.join(), deadline);
        assertEquals(Joins.JoinResult.ACCEPTED, reply.result());
        readConfirmation(pending, reply);

        runner.s.setNow(deadline);
        assertEquals("expired", runner.s.confirmJoin(pending.join()).reason());
        assertNull(runner.s.pipes().get(4));
        var fresh = claim(runner.s, authenticated(runner, 1, 6));
        assertEquals(Joins.JoinResult.ACCEPTED, fresh.result());
        assertNull(fresh.evicted());
    }

    @Test void reservesAuthorizationAndCarrierCapacitySeparately() throws Exception {
        var session = answerer().s;
        var limits = CapacityLimits.defaults();
        assertEquals(1, limits.steadyAttachments());
        assertEquals(8, limits.joinHeadroom());
        assertEquals(18, limits.localCarriers());
        assertEquals(4, limits.pendingPeerJoins());
        session.setSessionPolicy(new CoreSessionPolicy(2, 0, limits));

        for (int i = 0; i < limits.localCarriers(); i++) assertTrue(session.acquireCarriers(1));
        assertFalse(session.acquireCarriers(1));
        for (int i = 0; i < limits.pendingPeerJoins(); i++) assertTrue(session.acquireJoinAuthorization());
        assertFalse(session.acquireJoinAuthorization());
        session.releaseJoinAuthorization();
        assertTrue(session.acquireJoinAuthorization());
        assertFalse(session.acquireCarriers(1));
        session.releaseCarriers(1);
        assertTrue(session.acquireCarriers(1));
    }

    @Test void attachmentCapacityCanReturnToTheInitialEnvelope() throws Exception {
        var session = answerer().s;
        var initial = session.sessionPolicy().limits();
        var reduced = new CoreSessionPolicy(2, 0, new CapacityLimits(1, 0));
        session.setSessionPolicy(reduced);
        session.setSessionPolicy(reduced);
        assertEquals(reduced, session.sessionPolicy());
        assertThrows(IllegalArgumentException.class, () -> session.setSessionPolicy(
                new CoreSessionPolicy(3, 0, new CapacityLimits(initial.attachmentLimit() + 1, 0))));
        assertEquals(reduced, session.sessionPolicy());
        var restored = new CoreSessionPolicy(3, 0, initial);
        session.setSessionPolicy(restored);
        assertEquals(restored, session.sessionPolicy());
    }

    private static void switchGeneration(Session session) throws CoreException {
        CoreSessionPolicy policy = session.sessionPolicy();
        session.setSessionPolicy(new CoreSessionPolicy(policy.revision() + 1, policy.authGeneration() + 1,
                policy.limits()));
    }

    private static void readConfirmation(Peer pending, Joins.Claimed reply) throws Exception {
        pending.joiner().readReply(reply.reply(), 0, reply.reply().length);
        byte[] confirmation = pending.joiner().writeConfirm(0);
        assertEquals(confirmation.length, pending.join().readConfirm(confirmation, 0, confirmation.length));
    }

    @Test void oldEpochCanExpireWhileAuthorizationIsPending() throws Exception {
        var runner = answerer();
        var pending = authenticated(runner, 0, 4);
        assertEquals(Joins.JoinResult.ACCEPTED, claim(runner.s, authenticated(runner, 1, 6)).result());
        runner.s.setNow(runner.s.now() + CoreConfig.EPOCH_RETENTION);
        assertEquals(Joins.JoinResult.CLOSE, claim(runner.s, pending).result());
        assertNull(runner.s.pipes().get(4));
        assertEquals(Joins.JoinResult.ACCEPTED, claim(runner.s, authenticated(runner, 1, 4)).result());
    }

    @Test void inboundControlRecordSettlesOnlyAfterTheHealthyInterval() throws Exception {
        var runner = replay("ext/datagram/ack_cross_path.ndjson", 7);
        var session = runner.s;
        var initial = session.pipeHealth(2);
        assertNotNull(initial);
        assertTrue(initial.peerAttached());
        assertEquals(io.zmux.DirectionHealth.State.ABSENT, initial.send().state());
        assertFalse(initial.receive().settled());
        assertEquals(ProofKind.VERIFIED_RECORD, initial.receive().proofKind());
        assertEquals(1, initial.receive().evidenceGeneration());
        assertEquals(initial.attachedAt(), initial.receive().stableSince());
        long settle = initial.receive().stableSince().orElseThrow() + 5 * Time.SECOND;
        session.handleTimeout(settle - 1);
        assertFalse(session.pipeHealth(2).settled());
        session.handleTimeout(settle);
        var stable = session.pipeHealth(2);
        assertTrue(stable.receive().settled());
        assertEquals(initial.receive().stableSince(), stable.receive().stableSince());
        assertTrue(stable.seq() > initial.seq());
    }

    @Test void joinReplyAndAttachmentHaveDistinctInstants() throws Exception {
        String trace = "multipath/join/local_up.ndjson";
        var runner = replay(trace, 7);
        var session = runner.s;
        var pending = session.pipeHealth(1);
        assertTrue(pending.joinedAt().isEmpty());
        assertTrue(pending.attachedAt().isEmpty());
        assertTrue(pending.send().stableSince().isEmpty());

        runner.advance(Time.MILLISECOND);
        runner.step(SpecFixtures.ndjson("traces/" + trace).get(8));
        long replied = runner.now;
        var verified = session.pipeHealth(1);
        assertEquals(replied, verified.joinedAt().orElseThrow());
        assertTrue(verified.attachedAt().isEmpty());
        assertEquals(ProofKind.NONE, verified.send().proofKind());

        runner.advance(Time.MILLISECOND);
        runner.flush();
        acknowledge(runner, 0, 1, runner.pipes.get(1L).lane(0).written);
        var attached = session.pipeHealth(1);
        assertEquals(replied, attached.joinedAt().orElseThrow());
        assertEquals(runner.now, attached.attachedAt().orElseThrow());
        assertEquals(attached.attachedAt(), attached.send().stableSince());
        assertEquals(1, attached.send().evidenceGeneration());
        assertEquals(ProofKind.NONE, attached.send().proofKind());
        assertFalse(attached.settled());
    }

    @Test void probeEvidenceUsesTheReturnPathAndTheCurrentDirectionGeneration() throws Exception {
        var runner = replay("ext/datagram/ack_cross_path.ndjson", 7);
        var session = runner.s;
        var initial = session.pipeHealth(0);
        session.setPipeLiveness(0, 60 * Time.SECOND, 60 * Time.SECOND, -1, Double.NaN);
        session.observeStability(0);
        runner.flush();
        long probe = runner.pipes.get(0L).lane(0).written;
        runner.advance(Time.MILLISECOND);
        acknowledge(runner, 2, 0, probe);
        var alternate = session.pipeHealth(0);
        assertEquals(ProofKind.PROBE, alternate.send().proofKind());
        assertEquals(ProofKind.NONE, alternate.receive().proofKind());
        assertEquals(initial.send().stableSince(), alternate.send().stableSince());
        assertEquals(runner.now, alternate.send().since());

        acknowledge(runner, 0, 0, probe);
        var paired = session.pipeHealth(0);
        assertEquals(ProofKind.PROBE, paired.receive().proofKind());
        assertEquals(initial.receive().stableSince(), paired.receive().stableSince());
        assertTrue(paired.seq() > alternate.seq());

        runner.advance(Time.MILLISECOND);
        session.revalidatePipes(0, false);
        var invalid = session.pipeHealth(0);
        assertEquals(ProofKind.NONE, invalid.send().proofKind());
        assertEquals(ProofKind.NONE, invalid.receive().proofKind());
        assertTrue(invalid.send().stableSince().isEmpty());
        acknowledge(runner, 0, 0, probe);
        assertEquals(ProofKind.NONE, session.pipeHealth(0).send().proofKind());

        runner.flush();
        long fresh = runner.pipes.get(0L).lane(0).written;
        assertTrue(fresh > probe);
        acknowledge(runner, 0, 0, fresh);
        var recovered = session.pipeHealth(0);
        assertEquals(ProofKind.PROBE, recovered.send().proofKind());
        assertEquals(ProofKind.NONE, recovered.receive().proofKind());
        assertEquals(paired.send().evidenceGeneration() + 1, recovered.send().evidenceGeneration());
        assertEquals(paired.receive().evidenceGeneration() + 1, recovered.receive().evidenceGeneration());
        assertEquals(runner.now, recovered.send().stableSince().orElseThrow());
        assertEquals(initial.attachedAt(), recovered.attachedAt());
    }

    @Test void aReceiveGenerationCannotReuseAnEarlierProbeFromTheSamePipe() throws Exception {
        var runner = replay("ext/datagram/ack_cross_path.ndjson", 7);
        var session = runner.s;
        var before = session.pipeHealth(0);
        session.pipes().get(0).member().bind("primary");
        session.observeStability(0);
        runner.flush();
        long old = runner.pipes.get(0L).lane(0).written;

        var limits = session.sessionPolicy().limits();
        session.setSessionPolicy(new CoreSessionPolicy(1, 0, limits, Map.of("primary", Directions.SEND)));
        assertTrue(session.pipeHealth(0).receive().stableSince().isEmpty());
        runner.advance(Time.MILLISECOND);
        session.setSessionPolicy(new CoreSessionPolicy(2, 0, limits));

        acknowledge(runner, 0, 0, old);
        var restored = session.pipeHealth(0);
        assertEquals(ProofKind.PROBE, restored.send().proofKind());
        assertEquals(before.send().evidenceGeneration(), restored.send().evidenceGeneration());
        assertEquals(ProofKind.NONE, restored.receive().proofKind());
        assertEquals(before.receive().evidenceGeneration() + 1, restored.receive().evidenceGeneration());
        assertEquals(runner.now, restored.receive().stableSince().orElseThrow());

        session.observeStability(0);
        runner.flush();
        long fresh = runner.pipes.get(0L).lane(0).written;
        assertTrue(fresh > old);
        acknowledge(runner, 0, 0, fresh);
        assertEquals(ProofKind.PROBE, session.pipeHealth(0).receive().proofKind());
    }

    private static void acknowledge(TraceRunner runner, long from, long pipe, long position) {
        runner.step(Map.of("op", "peer_send", "pipe", from, "records", List.of(List.of(
                Map.of("type", "PIPE_ACK", "entries", List.of(Map.of("pipe", pipe, "lane", 0L, "position", position, "delay", 0L)))))));
    }
}
