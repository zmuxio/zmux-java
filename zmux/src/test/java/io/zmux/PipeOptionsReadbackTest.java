package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A pipe's options read back as it holds them: those it was added with, every update applied. */
@Timeout(30)
class PipeOptionsReadbackTest {
    private static final Duration WAIT = Duration.ofSeconds(5);

    /** The runtime settings and the given fixed ones read as given; what was left to a policy or default reads as actually in use. */
    private static void assertHeld(PipeOptions given, PipeOptions held) {
        assertEquals(given.settings, held.settings);
        assertEquals(given.direction, held.direction);
        assertEquals(given.fateGroup, held.fateGroup);
        assertEquals(given.returnOnReply, held.returnOnReply);
        given.recordMode().ifPresent(m -> assertEquals(m, held.recordMode().orElseThrow()));
        assertTrue(held.recordMode().isPresent(), "the mode in use, also where the record policy chose it");
        assertTrue(held.recordMode().get() != RecordMode.UNMANAGED);
        given.keyGroup().ifPresent(g -> assertEquals(g, held.keyGroup().orElseThrow()));
        assertTrue(held.keyGroup().isPresent(), "the key group the join used, also where the default chose it");
    }

    @Test void theOptionsAddedWithReadBackOnEveryHandle() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 4))) {
            var given = PipeOptions.builder().weight(3).sendPriority(2).receiveStandby(true).fateGroup(9).noProbation()
                    .liveness(PipeLiveness.builder().deadTimeout(Duration.ofSeconds(7)).build())
                    .qualityBand(QualityBand.builder().rtt(Duration.ofMillis(40)).build()).clearSendStandby().build();
            ZmuxPipe p = PeerAttachedTest.lateJoin(f, 0, given);
            p.awaitAttached(WAIT);
            assertHeld(given, p.options());
            assertEquals(OptionalLong.of(3), p.options().weight());
            assertTrue(p.options().sendStandby().isEmpty(), "cleared reads as unset");
            assertTrue(p.options().probationOff());
            var other = f.client.pipes().stream().filter(q -> q.id().value() == p.id().value()).findFirst().orElseThrow();
            assertEquals(p.options(), other.options(), "another handle of the pipe holds the same");
            var first = f.client.pipes().stream().filter(q -> q.id().value() == 0).findFirst().orElseThrow().options();
            assertEquals(PipeOptions.defaults().settings, first.settings);
            assertEquals(PipeOptions.Direction.BIDIRECTIONAL, first.direction());
            assertEquals(Optional.of(RecordMode.UNMANAGED), first.recordMode(), "a first pipe without the secure carrier");
        }
    }

    @Test void anUpdateReadsBackExactlyWhatItSetOrCleared() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 4))) {
            ZmuxPipe p = PeerAttachedTest.lateJoin(f, 0, PipeOptions.builder().weight(3).sendPriority(2).build());
            p.awaitAttached(WAIT);
            var liveness = PipeLiveness.builder().deadTimeout(Duration.ofSeconds(9)).build();
            p.update(PipeUpdate.builder().clearWeight().sendStandby(true).liveness(liveness).noQualityBand().probation(Duration.ofSeconds(2)).build());
            var expected = PipeOptions.builder().sendPriority(2).sendStandby(true).liveness(liveness).noQualityBand().probation(Duration.ofSeconds(2)).build();
            assertHeld(expected, p.options());
            assertTrue(p.options().weight().isEmpty(), "a cleared weight is the default again");
            p.update(PipeUpdate.builder().clearLiveness().clearQualityBand().build());
            assertHeld(PipeOptions.builder().sendPriority(2).sendStandby(true).probation(Duration.ofSeconds(2)).build(), p.options());
            // A pipe the peer joined holds the defaults and what its join fixed, with the updates since.
            var joined = f.server.pipes().stream().filter(q -> q.id().value() == p.id().value()).findFirst().orElseThrow();
            assertHeld(PipeOptions.defaults(), joined.options());
            joined.update(PipeUpdate.builder().weight(5).build());
            assertHeld(PipeOptions.builder().weight(5).build(), joined.options());
        }
    }

    @Test void theLastOptionsAreAnsweredAfterThePipeEnded() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 4))) {
            var given = PipeOptions.builder().weight(4).build();
            ZmuxPipe p = PeerAttachedTest.lateJoin(f, 0, given);
            p.awaitAttached(WAIT);
            p.options();
            p.close();
            p.awaitClosed(WAIT);
            assertHeld(given, p.options());
        }
    }

    /** The options read after the end are the last held, updates included, on the handle that updated and on one made before the end. */
    @Test void theLastUpdatedOptionsAreAnsweredAfterTheEndAndNothingIsHeldAgain() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 4))) {
            ZmuxPipe p = PeerAttachedTest.lateJoin(f, 0, PipeOptions.builder().weight(4).build());
            p.awaitAttached(WAIT);
            var other = f.client.pipes().stream().filter(q -> q.id().value() == p.id().value()).findFirst().orElseThrow();
            p.update(PipeUpdate.builder().weight(6).sendPriority(1).build());
            var expected = PipeOptions.builder().weight(6).sendPriority(1).build();
            // Never read before the end: the updated handle is the only one that touched the options.
            p.close();
            p.awaitClosed(WAIT);
            assertHeld(expected, p.options());
            assertHeld(expected, other.options());
            long id = p.id().value();
            assertFalse(f.client.heldOptions.containsKey(id), "the entry goes with the pipe");
            assertThrows(PipeGoneException.class, () -> p.update(PipeUpdate.builder().weight(9).build()));
            p.hold(PipeOptions.builder().weight(9).build());
            assertFalse(f.client.heldOptions.containsKey(id), "an ended pipe is not held again");
            assertEquals(OptionalLong.of(6), p.options().weight(), "the update that failed changed nothing");
        }
    }

    @Test void whatTheJoinFixedReadsAsActualOnBothEnds() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 4))) {
            var given = PipeOptions.builder().direction(PipeOptions.Direction.SEND_ONLY).fateGroup(9).returnOnReply(true).build();
            ZmuxPipe p = PeerAttachedTest.lateJoin(f, 0, given);
            var joiner = p.options();
            assertEquals(PipeOptions.Direction.SEND_ONLY, joiner.direction());
            assertEquals(9, joiner.fateGroup());
            assertTrue(joiner.returnOnReply(), "read as it was given");
            p.awaitAttached(WAIT); // returnOnReply returns before the acceptor has the pipe
            var peer = f.server.pipes().stream().filter(q -> q.id().value() == p.id().value()).findFirst().orElseThrow().options();
            assertEquals(PipeOptions.Direction.RECEIVE_ONLY, peer.direction(), "from the acceptor's view");
            assertEquals(9, peer.fateGroup());
            assertEquals(joiner.recordMode(), peer.recordMode(), "the mode the handshake used");
            assertEquals(joiner.keyGroup(), peer.keyGroup(), "the Group the key exchange used");
            assertTrue(peer.keyGroup().isPresent());
            assertFalse(peer.returnOnReply(), "the joiner's own field: the default");
            assertEquals(PipeOptions.defaults().settings, peer.settings);
        }
    }

    @Test void optionsReadFromAPipeAddAnEquivalentPipe() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 4))) {
            ZmuxPipe p = PeerAttachedTest.lateJoin(f, 0, PipeOptions.builder().weight(2).build());
            p.awaitAttached(WAIT);
            var read = p.options();
            ZmuxPipe q = PeerAttachedTest.lateJoin(f, 0, read);
            q.awaitAttached(WAIT);
            assertEquals(read.recordMode(), q.options().recordMode());
            assertEquals(read.settings, q.options().settings);
            // pipe 0 reads Unmanaged, which adds as the policy's choice
            var zero = f.client.pipes().stream().filter(x -> x.id().value() == 0).findFirst().orElseThrow().options();
            PeerAttachedTest.lateJoin(f, 0, zero).awaitAttached(WAIT);
        }
    }
}
