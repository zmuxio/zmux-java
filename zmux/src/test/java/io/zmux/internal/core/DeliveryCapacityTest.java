package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

/** The member view and cache feedback use the same interval, on a virtual link and clock. */
class DeliveryCapacityTest {
    @Test void creditedBacklogSuppliesCapacityOnlyWithoutLocalPacing() throws Exception {
        for (boolean queueSignal : new boolean[] {true, false}) {
            var path = new ProbeStormTest.Path(queueSignal, true);
            path.advance(100 * Time.MILLISECOND);
            var session = path.client.s;
            var pipe = session.pipes().get(0);
            var feedback = new ArrayList<PathEvidence.Delivery>();
            session.performanceFeedback((member, end, until, rtt, rate) -> {
                if (rate == 0) return;
                var sample = pipe.out().snd.quality(pipe.mp().projection.snapshot().send(), end).delivery().orElseThrow();
                assertEquals(pipe.member().id(), member);
                assertEquals(PathEvidence.Load.BACKLOGGED, sample.load());
                assertEquals((long) sample.bytesPerSecond(), rate);
                assertEquals(sample.validUntil(), until);
                assertEquals(end, sample.end());
                feedback.add(sample);
            });
            var stream = session.streams().openStream(true);
            var bytes = ByteBuffer.allocate(3 << 20);
            assertTrue(session.sending().write(stream, bytes, false) > 256 << 10);
            boolean measured = false;
            for (int i = 0; i < 60; i++) {
                path.advance(10 * Time.MILLISECOND);
                var quality = pipe.out().snd.quality(pipe.mp().projection.snapshot().send(), path.now);
                measured |= quality.delivery().isPresent();
                assertEquals(quality, pipe.out().snd.quality(pipe.mp().projection.snapshot().send(), path.now));
            }
            assertTrue(measured);
            assertEquals(queueSignal, !feedback.isEmpty());
        }
    }

    @Test void creditedBacklogCanProveOneOfTwoConfirmedPaths() throws Exception {
        var path = new ProbeStormTest.Path(true, true);
        path.advance(100 * Time.MILLISECOND);
        var session = path.client.s;
        var second = PipeTargetTest.appendOut(session, false, Attach.OUT_ACTIVE, Attach.IN_ENDED);
        assertTrue(session.pipePeerAttached(second.id));
        session.setPipe(second.id, Tiers.PipeSettings.builder().sendStandby(true).build());
        var pipe = session.pipes().get(0);
        assertTrue(session.pipePeerAttached(pipe.id));

        var stream = session.streams().openStream(true);
        assertTrue(session.sending().write(stream, ByteBuffer.allocate(3 << 20), false) > 256 << 10);
        var observed = new ArrayList<Long>();
        session.performanceFeedback((member, end, until, rtt, rate) -> {
            if (rate != 0) observed.add(end);
        });
        path.advance(600 * Time.MILLISECOND);
        assertFalse(observed.isEmpty());
        assertEquals(2, session.pipes().all().size());
    }

    @Test void aCustomStreamInTheAckIntervalPreventsBacklogProofFromAnotherStream() throws Exception {
        var path = new ProbeStormTest.Path(true, true);
        path.advance(100 * Time.MILLISECOND);
        var session = path.client.s;
        var pipe = session.pipes().get(0);
        var witness = session.streams().openStream(true);
        session.sending().write(witness, ByteBuffer.allocate(3 << 20), false);
        path.advance(90 * Time.MILLISECOND);
        assertNotNull(pipe.out().snd.deliveryBacklog());
        assertEquals(witness.id(), pipe.out().snd.deliveryBacklog().stream());

        var other = session.streams().openStream(true);
        session.setStreamPolicy(other, CoreConfig.Policy.CUSTOM);
        session.sending().write(other, ByteBuffer.allocate(16 << 10), false);
        for (int i = 0; i < 100 && !session.wantsPick(); i++) path.advance(Time.MILLISECOND);
        var request = new PathScheduler.PickRequest();
        assertTrue(session.takePick(request));
        var answers = request.frames.stream().map(frame -> new PathScheduler.PickAnswer(0, frame.len(), false)).toList();
        session.answerPick(request, answers, false);

        for (int i = 0; i < 100 && other.snd.sent() == 0; i++) {
            path.advance(Time.MILLISECOND);
        }
        assertTrue(other.snd.sent() > 0);
        long customTo = pipe.out().snd.so();
        long observed = 0;
        boolean covered = false;
        for (int i = 0; i < 200; i++) {
            path.advance(Time.MILLISECOND);
            var delivery = pipe.out().snd.quality(pipe.mp().projection.snapshot().send(), path.now).delivery().orElse(null);
            if (delivery == null || delivery.end() == observed) continue;
            observed = delivery.end();
            long upper = pipe.out().snd.delivered();
            if (upper >= customTo && upper - delivery.bytes() < customTo) {
                assertNotEquals(PathEvidence.Load.BACKLOGGED, delivery.load());
                covered = true;
            }
        }
        assertTrue(covered, "an ACK sampled the Custom stream together with earlier bytes");
    }

    @Test void withdrawalInvalidatesAnOlderSampleWithinTheSameHealthGeneration() {
        var evidence = new QualityEvidence();
        var empty = java.util.Optional.<PathEvidence.Timing>empty();
        var original = new PathEvidence.Timing(10, 50, java.time.Duration.ofMillis(1));
        var newer = new PathEvidence.Timing(20, 60, java.time.Duration.ofMillis(2));
        evidence.observe(new PathEvidence(3, java.util.Optional.of(original), empty, java.util.Optional.empty()), null, 10);
        var reference = evidence.latest().getFirst();
        evidence.observe(new PathEvidence(3, java.util.Optional.of(newer), empty, java.util.Optional.empty()), null, 20);
        assertTrue(evidence.contains(reference, 20));
        assertFalse(evidence.contains(reference, reference.validUntil()));
        var forged = new PathEvidence.Sample(reference.field(), reference.identity(),
                reference.sampledAt(), reference.validUntil() + 1, reference.connection(), reference.counterGeneration());
        assertFalse(evidence.contains(forged, 20));
        evidence.observe(new PathEvidence(3, empty, empty, java.util.Optional.empty()), null, 21);
        evidence.observe(new PathEvidence(3, java.util.Optional.of(newer), empty, java.util.Optional.empty()), null, 22);
        assertFalse(evidence.contains(reference, 22));
        assertEquals(1, evidence.latest().size());
    }
}
