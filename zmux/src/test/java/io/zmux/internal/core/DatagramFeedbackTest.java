package io.zmux.internal.core;

import io.zmux.internal.core.Session.DatagramLossReport;
import io.zmux.internal.wire.Frame;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DatagramFeedbackTest {
    private static final DatagramFeedback.Path ORIGINAL = new DatagramFeedback.Path(0, false);
    private static final DatagramFeedback.Path ALTERNATIVE = new DatagramFeedback.Path(2, true);

    @Test void lossThresholdsHoldThroughTheLatchAndResetWithTheSource() {
        var feedback = new DatagramFeedback(2);
        assertTrue(loss(feedback, ORIGINAL, 0, 1, 31, 31));
        assertTrue(loss(feedback, ALTERNATIVE, 0, 1, 100, 2));
        assertFalse(feedback.impairment(ORIGINAL, ALTERNATIVE, 0));

        long entry = Time.MICROSECOND;
        assertTrue(loss(feedback, ORIGINAL, entry, 1, 100, 5));
        assertTrue(feedback.impairment(ORIGINAL, ALTERNATIVE, entry));
        assertFalse(loss(feedback, ORIGINAL, entry, 1, 100, 0));
        assertFalse(loss(feedback, ORIGINAL, 0, 1, 100, 0));
        assertTrue(loss(feedback, ORIGINAL, 2 * entry, 1, 100, 2));
        long boundary = entry + 100 * Time.MILLISECOND;
        assertTrue(feedback.impairment(ORIGINAL, ALTERNATIVE, boundary - 1));
        assertFalse(feedback.impairment(ORIGINAL, ALTERNATIVE, boundary));

        assertTrue(loss(feedback, ORIGINAL, boundary, 1, 100, 5));
        assertTrue(loss(feedback, ALTERNATIVE, boundary, 1, 100, 2));
        assertTrue(feedback.impairment(ORIGINAL, ALTERNATIVE, boundary));
        var pair = feedback.bindPair(ORIGINAL, ALTERNATIVE, false);
        assertTrue(loss(feedback, ALTERNATIVE, entry, 2, 0, 0));
        assertFalse(feedback.current(pair, boundary));
        assertFalse(loss(feedback, ALTERNATIVE, boundary + 1, 1, 100, 0));
        assertTrue(loss(feedback, ALTERNATIVE, boundary + 1, 2, 100, 3));
        assertFalse(feedback.impairment(ORIGINAL, ALTERNATIVE, boundary + 1));
    }

    @Test void timingNeedsDistinctFreshMatchedDispatchesOnBothRepresentations() {
        var feedback = new DatagramFeedback(2);
        long now = Time.MILLISECOND;
        ack(feedback, ORIGINAL, 0, now, 120);
        ack(feedback, ORIGINAL, 1, now, 120);
        for (int sequence = 2; sequence < 5; sequence++) ack(feedback, ALTERNATIVE, sequence, now, 80);
        assertFalse(feedback.impairment(ORIGINAL, ALTERNATIVE, now));
        ack(feedback, ORIGINAL, 5, now, 120);
        assertFalse(feedback.impairment(ORIGINAL, ALTERNATIVE, now), "equality at 3/2 cannot enter");
        ack(feedback, ORIGINAL, 6, now, 400);
        assertTrue(feedback.impairment(ORIGINAL, ALTERNATIVE, now));
        assertTrue(loss(feedback, ORIGINAL, now, 1, 0, 0));
        assertTrue(feedback.impairment(ORIGINAL, ALTERNATIVE, now));
        var pair = feedback.bindPair(ORIGINAL, ALTERNATIVE, false);
        long stale = now + 10 * Time.SECOND;
        assertFalse(feedback.current(pair, stale));
        ack(feedback, ALTERNATIVE, 7, stale, 80);
        assertFalse(feedback.impairment(ORIGINAL, ALTERNATIVE, stale));
    }

    @Test void timingRetentionUsesStrictFiveFourthsAtAndAfterTheBoundary() {
        var feedback = new DatagramFeedback(2);
        long now = Time.MILLISECOND;
        for (int sequence = 0; sequence < 3; sequence++) {
            ack(feedback, ORIGINAL, sequence, now, 19);
            ack(feedback, ALTERNATIVE, sequence + 3, now, 12);
        }
        assertTrue(feedback.impairment(ORIGINAL, ALTERNATIVE, now)); // Estimates 43 and 28 us.
        long boundary = now + 100 * Time.MILLISECOND;
        ack(feedback, ORIGINAL, 6, boundary, 18);
        assertTrue(feedback.impairment(ORIGINAL, ALTERNATIVE, boundary)); // 39 / 28.
        ack(feedback, ORIGINAL, 7, boundary + 1, 18);
        assertFalse(feedback.impairment(ORIGINAL, ALTERNATIVE, boundary + 1)); // 35 / 28.
    }

    @Test void skippedAlternativeCannotResumeAnOldLatch() {
        var feedback = new DatagramFeedback(3);
        var other = new DatagramFeedback.Path(4, false);
        assertTrue(loss(feedback, ORIGINAL, 0, 1, 100, 5));
        assertTrue(loss(feedback, ALTERNATIVE, 0, 1, 100, 0));
        assertTrue(loss(feedback, other, 0, 1, 100, 0));
        assertTrue(feedback.impairment(ORIGINAL, ALTERNATIVE, 0));
        assertTrue(feedback.impairment(ORIGINAL, other, Time.MILLISECOND));

        long now = 101 * Time.MILLISECOND;
        assertTrue(loss(feedback, ORIGINAL, now, 1, 100, 3));
        assertFalse(feedback.impairment(ORIGINAL, ALTERNATIVE, now));
        assertTrue(feedback.impairment(ORIGINAL, other, now));
    }

    @Test void generationZeroAndAReplacedSourceAreDistinctRejections() {
        var feedback = new DatagramFeedback(2);
        var nativePath = new DatagramFeedback.Path(ORIGINAL.pipe(), true);
        assertEquals(DatagramLossReport.IGNORED, feedback.reportLoss(ORIGINAL, 0, 0, 100, 5, 0));
        assertTrue(loss(feedback, ORIGINAL, 0, 1, 100, 5));
        assertTrue(loss(feedback, nativePath, 0, 1, 100, 5));

        feedback.resetOutbound(nativePath);
        long now = Time.MILLISECOND;
        assertEquals(DatagramLossReport.SOURCE_EXPIRED, feedback.reportLoss(nativePath, now, 1, 100, 5, now));
        assertTrue(loss(feedback, ORIGINAL, now, 1, 100, 5));
        assertEquals(DatagramLossReport.ACCEPTED, feedback.reportLoss(nativePath, 0, 2, 100, 5, now));
        assertEquals(DatagramLossReport.IGNORED, feedback.reportLoss(nativePath, now, 1, 100, 5, now));
    }

    @Test void retiringPipesPreservesTheBootstrapSourceGeneration() {
        var session = PipeTargetTest.session(CoreConfig.builder().datagrams(true));
        var feedback = session.datagrams().feedback;
        assertTrue(loss(feedback, ORIGINAL, 0, 1, 100, 5));
        session.pipes().setCarrierConn(0, new Object());
        session.pipes().setCarrierConn(0, new Object());

        int limit = session.sessionPolicy().limits().attachmentLimit();
        for (long id = 2; id <= 8L * (limit + 1); id += 2) {
            var out = new Pipe.OutDir(new CarrierInfo(Framing.BYTES, false, false, false, false));
            var attachment = new Attach(true, 2, 0, 0, 0, -1, Double.NaN, Attach.OUT_ACTIVE, Attach.IN_NONE, false, 0);
            var pipe = new Pipe(id, null, out, attachment, null);
            session.pipes().add(pipe);
            assertTrue(loss(feedback, new DatagramFeedback.Path(id, false), 0, 1, 100, 0));
            assertTrue(loss(feedback, new DatagramFeedback.Path(id, true), 0, 1, 100, 0));
            session.mp().health.retire(pipe);
            assertNull(feedback.snapshot(id, false, 0));
            assertNull(feedback.snapshot(id, true, 0));
        }

        assertEquals(DatagramLossReport.SOURCE_EXPIRED, feedback.reportLoss(ORIGINAL, 1, 1, 100, 5, 1));
        assertEquals(0, feedback.rejectedPaths());
    }

    @Test void fullLedgerRejectsAndCountsNewPathsWithoutEvictingAnExistingGeneration() {
        var feedback = new DatagramFeedback(1);
        assertTrue(loss(feedback, ORIGINAL, 0, 1, 100, 5));
        feedback.resetOutbound(ORIGINAL);
        for (int pipe = 1; pipe < 8; pipe++) {
            assertTrue(loss(feedback, new DatagramFeedback.Path(pipe, false), 0, 1, 100, 0));
        }
        var extra = new DatagramFeedback.Path(8, false);
        assertEquals(DatagramLossReport.IGNORED, feedback.reportLoss(extra, 0, 1, 100, 0, 0));
        feedback.dispatched(extra.pipe(), false, 0, 0);
        assertNull(feedback.bindPair(ORIGINAL, extra, true));
        assertEquals(3, feedback.rejectedPaths());
        assertEquals(DatagramLossReport.SOURCE_EXPIRED, feedback.reportLoss(ORIGINAL, 1, 1, 100, 5, 1));
        feedback.retire(1);
        assertTrue(loss(feedback, extra, 0, 1, 100, 0));
    }

    private static boolean loss(DatagramFeedback feedback, DatagramFeedback.Path path,
                                long now, long generation, long finalized, long lost) {
        return feedback.reportLoss(path, now, generation, finalized, lost, now) == DatagramLossReport.ACCEPTED;
    }

    private static void ack(DatagramFeedback feedback, DatagramFeedback.Path path, long sequence, long now, long sample) {
        boolean nativePath = path.channel() == 1;
        feedback.dispatched(path.pipe(), nativePath, sequence, now - (sample + 7) * Time.MICROSECOND);
        var frame = new Frame();
        frame.pipeId = path.pipe();
        frame.datagramChannel = path.channel();
        frame.value = sequence;
        frame.datagramAckDelay = 7;
        if (nativePath) feedback.completed(path.pipe(), sequence, true);
        feedback.acknowledge(frame, now);
        feedback.acknowledge(frame, now);
    }
}
