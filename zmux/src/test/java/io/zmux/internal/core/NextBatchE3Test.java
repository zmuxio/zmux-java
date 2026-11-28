package io.zmux.internal.core;

import io.zmux.DatagramCopyPolicy;
import io.zmux.DatagramCopyPolicy.IndependentPair;
import io.zmux.DatagramSendOptions;
import io.zmux.PipeId;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NextBatchE3Test {
    @TestFactory Stream<DynamicTest> specVectors() throws Exception {
        return DatagramCheckpoints.tests("E3");
    }

    @Test void adaptiveIgnoresReliableAckStallAndUnreportedDatagram() throws Exception {
        var session = PipeTargetTest.session(CoreConfig.builder().datagrams(true).probation(0));
        Pipe first = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
        Pipe second = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
        session.setWritable(0, 0, false, 0);
        var datagrams = session.datagrams();
        datagrams.peer(65535, 65535, true);
        var writer = new RecordWriter();
        writer.init(new Batch(), Framing.BYTES, 65535, Session.DEFAULT_BATCH);
        var pair = new IndependentPair(PipeId.of(first.id), PipeId.of(second.id));
        var adaptive = new DatagramSendOptions(null, null, DatagramSendOptions.Copies.Adaptive.INSTANCE, false, false);
        var urgent = new DatagramSendOptions(null, null, DatagramSendOptions.Copies.Adaptive.INSTANCE, true, false);
        var connection = new Object();
        session.pipes().setCarrierConn(first.id, connection);
        session.pipes().setCarrierConn(second.id, connection);
        session.datagramIndependence(Set.of(pair));

        datagrams.enqueueSingle(null, 0, new byte[0], 0, Time.NEVER, urgent, null);
        assertTrue(datagrams.write(writer, first, 7));
        assertFalse(datagrams.write(writer, second, 7), "one physical connection has shared fate");

        session.pipes().setCarrierConn(second.id, new Object());
        session.datagramIndependence(Set.of(pair));
        Poke.set(first.mp().link, "unacked", true);
        Poke.set(first.mp().link, "lastAdvance", 0);
        long now = 11 * Time.SECOND;
        session.setNow(now);
        datagrams.enqueueSingle(null, 0, new byte[0], now, Time.NEVER, adaptive, null);
        assertTrue(datagrams.write(writer, first, 7));
        assertFalse(datagrams.write(writer, second, 7));

        datagrams.enqueueSingle(null, 0, new byte[0], now, Time.NEVER, urgent, null);
        assertTrue(datagrams.write(writer, first, 7));
        assertTrue(datagrams.write(writer, second, 7));
        datagrams.enqueueSingle(null, 0, new byte[0], now, Time.NEVER, adaptive, null);
        assertTrue(datagrams.write(writer, first, 7));
        assertFalse(datagrams.write(writer, second, 7), "Urgent cannot latch");
    }

    @Test void emptyCopiesSpendBurstAndDuplicateOneIsSingle() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new DatagramSendOptions.Copies.Duplicate(0));
        for (int percent : new int[]{0, 20}) {
            var session = PipeTargetTest.session(CoreConfig.builder().datagrams(true).probation(0)
                    .datagramCopyPolicy(new DatagramCopyPolicy(2, percent, false)));
            Pipe first = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
            Pipe second = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
            session.setWritable(0, 0, false, 0);
            var datagrams = session.datagrams();
            datagrams.peer(4, 65535, true);
            datagrams.peer(65535, 65535, true); // A later update cannot enlarge the burst.
            var writer = new RecordWriter();
            writer.init(new Batch(), Framing.BYTES, 65535, Session.DEFAULT_BATCH);
            for (int count : new int[]{1, 2, 2}) {
                var opts = new DatagramSendOptions(null, null, new DatagramSendOptions.Copies.Duplicate(count), null, false);
                datagrams.enqueueSingle(null, 0, new byte[0], 0, Time.NEVER, opts, null);
                assertTrue(datagrams.write(writer, first, 7));
                datagrams.write(writer, second, 7);
            }
            assertEquals(percent == 0 ? 3 : 4, session.stats().datagramsSent());
        }
    }

    @Test void budgetDeniedAdaptiveDoesNotEnterALatch() throws Exception {
        var session = PipeTargetTest.session(CoreConfig.builder().datagrams(true).probation(0));
        Pipe first = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
        Pipe second = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
        session.setWritable(0, 0, false, 0);
        session.datagramIndependence(Set.of(new IndependentPair(PipeId.of(first.id), PipeId.of(second.id))));
        var datagrams = session.datagrams();
        datagrams.peer(4, 65535, true);
        datagrams.budget.copy(0, 1);
        var primary = new DatagramFeedback.Path(first.id, false);
        var alternate = new DatagramFeedback.Path(second.id, false);
        assertEquals(Session.DatagramLossReport.ACCEPTED, datagrams.feedback.reportLoss(primary, 0, 1, 100, 5, 0));
        assertEquals(Session.DatagramLossReport.ACCEPTED, datagrams.feedback.reportLoss(alternate, 0, 1, 100, 0, 0));
        var opts = new DatagramSendOptions(null, null, DatagramSendOptions.Copies.Adaptive.INSTANCE, false, false);
        datagrams.enqueueSingle(null, 0, new byte[0], 0, Time.NEVER, opts, null);
        var writer = new RecordWriter();
        writer.init(new Batch(), Framing.BYTES, 65535, Session.DEFAULT_BATCH);
        assertTrue(datagrams.write(writer, first, 7));
        assertEquals(1, session.stats().datagramCopyBudgetDropped());
        assertEquals(Session.DatagramLossReport.ACCEPTED, datagrams.feedback.reportLoss(primary, 1, 1, 100, 2, 1));
        assertFalse(datagrams.feedback.impairment(primary, alternate, 1));
    }

    @Test void evaluatingWithNoUsableAlternativeClearsTheAdaptiveLatch() throws Exception {
        var session = PipeTargetTest.session(CoreConfig.builder().datagrams(true).probation(0));
        Pipe first = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
        Pipe second = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
        session.setWritable(0, 0, false, 0);
        session.datagramIndependence(Set.of(new IndependentPair(PipeId.of(first.id), PipeId.of(second.id))));
        var datagrams = session.datagrams();
        datagrams.peer(65535, 65535, true);
        var primary = new DatagramFeedback.Path(first.id, false);
        var alternate = new DatagramFeedback.Path(second.id, false);
        assertEquals(Session.DatagramLossReport.ACCEPTED, datagrams.feedback.reportLoss(primary, 0, 1, 100, 5, 0));
        assertEquals(Session.DatagramLossReport.ACCEPTED, datagrams.feedback.reportLoss(alternate, 0, 1, 100, 0, 0));
        var opts = new DatagramSendOptions(null, null, DatagramSendOptions.Copies.Adaptive.INSTANCE, false, false);
        var writer = new RecordWriter();
        writer.init(new Batch(), Framing.BYTES, 65535, Session.DEFAULT_BATCH);

        datagrams.enqueueSingle(null, 0, new byte[0], 0, Time.NEVER, opts, null);
        assertTrue(datagrams.write(writer, first, 7));
        assertTrue(datagrams.write(writer, second, 7));

        // datagram.md clears the latch when an evaluation has no usable alternative.
        session.setWritable(second.id, 0, false, Time.MILLISECOND);
        datagrams.enqueueSingle(null, 0, new byte[0], Time.MILLISECOND, Time.NEVER, opts, null);
        assertTrue(datagrams.write(writer, first, 7));
        assertFalse(datagrams.queued());

        session.setWritable(second.id, 0, true, 2 * Time.MILLISECOND);
        long now = 2 * Time.MILLISECOND;
        assertEquals(Session.DatagramLossReport.ACCEPTED, datagrams.feedback.reportLoss(primary, now, 1, 100, 3, now));
        datagrams.enqueueSingle(null, 0, new byte[0], now, Time.NEVER, opts, null);
        assertTrue(datagrams.write(writer, first, 7));
        assertFalse(datagrams.write(writer, second, 7), "recovery requires the entry threshold again");
    }

    @Test void invalidPathTakesPriorityWhenQueuedCopyAlsoExceedsBudget() throws Exception {
        var session = PipeTargetTest.session(CoreConfig.builder().datagrams(true).probation(0));
        Pipe first = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
        Pipe second = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
        session.setWritable(0, 0, false, 0);
        session.datagramIndependence(Set.of(new IndependentPair(PipeId.of(first.id), PipeId.of(second.id))));
        var datagrams = session.datagrams();
        datagrams.peer(4, 65535, true);
        var opts = new DatagramSendOptions(null, null, new DatagramSendOptions.Copies.Duplicate(2), null, true);
        datagrams.enqueueSingle(null, 0, new byte[0], 0, Time.NEVER, opts, null);
        var writer = new RecordWriter();
        writer.init(new Batch(), Framing.BYTES, 65535, Session.DEFAULT_BATCH);
        assertTrue(datagrams.write(writer, first, 7));
        assertTrue(datagrams.queued());
        datagrams.budget.copy(0, 1);
        session.datagramIndependence(Set.of());
        assertFalse(datagrams.write(writer, second, 7));
        assertEquals(1, session.stats().datagramCopyPathDropped());
        assertEquals(0, session.stats().datagramCopyBudgetDropped());
    }

    @Test void copyWaitsForInFlightWrite() throws Exception {
        var session = PipeTargetTest.session(CoreConfig.builder().datagrams(true).probation(0));
        Pipe first = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
        Pipe second = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
        session.setWritable(0, 0, false, 0);
        var lane = second.out().lane(0).tx;
        Poke.set(lane, "writing", true);
        var options = new DatagramSendOptions(null, null, new DatagramSendOptions.Copies.Duplicate(2), null, false);
        var datagrams = session.datagrams();
        datagrams.peer(65535, 65535, true);
        datagrams.enqueueSingle(null, 0, new byte[3], 0, Time.NEVER, options, null);
        var writer = new RecordWriter();
        writer.init(new Batch(), Framing.BYTES, 65535, Session.DEFAULT_BATCH);
        assertTrue(datagrams.write(writer, first, 7));
        assertTrue(datagrams.queued(), "the copy remains pending while its carrier finishes a write");
        Poke.set(lane, "writing", false);
        assertTrue(datagrams.write(writer, second, 7));
        assertEquals(2, session.stats().datagramsSent());
    }

    @Test void revokingPathIndependenceDiscardsAQueuedCopy() throws Exception {
        var session = PipeTargetTest.session(CoreConfig.builder().datagrams(true).probation(0));
        Pipe first = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
        Pipe second = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
        var pair = new IndependentPair(PipeId.of(first.id), PipeId.of(second.id));
        var options = new DatagramSendOptions(null, null, new DatagramSendOptions.Copies.Duplicate(2), null, true);
        var datagrams = session.datagrams();
        datagrams.peer(65535, 65535, true);

        session.datagramIndependence(Set.of(pair));
        datagrams.enqueueSingle(null, 0, new byte[3], 0, Time.NEVER, options, null);
        var writer = new RecordWriter();
        writer.init(new Batch(), Framing.BYTES, 65535, Session.DEFAULT_BATCH);
        assertTrue(datagrams.write(writer, first, 7));
        assertTrue(datagrams.queued());
        session.datagramIndependence(Set.of());
        assertFalse(datagrams.write(writer, second, 7));
        assertFalse(datagrams.queued());
        assertEquals(1, session.stats().datagramsSent());

        session.datagramIndependence(Set.of(pair));
        datagrams.enqueueSingle(null, 0, new byte[3], 0, Time.NEVER, options, null);
        assertTrue(datagrams.write(writer, first, 7));
        assertTrue(datagrams.write(writer, second, 7));
        assertEquals(3, session.stats().datagramsSent());
    }
}
