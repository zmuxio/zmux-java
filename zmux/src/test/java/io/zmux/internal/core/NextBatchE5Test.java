package io.zmux.internal.core;

import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameDecoder;
import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.FrameKind;
import io.zmux.internal.wire.VarInt;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import static org.junit.jupiter.api.Assertions.*;

class NextBatchE5Test {
    @TestFactory Stream<DynamicTest> specVectors() throws Exception {
        return DatagramCheckpoints.tests("E5");
    }

    @Test void feedbackMatchesEachActualCopyAndExpiresWithoutInferringLoss() throws Exception {
        var ledger = new DatagramFeedback(3);
        long time = 10 * Time.MILLISECOND;
        ledger.dispatched(2, true, 1, 0);
        ledger.dispatched(2, false, 1, 0);
        ledger.dispatched(2, true, 2, 0);
        ledger.completed(2, 2, false);
        ledger.acknowledge(ack(2, 1, 2, 2000), time);
        ledger.acknowledge(ack(2, 1, 1, 2000), time);
        ledger.acknowledge(ack(2, 1, 1, 0), time);
        assertEquals(0, ledger.snapshot(2, true, time).acknowledged());
        ledger.completed(2, 1, true);
        assertEquals(1, ledger.snapshot(2, true, time).acknowledged());
        assertEquals(8 * Time.MILLISECOND, ledger.snapshot(2, true, time).roundTripNanos());
        assertEquals(1, ledger.snapshot(2, false, time).pending());
        assertEquals(0, ledger.snapshot(2, false, time).acknowledged());
        long expired = 60 * Time.SECOND + time;
        ledger.acknowledge(ack(2, 0, 1, 0), expired);
        assertEquals(1, ledger.snapshot(2, false, expired).expired());
        assertEquals(0, ledger.snapshot(2, false, expired).acknowledged());
        assertEquals(0, ledger.snapshot(2, true, expired).rttSamples());
    }

    @Test void aBackupWriterRunningFirstCannotTakeControlFeedback() throws Exception {
        var session = PipeTargetTest.session(CoreConfig.builder().datagrams(true));
        var backup = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
        var primary = session.pipes().get(0);
        session.datagrams().feedback.arrived(backup.id, false, 0, 0);

        var backupBatch = new Batch();
        var writer = new RecordWriter();
        writer.init(backupBatch, Framing.BYTES, 65535, Session.DEFAULT_BATCH);
        session.tx.writeDatagrams(writer, backup);
        writer.end();
        assertFalse(hasReport(backupBatch, backup.id), "backup writer consumed control feedback");

        var primaryBatch = new Batch();
        writer.init(primaryBatch, Framing.BYTES, 65535, Session.DEFAULT_BATCH);
        session.tx.writeControl(writer, primary);
        writer.end();
        assertTrue(hasReport(primaryBatch, backup.id), "primary path did not return the backup report");
    }

    private static boolean hasReport(Batch batch, long pipe) throws Exception {
        var wire = ByteBuffer.wrap(BatchFixture.flat(batch));
        var frame = new Frame();
        var decoder = new FrameDecoder();
        for (int pos = 0; pos < wire.limit();) {
            long size = VarInt.get(wire, pos, wire.limit());
            int body = pos + VarInt.lengthOf(wire.get(pos));
            int end = body + Math.toIntExact(size);
            for (int at = body; at < end;) {
                at += decoder.decode(wire, at, end, frame);
                if (frame.kind == FrameKind.DATAGRAM_ACK && frame.pipeId == pipe) return true;
            }
            pos = end;
        }
        return false;
    }

    private static Frame ack(long pipe, int channel, long sequence, long delay) throws Exception {
        var wire = ByteBuffer.allocate(64);
        FrameEncoder.datagramAckHeader(wire, pipe, channel, sequence, delay, 0, 0);
        var frame = new Frame();
        new FrameDecoder().decode(wire, 0, wire.position(), frame);
        return frame;
    }
}
