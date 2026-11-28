package io.zmux.internal.core;

import io.zmux.internal.buffer.WriteWorkspace;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FrameHistoryTest {
    @Test void tinyFramesCannotSpendUnboundedEncoderDescriptors() {
        var batch = new Batch();
        batch.workspace(new WriteWorkspace());
        assertTrue(batch.beginEncoding(0, 0));
        var writer = new RecordWriter();
        writer.init(batch, Framing.BYTES, 16384, Session.DEFAULT_BATCH);
        for (int i = 0; i < Batch.MAX_CODEC_ENTRIES; i++) {
            assertTrue(writer.frame(1, false, bb -> bb.put((byte) 0)));
        }
        assertFalse(writer.frame(1, false, bb -> fail("refused frames are never encoded")));
        writer.end();
        assertTrue(batch.bytes() < Session.DEFAULT_BATCH);
        batch.discard();
        assertTrue(batch.ready());
    }

    @Test void fullFrameHistoryStopsLoggedWritesWithoutHoldingAcknowledgements() {
        Session session = PipeTargetTest.session(CoreConfig.builder());
        Pipe pipe = session.pipes.get(0);
        var lane = pipe.out().lane(0);
        var desc = FrameDesc.of(FrameDesc.Kind.MAX_DATA);
        for (int i = 1; i <= FrameHistory.SLOTS; i++) lane.log.add(new Retained(i, desc, 0, null));
        var signals = new java.util.concurrent.atomic.AtomicInteger();
        session.pipes.sharedWriter(signals::incrementAndGet);
        session.pipes.writer(pipe.id, lane.lane, () -> fail("no write was claimed"), null);

        var batch = new Batch();
        var writer = new RecordWriter();
        writer.init(batch, Framing.BYTES, 16384, Session.DEFAULT_BATCH);
        writer.retainFor(pipe, lane.log.room());
        assertFalse(writer.trackedFrame(1, false, bb -> bb.put((byte) 0), desc));
        assertTrue(writer.frame(1, false, bb -> bb.put((byte) 0)), "an ACK needs no retransmission slot");
        writer.end();
        assertEquals(1, batch.messageCount());
        assertEquals(0, lane.log.room());

        session.mp().retention.ackLog(lane, 0);
        assertEquals(0, signals.get());
        session.mp().retention.ackLog(lane, 1);
        assertEquals(1, signals.get());
        assertEquals(1, lane.log.room());
        batch.discard();
    }
}
