package io.zmux.internal.core;

import io.zmux.internal.buffer.CarrierCharge;



import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.memory.MemoryBudget;
import io.zmux.internal.wire.TransportParams;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static io.zmux.internal.buffer.OffsetPageBits.SIZE;
import static org.junit.jupiter.api.Assertions.*;

class SendHistoryTest {
    @Test void selectiveAckLossAndCopyAreExactForOneByteFragments() {
        var h = new SendHistory(null);
        long preamble = SIZE - 2;
        long end = 2L * SIZE;
        assertEquals(end, h.reserve(0, end));
        // Leave every even byte missing in the app data across a page boundary.
        for (int i = 0; i < 512; i++) {
            long offset = preamble + i * 2L + 1;
            h.acknowledge(offset, offset + 1, preamble);
            h.copied(offset, offset + 1);
        }
        h.lost(0, end);
        assertEquals(512, h.applicationAcked());
        assertEquals(end - 512, h.resendBytes());
        assertEquals(2, h.pageCount());
        assertEquals(preamble + 2, h.nextUncopied(preamble + 1, end));
        assertEquals(preamble + 1, h.resendEnd(0));
        h.resent(0, preamble + 1);
        assertEquals(preamble + 2, h.nextResend(0));
        h.acknowledge(0, SIZE - 1, preamble);
        assertEquals(SIZE, h.prefix());
        assertEquals(513, h.applicationAcked());
        assertEquals(1, h.pageCount());
        h.acknowledge(SIZE, end, preamble);
        assertEquals(end, h.prefix());
        assertEquals(end - preamble, h.applicationAcked());
        assertEquals(Long.MAX_VALUE, h.nextResend(0));
        assertEquals(0, h.pageCount());
    }

    @Test void overlappingReportsMatchAnOffsetReferenceModel() {
        var h = new SendHistory(null);
        var refAck = new java.util.BitSet();
        var refLoss = new java.util.BitSet();
        var rng = new java.util.Random(441);
        int end = 3 * SIZE;
        h.reserve(0, end);
        for (int i = 0; i < 1_024; i++) {
            int from = rng.nextInt(end);
            int to = Math.min(end, from + rng.nextInt(193));
            switch (rng.nextInt(3)) {
                case 0 -> {
                    h.acknowledge(from, to, 17);
                    refAck.set(from, to);
                    refLoss.clear(from, to);
                }
                case 1 -> {
                    h.lost(from, to);
                    refLoss.set(from, to);
                    refLoss.andNot(refAck);
                }
                default -> {
                    h.resent(from, to);
                    refLoss.clear(from, to);
                }
            }
            long next = refLoss.isEmpty() ? Long.MAX_VALUE : refLoss.nextSetBit(0);
            assertEquals(refAck.nextClearBit(0), h.prefix());
            assertEquals(refAck.get(17, end).cardinality(), h.applicationAcked());
            assertEquals(refLoss.cardinality(), h.resendBytes());
            assertEquals(next, h.nextResend(0));
            if (next != Long.MAX_VALUE) assertEquals(refLoss.nextClearBit((int) next), h.resendEnd(next));
        }
    }

    @Test void orderedSendingAndAckRentNoSparseNodes() {
        var h = new SendHistory(null);
        long off = 0;
        for (int i = 0; i < 512; i++) {
            assertEquals(3 * SIZE, h.reserve(off, 3 * SIZE));
            assertTrue(h.pageCount() > 0);
            h.acknowledge(off, off + 3 * SIZE, 0);
            assertEquals(0, h.markedPages());
            assertEquals(0, h.pageCount());
            off += 3 * SIZE;
        }
        assertEquals(off, h.applicationAcked());
    }

    @Test void spliceReceiptKeepsItsOwnSegmentAndSurvivesQueueRelease() {
        var q = new SendQueue();
        var root = new ReadBuf(ByteBuffer.allocate(2), null);
        var settled = new AtomicInteger();
        var first = new CarrierCharge(1, false, _ -> settled.incrementAndGet());
        var second = new CarrierCharge(1, false, _ -> settled.incrementAndGet());
        var batch = new Batch();
        try {
            q.writeRef(root.data().slice(0, 1), root, false, new CarrierCharge.Slice(first, 0));
            q.writeRef(root.data().slice(1, 1), root, false, new CarrierCharge.Slice(second, 0));
            first.release();
            second.release();
            var a = q.peek(0, 2);
            assertEquals(1, a.data().remaining(), "adjacent receipts do not merge under a single descriptor");
            a.attachReceipt(batch);
            q.keepReceipts(1);
            assertEquals(1, settled.get(), "reset abandoned only the second range");
            assertEquals(0, q.peek(1, 1).receiptBytes());
            q.drop();
            assertEquals(1, settled.get(), "the batch still owns the first receipt");
            batch.discard();
            assertEquals(2, settled.get());
        } finally {
            q.drop();
            batch.discard();
            root.unref();
        }
    }

    @Test void aFullAccountStillTracksAndRetransmitsThenReusesItsPrepaidPages() {
        long credit = ConnFlow.MIN_INITIAL_WINDOW;
        long cap = ReceiveLayout.WORKING + credit * ConnFlow.RECEIVE_COST
                + ReceiveLayout.STREAM_CHARGE + ConnFlow.sendFloor(true);
        var budget = new MemoryBudget(cap, null);
        var account = ConnFlow.reserveInitial(budget, credit, true, true);
        var notifications = new AtomicInteger();
        var conn = new ConnFlow(credit, () -> {}, account, true, notifications::incrementAndGet);
        var backing = conn.newBacking();
        var h = backing.newHistory();
        long floorEnd = (long) SIZE * SendHistory.FLOOR_PAGES;
        try {
            assertEquals(floorEnd, h.reserve(0, floorEnd + SIZE));
            assertEquals(0, h.allowed(floorEnd, 1));
            h.lost(0, SIZE);
            h.resent(0, SIZE / 2);
            assertEquals(SIZE / 2, h.nextResend(0));
            assertEquals(cap, budget.used());
            assertEquals(0, notifications.get());

            h.acknowledge(0, SIZE, 0);
            assertEquals(1, notifications.get(), "one prepaid page became available under full pressure");
            assertEquals(SIZE, h.reserve(floorEnd, SIZE));
            assertEquals(cap, budget.used());

            var room = new AtomicInteger();
            conn.onRoom(room::incrementAndGet);
            assertEquals(0, h.reserve(floorEnd + SIZE, SIZE));
            budget.limit(cap + SendHistory.PAGE_CHARGE);
            assertEquals(1, room.get());
            conn.backingRoom();
            assertEquals(2, notifications.get(), "optional space woke the owner");
            assertEquals(SIZE, h.reserve(floorEnd + SIZE, SIZE));
            assertEquals(cap + SendHistory.PAGE_CHARGE, budget.used());
            assertThrows(IllegalArgumentException.class, () -> budget.limit(cap));

            h.finish(floorEnd + SIZE);
            budget.limit(cap); // Speculative page after a short record was not committed.
            assertEquals(cap, budget.used());
        } finally {
            h.close();
            backing.close(() -> {});
            account.close();
        }
        assertEquals(0, budget.used());
    }

    @Test void ackAndPipeRetirementUnlinkBothCopyIndexesBeforeReturningTheCharge() throws Exception {
        var budget = new MemoryBudget(16L << 20, null);
        var builder = CoreConfig.builder().budget(budget);
        builder.local().present |= TransportParams.MULTIPATH;
        var s = PipeTargetTest.session(builder);
        try {
            s.streams.peerLimits(10, 0);
            var st = s.streams.openStream(true);
            s.streams.allocId(st);
            int n = 16;
            assertEquals(n, s.send.write(st, ByteBuffer.allocate(n), false));
            assertEquals(n, st.snd.reserveHistory(0, n));
            s.send.dataWritten(st, n, false);

            var pipe = s.pipes.get(0);
            var ret = s.mp().retention;
            var target = pipe.mp().ret.copyQ();
            long committed = budget.used();
            budget.limit(committed + Retention.COPY_CHARGE);
            assertTrue(ret.queueCopy(st, 0, n, false, target));
            assertEquals(n, ret.copiesOutstanding());
            assertFalse(ret.queueCopy(st, 0, n, false, target), "a second pending copy has no physical backing");
            assertThrows(IllegalArgumentException.class, () -> budget.limit(committed));

            ret.acked(FrameDesc.stream(st.tab.id(), 0, n, false));
            assertTrue(target.isEmpty(), "the stream ACK removes the target's pending reference");
            assertEquals(0, ret.copiesOutstanding());
            budget.limit(committed);
            assertEquals(committed, budget.used());

            budget.limit(committed + Retention.COPY_CHARGE);
            assertTrue(ret.queueCopy(st, n, n, true, target));
            ret.dropCopies(pipe);
            assertTrue(target.isEmpty());
            assertEquals(0, ret.copiesOutstanding());
            budget.limit(committed);
        } finally { s.finishClose(); }
        assertEquals(0, budget.used());
    }
}
