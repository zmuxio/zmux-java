package io.zmux.internal.core;

import io.zmux.internal.buffer.CarrierCharge;
import io.zmux.internal.buffer.OffsetPageBits;
import io.zmux.internal.memory.MemoryBudget;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.DatagramCopyPolicy.IndependentPair;
import io.zmux.DatagramSendOptions;
import io.zmux.PipeId;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class SpliceChargeTest {
    @Test void aPartialBatchSettlesOnlyTheCompleteCapsule() {
        var session = new Session(CoreConfig.builder().build(), 0);
        var complete = new ArrayList<Long>();
        var incomplete = new ArrayList<Long>();
        var prefix = new CarrierCharge(3, true, complete::add);
        var tail = new CarrierCharge(3, true, incomplete::add);
        assertTrue(tail.prepare(null));
        var batch = new Batch();
        batch.charge(prefix, 0, 3);
        batch.charge(tail, 0, 1);
        batch.refSeg(ByteBuffer.allocate(8));
        batch.addRecord(0, 1, 7);
        batch.charge(tail, 1, 3);
        batch.refSeg(ByteBuffer.allocate(8));
        batch.addRecord(1, 2, 7);
        batch.committed(null, Framing.BYTES, null);
        prefix.release();
        tail.release();

        batch.complete(session.tx, null, 9);
        assertEquals(List.of(3L), complete);
        assertEquals(List.of(0L), incomplete);
    }

    @Test void sparseReceiptsPrepayAndRefundAtTheContiguousPrefix() {
        long page = CarrierCharge.PAGE_CHARGE;
        long fixed = 256;
        int size = OffsetPageBits.SIZE;
        long working = ReceiveLayout.WORKING + 2 * ConnFlow.MIN_INITIAL_WINDOW;
        var budget = new MemoryBudget(working + 2 * page + fixed, null);
        var account = ConnFlow.reserveInitial(budget, ConnFlow.MIN_INITIAL_WINDOW, false, false);
        var settled = new ArrayList<Long>();
        var charge = new CarrierCharge(size + 2, false, settled::add);
        assertEquals(2 * page + fixed, charge.backingBytes());
        assertThrows(IllegalStateException.class, () -> charge.accepted(1, 2));
        assertTrue(charge.prepare(account));
        assertTrue(charge.prepare(account));
        assertEquals(working + 2 * page + fixed, budget.used());
        assertThrows(IllegalArgumentException.class, () -> budget.limit(working + page));

        for (int i = 0; i < 512; i++) {
            long offset = 2L * i + 1;
            charge.accepted(offset, offset + 1);
            charge.accepted(offset, offset + 1);
        }
        charge.accepted(size + 1, size + 2);
        assertEquals(working + 2 * page + fixed, budget.used());
        charge.accepted(0, size);
        budget.limit(working + page + fixed);
        assertEquals(working + page + fixed, budget.used());
        assertTrue(settled.isEmpty());

        charge.retain(); // An already submitted batch can complete after the session-owned reference goes away.
        charge.release();
        account.close();
        assertEquals(page + fixed, budget.used());
        charge.release();
        assertEquals(List.of(size + 1L), settled);
        assertEquals(0, budget.used());
    }

    @Test void insufficientReceiptBackingWaitsForTheWholeGrantAndCanBeWithdrawn() throws Exception {
        long page = CarrierCharge.PAGE_CHARGE;
        long total = 2 * page + 256;
        long working = ReceiveLayout.WORKING + 2 * ConnFlow.MIN_INITIAL_WINDOW + SendBacking.FLOOR;
        var budget = new MemoryBudget(working + total, null);
        var account = ConnFlow.reserveInitial(budget, ConnFlow.MIN_INITIAL_WINDOW, false, false);
        var conn = new ConnFlow(ConnFlow.MIN_INITIAL_WINDOW, () -> {}, account, false, () -> {});
        assertEquals(1, conn.reserveStreams(1, false, true));
        var backing = conn.newBacking();
        conn.onRoom(conn::backingRoom);
        var wakes = new AtomicInteger();
        var settled = new ArrayList<Long>();
        var charge = new CarrierCharge(OffsetPageBits.SIZE + 1, true, settled::add);
        assertTrue(budget.tryCharge(total));
        assertTrue(backing.blockReady(), "the payload floor is separate");
        assertFalse(backing.prepareReceipt(charge));
        backing.waitForBacking(charge.backingBytes(), wakes::incrementAndGet);
        budget.release(page);
        assertEquals(0, wakes.get());
        assertFalse(backing.prepareReceipt(charge));
        backing.cancelWait();
        budget.release(total - page);
        assertEquals(0, wakes.get(), "cancelled wait was removed from the account");

        assertTrue(budget.tryCharge(total));
        backing.waitForBacking(charge.backingBytes(), wakes::incrementAndGet);
        budget.release(total);
        assertEquals(1, wakes.get());
        assertTrue(backing.prepareReceipt(charge));
        charge.accepted(0, OffsetPageBits.SIZE);
        assertTrue(settled.isEmpty());
        charge.accepted(OffsetPageBits.SIZE, OffsetPageBits.SIZE + 1);
        assertEquals(List.of(OffsetPageBits.SIZE + 1L), settled);
        budget.limit(working + 256);
        assertEquals(working + 256, budget.used(), "settlement retains the grant descriptor until its last owner");
        charge.release();
        assertEquals(List.of(OffsetPageBits.SIZE + 1L), settled);
        budget.limit(working);
        assertEquals(working, budget.used());
        backing.close(() -> {});
        account.close();
        assertEquals(0, budget.used());
    }

    @Test void aDatagramSettlesOnceEvenWhenOnlyTheCopyIsHandedOff() throws Exception {
        for (boolean primaryAccepted : List.of(false, true)) {
            var session = PipeTargetTest.session(CoreConfig.builder().datagrams(true).probation(0));
            Pipe first = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
            Pipe second = PipeTargetTest.appendOut(session, true, Attach.OUT_ACTIVE, Attach.IN_NONE);
            session.datagramIndependence(Set.of(new IndependentPair(PipeId.of(first.id), PipeId.of(second.id))));
            var options = new DatagramSendOptions(null, null, new DatagramSendOptions.Copies.Duplicate(2), null, true);
            var datagrams = session.datagrams();
            datagrams.peer(65535, 65535, true);

            var charged = new ArrayList<Long>();
            var charge = new CarrierCharge(3, true, charged::add);
            datagrams.enqueueSingle(null, 0, new byte[3], 0, Time.NEVER, options, charge);
            charge.release();
            var original = datagramBatch(datagrams, first);
            var copy = datagramBatch(datagrams, second);
            original.releaseAfterWrite(primaryAccepted ? original.bytes() : 0);
            assertEquals(primaryAccepted ? List.of(3L) : List.of(), charged);
            copy.releaseAfterWrite(copy.bytes());
            assertEquals(List.of(3L), charged);
        }
    }

    private static Batch datagramBatch(Datagrams datagrams, Pipe pipe) {
        var writer = new RecordWriter();
        var batch = new Batch();
        writer.init(batch, Framing.BYTES, 65535, Session.DEFAULT_BATCH);
        assertTrue(datagrams.write(writer, pipe, 7));
        writer.end();
        return batch;
    }
}
