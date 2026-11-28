package io.zmux.internal.core;

import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.buffer.ReceiveLoans;
import io.zmux.internal.memory.MemoryBudget;
import io.zmux.internal.memory.SharedMemory;
import io.zmux.internal.wire.TransportParams;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReceiveCreditReservationTest {
    @Test void partialHandoffsKeepReusingOnePageAtTheHardCap() {
        long initial = ConnFlow.MIN_INITIAL_WINDOW;
        long cap = ReceiveLayout.WORKING + initial * ReceiveLayout.CREDIT_COST + ReceiveLayout.STREAM_CHARGE;
        var budget = new MemoryBudget(cap, null);
        var reservation = ConnFlow.reserveInitial(budget, initial, false, false);
        assertEquals(1, reservation.reserveStreams(1, ReceiveLayout.STREAM_CHARGE, 0));
        var storage = new RecvBuffer.Storage(reservation);
        var stream = new RecvBuffer();
        var source = new ReadBuf(ByteBuffer.allocate(1).put(0, (byte) 7), null);
        StreamRecv.Chunk pinned = null;
        try {
            for (int offset = 0; offset < 512; offset++) {
                assertEquals(1, stream.insert(storage, offset, source.data(), 0, 1, source));
                var piece = stream.take(offset, 1);
                assertEquals(7, piece.data().get(0));
                if (pinned == null) pinned = piece;
                else {
                    assertSame(pinned.buf(), piece.buf());
                    piece.buf().unref();
                }
                assertEquals(ReceiveLayout.PAGE_CHARGE, storage.bytes());
                assertEquals(cap, budget.used());
            }
        } finally {
            source.unref();
            if (pinned != null) pinned.buf().unref();
            stream.clear();
            reservation.close();
        }
        assertEquals(0, budget.used());
    }

    @Test void initialParametersCarryTheWindowTheListenerReserved() throws Exception {
        long granted = ConnFlow.MIN_INITIAL_WINDOW + 64;
        var budget = new MemoryBudget(ReceiveLayout.WORKING + granted * ConnFlow.RECEIVE_COST, null);
        var builder = CoreConfig.builder().budget(budget);
        builder.local().maxStreamsBidi = 0;
        var config = builder.build();
        var session = new Session(config, 0);
        try {
            byte[] encoded = session.localParams().toBytes();
            var decoded = TransportParams.decode(ByteBuffer.wrap(encoded), 0, encoded.length);
            assertEquals(granted, decoded.initialMaxData);
            assertEquals(granted, session.conn.advertised());
            assertEquals(budget.limit(), budget.used());
            assertEquals(CoreConfig.DEFAULT_CONNECTION_WINDOW, config.local().initialMaxData);
            assertThrows(SharedMemory.Unavailable.class, () -> new Session(config, 0));
        } finally {
            session.finishClose();
        }
        assertEquals(0, budget.used());
    }

    @Test void increasingTheBudgetWakesTheOwnerToPublishNewStreamCredit() {
        long initial = ConnFlow.MIN_INITIAL_WINDOW;
        long slot = ReceiveLayout.STREAM_CHARGE + SendBacking.FLOOR;
        long tight = ReceiveLayout.WORKING + initial * ReceiveLayout.CREDIT_COST + slot;
        var budget = new MemoryBudget(tight, null);
        var config = CoreConfig.builder().budget(budget);
        config.local().initialMaxData = initial;
        config.local().maxStreamsBidi = 2;
        config.local().maxStreamsUni = 0;
        var session = new Session(config.build(), 0);
        var wakes = new java.util.concurrent.atomic.AtomicInteger();
        try {
            session.onReceiveRoom(wakes::incrementAndGet);
            assertEquals(1, session.streams().remoteLimit(0));
            budget.limit(tight + slot);
            assertEquals(1, wakes.get());
            session.receiveRoom();
            assertEquals(2, session.streams().remoteLimit(0));
            assertEquals(budget.limit(), budget.used());
        } finally { session.finishClose(); }
        assertEquals(0, budget.used());
    }

    @Test void idleStreamCreditStartsSmallGrowsAndReturnsRetiredSlots() throws Exception {
        var budget = new MemoryBudget(1L << 30, null);
        var builder = CoreConfig.builder().role(CoreConfig.Role.RESPONDER).budget(budget);
        var session = new Session(builder.build(), 0);
        long batch = StreamTable.initialBatch(false);
        long slot = ReceiveLayout.STREAM_CHARGE + SendBacking.FLOOR;
        long idle = ReceiveLayout.WORKING + CoreConfig.DEFAULT_CONNECTION_WINDOW * ConnFlow.RECEIVE_COST
                + batch * (slot + ReceiveLayout.STREAM_CHARGE);
        try {
            assertEquals(batch, session.streams().remoteLimit(0));
            assertEquals(batch, session.streams().remoteLimit(1));
            assertEquals(idle, budget.used());
            var opened = session.streams().lookup(0, true);
            assertEquals(batch + 1, session.streams().remoteLimit(0));
            assertEquals(idle + slot, budget.used());
            Poke.set(opened.snd, "state", Stream.SEND_DATA_RECVD);
            Poke.set(opened.rcv, "state", Stream.RECV_DATA_READ);
            session.streams().release(opened);
            assertEquals(batch + 1, session.streams().remoteLimit(0), "an issued cumulative limit cannot be withdrawn");
            assertEquals(idle, budget.used());
            assertNotNull(session.streams().lookup(batch << 2, true));
            assertEquals(idle + batch * slot, budget.used(), "every implicitly opened direction retains its floor");
        } finally {
            session.finishClose();
        }
        assertEquals(0, budget.used());
    }

    @Test void aBlockedAccountCanUseOtherAccountsSpareAndCancelItsOwnWait() {
        long receive = ReceiveLayout.WORKING + ConnFlow.MIN_INITIAL_WINDOW * ConnFlow.RECEIVE_COST;
        long limit = 2 * receive + (2L << 20);
        var budget = new MemoryBudget(limit, null);
        var first = ConnFlow.reserveInitial(budget, ConnFlow.MIN_INITIAL_WINDOW, false, false);
        var second = ConnFlow.reserveInitial(budget, ConnFlow.MIN_INITIAL_WINDOW, false, false);
        long block = SendBacking.BLOCK_CHARGE;
        assertTrue(first.reserveView(block));
        first.releaseView(block);
        long otherPressure = limit - budget.used();
        assertTrue(budget.tryCharge(otherPressure));
        assertEquals(limit, budget.used());

        var calls = new java.util.concurrent.atomic.AtomicInteger();
        second.onRoom(calls::incrementAndGet);
        second.waitForBacking(() -> block);
        assertEquals(1, calls.get());
        assertTrue(second.reserveView(block));
        assertEquals(block, second.reservedBytes() - receive);
        assertTrue(budget.tryCharge(limit - budget.used()));
        second.waitForBacking(() -> block);
        second.waitForBacking(() -> Long.MAX_VALUE);
        second.releaseView(block);
        assertEquals(1, calls.get());
        first.close();
        second.close();
    }

    @Test void loweringTheBudgetReclaimsSpareWithoutIgnoringLiveViews() {
        long receive = ReceiveLayout.WORKING + ConnFlow.MIN_INITIAL_WINDOW * ConnFlow.RECEIVE_COST;
        var budget = new MemoryBudget(4L << 20, null);
        var session = ConnFlow.reserveInitial(budget, ConnFlow.MIN_INITIAL_WINDOW, false, false);
        long live = SendBacking.BLOCK_CHARGE;
        assertTrue(session.reserveView(live));
        assertTrue(budget.used() > receive + live);
        assertThrows(IllegalArgumentException.class, () -> budget.limit(receive + live - 1));
        budget.limit(receive + live);
        assertEquals(receive + live, budget.used());
        session.releaseView(live);
        session.close();
        assertEquals(0, budget.used());
    }

    @Test void growthReservesBeforeChangingCreditAndKeepsOpenHolesCommitted() {
        long initial = ConnFlow.MIN_INITIAL_WINDOW;
        var budget = new MemoryBudget(ReceiveLayout.WORKING + (initial + 64) * ConnFlow.RECEIVE_COST, null);
        var reservation = ConnFlow.reserveInitial(budget, initial, false, false);
        var conn = new ConnFlow(initial, () -> {}, reservation, false, () -> {});
        assertTrue(conn.received(initial)); // The peer can fill all but the four arrived bytes later.
        conn.buffer(4, initial, false);
        conn.grow(initial + 1000);
        assertEquals(initial + 64, conn.window());
        assertEquals(budget.limit(), budget.used());
        assertEquals(initial + 64, conn.maxDataValue());
        assertThrows(IllegalArgumentException.class, () -> budget.limit(budget.used() - 1));
        conn.releaseReservation();
        assertEquals(0, budget.used());
    }

    @Test void closingTheReservationKeepsAnOutstandingLoanCharged() throws Exception {
        long credit = ConnFlow.MIN_INITIAL_WINDOW;
        var budget = new MemoryBudget(ReceiveLayout.WORKING + credit * ConnFlow.RECEIVE_COST, null);
        var reservation = ConnFlow.reserveInitial(budget, credit, false, false);
        var storage = new RecvBuffer.Storage(reservation);
        var received = new RecvBuffer();
        var data = ByteBuffer.allocate(4096);
        var root = new ReadBuf(data, null);
        try { received.insert(storage, 0, data, 0, 1, root); } finally { root.unref(); }
        var loan = received.borrow(0, 1, new ReceiveLoans(data.capacity(), 1, reservation, null));
        try {
            received.clear();
            reservation.close();
            long charge = data.capacity() + ReceiveLoans.DESCRIPTOR;
            assertEquals(charge, budget.used());
            assertThrows(IllegalArgumentException.class, () -> budget.limit(charge - 1));
        } finally {
            loan.buf().unref();
        }
        assertEquals(0, budget.used());
    }
}
