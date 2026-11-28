package io.zmux.internal.core;

import io.zmux.internal.memory.SharedMemory;

import io.zmux.internal.memory.MemoryBudget;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class SendBackingTest {
    private static Session session(MemoryBudget budget) {
        var config = CoreConfig.builder().budget(budget);
        config.local().initialMaxData = ConnFlow.MIN_INITIAL_WINDOW;
        config.local().maxStreamsBidi = config.local().maxStreamsUni = 0;
        return new Session(config.build(), 0);
    }

    private static final long NO_STREAMS = ReceiveLayout.WORKING + ConnFlow.MIN_INITIAL_WINDOW * ConnFlow.RECEIVE_COST;

    @Test void tightAdmittedDirectionsEachWriteAndRejectTheNextBeforeAllocating() throws Exception {
        var budget = new MemoryBudget(NO_STREAMS + 2 * SendBacking.FLOOR, null);
        var session = session(budget);
        try {
            var first = session.streams.openStream(false);
            var second = session.streams.openStream(false);
            assertEquals(CoreException.Reason.STREAM_LIMIT,
                    assertThrows(CoreException.class, () -> session.streams.openStream(false)).reason());
            assertEquals(2, session.streams.count());
            assertEquals(budget.limit(), budget.used());
            for (Stream stream : new Stream[] {first, second}) {
                var data = ByteBuffer.allocate((int) Stream.SMALL_HELD);
                assertEquals(Stream.SMALL_HELD, session.send.write(stream, data, false));
                assertEquals(budget.limit(), budget.used());
            }
        } finally { session.finishClose(); }
        assertEquals(0, budget.used());
    }

    @Test void theEntireReceiveCommitmentAndTheSendFloorCanBeInUseTogether() throws Exception {
        long rx = NO_STREAMS;
        var budget = new MemoryBudget(rx + SendBacking.FLOOR, null);
        var session = session(budget);
        var receive = (SharedMemory.Receive) Poke.get(session.conn, "budget");
        var out = session.streams.openStream(false);
        assertTrue(receive.tryRetainReceive(rx));
        try {
            assertFalse(receive.tryRetainReceive(1), "the prepaid send floor is outside the receive commitment");
            assertTrue(out.snd.q().backing().blockReady());
            var data = ByteBuffer.allocate((int) Stream.SMALL_HELD);
            assertEquals(Stream.SMALL_HELD, session.send.write(out, data, false));
            assertEquals(budget.limit(), budget.used());
        } finally {
            receive.releaseRetainedReceive(rx);
            session.finishClose();
        }
        assertEquals(0, budget.used());
    }

    @Test void floorRemainsCommittedWhileTheLastBatchPinsIt() throws Exception {
        var budget = new MemoryBudget(NO_STREAMS + SendBacking.FLOOR, null);
        var session = session(budget);
        var stream = session.streams.openStream(false);
        session.send.write(stream, ByteBuffer.wrap(new byte[] {42}), false);
        Pin outstanding = stream.snd.q().peek(0, 1).pin();
        outstanding.ref();
        try {
            session.send.cancelUnopened(stream);
            assertEquals(budget.limit(), budget.used());
            assertThrows(CoreException.class, () -> session.streams.openStream(false));
        } finally {
            outstanding.unref();
        }
        assertEquals(NO_STREAMS, budget.used());
        assertNotNull(session.streams.openStream(false));
        session.finishClose();
        assertEquals(0, budget.used());
    }

    @Test void unresolvedWriteWakesForAnotherAccountsBackingRelease() throws Exception {
        long cap = NO_STREAMS + SendBacking.FLOOR + SendBacking.BLOCK_CHARGE;
        var budget = new MemoryBudget(cap, null);
        assertTrue(budget.tryCharge(SendBacking.BLOCK_CHARGE));
        var session = session(budget);
        session.onReceiveRoom(session::receiveRoom);
        try {
            var st = session.streams.openStream(false);
            var data = ByteBuffer.allocate(8 * SendBacking.BLOCK_BYTES);
            assertEquals(6 * SendBacking.BLOCK_BYTES, session.send.write(st, data, false));
            assertEquals(0, session.send.write(st, data, false));
            var wakes = new AtomicInteger();
            session.send.waitWrite(st, new Waiter() {
                @Override public void wake() { wakes.incrementAndGet(); }
            });
            budget.release(SendBacking.BLOCK_CHARGE);
            var ready = new java.util.ArrayList<Waiter>();
            session.outbox().takeWakes(ready);
            ready.forEach(Waiter::wake);
            assertEquals(1, wakes.get());
            assertEquals(SendBacking.BLOCK_BYTES, session.send.write(st, data, false));
            assertEquals(cap, budget.used());
        } finally { session.finishClose(); }
        assertEquals(0, budget.used());
    }
}
