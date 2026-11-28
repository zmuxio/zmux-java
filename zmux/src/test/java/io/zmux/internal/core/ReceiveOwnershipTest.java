package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.buffer.ReceiveLoans;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ReceiveOwnershipTest {
    @Test void aSecondLeaseOfTheSameRootNeedsItsOwnSlot() throws Exception {
        var storage = new RecvBuffer.Storage(null);
        var incoming = new RecvBuffer();
        var loans = new ReceiveLoans(256, 1);
        var root = new ReadBuf(ByteBuffer.allocate(256).put(0, (byte) 7).put(1, (byte) 8), null);
        incoming.insert(storage, 0, root.data(), 0, 2, root);
        root.unref();
        var first = incoming.borrow(0, 1, loans);
        first.buf().ref(); // A forwarded lease lives through its final send holder.
        try {
            first.buf().unref();
            assertEquals(CoreException.Reason.CHUNK_BUDGET,
                    assertThrows(CoreException.class, () -> incoming.borrow(1, 1, loans)).reason());
            ByteBuffer ordinary = ByteBuffer.allocate(1);
            assertEquals(1, incoming.read(1, ordinary, 1));
            assertEquals(8, ordinary.get(0));
            assertEquals(256, loans.bytes());
        } finally {
            first.buf().unref();
        }
        assertEquals(0, loans.bytes());
    }

    @Test void aCompleteLastPieceMovesItsReferenceToTheLoan() throws Exception {
        var storage = new RecvBuffer.Storage(null);
        var incoming = new RecvBuffer();
        var loans = new ReceiveLoans(256, ReceiveLoans.DEFAULT_SLOTS);
        var returns = new AtomicInteger();
        var root = new ReadBuf(ByteBuffer.allocate(256), ignored -> returns.incrementAndGet());
        incoming.insert(storage, 0, root.data(), 0, 1, root);
        root.unref();
        String before = root.trace();
        var chunk = incoming.borrow(0, 1, loans);
        try {
            assertTrue(incoming.isEmpty());
            assertSame(root, chunk.buf(), "the sole root carries the lease without a new owner");
            assertEquals(1, root.refs());
            assertEquals(before, root.trace(), "borrowing the whole piece adds no source ref or unref");
            assertEquals(256, loans.bytes());
        } finally {
            chunk.buf().unref();
        }
        assertEquals(1, returns.get());
        assertEquals(0, loans.bytes());
    }

    @Test void forwardedLeasesChargeOneRootAndPreserveBothSourceAccounts() {
        var first = new ReceiveLoans(256, ReceiveLoans.DEFAULT_SLOTS);
        var second = new ReceiveLoans(256, ReceiveLoans.DEFAULT_SLOTS);
        var destination = new ReceiveLoans(256, ReceiveLoans.DEFAULT_SLOTS);
        var returns = new AtomicInteger();
        var root = new ReadBuf(ByteBuffer.allocate(256), ignored -> returns.incrementAndGet());
        ReadBuf firstSource = java.util.Objects.requireNonNull(first.acquire(root, false));
        ReadBuf secondSource = java.util.Objects.requireNonNull(second.acquire(root, false));
        ReadBuf one = java.util.Objects.requireNonNull(destination.acquire(firstSource, false));
        ReadBuf two = java.util.Objects.requireNonNull(destination.acquire(secondSource, false));
        assertEquals(256, destination.bytes());
        root.unref();
        firstSource.unref();
        secondSource.unref();
        assertEquals(256, first.bytes());
        assertEquals(256, second.bytes());
        one.unref();
        assertEquals(0, first.bytes());
        assertEquals(256, second.bytes());
        assertEquals(256, destination.bytes());
        assertEquals(0, returns.get());
        two.unref();
        assertEquals(0, second.bytes());
        assertEquals(0, destination.bytes());
        assertEquals(1, returns.get());
    }

    @Test void overlappingRangesShareOneStorageReferenceAcrossStreams() throws Exception {
        var storage = new RecvBuffer.Storage(null);
        var loans = new ReceiveLoans(8192, ReceiveLoans.DEFAULT_SLOTS);
        var one = new RecvBuffer();
        var two = new RecvBuffer();
        ByteBuffer data = ByteBuffer.allocate(8192);
        data.put(345, (byte) 7);
        data.put(346, (byte) 8);
        data.put(347, (byte) 9);
        var returns = new AtomicInteger();
        var root = new ReadBuf(data, ignored -> returns.incrementAndGet());
        assertEquals(1, one.insert(storage, 1, data, 346, 1, root));
        assertEquals(2, one.insert(storage, 0, data, 345, 3, root));
        assertEquals(0, one.insert(storage, 0, data, 345, 3, root));
        assertEquals(1, two.insert(storage, 0, data, 345, 1, root));
        assertEquals(2, root.refs());
        assertEquals(0, storage.copied());
        root.unref();

        var chunk = one.borrow(0, 1, loans);
        var alias = two.borrow(0, 1, loans);
        assertSame(chunk.buf().data(), alias.buf().data());
        assertEquals(7, chunk.data().get());
        ByteBuffer out = ByteBuffer.allocate(2);
        assertEquals(2, one.read(1, out, 2));
        assertArrayEquals(new byte[] {8, 9}, out.array());
        assertEquals(0, storage.copied());
        assertEquals(8192, storage.bytes());
        alias.buf().unref();
        assertEquals(0, returns.get());
        chunk.buf().unref();
        assertEquals(1, returns.get());
        assertEquals(0, storage.bytes());
    }

    @Test void budgetFailurePrecedesConsumptionAndNormalReadsStillWork() throws Exception {
        var storage = new RecvBuffer.Storage(null);
        var loans = new ReceiveLoans(256, ReceiveLoans.DEFAULT_SLOTS);
        var incoming = new RecvBuffer();
        for (int i = 0; i < 2; i++) {
            ByteBuffer data = ByteBuffer.allocate(256);
            data.put(0, (byte) (i + 1));
            var root = new ReadBuf(data, null);
            incoming.insert(storage, i, data, 0, 1, root);
            root.unref();
        }
        var first = incoming.borrow(0, 1, loans);
        assertEquals(CoreException.Reason.CHUNK_BUDGET,
                assertThrows(CoreException.class, () -> incoming.borrow(1, 1, loans)).reason());
        ByteBuffer out = ByteBuffer.allocate(1);
        assertEquals(1, incoming.read(1, out, 1));
        assertEquals(2, out.get(0));
        first.buf().unref();
        assertEquals(0, storage.bytes());
    }

}
