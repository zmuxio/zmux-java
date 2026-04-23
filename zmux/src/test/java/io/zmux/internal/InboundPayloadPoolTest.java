package io.zmux.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class InboundPayloadPoolTest {
    @Test
    void retainedPayloadStorageIsCappedGlobally() {
        InboundPayloadPool pool = new InboundPayloadPool(8, 2);
        InboundPayloadPool.Handle eight = pool.acquire(8);
        InboundPayloadPool.Handle seven = pool.acquire(7);
        InboundPayloadPool.Handle six = pool.acquire(6);
        byte[] eightBytes = eight.bytes();
        byte[] sevenBytes = seven.bytes();
        byte[] sixBytes = six.bytes();

        eight.release();
        seven.release();
        six.release();

        assertEquals(16L, pool.maxRetainedBytes());
        assertEquals(15L, pool.retainedBytes());
        assertSame(eightBytes, pool.acquire(8).bytes());
        assertSame(sevenBytes, pool.acquire(7).bytes());
        assertNotSame(sixBytes, pool.acquire(6).bytes());
        assertEquals(0L, pool.retainedBytes());
    }

    @Test
    void retainedPayloadStorageKeepsPerLengthCap() {
        InboundPayloadPool pool = new InboundPayloadPool(8, 1);
        InboundPayloadPool.Handle first = pool.acquire(4);
        InboundPayloadPool.Handle second = pool.acquire(4);
        byte[] firstBytes = first.bytes();
        byte[] secondBytes = second.bytes();

        first.release();
        second.release();

        assertEquals(4L, pool.retainedBytes());
        assertSame(firstBytes, pool.acquire(4).bytes());
        assertNotSame(secondBytes, pool.acquire(4).bytes());
    }
}
