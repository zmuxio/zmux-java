package io.zmux;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class InboundPayloadPoolTest {
    private static InboundPayloadPool.Handle acquire(InboundPayloadPool pool, int length) {
        InboundPayloadPool.Handle handle = pool.acquire(length);
        assertNotNull(handle, "test setup should receive a pooled payload handle");
        return handle;
    }

    @Test
    void retainedPayloadStorageIsCappedGlobally() {
        InboundPayloadPool pool = new InboundPayloadPool(8, 2);
        InboundPayloadPool.Handle eight = acquire(pool, 8);
        InboundPayloadPool.Handle seven = acquire(pool, 7);
        InboundPayloadPool.Handle six = acquire(pool, 6);
        byte[] eightBytes = eight.bytes();
        byte[] sevenBytes = seven.bytes();
        byte[] sixBytes = six.bytes();

        eight.release();
        seven.release();
        six.release();

        assertEquals(16L, pool.maxRetainedBytes());
        assertEquals(15L, pool.retainedBytes());
        assertSame(eightBytes, acquire(pool, 8).bytes());
        assertSame(sevenBytes, acquire(pool, 7).bytes());
        assertNotSame(sixBytes, acquire(pool, 6).bytes());
        assertEquals(0L, pool.retainedBytes());
    }

    @Test
    void retainedPayloadStorageKeepsPerLengthCap() {
        InboundPayloadPool pool = new InboundPayloadPool(8, 1);
        InboundPayloadPool.Handle first = acquire(pool, 4);
        InboundPayloadPool.Handle second = acquire(pool, 4);
        byte[] firstBytes = first.bytes();
        byte[] secondBytes = second.bytes();

        first.release();
        second.release();

        assertEquals(4L, pool.retainedBytes());
        assertSame(firstBytes, acquire(pool, 4).bytes());
        assertNotSame(secondBytes, acquire(pool, 4).bytes());
    }
}
