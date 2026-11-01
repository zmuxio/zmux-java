package io.zmux.internal.loop;

import io.zmux.internal.core.ReadBuf;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Read buffers shared by the sessions of one loop (IMPLEMENTATION.md Section 2.8). A buffer
 * returns here when its last reference goes, from any thread; idle buffers are kept up to the
 * configured idle bytes, the rest are left to the garbage collector.
 */
public final class BufferPool {
    static final int SIZE = 128 << 10;
    /** Idle bytes a loop keeps by default: 16 buffers. */
    public static final long DEFAULT_IDLE_BYTES = 16L * SIZE;

    private final int maxIdle;

    BufferPool(long idleBytes) {
        this.maxIdle = (int) Math.min(Integer.MAX_VALUE, idleBytes / SIZE);
    }

    private final ConcurrentLinkedQueue<ByteBuffer> idle = new ConcurrentLinkedQueue<>();
    private final AtomicInteger idleCount = new AtomicInteger();
    private final AtomicInteger outstanding = new AtomicInteger();

    /** A buffer with one reference held by the caller. */
    ReadBuf take() {
        ByteBuffer b = idle.poll();
        if (b == null) {
            b = ByteBuffer.allocateDirect(SIZE);
        } else {
            idleCount.decrementAndGet();
        }
        b.clear();
        outstanding.incrementAndGet();
        return new ReadBuf(b, this::give);
    }

    private void give(ReadBuf rb) {
        outstanding.decrementAndGet();
        if (idleCount.incrementAndGet() <= maxIdle) {
            idle.add(rb.data());
        } else {
            idleCount.decrementAndGet();
        }
    }

    /** Buffers taken and not yet returned (tests: leak checks). */
    int outstanding() {
        return outstanding.get();
    }
}
