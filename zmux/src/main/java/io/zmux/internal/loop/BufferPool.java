package io.zmux.internal.loop;

import io.zmux.internal.core.ReadBuf;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Read buffers shared by the sessions of one loop. A buffer
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
    /** The buffers taken and not yet returned, when {@link ReadBuf#TRACKING} (tests). */
    private final java.util.Set<ReadBuf> live = ReadBuf.TRACKING ? java.util.concurrent.ConcurrentHashMap.newKeySet() : null;

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
        ReadBuf rb = new ReadBuf(b, this::give);
        if (live != null) live.add(rb);
        return rb;
    }

    private void give(ReadBuf rb) {
        outstanding.decrementAndGet();
        if (live != null) live.remove(rb);
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

    /** The reference histories of the buffers not yet returned; empty unless tracking (tests). */
    String outstandingTrace() {
        if (live == null) return "";
        var out = new StringBuilder();
        for (ReadBuf rb : live) out.append("outstanding read buffer, refs ").append(rb.refs()).append('\n').append(rb.trace());
        return out.toString();
    }
}
