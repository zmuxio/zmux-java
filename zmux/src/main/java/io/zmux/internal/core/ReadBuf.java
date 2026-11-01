package io.zmux.internal.core;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A read buffer of a pipe. Pieces of received stream data may reference it; the
 * count is atomic because a reader on another thread can drop the last reference.
 * When the count reaches zero the buffer goes back to its pool.
 */
public final class ReadBuf {
    private final ByteBuffer data;
    private final AtomicInteger refs = new AtomicInteger(1);
    private final Consumer<ReadBuf> free;

    /** A buffer holding one reference (the creator's). */
    public ReadBuf(ByteBuffer data, Consumer<ReadBuf> free) {
        this.data = data;
        this.free = free;
    }

    public ByteBuffer data() { return data; }

    public void ref() { refs.incrementAndGet(); }

    public void unref() {
        int n = refs.decrementAndGet();
        if (n == 0) {
            if (free != null) free.accept(this);
        } else if (n < 0) {
            throw new IllegalStateException("read buffer released twice");
        }
    }

    public int refs() { return refs.get(); }
}
