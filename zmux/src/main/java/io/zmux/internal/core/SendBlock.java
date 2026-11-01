package io.zmux.internal.core;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A send buffer of {@link #SIZE} bytes. The count is the send queue's hold plus
 * every batch that references it; atomic because batches are released by the
 * writer. Bytes below {@link #n} never change while referenced.
 */
final class SendBlock {
    static final int SIZE = 16384;

    final byte[] b = new byte[SIZE];
    private int n;
    int n() { return n; }
    private final AtomicInteger refs = new AtomicInteger(1);

    /** Copies c bytes of p after the block's bytes. */
    void append(java.nio.ByteBuffer p, int c) {
        p.get(b, n, c);
        n += c;
    }

    void ref() { refs.incrementAndGet(); }

    void unref() {
        if (refs.decrementAndGet() < 0) throw new IllegalStateException("send block released twice");
    }
}
