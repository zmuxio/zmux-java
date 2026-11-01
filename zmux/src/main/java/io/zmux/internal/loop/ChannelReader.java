package io.zmux.internal.loop;

import io.zmux.internal.core.ReadBuf;
import java.nio.ByteBuffer;

/**
 * The inbound half's read buffer: unprocessed bytes [pos, lim) of a pooled buffer. Complete
 * records go to the core, which may keep references to the buffer; a partial record stays.
 * An idle half holds no buffer (IMPLEMENTATION.md Section 2.8).
 */
final class ChannelReader {
    private final BufferPool pool;
    ReadBuf buf;
    int pos;
    int lim;

    ChannelReader(BufferPool pool) {
        this.pool = pool;
    }

    /** A buffer positioned for the next read into [lim, capacity). */
    ByteBuffer room() {
        if (buf == null) {
            buf = pool.take();
            pos = lim = 0;
        }
        ByteBuffer b = buf.data();
        b.limit(b.capacity()).position(lim);
        return b;
    }

    int capacity() {
        return buf.data().capacity();
    }

    boolean hasPartial() {
        return buf != null && pos < lim;
    }

    /** After the core consumed [pos, pos + n): keeps the rest, moving it so the next read has room. */
    void consumed(int n) {
        pos += n;
        if (pos == lim) {
            buf.unref();
            buf = null;
            pos = lim = 0;
            return;
        }
        if (pos > 0 && (lim == capacity() || capacity() - lim < 16 << 10)) {
            ByteBuffer src = buf.data();
            if (buf.refs() == 1) {
                src.limit(lim).position(pos);
                ByteBuffer dup = src.slice();
                src.clear();
                src.put(dup);
                lim -= pos;
                pos = 0;
            } else {
                ReadBuf nb = pool.take();
                ByteBuffer d = nb.data();
                d.clear();
                d.put(0, src, pos, lim - pos);
                lim -= pos;
                pos = 0;
                buf.unref();
                buf = nb;
            }
        }
    }

    /** Drops the buffer (the half ended). */
    void release() {
        if (buf != null) {
            buf.unref();
            buf = null;
        }
        pos = lim = 0;
    }
}
