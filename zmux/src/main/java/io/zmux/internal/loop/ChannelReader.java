package io.zmux.internal.loop;

import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.buffer.ReceiveSpans;
import java.nio.ByteBuffer;

/**
 * The inbound half's read buffer: unprocessed bytes [pos, lim) of a pooled buffer. Complete
 * records go to the core, which may keep references to the buffer; a partial record stays.
 * An idle half holds no buffer.
 */
final class ChannelReader {
    /** Two carried backings, the active read, and a replacement during compaction. */
    static final long WORKING_BYTES = 4L * BufferPool.SIZE + ReceiveSpans.spanOverhead(4);

    private final BufferPool pool;
    ReadBuf buf;
    int pos;
    int lim;
    private ReceiveSpans ranges;
    private boolean compacted;

    ChannelReader(BufferPool pool) {
        this.pool = pool;
    }

    /** A buffer positioned for the next read into [lim, capacity). */
    ByteBuffer room() {
        return room(false, false, 1);
    }

    /** Reserves a tail for a socket read, borrowed input, or one TLS unwrap. */
    ByteBuffer room(boolean segmented, boolean idle, int minimum) {
        if (segmented && buf != null && capacity() - lim < minimum) stash();
        if (buf == null) {
            buf = idle && !hasRanges() ? pool.takeIdle() : pool.take();
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
        return hasRanges() || (buf != null && pos < lim);
    }

    boolean hasRanges() {
        return ranges != null && ranges.size() != 0;
    }

    int unreadBytes() {
        return (ranges == null ? 0 : ranges.size()) + lim - pos;
    }

    void hold(ReadBuf owner, ByteBuffer data, int from, int to) {
        stash();
        if (ranges == null) ranges = new ReceiveSpans();
        ranges.retain(owner, data, from, to);
    }

    private void publishTail() {
        if (buf != null && pos != lim) {
            if (ranges == null) ranges = new ReceiveSpans();
            ranges.retain(buf, buf.data(), pos, lim);
            pos = lim;
        }
    }

    private void stash() {
        if (buf == null) return;
        publishTail();
        releaseTail();
    }

    private void releaseTail() {
        if (buf != null) buf.unref();
        buf = null;
        pos = lim = 0;
        compacted = false;
    }

    ReceiveSpans ranges() {
        publishTail();
        return ranges;
    }

    /** A sparse suffix keeps at most two batch capacities between delivery calls. */
    int compactSparseCarry() {
        if (!hasRanges()) return 0;
        publishTail();
        if (ranges.footprint() <= 2L * BufferPool.SIZE + ReceiveSpans.spanOverhead(2)) return 0;
        int bytes = ranges.size();
        // A long batch may be in an admission replay, before framing has examined it.
        if (bytes > BufferPool.SIZE) return 0;
        ReadBuf packed = pool.take();
        try {
            ranges.write(0, packed.data(), bytes);
        } catch (RuntimeException | Error failure) {
            packed.unref();
            throw failure;
        }
        ranges.release();
        releaseTail();
        buf = packed;
        lim = bytes;
        compacted = true;
        return bytes;
    }

    boolean compacted() { return compacted; }

    byte[] copyUnread() {
        byte[] bytes = new byte[unreadBytes()];
        int head = ranges == null ? 0 : ranges.size();
        if (head != 0) ranges.copy(0, bytes, 0, head);
        if (buf != null) buf.data().get(pos, bytes, head, lim - pos);
        return bytes;
    }

    /** Moves the saved input to a reader that will fill future blocks from its own pool. */
    void moveTo(ChannelReader next) {
        if (next.buf != null) throw new IllegalStateException("target reader already holds input");
        next.buf = buf;
        next.pos = pos;
        next.lim = lim;
        next.ranges = ranges;
        next.compacted = compacted;
        buf = null;
        ranges = null;
        compacted = false;
        pos = lim = 0;
    }

    /** Keeps an incomplete suffix. Only framing gates that require contiguous input move bytes. */
    void consumed(int n, boolean segmented) {
        if (hasRanges()) {
            ranges.consume(n);
            if (!hasRanges()) releaseTail();
            return;
        }
        // The delivery ended the half, which dropped the buffer.
        if (buf == null) return;
        pos += n;
        if (pos == lim) {
            releaseTail();
            return;
        }
        if (!segmented && pos > 0 && (lim == capacity() || capacity() - lim < 16 << 10)) {
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
        if (ranges != null) ranges.release();
        releaseTail();
    }
}
