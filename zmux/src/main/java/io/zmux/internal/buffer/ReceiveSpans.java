package io.zmux.internal.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/** Unconsumed owned ranges of a byte carrier. Indices start at the current read position. */
public final class ReceiveSpans {
    private static final long SPAN_BYTES = 64;
    /** One contiguous range with its owner and the logical end in the carry. */
    public static final class Span {
        private final ReadBuf owner;
        private final ByteBuffer data;
        private int to;
        private long end;

        private Span(ReadBuf owner, ByteBuffer data, int to, long end) {
            this.owner = owner;
            this.data = data;
            this.to = to;
            this.end = end;
        }

        public ReadBuf owner() { return owner; }
        public ByteBuffer data() { return data; }
        public int to() { return to; }
    }

    private final ArrayList<Span> ranges = new ArrayList<>(2);
    private long read, written;
    private long footprint;
    private int first;

    private void append(ReadBuf owner, ByteBuffer data, int from, int to) {
        if (first != 0 && first >= ranges.size() / 2) {
            ranges.subList(0, first).clear();
            first = 0;
        }
        long end = written + to - from;
        ranges.add(new Span(owner, data, to, end));
        footprint += owner.capacity() + SPAN_BYTES;
        written = end;
    }

    /** Publishes more of an existing tail, or retains this block once for the carry. */
    public void retain(ReadBuf owner, ByteBuffer data, int from, int to) {
        if (from == to) return;
        if (first < ranges.size()) {
            Span tail = ranges.getLast();
            if (tail.owner == owner && tail.data == data && tail.to == from) {
                written += to - from;
                tail.to = to;
                tail.end = written;
                return;
            }
        }
        owner.ref();
        try {
            append(owner, data, from, to);
        } catch (RuntimeException | Error failure) {
            owner.unref();
            throw failure;
        }
    }

    public int size() { return Math.toIntExact(written - read); }

    /** A conservative bound including retained allocations and span descriptors. */
    public long footprint() { return footprint; }

    /** Upper bound for span bookkeeping when no more than count ranges are retained. */
    public static long spanOverhead(int count) { return count * SPAN_BYTES; }

    public Span span(int index) {
        long at = read + index;
        int lo = first, hi = ranges.size();
        while (lo < hi) {
            int mid = lo + (hi - lo) / 2;
            if (ranges.get(mid).end <= at) lo = mid + 1;
            else hi = mid;
        }
        return ranges.get(lo);
    }

    public int position(Span span, int index) {
        return span.to - Math.toIntExact(span.end - read - index);
    }

    public byte get(int index) {
        Span span = span(index);
        return span.data().get(position(span, index));
    }

    public static byte get(ByteBuffer data, ReceiveSpans spans, int index) {
        return spans == null ? data.get(index) : spans.get(index);
    }

    public static void copy(ByteBuffer data, ReceiveSpans spans, int index, byte[] dst, int offset, int length) {
        if (spans == null) data.get(index, dst, offset, length);
        else spans.copy(index, dst, offset, length);
    }

    public void copy(int from, byte[] dst, int offset, int length) {
        while (length != 0) {
            Span span = span(from);
            int pos = position(span, from);
            int count = Math.min(span.to() - pos, length);
            span.data().get(pos, dst, offset, count);
            from += count;
            offset += count;
            length -= count;
        }
    }

    public void write(int from, ByteBuffer dst, int length) {
        while (length != 0) {
            Span span = span(from);
            int pos = position(span, from);
            int count = Math.min(span.to() - pos, length);
            dst.put(dst.position(), span.data(), pos, count);
            dst.position(dst.position() + count);
            from += count;
            length -= count;
        }
    }

    public void consume(int count) {
        read += count;
        while (first < ranges.size()) {
            Span span = ranges.get(first);
            if (span.end > read) break;
            ranges.set(first++, null);
            footprint -= span.owner().capacity() + SPAN_BYTES;
            span.owner().unref();
        }
        if (first == ranges.size()) {
            ranges.clear();
            read = written = 0;
            first = 0;
        }
    }

    public void release() { consume(size()); }
}
