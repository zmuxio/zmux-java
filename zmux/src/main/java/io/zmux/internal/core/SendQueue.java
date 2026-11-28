package io.zmux.internal.core;

import io.zmux.internal.buffer.CarrierCharge;

import io.zmux.internal.buffer.ReadBuf;
import java.nio.ByteBuffer;

/** The retained bytes [base, end) of one sending direction. */
final class SendQueue {
    private static final class Segment {
        private ByteBuffer data;
        private final ReadBuf owner;
        private final boolean paidView;
        private CarrierCharge.Slice receipt;
        private int receiptEnd;
        private Segment next;

        Segment(ByteBuffer data, ReadBuf owner, boolean paidView, CarrierCharge.Slice receipt) {
            this.data = data;
            this.owner = owner;
            this.paidView = paidView;
            this.receipt = receipt;
            if (receipt != null) {
                receipt.charge().retain();
                receiptEnd = data.limit();
            }
        }

    }

    private Segment first;
    private Segment last;
    private ReadBuf writable;
    private int liveReceipts;
    private long base;
    private long end;
    private SendBacking backing;

    long base() { return base; }
    long end() { return end; }

    void backing(SendBacking backing) { this.backing = backing; }

    SendBacking backing() {
        if (backing == null) backing = new SendBacking(null, null, SendBacking.FLOOR, 0);
        return backing;
    }

    /** Appends as many bytes as the existing tail and newly reserved blocks can back. */
    int write(ByteBuffer p) {
        int start = p.position();
        while (p.hasRemaining()) {
            if (writable == null || last.data.limit() == SendBacking.BLOCK_BYTES) {
                ReadBuf root = backing().block();
                if (root == null) break;
                writable = root;
                append(new Segment(root.data().duplicate().limit(0), root, false, null));
            }
            ByteBuffer tail = last.data;
            int written = tail.limit();
            int count = Math.min(p.remaining(), SendBacking.BLOCK_BYTES - written);
            writable.data().put(written, p, p.position(), count);
            p.position(p.position() + count);
            tail.limit(written + count);
            end += count;
        }
        return p.position() - start;
    }

    /** Takes a queue reference and, for a foreign view, its paid descriptor. */
    void writeRef(ByteBuffer p, ReadBuf root, boolean paidView, CarrierCharge.Slice receipt) {
        end += p.remaining();
        writable = null;
        if (last != null && last.owner == root && receipt == null && last.receipt == null
                && p.hasArray() && last.data.hasArray()) {
            ByteBuffer prior = last.data;
            if (prior.array() == p.array() && prior.arrayOffset() + prior.limit() == p.arrayOffset() + p.position()) {
                last.data = ByteBuffer.wrap(prior.array(), prior.arrayOffset() + prior.position(),
                        prior.remaining() + p.remaining());
                if (paidView) backing().releaseSlice();
                return;
            }
        }
        root.ref();
        append(new Segment(p.slice(), root, paidView, receipt));
        if (receipt != null) liveReceipts++;
    }

    private void append(Segment segment) {
        if (last == null) first = segment;
        else last.next = segment;
        last = segment;
    }

    /** Inserts the preamble in its own prepaid backing before existing data. */
    void prepend(byte[] p) {
        ReadBuf owner = backing().preamble(p);
        var segment = new Segment(owner.data(), owner, false, null);
        segment.next = first;
        first = segment;
        if (last == null) last = segment;
        end += p.length;
    }

    /** A view of up to max bytes at off (base <= off < end) in one segment. */
    record Peek(ByteBuffer data, Pin pin, CarrierCharge.Slice receipt, int position, int receiptBytes) {
        void attachReceipt(Batch batch) {
            if (receiptBytes == 0) return;
            long start = receipt.offset() + position;
            batch.charge(receipt.charge(), start, start + receiptBytes);
        }
    }

    Peek peek(long off, int max) {
        long pos = base;
        for (Segment segment = first; segment != null; segment = segment.next) {
            int length = segment.data.remaining();
            if (off < pos + length) {
                int skip = (int) (off - pos);
                int len = Math.min(length - skip, max);
                int position = segment.data.position() + skip;
                int receipted = segment.receipt == null ? 0 : Math.clamp(segment.receiptEnd - position, 0, len);
                return new Peek(segment.data.slice(position, len), new Pin(segment.owner), segment.receipt, position, receipted);
            }
            pos += length;
        }
        return null;
    }

    /** Drops bytes below off. A batch may still hold the underlying backing. */
    void release(long off) {
        while (base < off && first != null) {
            int length = first.data.remaining();
            if (base + length <= off) {
                base += length;
                unlink();
                continue;
            }
            first.data.position(first.data.position() + (int) (off - base));
            if (first.data.position() >= first.receiptEnd) clearReceipt(first);
            base = off;
        }
        if (first == null) base = off;
    }

    private void unlink() {
        Segment segment = first;
        first = segment.next;
        if (first == null) last = null;
        if (segment.owner == writable) writable = null;
        try {
            clearReceipt(segment);
        } finally {
            try { segment.owner.unref(); }
            finally { if (segment.paidView) backing().releaseSlice(); }
        }
    }

    private void clearReceipt(Segment segment) {
        CarrierCharge.Slice released = segment.receipt;
        if (released == null) return;
        segment.receipt = null;
        segment.receiptEnd = 0;
        liveReceipts--;
        released.charge().release();
    }

    /** The reset can abandon receipt ownership while already queued payload still has other pins. */
    void keepReceipts(long end) {
        if (liveReceipts == 0) return;
        long pos = base;
        for (Segment segment = first; segment != null; segment = segment.next) {
            int count = segment.data.remaining();
            if (segment.receipt != null && end - pos < count) {
                if (end <= pos) clearReceipt(segment);
                else segment.receiptEnd = Math.min(segment.receiptEnd, segment.data.position() + (int) (end - pos));
            }
            pos += count;
        }
    }

    void drop() {
        while (first != null) unlink();
        base = end;
    }
}
