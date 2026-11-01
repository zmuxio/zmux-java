package io.zmux.internal.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * The retained bytes [base, end) of a sending part, as slices of send blocks,
 * owned buffers or received buffers passed on without a copy.
 */
final class SendQueue {
    /** One segment; data is a view whose position..limit are the bytes. */
    record Seg(ByteBuffer data, SendBlock block, ReadBuf buf) {}

    private final ArrayList<Seg> segs = new ArrayList<>(0);
    private SendBlock tail;
    private long base;
    long base() { return base; }
    private long end;
    long end() { return end; }

    /** Appends p by copying into blocks. */
    void write(ByteBuffer p) {
        end += p.remaining();
        while (p.hasRemaining()) {
            if (tail != null && tail.n() < SendBlock.SIZE && !segs.isEmpty() && segs.getLast().block == tail) {
                int c = Math.min(p.remaining(), SendBlock.SIZE - tail.n());
                tail.append(p, c);
                Seg last = segs.removeLast();
                ByteBuffer d = last.data;
                segs.add(new Seg(ByteBuffer.wrap(tail.b, d.position(), d.remaining() + c), tail, null));
                continue;
            }
            SendBlock blk = new SendBlock();
            int c = Math.min(p.remaining(), SendBlock.SIZE);
            blk.append(p, c);
            tail = blk;
            segs.add(new Seg(ByteBuffer.wrap(blk.b, 0, c), blk, null));
        }
    }

    /** Appends a received buffer's bytes without copying; takes one reference of rb. */
    void writeRef(ByteBuffer p, ReadBuf rb) {
        end += p.remaining();
        if (!segs.isEmpty() && p.hasArray()) {
            Seg last = segs.getLast();
            ByteBuffer d = last.data;
            // The bytes continue the last segment of the same buffer: extend it under its reference.
            if (last.buf == rb && d.hasArray() && d.array() == p.array()
                    && d.arrayOffset() + d.limit() == p.arrayOffset() + p.position()) {
                segs.set(segs.size() - 1, new Seg(ByteBuffer.wrap(d.array(), d.arrayOffset() + d.position(),
                        d.remaining() + p.remaining()).slice(), null, rb));
                tail = null;
                return;
            }
        }
        rb.ref();
        segs.add(new Seg(p.slice(), null, rb));
        tail = null;
    }

    /** Appends bytes that pn keeps alive, without a copy. */
    void writePin(ByteBuffer p, Pin pn) {
        end += p.remaining();
        pn.ref();
        segs.add(new Seg(p.slice(), pn.block(), pn.buf()));
        tail = null;
    }

    /** Inserts owned bytes before base (the preamble at identifier allocation). */
    void prepend(byte[] p) {
        segs.addFirst(new Seg(ByteBuffer.wrap(p), null, null));
        end += p.length;
    }

    /** A view of up to max bytes at off (base <= off < end) in one segment, and what keeps it alive. */
    record Peek(ByteBuffer data, Pin pin) {}

    Peek peek(long off, int max) {
        long pos = base;
        for (Seg sg : segs) {
            int l = sg.data.remaining();
            if (off < pos + l) {
                int skip = (int) (off - pos);
                int len = Math.min(l - skip, max);
                return new Peek(sg.data.slice(sg.data.position() + skip, len),
                        sg.block == null && sg.buf == null ? Pin.NONE : new Pin(sg.block, sg.buf));
            }
            pos += l;
        }
        return null;
    }

    /** Drops bytes below off. */
    void release(long off) {
        while (base < off && !segs.isEmpty()) {
            Seg sg = segs.getFirst();
            int l = sg.data.remaining();
            if (base + l <= off) {
                if (sg.block != null) {
                    if (sg.block == tail) tail = null;
                    sg.block.unref();
                }
                if (sg.buf != null) sg.buf.unref();
                base += l;
                segs.removeFirst();
                continue;
            }
            int cut = (int) (off - base);
            sg.data.position(sg.data.position() + cut);
            base = off;
        }
        if (segs.isEmpty()) base = off;
    }

    /** Releases everything. */
    void drop() {
        for (Seg sg : segs) {
            if (sg.block != null) sg.block.unref();
            if (sg.buf != null) sg.buf.unref();
        }
        segs.clear();
        tail = null;
        base = end;
    }
}
