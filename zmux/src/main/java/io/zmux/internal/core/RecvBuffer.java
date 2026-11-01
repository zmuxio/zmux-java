package io.zmux.internal.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Received, unread data of one receiving part as disjoint pieces sorted by
 * offset (IMPLEMENTATION.md Section 3): pieces below {@link #REF_THRESHOLD} are
 * copied, larger ones reference the read buffer.
 */
final class RecvBuffer {
    static final int REF_THRESHOLD = 4096;

    /** One piece: [off, off + data.remaining()), data absolute in its buffer. */
    static final class Piece {
        private long off;
        private ByteBuffer data;
        private final ReadBuf buf;

        Piece(long off, ByteBuffer data, ReadBuf buf) {
            this.off = off;
            this.data = data;
            this.buf = buf;
        }

        long end() { return off + data.remaining(); }
    }

    private final ArrayList<Piece> ps = new ArrayList<>(0);
    private long bytes;

    boolean isEmpty() { return ps.isEmpty(); }

    /** The bytes stored. */
    long bytes() { return bytes; }

    private static Piece newPiece(long off, ByteBuffer d, ReadBuf buf) {
        if (d.remaining() >= REF_THRESHOLD && buf != null) {
            buf.ref();
            return new Piece(off, d.slice(), buf);
        }
        byte[] c = new byte[d.remaining()];
        d.duplicate().get(c);
        return new Piece(off, ByteBuffer.wrap(c), null);
    }

    /** Stores the parts of [off, off + d.remaining()) not already present; returns the new bytes. */
    long insert(long off, ByteBuffer d, ReadBuf buf) {
        long end = off + d.remaining();
        int n = ps.size();
        if (n == 0 || off >= ps.get(n - 1).end()) {
            append(off, d, buf);
            return d.remaining();
        }
        long added = 0, cur = off;
        for (int i = 0; i <= ps.size() && cur < end; i++) {
            long gapEnd = end;
            if (i < ps.size()) {
                Piece p = ps.get(i);
                long pe = p.end();
                if (pe <= cur) continue;
                if (p.off <= cur) {
                    cur = pe;
                    continue;
                }
                gapEnd = Math.min(end, p.off);
            }
            ByteBuffer part = d.slice(d.position() + (int) (cur - off), (int) (gapEnd - cur));
            bytes += part.remaining();
            ps.add(i, newPiece(cur, part, buf));
            added += part.remaining();
            cur = gapEnd;
        }
        return added;
    }

    private void append(long off, ByteBuffer d, ReadBuf buf) {
        bytes += d.remaining();
        int n = ps.size();
        if (n > 0 && (d.remaining() < REF_THRESHOLD || buf == null)) {
            Piece last = ps.get(n - 1);
            if (last.buf == null && last.end() == off && last.data.remaining() + d.remaining() <= REF_THRESHOLD) {
                byte[] c = new byte[last.data.remaining() + d.remaining()];
                int l = last.data.remaining();
                last.data.duplicate().get(c, 0, l);
                d.duplicate().get(c, l, d.remaining());
                last.data = ByteBuffer.wrap(c);
                return;
            }
        }
        ps.add(newPiece(off, d, buf));
    }

    /** End of the data contiguous from off. */
    long contiguous(long off) {
        for (Piece p : ps) {
            if (p.off > off) break;
            long e = p.end();
            if (e > off) off = e;
        }
        return off;
    }

    /** Copies contiguous data at off into dst and consumes it; returns the bytes copied. */
    int read(long off, ByteBuffer dst) {
        int n = 0;
        while (!ps.isEmpty() && dst.hasRemaining()) {
            Piece p = ps.getFirst();
            if (p.off > off + n) break;
            int skip = (int) (off + n - p.off);
            ByteBuffer src = p.data;
            int c = Math.min(dst.remaining(), src.remaining() - skip);
            dst.put(dst.position(), src, src.position() + skip, c);
            dst.position(dst.position() + c);
            n += c;
            if (skip + c == src.remaining()) {
                dropFirst();
            } else {
                src.position(src.position() + skip + c);
                p.off += skip + c;
            }
        }
        bytes -= n;
        return n;
    }

    /** A chunk taken without copying: its data and, when it references a read buffer, a reference for the caller. */
    record Chunk(ByteBuffer data, ReadBuf buf) {}

    /** Consumes up to max contiguous bytes at off without copying; null when none. */
    Chunk take(long off, int max) {
        if (ps.isEmpty() || max <= 0) return null;
        Piece p = ps.getFirst();
        if (p.off > off) return null;
        int skip = (int) (off - p.off);
        int len = Math.min(p.data.remaining() - skip, max);
        ByteBuffer d = p.data.slice(p.data.position() + skip, len);
        ReadBuf buf = p.buf;
        if (buf != null) buf.ref();
        if (skip + len == p.data.remaining()) {
            dropFirst();
        } else {
            if (buf == null) {
                byte[] c = new byte[len];
                d.get(c);
                d = ByteBuffer.wrap(c);
            }
            p.data.position(p.data.position() + skip + len);
            p.off += skip + len;
        }
        bytes -= len;
        return new Chunk(d, buf);
    }

    private void dropFirst() {
        Piece p = ps.removeFirst();
        if (p.buf != null) p.buf.unref();
    }

    /** Drops everything below off; returns the bytes dropped. */
    long discard(long off) {
        long d = 0;
        while (!ps.isEmpty()) {
            Piece p = ps.getFirst();
            long pe = p.end();
            if (p.off >= off) break;
            if (pe <= off) {
                d += p.data.remaining();
                dropFirst();
                continue;
            }
            int cut = (int) (off - p.off);
            d += cut;
            p.data.position(p.data.position() + cut);
            p.off = off;
            break;
        }
        bytes -= d;
        return d;
    }

    /** Drops everything at or above off; returns the bytes dropped. */
    long discardFrom(long off) {
        long d = 0;
        while (!ps.isEmpty()) {
            int i = ps.size() - 1;
            Piece p = ps.get(i);
            long pe = p.end();
            if (pe <= off) break;
            if (p.off >= off) {
                d += p.data.remaining();
                if (p.buf != null) p.buf.unref();
                ps.remove(i);
                continue;
            }
            d += pe - off;
            p.data.limit(p.data.position() + (int) (off - p.off));
            break;
        }
        bytes -= d;
        return d;
    }

    /** Drops everything; returns the bytes dropped. */
    long clear() {
        long d = bytes;
        for (Piece p : ps) if (p.buf != null) p.buf.unref();
        ps.clear();
        bytes = 0;
        return d;
    }
}
