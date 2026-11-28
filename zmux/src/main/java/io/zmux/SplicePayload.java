package io.zmux;

import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.core.StreamRecv;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;

/** Direction-owned immutable segments and their local backing charges. */
final class SplicePayload implements AutoCloseable {
    static final class BudgetExceeded extends IllegalStateException {
        BudgetExceeded() { super("splice_budget"); }
    }

    static final class Budget {
        private final SpliceRoots roots = new SpliceRoots();
        private final long maxBytes;
        private final int maxSegments;

        void requireRoom(long extra) {
            if (extra > maxBytes - bytes()) throw new BudgetExceeded();
        }

        Budget(SpliceLimits limits) {
            maxBytes = limits.maxBackingBytes();
            maxSegments = limits.maxSegments();
        }

        boolean fits(Object key, int capacity) {
            return roots.extra(key, capacity) <= maxBytes - bytes();
        }

        long bytes() { return roots.bytes(); }

        byte[] allocate(int size) {
            if (!fits(null, size)) throw new BudgetExceeded();
            return new byte[size];
        }

        void acquire(Object key, int capacity) {
            if (!fits(key, capacity)) throw new BudgetExceeded();
            roots.retain(key, capacity);
        }

        void release(Object key) { roots.release(key); }
    }

    private record Piece(ByteBuffer bytes, ReadBuf owner, Object key, int capacity) {
        static Piece of(StreamRecv.Chunk chunk) {
            ReadBuf owner = chunk.buf();
            ByteBuffer data = chunk.data();
            return new Piece(data.slice(), owner, owner == null ? data : owner.root(),
                    owner == null ? data.capacity() : owner.capacity());
        }
    }

    private final Budget budget;
    private final List<Piece> pieces = new ArrayList<>();
    private int length;

    SplicePayload(Budget budget) { this.budget = budget; }

    static SplicePayload of(Budget budget, byte[] data) {
        return of(budget, data, data.length);
    }

    static SplicePayload of(Budget budget, byte[] data, int length) {
        SplicePayload payload = new SplicePayload(budget);
        if (data.length != 0) {
            ByteBuffer view = ByteBuffer.wrap(data, 0, length).slice();
            ReadBuf owner = new ReadBuf(view, data.length, null);
            try { payload.add(new Piece(view, owner, data, data.length)); }
            finally { owner.unref(); }
        }
        return payload;
    }

    Budget budget() { return budget; }
    int length() { return length; }
    int segments() { return pieces.size(); }

    boolean append(StreamRecv.Chunk chunk) {
        Piece piece = Piece.of(chunk);
        if (pieces.size() >= budget.maxSegments || !budget.fits(piece.key(), piece.capacity())) return false;
        add(piece);
        return true;
    }

    private void add(Piece piece) {
        budget.acquire(piece.key(), piece.capacity());
        if (piece.owner() != null) piece.owner().ref();
        pieces.add(piece);
        length += piece.bytes().remaining();
    }

    byte get(int index) {
        Objects.checkIndex(index, length);
        for (Piece piece : pieces) {
            int n = piece.bytes().remaining();
            if (index < n) return piece.bytes().get(index);
            index -= n;
        }
        throw new AssertionError();
    }

    void copyTo(int offset, ByteBuffer destination, int size) {
        Objects.checkFromIndexSize(offset, size, length);
        if (size > destination.remaining()) throw new java.nio.BufferOverflowException();
        for (Piece piece : pieces) {
            if (size == 0) break;
            int n = piece.bytes().remaining();
            if (offset >= n) { offset -= n; continue; }
            int take = Math.min(size, n - offset);
            destination.put(piece.bytes().slice(offset, take));
            size -= take;
            offset = 0;
        }
    }

    SplicePayload slice(int offset, int size) {
        Objects.checkFromIndexSize(offset, size, length);
        SplicePayload result = new SplicePayload(budget);
        try {
            for (Piece piece : pieces) {
                if (size == 0) break;
                int n = piece.bytes().remaining();
                if (offset >= n) { offset -= n; continue; }
                int take = Math.min(size, n - offset);
                result.add(new Piece(piece.bytes().slice(offset, take), piece.owner(), piece.key(), piece.capacity()));
                size -= take;
                offset = 0;
            }
            return result;
        } catch (RuntimeException | Error failure) {
            result.close();
            throw failure;
        }
    }

    /** Returns a view and takes a new reference for the splice's reliable writer. */
    StreamRecv.Chunk chunk(int index, int offset, int max) {
        Piece piece = pieces.get(index);
        ByteBuffer view = piece.bytes().slice(offset, Math.min(max, piece.bytes().remaining() - offset));
        if (piece.owner() != null) piece.owner().ref();
        return new StreamRecv.Chunk(view, piece.owner(), false);
    }

    int segmentLength(int index) { return pieces.get(index).bytes().remaining(); }

    /** Only independent immutable heap allocations can be leased to a mirror consumer. */
    boolean mirrorRanges(int prefix, List<ByteBuffer> views, IdentityHashMap<Object, SpliceRoots.Size> roots) {
        for (Piece piece : pieces) {
            if (prefix == 0) break;
            if (!(piece.key() instanceof byte[])) return false;
            int n = Math.min(prefix, piece.bytes().remaining());
            roots.put(piece.key(), new SpliceRoots.Size(piece.capacity()));
            views.add(piece.bytes().slice(0, n).asReadOnlyBuffer());
            prefix -= n;
        }
        return true;
    }

    void roots(IdentityHashMap<Object, SpliceRoots.Size> into) { budget.roots.snapshot(into); }

    int copyForDatagram() {
        return pieces.size() == 1 && pieces.getFirst().bytes().hasArray() ? 0 : length;
    }

    /** The group keeps the borrowed reference through the target's atomic enqueue decision. */
    StreamRecv.Chunk datagram() {
        if (pieces.size() == 1 && pieces.getFirst().bytes().hasArray()) {
            Piece piece = pieces.getFirst();
            return new StreamRecv.Chunk(piece.bytes().duplicate(), piece.owner(), false);
        }
        byte[] joined = new byte[length];
        copyTo(0, ByteBuffer.wrap(joined), length);
        return new StreamRecv.Chunk(ByteBuffer.wrap(joined), null, false);
    }

    @Override public void close() {
        for (Piece piece : pieces) {
            budget.release(piece.key());
            if (piece.owner() != null) piece.owner().unref();
        }
        pieces.clear();
        length = 0;
    }
}
