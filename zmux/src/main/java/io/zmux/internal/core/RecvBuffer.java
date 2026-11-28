package io.zmux.internal.core;

import io.zmux.internal.buffer.OffsetPageBits;

import io.zmux.internal.memory.SharedMemory;

import io.zmux.internal.buffer.ReceiveLoans;

import io.zmux.internal.buffer.IdentityIndex;
import io.zmux.internal.buffer.ReadBuf;

import java.nio.ByteBuffer;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/** Unread disjoint intervals. In-order input needs only the head and tail links. */
final class RecvBuffer {
    /** An owned range retained on the ordinary receive path. */
    static final class Piece {
        private long off;
        private final ByteBuffer data;
        private int pos;
        private int length;
        private Storage.Block block;
        private Piece prev, next;

        Piece(long off, ByteBuffer data, int pos, int length) {
            this.off = off;
            this.data = data;
            this.pos = pos;
            this.length = length;
        }

        long end() { return off + length; }
    }

    private Piece first, last;
    /** Set once input needs a gap search, released when the queue is drained. */
    private TreeMap<Long, Piece> index;
    private Storage storage;
    private long bytes;
    private PackedReceive packed;

    boolean isEmpty() { return bytes() == 0; }

    long bytes() { return packed == null ? bytes : packed.bytes(); }

    boolean packed() { return packed != null; }

    /** Retains only the portions not already queued. */
    long insert(Storage in, long off, ByteBuffer data, int pos, int len, ReadBuf owner) {
        if (len == 0) return 0;
        if (packed != null) return packed.insert(off, data, pos, len);
        storage = in;
        if (last == null || off >= last.end()) {
            if (insertAfter(last, off, data, pos, len, owner)) return len;
            return pack().insert(off, data, pos, len);
        }
        if (index == null) {
            index = new TreeMap<>();
            for (Piece p = first; p != null; p = p.next) index.put(p.off, p);
        }
        var floor = index.floorEntry(off);
        Piece previous = floor == null ? null : floor.getValue();
        Piece next = previous == null ? first : previous.next;
        long cur = previous == null ? off : Math.max(off, previous.end());
        long end = off + len;
        long added = 0;
        while (cur < end) {
            long hi = next == null ? end : Math.min(end, next.off);
            if (cur < hi) {
                int n = (int) (hi - cur);
                if (!insertAfter(previous, cur, data, pos + (int) (cur - off), n, owner)) {
                    return added + pack().insert(off, data, pos, len);
                }
                added += n;
                cur = hi;
            }
            if (next == null || cur >= end) break;
            cur = Math.max(cur, next.end());
            previous = next;
            next = next.next;
        }
        return added;
    }

    private boolean insertAfter(Piece prev, long off, ByteBuffer data, int pos, int len, ReadBuf owner) {
        if (prev != null && prev.block.source == owner && prev.data == data && prev.end() == off && prev.pos + prev.length == pos) {
            prev.length += len;
            bytes += len;
            return true;
        }
        if (!storage.reserveView(ReceiveLayout.VIEW_PIECE)) return false;
        Storage.Block block = storage.block(owner);
        if (block == null) {
            storage.releaseView(ReceiveLayout.VIEW_PIECE);
            return false;
        }
        bytes += len;
        Piece p = new Piece(off, data, pos, len);
        p.block = block;
        storage.add(block);
        p.prev = prev;
        p.next = prev == null ? first : prev.next;
        if (prev == null) first = p;
        else prev.next = p;
        if (p.next == null) last = p;
        else p.next.prev = p;
        if (index != null) index.put(off, p);
        return true;
    }

    private PackedReceive pack() {
        packed = new PackedReceive(storage);
        while (first != null) {
            Piece piece = first;
            packed.insert(piece.off, piece.data, piece.pos, piece.length);
            drop(piece);
        }
        return packed;
    }

    /** End of the contiguous data from off. */
    long contiguous(long off) {
        if (packed != null) return packed.contiguous(off);
        for (Piece p = first; p != null && p.off <= off; p = p.next) off = Math.max(off, p.end());
        return off;
    }

    /** Copies the available prefix to its final destination. */
    int read(long off, ByteBuffer dst, int max) {
        if (packed != null) return packed.read(off, dst, max);
        int n = 0;
        while (first != null && first.off == off + n && n < max) {
            Piece p = first;
            int count = Math.min(max - n, p.length);
            dst.put(dst.position(), p.data, p.pos, count);
            dst.position(dst.position() + count);
            consume(p, count);
            n += count;
        }
        return n;
    }

    /** Takes one view. The caller gets a reference across the execution boundary. */
    StreamRecv.Chunk take(long off, int max) {
        if (packed != null) return packed.take(off, max);
        Piece p = available(off, max);
        if (p == null) return null;
        ReadBuf buf = p.block.source;
        int len = Math.min(p.length, max);
        ByteBuffer view = p.data.slice(p.pos, len);
        boolean move = moveBacking(p, len);
        if (!move) buf.ref();
        return take(p, view, buf, move);
    }

    StreamRecv.Chunk borrow(long off, int max, ReceiveLoans loans) throws CoreException {
        if (packed != null) return packed.borrow(off, max, loans);
        Piece p = available(off, max);
        if (p == null) return null;
        int len = Math.min(p.length, max);
        ByteBuffer view = p.data.slice(p.pos, len);
        boolean move = moveBacking(p, len);
        ReadBuf loan = loans.acquire(p.block.source, move);
        if (loan == null) throw CoreException.of(CoreException.Reason.CHUNK_BUDGET);
        return take(p, view, loan, move);
    }

    private static boolean moveBacking(Piece p, int len) {
        return len == p.length && p.block.pieces == 1;
    }

    private Piece available(long off, int max) {
        return first != null && first.off == off && max > 0 ? first : null;
    }

    private StreamRecv.Chunk take(Piece p, ByteBuffer view, ReadBuf buf, boolean move) {
        if (move) drop(p, true);
        else consume(p, view.remaining());
        return new StreamRecv.Chunk(view, buf, false);
    }

    private void consume(Piece p, int n) {
        if (n == p.length) {
            drop(p);
            return;
        }
        if (index != null) index.remove(p.off);
        p.off += n;
        p.pos += n;
        trim(p, n);
        if (index != null) index.put(p.off, p);
    }

    private void trim(Piece p, int n) {
        p.length -= n;
        bytes -= n;
    }

    private void drop(Piece p) {
        drop(p, false);
    }

    private void drop(Piece p, boolean moveBacking) {
        if (p.prev == null) first = p.next;
        else p.prev.next = p.next;
        if (p.next == null) last = p.prev;
        else p.next.prev = p.prev;
        if (index != null) {
            index.remove(p.off);
            if (first == null) index = null;
        }
        bytes -= p.length;
        storage.remove(p.block, moveBacking);
    }

    long discard(long off) {
        if (packed != null) return packed.discard(0, off);
        long old = bytes;
        while (first != null && first.off < off) {
            Piece p = first;
            consume(p, (int) Math.min(p.length, off - p.off));
        }
        return old - bytes;
    }

    long discardFrom(long off) {
        if (packed != null) return packed.discard(off, Long.MAX_VALUE);
        long old = bytes;
        while (last != null && last.end() > off) {
            Piece p = last;
            if (off <= p.off) drop(p);
            else trim(p, (int) (p.end() - off));
        }
        return old - bytes;
    }

    long clear() {
        long dropped = bytes;
        if (packed != null) {
            dropped += packed.discard(0, Long.MAX_VALUE);
            packed = null;
        }
        while (first != null) drop(first);
        return dropped;
    }

    /** Accounts for the roots and descriptors shared by receive streams. */
    static final class Storage {
        private IdentityIndex<ReadBuf, Block> others;
        private final RootAccount roots;
        private final RootAccount pages;
        private final SharedMemory.Receive budget;
        private long descriptors;
        private long copied;
        private Block last;

        Storage(SharedMemory.Receive budget) {
            this.budget = budget;
            roots = new RootAccount(budget, false);
            pages = new RootAccount(budget, true);
        }

        /** Root release can run after the core and its queue are gone. */
        private static final class RootAccount implements ReadBuf.Account {
            private final AtomicLong capacity = new AtomicLong();
            private final SharedMemory.Receive budget;
            private final boolean packed;

            RootAccount(SharedMemory.Receive budget, boolean packed) {
                this.budget = budget;
                this.packed = packed;
            }

            @Override public Object domain() { return budget == null ? null : budget.domain(); }

            @Override public boolean reserve(long bytes) {
                if (budget != null && !(packed ? budget.tryRetainReceive(bytes) : budget.reserveView(bytes))) return false;
                capacity.addAndGet(bytes);
                return true;
            }

            @Override public void release(long bytes) {
                capacity.addAndGet(-bytes);
                if (budget == null) return;
                if (packed) budget.releaseRetainedReceive(bytes);
                else budget.releaseView(bytes);
            }
        }

        static final class Block {
            private final ReadBuf source;
            private int pieces;

            Block(ReadBuf source) { this.source = source; }
        }

        long bytes() { return roots.capacity.get() + pages.capacity.get() + descriptors; }

        long copied() { return copied; }

        void packedCopy(long n) { copied += n; }

        boolean reserveView(long n) {
            if (budget != null && !budget.reserveView(n)) return false;
            descriptors += n;
            return true;
        }

        void releaseView(long n) {
            descriptors -= n;
            if (budget != null) budget.releaseView(n);
        }

        Block block(ReadBuf source) {
            if (source.capacity() > ReceiveLoans.DEFAULT_BYTES) return null;
            if (last != null && last.source == source) return last;
            Block block = others == null ? null : others.get(source);
            if (block == null) {
                if (!reserveView(ReceiveLayout.VIEW_ROOT)) return null;
                if (!source.cover(roots)) {
                    releaseView(ReceiveLayout.VIEW_ROOT);
                    return null;
                }
                source.ref();
                block = new Block(source);
            } else {
                others.remove(source);
            }
            if (last != null) {
                if (others == null) others = new IdentityIndex<>();
                others.put(last.source, last);
            }
            last = block;
            return block;
        }

        void add(Block block) { block.pieces++; }

        void remove(Block block, boolean move) {
            releaseView(ReceiveLayout.VIEW_PIECE);
            if (--block.pieces != 0) return;
            if (last == block) last = null;
            else others.remove(block.source);
            if (others != null && others.isEmpty()) others = null;
            releaseView(ReceiveLayout.VIEW_ROOT);
            if (!move) block.source.unref();
        }

        ReadBuf packedPage() {
            if (budget != null && !budget.tryRetainReceive(ReceiveLayout.PAGE_CHARGE)) {
                throw new IllegalStateException("receive page outside its reserved backing");
            }
            descriptors += ReceiveLayout.PACKED_DESCRIPTOR;
            try {
                var page = new ReadBuf(ByteBuffer.allocate(OffsetPageBits.SIZE), null);
                pages.capacity.addAndGet(OffsetPageBits.SIZE);
                page.prepaid(pages, true);
                return page;
            } catch (RuntimeException | Error failure) {
                descriptors -= ReceiveLayout.PACKED_DESCRIPTOR;
                if (budget != null) budget.releaseRetainedReceive(ReceiveLayout.PAGE_CHARGE);
                throw failure;
            }
        }

        void dropPackedPage(ReadBuf page, boolean move) {
            dropPackedDescriptor();
            if (!move) page.unref();
        }

        private void dropPackedDescriptor() {
            descriptors -= ReceiveLayout.PACKED_DESCRIPTOR;
            if (budget != null) budget.releaseRetainedReceive(ReceiveLayout.PACKED_DESCRIPTOR);
        }
    }
}
