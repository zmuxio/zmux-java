package io.zmux.internal.core;

import io.zmux.internal.buffer.OffsetPageBits;

import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.buffer.ReceiveLoans;
import java.nio.ByteBuffer;
import java.util.TreeMap;

import static io.zmux.internal.buffer.OffsetPageBits.SIZE;

/** The bounded receive representation used when a retained view cannot be afforded. */
final class PackedReceive {
    private static final class Page {
        private final OffsetPageBits present = new OffsetPageBits();
        private final ReadBuf backing;

        Page(ReadBuf backing) { this.backing = backing; }
    }

    private final TreeMap<Long, Page> pages = new TreeMap<>();
    private final RecvBuffer.Storage storage;
    private long bytes;

    PackedReceive(RecvBuffer.Storage storage) { this.storage = storage; }

    long bytes() { return bytes; }

    long insert(long offset, ByteBuffer source, int pos, int length) {
        long added = 0;
        while (length != 0) {
            long base = OffsetPageBits.base(offset);
            int start = (int) (offset - base);
            int n = Math.min(SIZE - start, length);
            Page page = pages.get(base);
            if (page == null) {
                ReadBuf backing = storage.packedPage();
                try {
                    page = new Page(backing);
                    pages.put(base, page);
                } catch (RuntimeException | Error failure) {
                    storage.dropPackedPage(backing, false);
                    throw failure;
                }
            }
            var bits = page.present;
            int end = start + n;
            for (int at = bits.next(start, false); at < end; at = bits.next(at, false)) {
                int stop = Math.min(end, bits.next(at, true));
                page.backing.data().put(at, source, pos + at - start, stop - at);
                added += bits.add(at, stop);
                at = stop;
            }
            offset += n;
            pos += n;
            length -= n;
        }
        bytes += added;
        storage.packedCopy(added);
        return added;
    }

    long contiguous(long offset) {
        while (true) {
            long base = OffsetPageBits.base(offset);
            Page page = pages.get(base);
            if (page == null) return offset;
            int end = page.present.next((int) (offset - base), false);
            offset = base + end;
            if (end < SIZE) return offset;
        }
    }

    int read(long offset, ByteBuffer dst, int max) {
        int copied = 0;
        while (copied < max) {
            long base = OffsetPageBits.base(offset);
            Page page = pages.get(base);
            if (page == null) break;
            int start = (int) (offset - base);
            int n = Math.min(page.present.next(start, false) - start, max - copied);
            if (n == 0) break;
            dst.put(dst.position(), page.backing.data(), start, n);
            dst.position(dst.position() + n);
            remove(base, page, start, start + n, false, start + n == SIZE);
            offset += n;
            copied += n;
        }
        return copied;
    }

    StreamRecv.Chunk take(long offset, int max) {
        long base = OffsetPageBits.base(offset);
        Page page = pages.get(base);
        if (page == null || max == 0) return null;
        int start = (int) (offset - base);
        int n = Math.min(page.present.next(start, false) - start, max);
        if (n == 0) return null;
        boolean move = start + n == SIZE && page.present.count() == n;
        if (!move) page.backing.ref();
        return take(base, page, start, n, page.backing, move);
    }

    StreamRecv.Chunk borrow(long offset, int max, ReceiveLoans loans) throws CoreException {
        long base = OffsetPageBits.base(offset);
        Page page = pages.get(base);
        if (page == null || max == 0) return null;
        int start = (int) (offset - base);
        int n = Math.min(page.present.next(start, false) - start, max);
        if (n == 0) return null;
        boolean move = start + n == SIZE && page.present.count() == n;
        ReadBuf loan = loans.acquire(page.backing, move);
        if (loan == null) throw CoreException.of(CoreException.Reason.CHUNK_BUDGET);
        return take(base, page, start, n, loan, move);
    }

    private StreamRecv.Chunk take(long base, Page page, int start, int n, ReadBuf owner, boolean move) {
        ByteBuffer view = page.backing.data().slice(start, n);
        remove(base, page, start, start + n, move, start + n == SIZE);
        return new StreamRecv.Chunk(view, owner, false);
    }

    private void remove(long base, Page page, int from, int to, boolean move, boolean evict) {
        bytes -= page.present.remove(from, to);
        if (evict && page.present.count() == 0) {
            pages.remove(base);
            storage.dropPackedPage(page.backing, move);
        }
    }

    long discard(long from, long to) {
        long before = bytes;
        Long key = pages.ceilingKey(OffsetPageBits.base(from));
        while (key != null && key < to) {
            long base = key;
            remove(base, pages.get(key), (int) Math.max(0, from - base), (int) Math.min(SIZE, to - base), false, true);
            key = pages.higherKey(key);
        }
        return before - bytes;
    }
}
