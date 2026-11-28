package io.zmux.internal.core;

import io.zmux.internal.buffer.OffsetPageBits;

import java.util.TreeMap;

import static io.zmux.internal.buffer.OffsetPageBits.SIZE;
import static io.zmux.internal.buffer.OffsetPageBits.base;

/** The owner-only multipath scoreboard. Only first transmission may extend its paid span. */
final class SendHistory {
    // Three 512 B maps, with 512 B for wrappers, a tree node, the offset key and the index.
    static final long PAGE_CHARGE = 3L * OffsetPageBits.BITMAP_BYTES + 512;
    // 64 KiB plus the largest preamble, with both ends potentially unaligned.
    static final int FLOOR_PAGES = (int) ((Stream.SMALL_HELD + StreamTable.MAX_PREAMBLE - 1) / SIZE + 2);
    static final long FLOOR = FLOOR_PAGES * PAGE_CHARGE;

    private static final class Page {
        private OffsetPageBits acked, resend, copied;
    }

    final OwnerQueue.Link<SendHistory> roomLink = new OwnerQueue.Link<>(this);
    private final ConnFlow conn;
    // The paid span is contiguous. Ordered traffic does not allocate index nodes or bitmaps.
    private TreeMap<Long, Page> pages;
    private long paidUntil;
    private long prefix;
    private long applicationAcked;
    private long resendBytes;
    private long firstResend = Long.MAX_VALUE;
    private boolean waiting, closed;

    SendHistory(ConnFlow conn) {
        this.conn = conn;
    }

    long prefix() { return prefix; }
    long applicationAcked() { return applicationAcked; }
    long resendBytes() { return resendBytes; }
    long pageCount() { return (paidUntil - base(prefix)) / SIZE; }
    int markedPages() { return pages == null ? 0 : pages.size(); }

    /** Pure scheduling check after an actual failed extension. Existing pages remain writable. */
    long allowed(long off, long length) {
        if (!waiting) return length;
        return Math.clamp(paidUntil - off, 0, length);
    }

    /** Takes every affordable page before the first frame can publish any byte in it. */
    long reserve(long off, long length) {
        long end = off + length;
        if (end <= paidUntil) return length;
        long prepaid = Math.max(0, FLOOR_PAGES - pageCount());
        long wanted = base(end - 1) + SIZE;
        paidUntil = Math.min(wanted, paidUntil + prepaid * SIZE);
        while (paidUntil < wanted && !waiting) {
            if (conn != null && !conn.reserveOptional(PAGE_CHARGE)) {
                waiting = true;
                conn.waitHistory(this, true);
                break;
            }
            paidUntil += SIZE;
        }
        return Math.clamp(paidUntil - off, 0, length);
    }

    /** A shorter frame or failed build returns any tail pages it did not commit. */
    void finish(long sent) {
        long end = sent == 0 ? 0 : base(sent - 1) + SIZE;
        if (end >= paidUntil) return;
        long previous = pageCount();
        paidUntil = end;
        if (pages != null && !pages.isEmpty() && pages.lastKey() >= end) {
            throw new IllegalStateException("unpublished history has marks");
        }
        released(previous);
    }

    void ready() {
        if (!waiting) return;
        waiting = false;
        conn.waitHistory(this, false);
    }

    /** Adds exact selective ACKs. Ordered ACKs advance the scalar prefix without renting a bitmap. */
    void acknowledge(long from, long to, long preamble) {
        from = Math.max(from, prefix);
        if (closed || from >= to) return;
        boolean ordered = from == prefix;
        long newlyAcked = Math.max(0, to - Math.max(from, preamble));
        if (!ordered) {
            for (long at = base(from); at < to; at += SIZE) {
                newlyAcked -= acknowledgePage(mark(at), at, from, to, preamble, true);
            }
        } else if (pages != null && !pages.isEmpty()) {
            for (var entry : pages.subMap(base(from), true, to, false).entrySet()) {
                newlyAcked -= acknowledgePage(entry.getValue(), entry.getKey(), from, to, preamble, false);
            }
        }
        applicationAcked += newlyAcked;
        if (ordered) {
            long previous = pageCount();
            prefix = nextClear(to, paidUntil, false);
            if (pages != null) {
                while (!pages.isEmpty() && pages.firstKey() < base(prefix)) pages.pollFirstEntry();
            }
            released(previous);
        }
        if (firstResend < to) firstResend = scanResend(Math.max(firstResend, prefix));
    }

    /** Applies an ACK and returns application bytes it had already counted. */
    private int acknowledgePage(Page p, long at, long from, long to, long preamble, boolean selective) {
        int lo = (int) Math.max(0, from - at), hi = (int) Math.min(SIZE, to - at);
        int app = Math.clamp(preamble - at, lo, hi);
        int duplicate = p.acked == null || app == hi ? 0 : p.acked.covered(app, hi);
        if (selective) {
            if (p.acked == null) p.acked = new OffsetPageBits();
            p.acked.add(lo, hi);
        }
        if (p.resend != null) resendBytes -= p.resend.remove(lo, hi);
        return duplicate;
    }

    void lost(long from, long to) {
        from = Math.max(prefix, from);
        if (closed || from >= to) return;
        long earlier = Math.min(firstResend, from);
        for (long at = base(from); at < to; at += SIZE) {
            Page p = mark(at);
            int lo = (int) Math.max(0, from - at), hi = (int) Math.min(SIZE, to - at);
            if (p.acked != null && p.acked.covered(lo, hi) == hi - lo) continue;
            if (p.resend == null) p.resend = new OffsetPageBits();
            resendBytes += p.resend.addMissing(lo, hi, p.acked);
        }
        if (earlier < firstResend) firstResend = scanResend(earlier);
    }

    long nextResend(long from) {
        return from <= firstResend ? firstResend : scanResend(from);
    }

    long resendEnd(long from) {
        return nextClear(from, paidUntil, true);
    }

    void resent(long from, long to) {
        from = Math.max(from, prefix);
        if (resendBytes == 0 || from >= to) return;
        for (var entry : pages.subMap(base(from), true, to, false).entrySet()) {
            long at = entry.getKey();
            OffsetPageBits bits = entry.getValue().resend;
            if (bits != null) {
                resendBytes -= bits.remove((int) Math.max(0, from - at), (int) Math.min(SIZE, to - at));
            }
        }
        if (firstResend < to) firstResend = scanResend(Math.max(firstResend, prefix));
    }

    long nextUncopied(long from, long to) {
        if (closed || from >= to) return to;
        for (long at = base(from); at < to; at += SIZE) {
            Page p = page(at);
            int lo = (int) Math.max(0, from - at);
            int end = p == null || p.copied == null ? lo : p.copied.next(lo, false);
            if (end < SIZE) return Math.min(to, at + end);
        }
        return to;
    }

    long unacknowledged(long from, long to) {
        from = Math.max(from, prefix);
        if (closed || from >= to) return 0;
        long bytes = to - from;
        if (pages != null) {
            for (var entry : pages.subMap(base(from), true, to, false).entrySet()) {
                Page p = entry.getValue();
                if (p.acked == null) continue;
                long at = entry.getKey();
                bytes -= p.acked.covered((int) Math.max(0, from - at), (int) Math.min(SIZE, to - at));
            }
        }
        return bytes;
    }

    /** End of the contiguous unsent-as-a-copy, unacknowledged range at from. */
    long repairableEnd(long from, long to) {
        for (long at = base(from); at < to; at += SIZE) {
            Page p = page(at);
            if (p == null) continue;
            int lo = (int) Math.max(0, from - at);
            int acked = p.acked == null ? SIZE : p.acked.next(lo, true);
            int copied = p.copied == null ? SIZE : p.copied.next(lo, true);
            int end = Math.min(acked, copied);
            if (end < SIZE) return Math.min(to, at + end);
        }
        return to;
    }

    void copied(long from, long to) {
        from = Math.max(from, prefix);
        if (closed || from >= to) return;
        for (long at = base(from); at < to; at += SIZE) {
            Page p = mark(at);
            if (p.copied == null) p.copied = new OffsetPageBits();
            p.copied.add((int) Math.max(0, from - at), (int) Math.min(SIZE, to - at));
        }
    }

    void close() {
        if (closed) return;
        closed = true;
        long previous = pageCount();
        pages = null;
        paidUntil = base(prefix);
        resendBytes = 0;
        firstResend = Long.MAX_VALUE;
        ready();
        released(previous);
    }

    private void released(long previous) {
        if (conn == null || previous == pageCount()) return;
        long returned = Math.max(0, previous - FLOOR_PAGES) - Math.max(0, pageCount() - FLOOR_PAGES);
        if (returned != 0) conn.releaseOptional(returned * PAGE_CHARGE);
        if (waiting && pageCount() < FLOOR_PAGES) conn.historyFloorFreed(this);
    }

    private Page page(long at) { return pages == null ? null : pages.get(at); }

    private Page mark(long at) {
        if (at < base(prefix) || at >= paidUntil) throw new IllegalStateException("send history offsets must be prepaid");
        if (pages == null) pages = new TreeMap<>();
        return pages.computeIfAbsent(at, _ -> new Page());
    }

    private long scanResend(long from) {
        if (resendBytes == 0 || from == Long.MAX_VALUE) return Long.MAX_VALUE;
        for (var entry : pages.tailMap(base(from), true).entrySet()) {
            long at = entry.getKey();
            OffsetPageBits bits = entry.getValue().resend;
            if (bits == null) continue;
            int found = bits.next((int) Math.max(0, from - at), true);
            if (found < SIZE) return at + found;
        }
        return Long.MAX_VALUE;
    }

    private long nextClear(long from, long to, boolean resend) {
        for (long at = base(from); at < to; at += SIZE) {
            Page p = page(at);
            OffsetPageBits bits = p == null ? null : resend ? p.resend : p.acked;
            int lo = (int) Math.max(0, from - at);
            int end = bits == null ? lo : bits.next(lo, false);
            if (end < SIZE) return at + end;
        }
        return to;
    }
}
