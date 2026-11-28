package io.zmux.internal.buffer;

import java.util.TreeMap;

/** Exact offset coverage with an allocation-free contiguous prefix and a paged sparse tail. */
public final class OffsetCoverage {
    private TreeMap<Long, OffsetPageBits> pages;
    private long prefix;
    private long total;

    public long prefix() { return prefix; }

    public long covered(long from, long to) {
        if (from >= to) return 0;
        long covered = Math.max(0, Math.min(prefix, to) - from);
        from = Math.max(from, prefix);
        if (from >= to || pages == null) return covered;
        for (var entry : pages.tailMap(OffsetPageBits.base(from), true).entrySet()) {
            long at = entry.getKey();
            if (at >= to) break;
            covered += entry.getValue().covered((int) Math.max(0, from - at),
                    (int) Math.min(OffsetPageBits.SIZE, to - at));
        }
        return covered;
    }

    public long total() { return total; }

    public void discardTail() { pages = null; }

    public long add(long from, long to) {
        if (from >= to || to <= prefix) return 0;
        long added;
        if (from <= prefix) {
            added = to - prefix - covered(prefix, to);
            prefix = to;
            joinPrefix();
        } else {
            if (pages == null) pages = new TreeMap<>();
            added = 0;
            while (from < to) {
                long base = OffsetPageBits.base(from);
                int end = (int) Math.min(OffsetPageBits.SIZE, to - base);
                var bits = pages.computeIfAbsent(base, _ -> new OffsetPageBits());
                added += bits.add((int) (from - base), end);
                from = base + end;
            }
        }
        total += added;
        return added;
    }

    private void joinPrefix() {
        if (pages == null) return;
        long base = OffsetPageBits.base(prefix);
        pages.headMap(base).clear();
        for (OffsetPageBits bits; (bits = pages.get(base)) != null; ) {
            int from = (int) (prefix - base);
            int end = bits.next(from, false);
            prefix = base + end;
            if (end < OffsetPageBits.SIZE) {
                bits.remove(0, end);
                if (bits.count() == 0) pages.remove(base);
                break;
            }
            pages.remove(base);
            base = prefix;
        }
        if (pages.isEmpty()) pages = null;
    }
}
