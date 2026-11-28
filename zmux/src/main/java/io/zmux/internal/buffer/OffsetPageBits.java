package io.zmux.internal.buffer;

/** One bit per offset in an offset page. A page covers a fixed aligned range. */
public final class OffsetPageBits {
    public static final int SIZE = 4096;
    static final int MASK = SIZE - 1;
    public static final int BITMAP_BYTES = SIZE / Byte.SIZE;

    private final long[] words = new long[SIZE / Long.SIZE];
    private int count;

    public static long base(long offset) { return offset & ~((long) MASK); }

    public int count() { return count; }

    public int add(int from, int to) { return change(from, to, true, null); }

    public int remove(int from, int to) { return change(from, to, false, null); }

    public int addMissing(int from, int to, OffsetPageBits excluded) {
        return change(from, to, true, excluded);
    }

    private int change(int from, int to, boolean present, OffsetPageBits excluded) {
        int changed = 0;
        for (int i = from; i < to; ) {
            int word = i / Long.SIZE;
            int end = Math.min(to, (word + 1) * Long.SIZE);
            long mask = mask(i, end);
            if (excluded != null) mask &= ~excluded.words[word];
            long value = words[word];
            long next = present ? value | mask : value & ~mask;
            changed += Long.bitCount(value ^ next);
            words[word] = next;
            i = end;
        }
        count += present ? changed : -changed;
        return changed;
    }

    public int covered(int from, int to) {
        int present = 0;
        for (int i = from; i < to; ) {
            int word = i / Long.SIZE;
            int end = Math.min(to, (word + 1) * Long.SIZE);
            present += Long.bitCount(words[word] & mask(i, end));
            i = end;
        }
        return present;
    }

    private static long mask(int from, int to) {
        return (-1L << (from % Long.SIZE)) & (-1L >>> ( -to & (Long.SIZE - 1)));
    }

    public int next(int from, boolean present) {
        if (from >= SIZE || (present && count == 0)) return SIZE;
        if (!present && count == 0) return from;
        int word = from / Long.SIZE;
        long candidates = (present ? words[word] : ~words[word]) & (-1L << (from % Long.SIZE));
        for (;;) {
            if (candidates != 0) return word * Long.SIZE + Long.numberOfTrailingZeros(candidates);
            if (++word == words.length) return SIZE;
            candidates = present ? words[word] : ~words[word];
        }
    }
}
