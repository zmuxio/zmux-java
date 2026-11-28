package io.zmux.internal.core;

import java.util.Arrays;

/** A growable list of longs with removal from the front. */
final class LongList {
    private long[] a = new long[0];
    private int head;
    private int n;

    int size() { return n - head; }

    long get(int i) { return a[head + i]; }

    void add(long v) {
        if (n == a.length) {
            if (head > 0) {
                System.arraycopy(a, head, a, 0, n - head);
                n -= head;
                head = 0;
            }
            if (n == a.length) a = Arrays.copyOf(a, Math.max(4, n * 2));
        }
        a[n++] = v;
    }

    /** Keeps the earliest entries and coalesces excess progress into the newest mark. */
    void addBounded(long v, int limit) {
        if (size() < limit) add(v);
        else a[n - 1] = v;
    }

    void removeFirst(int k) {
        head += k;
        if (head == n) head = n = 0;
    }

    void clear() { head = n = 0; }

    void release() {
        a = new long[0];
        clear();
    }
}
