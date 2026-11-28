package io.zmux.internal.core;

import java.util.Arrays;

/** A sorted set of disjoint half-open ranges [lo, hi) of non-negative longs. */
final class RangeSet {
    private long[] lo;
    private long[] hi;
    private int n;

    RangeSet() {
        lo = new long[1];
        hi = new long[1];
    }

    RangeSet(RangeSet source) {
        n = source.n;
        lo = Arrays.copyOf(source.lo, Math.max(1, n));
        hi = Arrays.copyOf(source.hi, Math.max(1, n));
    }

    int size() { return n; }

    long lo(int i) { return lo[i]; }

    long hi(int i) { return hi[i]; }

    private void grow() {
        if (n == lo.length) {
            lo = Arrays.copyOf(lo, n * 2);
            hi = Arrays.copyOf(hi, n * 2);
        }
    }

    /** Inserts [a, b) and returns how many bytes were new. */
    long add(long a, long b) {
        if (a >= b) return 0;
        if (n == 0 || a > hi[n - 1]) {
            grow();
            lo[n] = a;
            hi[n] = b;
            n++;
            return b - a;
        }
        if (a >= lo[n - 1]) {
            if (b <= hi[n - 1]) return 0;
            long nw = b - hi[n - 1];
            hi[n - 1] = b;
            return nw;
        }
        int i = 0;
        while (i < n && hi[i] < a) i++;
        long newBytes = b - a;
        long nlo = a, nhi = b;
        int j = i;
        while (j < n && lo[j] <= b) {
            long ol = Math.max(lo[j], a), oh = Math.min(hi[j], b);
            if (oh > ol) newBytes -= oh - ol;
            nlo = Math.min(nlo, lo[j]);
            nhi = Math.max(nhi, hi[j]);
            j++;
        }
        if (i == j) {
            grow();
            System.arraycopy(lo, i, lo, i + 1, n - i);
            System.arraycopy(hi, i, hi, i + 1, n - i);
            n++;
        } else if (j > i + 1) {
            System.arraycopy(lo, j, lo, i + 1, n - j);
            System.arraycopy(hi, j, hi, i + 1, n - j);
            n -= j - i - 1;
        }
        lo[i] = nlo;
        hi[i] = nhi;
        return newBytes;
    }

    /** Bytes of [a, b) in the set. */
    long covered(long a, long b) {
        long c = 0;
        for (int i = 0; i < n; i++) {
            if (hi[i] <= a) continue;
            if (lo[i] >= b) break;
            c += Math.min(hi[i], b) - Math.max(lo[i], a);
        }
        return c;
    }

    boolean contains(long a, long b) {
        return covered(a, b) == b - a;
    }

    /** End of the range starting at 0, or 0. */
    long prefix() {
        return n > 0 && lo[0] == 0 ? hi[0] : 0;
    }

    /** Removes [a, b). */
    void remove(long a, long b) {
        if (a >= b || n == 0) return;
        long[] nlo = new long[n + 1], nhi = new long[n + 1];
        int k = 0;
        for (int i = 0; i < n; i++) {
            if (hi[i] <= a || lo[i] >= b) {
                nlo[k] = lo[i];
                nhi[k++] = hi[i];
                continue;
            }
            if (lo[i] < a) {
                nlo[k] = lo[i];
                nhi[k++] = a;
            }
            if (hi[i] > b) {
                nlo[k] = b;
                nhi[k++] = hi[i];
            }
        }
        lo = nlo;
        hi = nhi;
        n = k;
    }

    /** Takes length bytes from the first range with enough room, or returns -1. */
    long takeFirst(long length) {
        for (int i = 0; i < n; i++) {
            long start = lo[i];
            if (hi[i] - start < length) continue;
            lo[i] += length;
            if (lo[i] == hi[i]) {
                int remaining = --n - i;
                System.arraycopy(lo, i + 1, lo, i, remaining);
                System.arraycopy(hi, i + 1, hi, i, remaining);
            }
            return start;
        }
        return -1;
    }

    void clear() {
        n = 0;
    }
}
