package io.zmux.internal.core;

import java.util.Arrays;

/** Minimum or maximum over a sliding time span (monotonic deque). */
final class Windowed {
    private final boolean max;
    private long[] at = new long[0];
    private double[] v = new double[0];
    private int n;

    Windowed(boolean max) {
        this.max = max;
    }

    private boolean dominated(double last, double x) {
        return max ? last <= x : last >= x;
    }

    void update(long now, double x, long span) {
        while (n > 0 && dominated(v[n - 1], x)) n--;
        if (n == at.length) {
            at = Arrays.copyOf(at, Math.max(4, n * 2));
            v = Arrays.copyOf(v, at.length);
        }
        at[n] = now;
        v[n] = x;
        n++;
        if (max) {
            prune(now, span);
        } else {
            int cut = 0;
            while (cut < n - 1 && Time.sub(now, at[cut]) > span) cut++;
            drop(cut);
        }
    }

    private void drop(int cut) {
        if (cut > 0) {
            System.arraycopy(at, cut, at, 0, n - cut);
            System.arraycopy(v, cut, v, 0, n - cut);
            n -= cut;
        }
    }

    void prune(long now, long span) {
        int cut = 0;
        while (cut < n && Time.sub(now, at[cut]) > span) cut++;
        drop(cut);
    }

    boolean has() { return n > 0; }

    double get() { return v[0]; }
}
