package io.zmux.internal.core;

/**
 * Session clock arithmetic: instants and durations in nanoseconds as {@code long},
 * {@link #NEVER} for a deadline that never fires. Every addition saturates
 * (IMPLEMENTATION.md Section 13.4 T1).
 */
public final class Time {
    public static final long NEVER = Long.MAX_VALUE;
    public static final long MICROSECOND = 1_000L;
    public static final long MILLISECOND = 1_000_000L;
    public static final long SECOND = 1_000_000_000L;

    private Time() {}

    /** t + d, saturating: an overflow is NEVER. */
    public static long add(long t, long d) {
        if (t == NEVER) return NEVER;
        long r = t + d;
        if (((t ^ r) & (d ^ r)) < 0) return d > 0 ? NEVER : Long.MIN_VALUE;
        return r;
    }

    /** t - u, saturating. */
    public static long sub(long t, long u) {
        if (t == NEVER) return Long.MAX_VALUE;
        long r = t - u;
        if (((t ^ u) & (t ^ r)) < 0) return t > u ? Long.MAX_VALUE : Long.MIN_VALUE;
        return r;
    }

    /** d * k, saturating. */
    public static long mul(long d, long k) {
        if (d == 0 || k == 0) return 0;
        long hi = Math.multiplyHigh(d, k);
        long lo = d * k;
        if ((hi == 0 && lo >= 0) || (hi == -1 && lo < 0)) return lo;
        return (d > 0) == (k > 0) ? Long.MAX_VALUE : Long.MIN_VALUE;
    }

    public static long millis(long ms) {
        return mul(ms, MILLISECOND);
    }
}
