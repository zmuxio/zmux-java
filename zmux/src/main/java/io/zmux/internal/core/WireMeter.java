package io.zmux.internal.core;

import io.zmux.shaping.WireAccount;

/** The session direction's cover balance and committed overhead. Owner thread only. */
final class WireMeter {
    private static final long MILLION = 1_000_000;

    private WireAccount parameters;
    private boolean initialized;
    private long level, remainder, lastUs, spent;
    private final long[] fraction = new long[1];

    WireMeter(long startUs) { lastUs = startUs; }

    /** Refill for unsigned d, sharing the fraction across refill sources. */
    static long refill(long d, long per, long[] rem) {
        long q = Long.divideUnsigned(d, MILLION), r = Long.remainderUnsigned(d, MILLION);
        long x = r * per + rem[0];
        rem[0] = x % MILLION;
        if (Math.unsignedMultiplyHigh(q, per) != 0) return -1L;
        return addSat(q * per, x / MILLION);
    }

    static long addSat(long a, long b) {
        long s = a + b;
        return Long.compareUnsigned(s, a) < 0 ? -1L : s;
    }

    void accrue(long us, long own) {
        level = projected(us, own);
        remainder = fraction[0];
        if (Long.compareUnsigned(us, lastUs) > 0) lastUs = us;
    }

    private long projected(long us, long own) {
        WireAccount p = parameters;
        fraction[0] = remainder;
        if (p == null || level >= p.ceiling()) return level;
        long add = refill(own, p.perMb(), fraction);
        long room = p.ceiling() - level;
        if (Long.compareUnsigned(add, room) < 0) {
            long elapsed = Long.compareUnsigned(us, lastUs) > 0 ? us - lastUs : 0;
            long more = refill(elapsed, p.perSec(), fraction);
            if (Long.compareUnsigned(more, room - add) < 0) return level + add + more;
        }
        fraction[0] = 0;
        return p.ceiling();
    }

    long left(long us) { return parameters == null ? 0 : projected(us, 0); }
    long spent() { return spent; }

    void charge(long us, long cost) {
        accrue(us, 0);
        if (cost <= 0 || parameters == null || cost > level) throw new IllegalStateException("cover charge exceeds balance");
        level -= cost;
        spent = addSat(spent, cost);
    }

    void set(long us, WireAccount next) {
        accrue(us, 0);
        parameters = next;
        if (next == null) {
            remainder = 0;
            return;
        }
        if (!initialized && next.ceiling() > 0) {
            level = next.initial();
            initialized = true;
        }
        if (level >= next.ceiling()) {
            level = next.ceiling();
            remainder = 0;
        }
    }
}
