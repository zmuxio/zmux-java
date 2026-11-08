package io.zmux.internal.core;

import io.zmux.shaping.WireAccount;

/**
 * The session's wire account and the meter of all added wire bytes. All values are unsigned; the level never goes below
 * zero and stays within a signed long. Owner thread only.
 */
final class WireMeter {
    private static final long MILLION = 1_000_000;

    private WireAccount p = WireAccount.NONE;
    private boolean on;
    private long level, remMb, remSec, lastUs, earn, spent;

    WireMeter(long startUs) { lastUs = startUs; }

    /**
     * What d units earn at per per 10^6 units; rem[0] keeps the remainder. per is at most 2^43, so the part below 10^6
     * stays below 2^63 and only the whole millions can overflow, which saturates.
     */
    static long refill(long d, long per, long[] rem) {
        long q = Long.divideUnsigned(d, MILLION), r = Long.remainderUnsigned(d, MILLION);
        long x = r * per + rem[0];
        rem[0] = Long.remainderUnsigned(x, MILLION);
        if (Math.unsignedMultiplyHigh(q, per) != 0) return -1L;
        return addSat(q * per, Long.divideUnsigned(x, MILLION));
    }

    static long addSat(long a, long b) {
        long s = a + b;
        return Long.compareUnsigned(s, a) < 0 ? -1L : s;
    }

    private final long[] rem = new long[1];

    private long refillMb(long d) {
        rem[0] = remMb;
        long add = refill(d, p.perMb(), rem);
        remMb = rem[0];
        return add;
    }

    private long refillSec(long d) {
        rem[0] = remSec;
        long add = refill(d, p.perSec(), rem);
        remSec = rem[0];
        return add;
    }

    /** Brings the level to now; earned is the running total of the session's own bytes. */
    void accrue(long us, long earned) {
        if (on) {
            long add = addSat(refillMb(earned - earn), refillSec(us - lastUs));
            level = Long.compareUnsigned(add, Long.MAX_VALUE - level) > 0 ? Long.MAX_VALUE : level + add;
        }
        lastUs = us;
        earn = earned;
    }

    long left() { return on ? level : -1L; }

    /** The level; meaningful while the account is set. */
    long level() { return level; }

    boolean set() { return on; }

    long spent() { return spent; }

    /** Meters an action that accompanies a record: it never fails and takes nothing. */
    void spend(long c) { spent = addSat(spent, c); }

    /** Takes c for a spontaneous action if the account has it. */
    boolean charge(long us, long earned, long c) {
        accrue(us, earned);
        if (on) {
            if (Long.compareUnsigned(c, level) > 0) return false;
            level -= c;
        }
        spend(c);
        return true;
    }

    /** Replaces the parameters. What was spent stays spent. */
    void set(long us, long earned, WireAccount n) {
        accrue(us, earned);
        if (!n.isSet()) {
            on = false;
            level = remMb = remSec = 0;
        } else if (!on) {
            on = true;
            level = n.initial();
            remMb = remSec = 0;
        } else if (n.initial() >= p.initial()) {
            long d = n.initial() - p.initial();
            level = d > Long.MAX_VALUE - level ? Long.MAX_VALUE : level + d;
        } else {
            level -= Math.min(level, p.initial() - n.initial());
        }
        p = n;
    }
}
