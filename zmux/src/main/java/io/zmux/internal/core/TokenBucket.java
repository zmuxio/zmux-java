package io.zmux.internal.core;

import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.WireAccount;
import java.math.BigInteger;

/** The integer gate of a pipe: bytes, against the session clock in microseconds. Tokens may go negative. */
final class TokenBucket {
    private static final long MILLION = 1_000_000;

    private long rate, burst, tokens, filled;

    TokenBucket() {}

    long rate() { return rate; }
    void rate(long r) { rate = r; }
    long burst() { return burst; }
    void burst(long b) { burst = b; }
    long tokens() { return tokens; }
    private final long[] rem = new long[1];

    /** The lowest rate a bound of mdUs on one wait leaves (mdUs negative: none); rate 0 is no gate. */
    static long floor(long rate, long mdUs) {
        if (mdUs < 0 || rate == 0) return rate;
        long md = Math.max(mdUs, 1), f = (64 * MILLION + md - 1) / md;
        return Long.compareUnsigned(rate, f) < 0 ? f : rate;
    }

    /** The burst in force for a burst asked (unsigned): 0 is a record, what lies below the least record is raised to it. */
    static long burstFloor(long burst, long recordMax) {
        long least = RecordPlan.MIN_MAX_LENGTH;
        if (burst == 0) return Math.max(recordMax, least);
        if (Long.compareUnsigned(burst, least) < 0) return least;
        return Long.compareUnsigned(burst, WireAccount.MAX_INITIAL) < 0 ? burst : WireAccount.MAX_INITIAL;
    }

    void reset(long us) {
        tokens = burst;
        rem[0] = 0;
        filled = us;
    }

    void fill(long us) {
        if (Long.compareUnsigned(us, filled) <= 0) return;
        long add = WireMeter.refill(us - filled, rate, rem);
        filled = us;
        long room = burst - tokens; // burst at most 2^53, and the debt is what a session sent
        if (room <= 0 || Long.compareUnsigned(add, room) >= 0) {
            tokens = burst;
            rem[0] = 0;
        } else {
            tokens += add;
        }
    }

    // Not Math.clamp: burst is 0 after a reset, below MIN_MAX_LENGTH, and clamp would throw there.
    //noinspection MathClampMigration
    long quantum() { return Math.max(RecordPlan.MIN_MAX_LENGTH, Math.min(burst, rate * 1000 / MILLION)); }

    boolean held() { return tokens < quantum(); }

    void take(long n) { tokens -= n; }

    /** The microsecond a held bucket reaches its quantum; -1 when it lies beyond any clock. */
    long release() {
        long need = quantum() - tokens;
        long q;
        if (need <= SAFE_NEED) {
            q = Long.divideUnsigned(need * MILLION - rem[0] + rate - 1, rate);
        } else {
            BigInteger n = BigInteger.valueOf(need).multiply(BigInteger.valueOf(MILLION)).subtract(BigInteger.valueOf(rem[0])).add(BigInteger.valueOf(rate - 1));
            BigInteger b = n.divide(BigInteger.valueOf(rate));
            if (b.bitLength() > 62) return -1;
            q = b.longValue();
        }
        return q > Long.MAX_VALUE - filled ? -1 : filled + q;
    }

    /** Below this need * 10^6 plus a rate of at most 2^43 stays below 2^64. */
    private static final long SAFE_NEED = 1L << 44;
}
