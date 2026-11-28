package io.zmux.shaping;

import java.math.BigInteger;
import java.time.Duration;

/** Direction-local readings. Byte counters are unsigned and saturating. */
public record ShapingMeters(long wireLeft, long stallLeft, long wireSpent, long stallSpentMicros, long earnedBytes,
                            OverheadMeters overhead, Duration maxAddedDelay, long generation) {
    public static final ShapingMeters ZERO = new ShapingMeters(0, 0, 0, 0, 0, null, Duration.ZERO, 0);

    /** The boundary where byte counts and local release times are measured. */
    public String boundary() { return "carrier_interface"; }

    public long wirePpm() { return wirePpm(wireSpent, earnedBytes); }

    public static long wirePpm(long added, long own) {
        if (added == 0) return 0;
        if (own == 0) return -1L;
        if (Long.compareUnsigned(added, Long.divideUnsigned(-1L, 1_000_000)) <= 0) {
            return Long.divideUnsigned(added * 1_000_000, own);
        }
        BigInteger ppm = unsigned(added).multiply(BigInteger.valueOf(1_000_000)).divide(unsigned(own));
        return ppm.bitLength() > 64 ? -1L : ppm.longValue();
    }

    private static BigInteger unsigned(long n) {
        BigInteger low = BigInteger.valueOf(n & Long.MAX_VALUE);
        return n < 0 ? low.setBit(63) : low;
    }
}
