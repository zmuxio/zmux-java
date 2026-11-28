package io.zmux.shaping;

import java.time.Duration;
import java.util.Objects;

/** Hard bounds on overhead per committed baseline byte and on a unit's shaping delay. */
public record ProfileLimits(long windowSeconds, int extraBytesPpm, long floorBytes, Duration maxAddedDelay) {
    public static final ProfileLimits LIGHTWEIGHT = new ProfileLimits(10, 50_000, 540, Duration.ofMillis(2));

    public ProfileLimits {
        if (windowSeconds <= 0 || windowSeconds > 0xffff_ffffL) throw new IllegalArgumentException("window_seconds outside u32 range");
        if (floorBytes < 0 || floorBytes > WireAccount.MAX_CEILING) throw new IllegalArgumentException("floor_bytes outside [0, 2^53]");
        if (extraBytesPpm < 0 || extraBytesPpm > 1_000_000) throw new IllegalArgumentException("extraBytesPpm outside [0, 1000000]");
        if (Objects.requireNonNull(maxAddedDelay, "maxAddedDelay").isNegative() || maxAddedDelay.compareTo(Duration.ofSeconds(1_000_000_000L)) > 0) {
            throw new IllegalArgumentException("max_added_delay outside [0, 10^15] microseconds");
        }
    }

    /** Unsigned byte allowance. Divide the 128-bit product in base 2^32. */
    public long allowance(long baseline) {
        long high = Math.multiplyHigh(baseline, extraBytesPpm) + ((baseline >> 63) & extraBytesPpm);
        long low = baseline * extraBytesPpm;
        long top = (high << 32) | (low >>> 32);
        long bottom = (Long.remainderUnsigned(top, 1_000_000) << 32) | (low & 0xffff_ffffL);
        long ratio = (Long.divideUnsigned(top, 1_000_000) << 32) | (bottom / 1_000_000);
        return Long.compareUnsigned(ratio, floorBytes) < 0 ? floorBytes : ratio;
    }
}
