package io.zmux.shaping;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * Upper bounds over a session; an unset bound is unbounded, and a bound set to zero allows none.
 *
 * @param maxPaddingBytes PADDING bytes added, zero or more
 * @param maxPaddingRatio PADDING bytes over other bytes sent, in [0, 1]; once set, the first
 *     {@link RecordPlan#MAX_RECORD_LENGTH} bytes of PADDING are allowed whatever the ratio
 * @param maxInjected records of PADDING only, zero or more
 * @param maxPauseTotal sum of pauses and delays, zero or more
 * @param maxDelay one pause or delay, zero or more
 */
public record ShapingLimits(OptionalLong maxPaddingBytes, OptionalDouble maxPaddingRatio, OptionalLong maxInjected,
                            Optional<Duration> maxPauseTotal, Optional<Duration> maxDelay) {
    private static final ShapingLimits NONE = new ShapingLimits(OptionalLong.empty(), OptionalDouble.empty(), OptionalLong.empty(),
            Optional.empty(), Optional.empty());

    public ShapingLimits {
        if (Objects.requireNonNull(maxPaddingBytes).orElse(0) < 0 || Objects.requireNonNull(maxInjected).orElse(0) < 0) {
            throw new IllegalArgumentException("negative limit");
        }
        if (Objects.requireNonNull(maxPaddingRatio).isPresent() && !(maxPaddingRatio.getAsDouble() >= 0 && maxPaddingRatio.getAsDouble() <= 1)) {
            throw new IllegalArgumentException("maxPaddingRatio outside [0, 1]");
        }
        if (Objects.requireNonNull(maxPauseTotal).map(Duration::isNegative).orElse(false)
                || Objects.requireNonNull(maxDelay).map(Duration::isNegative).orElse(false)) {
            throw new IllegalArgumentException("negative duration");
        }
    }

    /** No bounds. */
    public static ShapingLimits none() { return NONE; }

    public ShapingLimits withMaxPaddingBytes(long n) { return new ShapingLimits(OptionalLong.of(n), maxPaddingRatio, maxInjected, maxPauseTotal, maxDelay); }
    public ShapingLimits withMaxPaddingRatio(double r) { return new ShapingLimits(maxPaddingBytes, OptionalDouble.of(r), maxInjected, maxPauseTotal, maxDelay); }
    public ShapingLimits withMaxInjected(long n) { return new ShapingLimits(maxPaddingBytes, maxPaddingRatio, OptionalLong.of(n), maxPauseTotal, maxDelay); }
    public ShapingLimits withMaxPauseTotal(Duration d) { return new ShapingLimits(maxPaddingBytes, maxPaddingRatio, maxInjected, Optional.of(d), maxDelay); }
    public ShapingLimits withMaxDelay(Duration d) { return new ShapingLimits(maxPaddingBytes, maxPaddingRatio, maxInjected, maxPauseTotal, Optional.of(d)); }
}
