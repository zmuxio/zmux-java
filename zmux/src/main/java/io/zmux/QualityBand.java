package io.zmux;

import java.time.Duration;
import java.util.Objects;

/**
 * RTT/rate departure from the last report. Zero disables a half.
 *
 * @param rtt the RTT departure, zero or more
 * @param rateRatio the rate departure as a ratio above 1, or 0
 * @param minInterval the least time between two reports of a pipe, zero or more; zero: no
 *     floor. Without it ({@link #QualityBand(Duration, double)}): 1 s
 */
public record QualityBand(Duration rtt, double rateRatio, Duration minInterval) {
    /** The default minInterval. */
    public static final Duration DEFAULT_MIN_INTERVAL = Duration.ofSeconds(1);

    public QualityBand {
        Objects.requireNonNull(rtt);
        Objects.requireNonNull(minInterval);
        if (rtt.isNegative() || !Double.isFinite(rateRatio) || rateRatio != 0 && rateRatio <= 1) {
            throw new IllegalArgumentException("quality band needs a nonnegative RTT and a ratio above 1 (or 0)");
        }
        if (minInterval.isNegative()) throw new IllegalArgumentException("negative minInterval");
    }

    /** A band with the default minInterval of 1 s. */
    public QualityBand(Duration rtt, double rateRatio) { this(rtt, rateRatio, DEFAULT_MIN_INTERVAL); }

    public static QualityBand disabled() { return new QualityBand(Duration.ZERO, 0); }
}
