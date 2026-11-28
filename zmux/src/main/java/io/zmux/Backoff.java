package io.zmux;

import java.time.Duration;
import java.util.Objects;

/**
 * The one retry backoff of zmux (carrier retries, physical connection pools and the exchange carrier): a wait
 * that starts at min after a failure and doubles up to max, each wait shortened by a random part
 * of up to jitter of it, and back to min after a success.
 *
 * @param min the first wait, positive
 * @param max the longest wait, at least min
 * @param jitter downward jitter in [0, 1]
 * @param upwardJitter upward jitter in [0, 1]
 */
public record Backoff(Duration min, Duration max, double jitter, double upwardJitter) {
    /** The default jitter: a wait is shortened by up to a fifth. */
    public static final double DEFAULT_JITTER = 0.2;
    /** The default min: the first wait after a failure. */
    public static final Duration DEFAULT_MIN = Duration.ofSeconds(1);
    /** The default max: the longest wait between failed attempts. */
    public static final Duration DEFAULT_MAX = Duration.ofSeconds(60);

    private static final Backoff DEFAULTS = new Backoff(DEFAULT_MIN, DEFAULT_MAX, DEFAULT_JITTER);

    public Backoff {
        long lower = positiveNanos(min, "min");
        long upper = positiveNanos(max, "max");
        if (lower > upper) throw new IllegalArgumentException("backoff min above max");
        if (!(jitter >= 0 && jitter <= 1 && upwardJitter >= 0 && upwardJitter <= 1)) {
            throw new IllegalArgumentException("backoff jitter outside [0, 1]");
        }
        if (maximumWait(upper, upwardJitter).compareTo(java.math.BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
            throw new IllegalArgumentException("maximum backoff with jitter exceeds nanosecond range");
        }
    }

    private static java.math.BigDecimal maximumWait(long nominal, double up) {
        return java.math.BigDecimal.valueOf(nominal)
                .multiply(java.math.BigDecimal.ONE.add(java.math.BigDecimal.valueOf(up)));
    }

    long maximumWaitNanos() { return maximumWait(max.toNanos(), upwardJitter).longValue(); }

    private static long positiveNanos(Duration duration, String name) {
        long value;
        try {
            value = Objects.requireNonNull(duration, name).toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("backoff " + name + " exceeds nanosecond range", overflow);
        }
        if (value <= 0) throw new IllegalArgumentException("backoff " + name + " must be positive");
        return value;
    }

    /** A backoff with downward jitter and no upward jitter. */
    public Backoff(Duration min, Duration max, double jitter) {
        this(min, max, jitter, 0);
    }

    long sample(long nominal, java.util.random.RandomGenerator random) {
        return io.zmux.internal.spi.RetryTiming.sample(nominal, jitter, upwardJitter, random);
    }

    /** A backoff with the default jitter, {@link #DEFAULT_JITTER}. */
    public Backoff(Duration min, Duration max) {
        this(min, max, DEFAULT_JITTER);
    }

    /** {@link #DEFAULT_MIN}, {@link #DEFAULT_MAX} and {@link #DEFAULT_JITTER}. */
    public static Backoff defaults() {
        return DEFAULTS;
    }
}
