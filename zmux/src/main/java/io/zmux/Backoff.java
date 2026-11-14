package io.zmux;

import java.time.Duration;
import java.util.Objects;

/**
 * The one retry backoff of zmux (the redial hook, the pipe pools, the exchange carrier): a wait
 * that starts at min after a failure and doubles up to max, each wait shortened by a random part
 * of up to jitter of it, and back to min after a success.
 *
 * @param min the first wait, positive
 * @param max the longest wait, at least min
 * @param jitter the largest random part taken off a wait, at least 0 and below 1; 0: no jitter
 */
public record Backoff(Duration min, Duration max, double jitter) {
    /** The default jitter: a wait is shortened by up to a fifth. */
    public static final double DEFAULT_JITTER = 0.2;
    /** The default min: the first wait after a failure. */
    public static final Duration DEFAULT_MIN = Duration.ofSeconds(1);
    /** The default max: the longest wait between failed attempts. */
    public static final Duration DEFAULT_MAX = Duration.ofSeconds(60);

    private static final Backoff DEFAULTS = new Backoff(DEFAULT_MIN, DEFAULT_MAX, DEFAULT_JITTER);

    public Backoff {
        if (PipePool.nanos(Objects.requireNonNull(min, "min"), false) > PipePool.nanos(Objects.requireNonNull(max, "max"), false))
            throw new IllegalArgumentException("backoff min above max");
        if (!Double.isFinite(jitter) || jitter < 0 || jitter >= 1) throw new IllegalArgumentException("backoff jitter outside [0, 1)");
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
