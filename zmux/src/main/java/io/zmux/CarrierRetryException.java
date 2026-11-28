package io.zmux;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/** A carrier opener's authenticated request to delay the next attempt for this candidate. */
public final class CarrierRetryException extends IOException {
    private final Duration retryAfter;

    public CarrierRetryException(Duration retryAfter) {
        super("carrier requested a retry delay");
        this.retryAfter = Objects.requireNonNull(retryAfter);
        if (retryAfter.isNegative() || retryAfter.isZero()) throw new IllegalArgumentException("retry delay must be positive");
    }

    public Duration retryAfter() { return retryAfter; }

    static CarrierRetryException find(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof CarrierRetryException retry) return retry;
        }
        return null;
    }
}
