package io.zmux.support;

import java.time.Duration;
import java.time.Instant;

public final class DeadlineSupport {
    public static final byte[] EMPTY_BYTES = new byte[0];

    private DeadlineSupport() {
    }

    public static Instant after(Duration timeout) {
        if (timeout == null) {
            return null;
        }
        try {
            return Instant.now().plus(timeout);
        } catch (ArithmeticException overflow) {
            return timeout.isNegative() ? Instant.MIN : Instant.MAX;
        }
    }
}
