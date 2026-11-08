package io.zmux.shaping;

import java.time.Duration;
import java.util.Objects;

/**
 * What a shaping configuration allows one session.
 *
 * @param wire the allowance for what a session sends on its own
 * @param maxDelay the bound on one wait; zero or negative: none
 * @param noInjection no injection is allowed
 */
public record ShapingBudget(WireAccount wire, Duration maxDelay, boolean noInjection) {
    public static final ShapingBudget NONE = new ShapingBudget(WireAccount.NONE, Duration.ZERO, false);

    public ShapingBudget {
        Objects.requireNonNull(wire, "wire");
        Objects.requireNonNull(maxDelay, "maxDelay");
    }
}
