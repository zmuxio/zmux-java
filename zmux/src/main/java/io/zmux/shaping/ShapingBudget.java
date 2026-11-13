package io.zmux.shaping;

import java.time.Duration;
import java.util.Objects;

/**
 * What a shaping configuration allows one session.
 *
 * @param wire the allowance for what a session sends on its own
 * @param maxDelay the bound on one wait, positive; null: none
 * @param injection injection is allowed
 */
public record ShapingBudget(WireAccount wire, Duration maxDelay, boolean injection) {
    /** No budget: nothing bounded beyond the session's own limits. */
    public static final ShapingBudget NONE = new ShapingBudget(WireAccount.NONE, null, true);

    public ShapingBudget {
        Objects.requireNonNull(wire, "wire");
        if (maxDelay != null && !maxDelay.isPositive()) throw new IllegalArgumentException("maxDelay must be positive");
    }
}
