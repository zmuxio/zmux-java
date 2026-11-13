package io.zmux.shaping;

import java.time.Duration;
import java.util.Objects;

/**
 * A session's local bounds on shaping, set by the application and never sent. A configuration's budget cannot widen them.
 * Start from {@link #defaults()}.
 *
 * @param wire the allowance for what a session sends on its own
 * @param maxDelay the bound on one wait, and so the lowest rate of a gate, positive; null: no bound
 *     ({@link #withNoMaxDelay()}). Default {@link #DEFAULT_MAX_DELAY}
 * @param padding padding allowed, on handshakes and injections too (default true)
 * @param pause pauses and delays allowed (default true)
 * @param injection injection allowed (default true)
 */
public record ShapingLimits(WireAccount wire, Duration maxDelay, boolean padding, boolean pause, boolean injection) {
    /** The default bound on one wait. */
    public static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(1);
    private static final ShapingLimits DEFAULTS = new ShapingLimits(WireAccount.NONE, DEFAULT_MAX_DELAY, true, true, true);

    public ShapingLimits {
        Objects.requireNonNull(wire, "wire");
        if (maxDelay != null && !maxDelay.isPositive()) throw new IllegalArgumentException("maxDelay must be positive");
    }

    /** No account, every feature allowed, with the default bound on one wait. */
    public static ShapingLimits defaults() { return DEFAULTS; }

    public ShapingLimits withWire(WireAccount a) { return new ShapingLimits(a, maxDelay, padding, pause, injection); }
    /** The bound on one wait, positive. */
    public ShapingLimits withMaxDelay(Duration d) { return new ShapingLimits(wire, Objects.requireNonNull(d, "maxDelay"), padding, pause, injection); }
    /** No bound on one wait. */
    public ShapingLimits withNoMaxDelay() { return new ShapingLimits(wire, null, padding, pause, injection); }
    public ShapingLimits withPadding(boolean on) { return new ShapingLimits(wire, maxDelay, on, pause, injection); }
    public ShapingLimits withPause(boolean on) { return new ShapingLimits(wire, maxDelay, padding, on, injection); }
    public ShapingLimits withInjection(boolean on) { return new ShapingLimits(wire, maxDelay, padding, pause, on); }

    /** These limits in force under b. */
    public ShapingLimits under(ShapingBudget b) {
        Duration d = maxDelay;
        Duration c = b.maxDelay();
        if (c != null && (d == null || c.compareTo(d) < 0)) d = c;
        WireAccount a = wire.under(b.wire());
        // Both set and nothing left in common: an account that allows nothing, not one that is absent.
        boolean nothing = wire.isSet() && b.wire().isSet() && !a.isSet();
        return new ShapingLimits(a, d, padding, pause, injection && b.injection() && !nothing);
    }
}
