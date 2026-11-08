package io.zmux.shaping;

import java.time.Duration;
import java.util.Objects;

/**
 * A session's local bounds on shaping, set by the application and never sent. A configuration's budget cannot widen them.
 *
 * @param wire the allowance for what a session sends on its own
 * @param maxDelay the bound on one wait, and so the lowest rate of a gate: zero is {@link #DEFAULT_MAX_DELAY}, negative no bound
 * @param noPadding no padding at all, on handshakes and injections too
 * @param noPause no pause or delay
 * @param noInjection no injection
 */
public record ShapingLimits(WireAccount wire, Duration maxDelay, boolean noPadding, boolean noPause, boolean noInjection) {
    /** The bound on one wait when none is given. */
    public static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(1);
    private static final ShapingLimits DEFAULTS = new ShapingLimits(WireAccount.NONE, Duration.ZERO, false, false, false);

    public ShapingLimits {
        Objects.requireNonNull(wire, "wire");
        Objects.requireNonNull(maxDelay, "maxDelay");
    }

    /** No account and no switch, with the default bound on one wait. */
    public static ShapingLimits defaults() { return DEFAULTS; }

    public ShapingLimits withWire(WireAccount a) { return new ShapingLimits(a, maxDelay, noPadding, noPause, noInjection); }
    public ShapingLimits withMaxDelay(Duration d) { return new ShapingLimits(wire, d, noPadding, noPause, noInjection); }
    public ShapingLimits withNoPadding(boolean v) { return new ShapingLimits(wire, maxDelay, v, noPause, noInjection); }
    public ShapingLimits withNoPause(boolean v) { return new ShapingLimits(wire, maxDelay, noPadding, v, noInjection); }
    public ShapingLimits withNoInjection(boolean v) { return new ShapingLimits(wire, maxDelay, noPadding, noPause, v); }

    /** These limits in force under b; the bound on one wait is then never zero. */
    public ShapingLimits under(ShapingBudget b) {
        Duration d = maxDelay.isZero() ? DEFAULT_MAX_DELAY : maxDelay;
        Duration c = b.maxDelay();
        if (c.isPositive() && (d.isNegative() || c.compareTo(d) < 0)) d = c;
        WireAccount a = wire.under(b.wire());
        // Both set and nothing left in common: an account that allows nothing, not one that is absent.
        boolean nothing = wire.isSet() && b.wire().isSet() && !a.isSet();
        return new ShapingLimits(a, d, noPadding, noPause, noInjection || b.noInjection() || nothing);
    }
}
