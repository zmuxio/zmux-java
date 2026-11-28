package io.zmux.shaping;

import java.time.Duration;

/** Local restrictions on the installed policy. An absent cap leaves the policy grant intact. */
public record ShapingLimits(WireAccount wire, ProfileLimits profile, boolean padding, boolean pause, boolean injection) {
    private static final ShapingLimits DEFAULTS = new ShapingLimits(null, null, true, true, true);

    public static ShapingLimits defaults() { return DEFAULTS; }
    public ShapingLimits withWire(WireAccount cap) { return new ShapingLimits(cap, profile, padding, pause, injection); }
    public ShapingLimits withProfile(ProfileLimits cap) { return new ShapingLimits(wire, cap, padding, pause, injection); }
    public ShapingLimits withPadding(boolean on) { return new ShapingLimits(wire, profile, on, pause, injection); }
    public ShapingLimits withPause(boolean on) { return new ShapingLimits(wire, profile, padding, on, injection); }
    public ShapingLimits withInjection(boolean on) { return new ShapingLimits(wire, profile, padding, pause, on); }

    /** Policy and local byte tests can have different windows. */
    public record Effective(WireAccount wire, ProfileLimits policy, ProfileLimits local,
                            boolean padding, boolean pause, boolean injection) {
        public Duration maxAddedDelay() {
            return local != null && local.maxAddedDelay().compareTo(policy.maxAddedDelay()) < 0
                    ? local.maxAddedDelay() : policy.maxAddedDelay();
        }
    }

    public Effective under(ShapingBudget budget) {
        WireAccount grant = budget.wire();
        return new Effective(grant == null ? null : grant.under(wire), budget.profile(), profile,
                padding, pause, injection && budget.injection());
    }
}
