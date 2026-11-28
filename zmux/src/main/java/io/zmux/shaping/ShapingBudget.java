package io.zmux.shaping;

/** Allowances granted by the installed policy for the whole send direction. */
public record ShapingBudget(WireAccount wire, ProfileLimits profile, boolean injection) {
    public static final ShapingBudget NONE = new ShapingBudget(null, ProfileLimits.LIGHTWEIGHT, true);

    public ShapingBudget {
        if (profile == null) profile = ProfileLimits.LIGHTWEIGHT;
    }
}
