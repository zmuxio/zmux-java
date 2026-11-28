package io.zmux.internal.core;

import io.zmux.CapacityLimits;
import java.util.Objects;

/** The immutable facts the protocol owner needs to commit session policy. */
public record CoreSessionPolicy(long revision, long authGeneration, CapacityLimits limits,
                                java.util.Map<String, io.zmux.Directions> topology) {
    public static final CoreSessionPolicy DEFAULT = new CoreSessionPolicy(CapacityLimits.defaults());

    public CoreSessionPolicy {
        Objects.requireNonNull(limits, "limits");
        topology = java.util.Map.copyOf(topology);
    }

    public CoreSessionPolicy(long revision, long authGeneration, CapacityLimits limits) {
        this(revision, authGeneration, limits, java.util.Map.of());
    }

    public CoreSessionPolicy(CapacityLimits limits) { this(0, 0, limits, java.util.Map.of()); }

    public boolean allows(String candidate, int mask) {
        var allowed = topology.get(candidate);
        return allowed == null || (((mask & Action.DIR_IN) == 0 || allowed.receives())
                && ((mask & Action.DIR_OUT) == 0 || allowed.sends()));
    }
}
