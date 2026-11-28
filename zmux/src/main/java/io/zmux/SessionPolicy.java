package io.zmux;

import java.util.Objects;

/**
 * Versioned session policy installed on the protocol owner. Versions are unsigned 64-bit values.
 * A null authorizer permits authenticated Joins subject to the session's record policy.
 */
public record SessionPolicy(long revision, long authGeneration, AuthorizeJoin authorizeJoin, CapacityLimits limits,
                            java.util.Map<String, Directions> topology) {
    private static final SessionPolicy DEFAULT = new SessionPolicy(CapacityLimits.defaults());

    public SessionPolicy {
        Objects.requireNonNull(limits, "limits");
        topology = java.util.Map.copyOf(topology);
        if (topology.containsKey("")) throw new IllegalArgumentException("empty candidate ID");
    }

    public SessionPolicy(long revision, long authGeneration, AuthorizeJoin authorizeJoin, CapacityLimits limits) {
        this(revision, authGeneration, authorizeJoin, limits, java.util.Map.of());
    }

    public SessionPolicy(CapacityLimits limits) { this(0, 0, null, limits); }

    public static SessionPolicy defaults() { return DEFAULT; }

    io.zmux.internal.core.CoreSessionPolicy core() {
        return new io.zmux.internal.core.CoreSessionPolicy(revision, authGeneration, limits, topology);
    }
}
