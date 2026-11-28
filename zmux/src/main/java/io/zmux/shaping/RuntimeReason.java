package io.zmux.shaping;

/** The classified reason for clipping, cancelling or losing an optional shaping action. */
public enum RuntimeReason {
    UNSUPPORTED_CAPABILITY,
    CLIPPED_ROOM,
    POLICY_DEADLINE,
    APPLICATION_EXPIRED,
    INSUFFICIENT_COVER_CREDIT,
    OVERHEAD_LIMIT,
    STALE_GENERATION,
    CARRIER_REJECTED,
    PARTIAL_PROGRESS,
    CANCELLED,
    DISABLED,
    RATE_LIMIT
}
