package io.zmux.shaping;

import java.time.Duration;
import java.util.Map;

/** Cumulative shaping counters for one session direction. Byte counters are unsigned and saturating. */
public record ShapingStats(long wireSpent, Duration stallSpent, Duration maxAddedDelay, long earnedBytes, OverheadMeters overhead,
                           long injectionAcceptedBytes, long injectionSentBytes, long injectionAccepted,
                           long injectionRejected, Map<RuntimeReason, Long> injectionRejections, Map<RuntimeReason, Long> reasons,
                           long clamped, long faults, long handshakeUnplanned) {
    public static final ShapingStats ZERO = new ShapingStats(
            0, Duration.ZERO, Duration.ZERO, 0, null, 0, 0, 0, 0, Map.of(), Map.of(), 0, 0, 0);

    public ShapingStats {
        injectionRejections = Map.copyOf(injectionRejections);
        reasons = Map.copyOf(reasons);
    }

}
