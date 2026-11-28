package io.zmux.carrier;

import java.util.ArrayList;
import java.util.List;

/** Completed Establish writes transferred together when the session accepts the carrier. */
public record HandshakeCost(List<Unit> units) {
    public static final HandshakeCost ZERO = new HandshakeCost(List.of());

    /** Dispatch in the System.nanoTime clock domain, bytes at the adapter boundary and intentional delay. */
    public record Unit(long dispatchNanoTime, long baselineBytes, int allowanceBytes, int extraBytes, long addedDelayNanos) {
        public Unit {
            if (baselineBytes < 0 || extraBytes < 0 || extraBytes > allowanceBytes || addedDelayNanos < 0)
                throw new IllegalArgumentException("invalid handshake receipt");
        }
    }

    public HandshakeCost { units = List.copyOf(units); }

    public HandshakeCost plus(HandshakeCost other) {
        if (units.isEmpty()) return other;
        if (other.units.isEmpty()) return this;
        var combined = new ArrayList<>(units);
        combined.addAll(other.units);
        return new HandshakeCost(combined);
    }

    public boolean baselineOnly() {
        return units.stream().allMatch(u -> u.extraBytes == 0 && u.addedDelayNanos == 0);
    }
}
