package io.zmux;

/**
 * Floor of the summed historical delivery-rate estimates of usable non-standby
 * outbound pipes, in bytes/s. It is not guaranteed physical or currently available
 * bandwidth, including when pipes share a connection. Zero disables monitoring (no
 * capacity is below it); a recovery ratio must be above 1, and zero, which never is one,
 * means 1.25.
 */
public record CapacityFloor(double rate, double recoveryRatio) {
    public CapacityFloor {
        if (!Double.isFinite(rate) || rate < 0 || !Double.isFinite(recoveryRatio)
                || recoveryRatio != 0 && recoveryRatio <= 1) throw new IllegalArgumentException("capacity floor");
    }
}
