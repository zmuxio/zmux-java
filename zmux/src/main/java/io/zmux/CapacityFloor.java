package io.zmux;

/**
 * Floor of the summed historical delivery-rate estimates of usable non-standby
 * outbound pipes, in bytes/s. It is not guaranteed physical or currently available
 * bandwidth, including when pipes share a connection. Zero disables monitoring (no
 * capacity is below it). Once below, the capacity has recovered when it reaches the floor
 * times the recovery ratio: at least 1 (1, no hysteresis), or zero for the default 1.25;
 * any other value is an error.
 */
public record CapacityFloor(long rate, double recoveryRatio) {
    public CapacityFloor {
        if (rate < 0 || !Double.isFinite(recoveryRatio)
                || (recoveryRatio != 0 && recoveryRatio < 1)) throw new IllegalArgumentException("capacity floor");
    }
}
