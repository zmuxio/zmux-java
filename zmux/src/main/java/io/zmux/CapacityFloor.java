package io.zmux;

/**
 * Floor of the summed historical delivery-rate estimates of usable non-standby outbound pipes, in bytes/s
 * ({@link ZmuxSession#setCapacityFloor}). It is not guaranteed physical or currently available bandwidth,
 * including when pipes share a connection. Once below, the capacity has recovered when it reaches the floor
 * times the recovery ratio.
 *
 * @param rate the floor, at least 1 byte per second
 * @param recoveryRatio the recovery ratio, a finite number of at least 1 (1: no hysteresis)
 */
public record CapacityFloor(long rate, double recoveryRatio) {
    /** The default recovery ratio, 1.25. */
    public static final double DEFAULT_RECOVERY_RATIO = 1.25;

    public CapacityFloor {
        if (rate < 1) throw new IllegalArgumentException("capacity floor rate must be at least 1");
        if (!Double.isFinite(recoveryRatio) || recoveryRatio < 1)
            throw new IllegalArgumentException("recoveryRatio must be a finite number of at least 1");
    }

    /** A floor with the default recovery ratio. */
    public CapacityFloor(long rate) { this(rate, DEFAULT_RECOVERY_RATIO); }
}
