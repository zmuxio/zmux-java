package io.zmux;

/** A local outgoing loss interval shared by all attachments on one physical connection.
 * Counts are unsigned. Times use the carrier sampler's monotonic clock. */
public record CarrierLoss(CarrierId carrierId, long generation, CarrierSource source, Unit unit,
                          long start, long end, long lost, long sent) {
    /** The counter unit reported by the native carrier. */
    public enum Unit { QUIC_PACKETS, TCP_PACKETS, TCP_SEGMENTS }
}
