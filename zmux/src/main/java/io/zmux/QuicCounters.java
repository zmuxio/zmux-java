package io.zmux;

/**
 * QUIC counts in packets, as reported by the provider.
 *
 * @param packetsSent outbound QUIC packets
 * @param packetsLost QUIC packets the sender declared lost
 */
public record QuicCounters(long packetsSent, long packetsLost) implements CarrierCounts {}
