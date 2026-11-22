package io.zmux;

/**
 * TCP counts in packets.
 *
 * @param packetsSent all outbound TCP packets, including retransmissions
 * @param packetsRetransmitted retransmitted TCP packets
 */
public record TcpPacketCounters(long packetsSent, long packetsRetransmitted) implements CarrierCounts {}
