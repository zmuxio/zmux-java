package io.zmux;

/**
 * TCP counts in segments.
 *
 * @param dataSegsOut outbound data segments, including retransmissions
 * @param totalRetrans retransmitted segments
 */
public record TcpSegmentCounters(long dataSegsOut, long totalRetrans) implements CarrierCounts {}
