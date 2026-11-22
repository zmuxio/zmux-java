package io.zmux;

/** A native pair of send and loss counters. Both values in a pair are unsigned longs. */
public sealed interface CarrierCounts permits TcpSegmentCounters, TcpPacketCounters, QuicCounters {}
