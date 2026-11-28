package io.zmux.shaping;

import io.zmux.PipeId;

/** A selected native datagram at its writer, including any zmux stream ID in its length. */
public record DatagramView(PipeId pipe, long nowNanos, int length, int limit, int room, boolean padding,
                           boolean replica, ShapingMeters meters) {}
