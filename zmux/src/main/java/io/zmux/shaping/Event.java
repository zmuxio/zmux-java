package io.zmux.shaping;

import io.zmux.PipeId;

/** A notification from the session owner. Length and cost are bytes at the carrier boundary. */
public record Event(EventKind kind, PipeId pipe, long lane, long nowNanos, int length, long stream,
                    ShapingMeters meters, Unit unit, boolean data, boolean control, long requestId, int wireCost,
                    RuntimeReason reason) {
    /** The carrier unit that completed. */
    public enum Unit { RECORD, NATIVE_DATAGRAM }

    /** A lifecycle or stream event. */
    public Event(EventKind kind, PipeId pipe, long lane, long nowNanos, int length, long stream, ShapingMeters meters) {
        this(kind, pipe, lane, nowNanos, length, stream, meters, null, false, false, 0, 0, null);
    }
}
