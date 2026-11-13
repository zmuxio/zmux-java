package io.zmux.shaping;

/**
 * A notification.
 *
 * @param nowNanos the session clock, nanoseconds, never negative
 * @param length frames length, bytes ({@link EventKind#RECORD_IN}, {@link EventKind#INJECTED})
 * @param paddingOnly the record holds PADDING only ({@link EventKind#RECORD_IN})
 * @param stream the stream ID ({@link EventKind#STREAM_OPEN}, {@link EventKind#STREAM_END})
 * @param meters the session's readings with {@link Capability#BUDGET}, otherwise {@link ShapingMeters#ZERO}
 */
public record Event(EventKind kind, io.zmux.PipeId pipe, long lane, long nowNanos, int length, boolean paddingOnly, long stream, ShapingMeters meters) {
    /** An event without readings. */
    public Event(EventKind kind, io.zmux.PipeId pipe, long lane, long nowNanos, int length, boolean paddingOnly, long stream) {
        this(kind, pipe, lane, nowNanos, length, paddingOnly, stream, ShapingMeters.ZERO);
    }
}
