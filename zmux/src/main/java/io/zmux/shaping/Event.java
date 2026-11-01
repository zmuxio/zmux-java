package io.zmux.shaping;

/**
 * A notification.
 *
 * @param nowNanos the session clock, nanoseconds
 * @param length frames length, bytes ({@link EventKind#RECORD_IN}, {@link EventKind#INJECTED})
 * @param paddingOnly the record holds PADDING only ({@link EventKind#RECORD_IN})
 * @param timer the timer slot, 0 or 1 ({@link EventKind#TIMER})
 * @param stream the stream ID ({@link EventKind#STREAM_OPEN}, {@link EventKind#STREAM_END})
 */
public record Event(EventKind kind, long pipe, long lane, long nowNanos, int length, boolean paddingOnly, int timer,
                    long stream) {}
