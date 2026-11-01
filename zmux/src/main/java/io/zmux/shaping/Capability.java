package io.zmux.shaping;

/** What a {@link Shaper} is given. */
public enum Capability {
    /** {@link Shaper#plan} is called. */
    PLAN,
    /** {@link EventKind#RECORD_IN} is delivered. */
    RECORD_IN,
    /** {@link EventKind#STREAM_OPEN} and {@link EventKind#STREAM_END} are delivered. */
    STREAMS
}
