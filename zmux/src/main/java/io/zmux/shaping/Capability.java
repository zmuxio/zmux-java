package io.zmux.shaping;

/** What a {@link Shaper} is given. */
public enum Capability {
    /** {@link Shaper#plan} is called. */
    PLAN,
    /** {@link Shaper#planDatagram} is called. */
    DATAGRAM_PLAN,
    /** {@link EventKind#OUTBOUND} is delivered. */
    OUTBOUND,
    /** Outbound control-only units are included. */
    INCLUDE_CONTROL,
    /** {@link EventKind#STREAM_OPEN} and {@link EventKind#STREAM_END} are delivered. */
    STREAMS,
    /** {@link RecordView#meters()} and {@link Event#meters()} are filled. */
    BUDGET
}
