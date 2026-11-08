package io.zmux.shaping;

/** The kind of an {@link Event}. */
public enum EventKind {
    /** The instance was installed. */
    START,
    /** The instance was replaced; no further calls. */
    REPLACED,
    /** A record arrived on the pipe ({@link Capability#RECORD_IN}). */
    RECORD_IN,
    /** The pending injection was sent, as a record of PADDING only or as PADDING of a record. */
    INJECTED,
    /** The pending injection was given up. */
    INJECT_DROPPED,
    /** The time of the wake-up came. */
    WAKE,
    /** Shaping of the pipe ended; no further calls. */
    STOPPED,
    /** The pipe's outbound direction failed; no further calls. */
    PIPE_FAILED,
    /** A stream got its identifier ({@link Capability#STREAMS}). */
    STREAM_OPEN,
    /** A stream's state was discarded ({@link Capability#STREAMS}). */
    STREAM_END
}
