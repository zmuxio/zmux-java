package io.zmux.shaping;

/** The kind of an {@link Event}. */
public enum EventKind {
    /** The instance was installed. */
    START,
    /** The instance was replaced; no further calls. */
    REPLACED,
    /** The carrier took a qualifying outbound unit. */
    OUTBOUND,
    /** The engine committed the requested cover. */
    INJECTION_ACCEPTED,
    /** The carrier completed the cover dispatch, possibly with a partial prefix. */
    INJECTION_CARRIER_COMPLETED,
    /** The engine declined the requested cover. */
    INJECTION_REJECTED,
    /** A scheduled timer fired. */
    TIMER,
    /** Shaping of the pipe ended; no further calls. */
    STOPPED,
    /** The pipe's outbound direction failed; no further calls. */
    PIPE_FAILED,
    /** A stream got its identifier ({@link Capability#STREAMS}). */
    STREAM_OPEN,
    /** A stream's state was discarded ({@link Capability#STREAMS}). */
    STREAM_END
}
