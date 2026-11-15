package io.zmux.internal.core;

/**
 * A stream-level outcome.
 *
 * @param code the error code; for PREAMBLE_TOO_LARGE the largest preamble accepted
 */
public record StreamError(Kind kind, long streamId, long code, long reliableSize, boolean remote) {

    /** Outcome kinds. */
    public enum Kind { RESET, STOPPED, CLOSED_LOCALLY, REJECTED, GOING_AWAY, PREAMBLE_TOO_LARGE }

    public static StreamError of(Kind kind, long id) {
        return new StreamError(kind, id, 0, 0, false);
    }
}
