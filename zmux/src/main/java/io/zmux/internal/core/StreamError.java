package io.zmux.internal.core;

/** A stream-level outcome (STATE_MACHINE.md Section 2.5). */
public record StreamError(Kind kind, long streamId, long code, long reliableSize, boolean remote) {

    /** Outcome kinds. */
    public enum Kind { RESET, STOPPED, CLOSED_LOCALLY, REJECTED, GOING_AWAY, PREAMBLE_TOO_LARGE }

    public static StreamError of(Kind kind, long id) {
        return new StreamError(kind, id, 0, 0, false);
    }
}
