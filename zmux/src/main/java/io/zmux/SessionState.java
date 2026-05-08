package io.zmux;

/**
 * Public session lifecycle state.
 *
 * <p>This hides internal runtime substates and is stable for callers, stats,
 * and lifecycle events.
 */
public enum SessionState {
    /**
     * Not usable as a negotiated zmux session.
     */
    INVALID,

    /**
     * Negotiated and not draining or closing.
     */
    READY,

    /**
     * Graceful drain started; new work may be refused.
     */
    DRAINING,

    /**
     * Terminal close started.
     */
    CLOSING,

    /**
     * Terminated gracefully.
     */
    CLOSED,

    /**
     * Terminated with a failure.
     */
    FAILED;

    public boolean terminal() {
        return this == CLOSED || this == FAILED;
    }
}
