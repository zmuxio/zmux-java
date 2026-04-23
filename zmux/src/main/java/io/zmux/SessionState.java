package io.zmux;

/**
 * Public session lifecycle state.
 *
 * <p>The public state intentionally hides internal runtime substates. It is
 * stable for callers, statistics, and lifecycle events, but it is not a
 * promise that an operation will succeed without checking the operation's own
 * flow-control, deadline, or terminal conditions.
 */
public enum SessionState {
    /**
     * The session is not usable as a negotiated zmux session.
     */
    INVALID,

    /**
     * The session is negotiated and not currently draining or closing.
     */
    READY,

    /**
     * A graceful drain has started. Existing accepted or opened streams may
     * still finish, but new work can be refused by GOAWAY or close admission.
     */
    DRAINING,

    /**
     * A terminal close has started and the runtime is flushing or observing
     * final transport state.
     */
    CLOSING,

    /**
     * The session terminated gracefully.
     */
    CLOSED,

    /**
     * The session terminated with a protocol, transport, local, or application
     * failure.
     */
    FAILED;

    public boolean terminal() {
        return this == CLOSED || this == FAILED;
    }
}
