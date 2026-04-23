package io.zmux;

public enum ZmuxTerminationKind {
    UNKNOWN,
    GRACEFUL,
    STOPPED,
    RESET,
    ABORT,
    SESSION_TERMINATION,
    TIMEOUT,
    INTERRUPTED
}
