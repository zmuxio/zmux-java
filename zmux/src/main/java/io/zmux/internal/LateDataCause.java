package io.zmux.internal;

enum LateDataCause {
    NONE,
    CLOSE_READ,
    RESET,
    ABORT
}
