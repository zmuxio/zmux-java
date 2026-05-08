package io.zmux.runtime;

enum LateDataCause {
    NONE,
    CLOSE_READ,
    RESET,
    ABORT
}
