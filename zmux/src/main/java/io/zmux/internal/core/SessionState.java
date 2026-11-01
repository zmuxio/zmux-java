package io.zmux.internal.core;

/** Session states (STATE_MACHINE.md Section 4.1); the order is significant. */
public enum SessionState { ESTABLISHING, ACTIVE, DRAINING, SUSPENDED, CLOSING, CLOSED }
