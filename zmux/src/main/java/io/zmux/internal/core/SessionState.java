package io.zmux.internal.core;

/** Session states; the order is significant. */
public enum SessionState { ESTABLISHING, ACTIVE, DRAINING, SUSPENDED, CLOSING, CLOSED }
