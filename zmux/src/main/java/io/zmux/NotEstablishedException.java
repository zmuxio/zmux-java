package io.zmux;

/** The value is not known before the peer's parameters arrived. */
public final class NotEstablishedException extends ZmuxException {
    public NotEstablishedException() {
        super("session not established");
    }
}
