package io.zmux;

/** The peer refused a new session in the protocol. */
public final class ConnectionRefusedException extends SessionClosedException {
    ConnectionRefusedException(SessionError.TransportClosed error) { super(error); }

    /** Reason supplied by the listener. */
    public String reason() { return ((SessionError.TransportClosed) error()).reason(); }
}
