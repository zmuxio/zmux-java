package io.zmux;

/** The feature is not in use in this session (the peer did not offer it, or it was not enabled). */
public final class NotNegotiatedException extends ZmuxException {
    public NotNegotiatedException(String what) {
        super(what + " not negotiated");
    }
}
