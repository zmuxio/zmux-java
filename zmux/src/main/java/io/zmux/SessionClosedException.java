package io.zmux;

/** The session ended; {@link #error()} says why. */
public final class SessionClosedException extends ZmuxException {
    private final transient SessionError error;

    public SessionClosedException(SessionError error) {
        super(error.message());
        this.error = error;
    }

    public SessionError error() { return error; }
}
