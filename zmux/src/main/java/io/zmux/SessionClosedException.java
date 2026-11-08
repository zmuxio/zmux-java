package io.zmux;

/**
 * The session ended; {@link #error()} says why. For {@link SessionError.ResumeTimeout} and
 * {@link SessionError.PipesLost} the cause is the error of the last pipe lost, when known (for
 * example a join the peer refused while the session tried to resume).
 */
public final class SessionClosedException extends ZmuxException {
    private final transient SessionError error;

    public SessionClosedException(SessionError error) {
        super(error.message());
        this.error = error;
        Throwable last = switch (error) {
            case SessionError.ResumeTimeout t -> t.lastPipeError();
            case SessionError.PipesLost l -> l.lastPipeError();
            default -> null;
        };
        if (last != null) initCause(last);
    }

    public SessionError error() { return error; }
}
