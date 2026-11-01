package io.zmux.internal.core;

/**
 * An outcome of a core API call that is not a success: a stream outcome, the
 * session's error, or an API condition. Carries no stack trace.
 */
public final class CoreException extends Exception {
    /** API conditions that end nothing. */
    public enum Reason {
        STREAM, SESSION, STREAM_LIMIT, NOT_NEGOTIATED, NOT_ESTABLISHED, TOO_LATE, INVALID, DATAGRAM_TOO_LARGE,
        /** A pipe the session cannot take now: its carrier nests in this session, or its reservation was retired. */
        PIPE_REFUSED,
        /** A stream preamble beyond the size the peer accepts (or the bound assumed before its parameters). */
        PREAMBLE_TOO_LARGE
    }

    private final Reason reason;
    private final transient StreamError stream;
    private final transient SessionError session;
    private final int max;

    private CoreException(Reason reason, StreamError stream, SessionError session, int max) {
        super(null, null, false, false);
        this.reason = reason;
        this.stream = stream;
        this.session = session;
        this.max = max;
    }

    public static CoreException of(Reason r) {
        return new CoreException(r, null, null, 0);
    }

    public static CoreException stream(StreamError e) {
        return new CoreException(Reason.STREAM, e, null, 0);
    }

    public static CoreException session(SessionError e) {
        return new CoreException(Reason.SESSION, null, e, 0);
    }

    public static CoreException tooLarge(int max) {
        return new CoreException(Reason.DATAGRAM_TOO_LARGE, null, null, max);
    }

    public Reason reason() { return reason; }
    public StreamError streamError() { return stream; }
    public SessionError sessionError() { return session; }
    /** The largest datagram payload for DATAGRAM_TOO_LARGE. */
    public int max() { return max; }

    @Override
    public String getMessage() {
        return switch (reason) {
            case STREAM -> stream.toString();
            case SESSION -> session.message();
            case DATAGRAM_TOO_LARGE -> "datagram too large (max " + max + ")";
            default -> reason.name();
        };
    }
}
