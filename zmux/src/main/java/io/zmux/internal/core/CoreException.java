package io.zmux.internal.core;

/**
 * An outcome of a core API call that is not a success: a stream outcome, the
 * session's error, or an API condition. Carries no stack trace.
 */
public final class CoreException extends Exception {
    /** API conditions that end nothing. */
    public enum Reason {
        STREAM, SESSION, STREAM_LIMIT, NOT_NEGOTIATED, NOT_ESTABLISHED, TOO_LATE, INVALID, DATAGRAM_TOO_LARGE,
        DATAGRAM_DROPPED, INVALID_DATAGRAM_OPTIONS,
        /** The next receive backing would exceed the application lease allowance. */
        CHUNK_BUDGET,
        /** A pipe the session cannot take now: its carrier nests in this session, or it was retired. */
        PIPE_REFUSED,
        /** A stream preamble beyond the size the peer accepts (or the bound assumed before its parameters). */
        PREAMBLE_TOO_LARGE,
        /** A local join of a session without join keys yet: {@link #max()} holds what is missing ({@link #MISSING_END_TO_END}, {@link #MISSING_PSK}). */
        JOIN_PENDING,
        /** The deferred PSK was supplied already. */
        JOIN_SECRET_SET,
        /** The peer's verified Reply refused a local join. */
        JOIN_REFUSED,
        /** The wait for a pipe ended because this endpoint aborted the pipe (by the application, at its bound, or to make room). */
        PIPE_ABORTED
    }

    /** A datagram refused by the sender before queueing. */
    public enum DatagramDrop {
        DEADLINE("deadline"),
        NO_NATIVE_PATH("no_native_path"),
        LIMITER("limiter"),
        QUEUE_OVERFLOW("queue_overflow"),
        SEQUENCE_EXHAUSTED("sequence_exhausted");

        private final String code;

        DatagramDrop(String code) { this.code = code; }

        public String code() { return code; }
    }

    private final Reason reason;
    private final transient StreamError stream;
    private final transient SessionError session;
    private final int max;
    private final long pipe;
    private final DatagramDrop datagramDrop;

    private CoreException(Reason reason, StreamError stream, SessionError session, int max, long pipe) {
        this(reason, stream, session, max, pipe, null);
    }

    private CoreException(Reason reason, StreamError stream, SessionError session, int max, long pipe,
                          DatagramDrop datagramDrop) {
        super(null, null, false, false);
        this.reason = reason;
        this.stream = stream;
        this.session = session;
        this.max = max;
        this.pipe = pipe;
        this.datagramDrop = datagramDrop;
    }

    public static CoreException of(Reason r) {
        return new CoreException(r, null, null, 0, -1);
    }

    public static CoreException stream(StreamError e) {
        return new CoreException(Reason.STREAM, e, null, 0, -1);
    }

    /** A preamble beyond max, the largest one accepted now. */
    public static CoreException preambleTooLarge(int max) {
        return new CoreException(Reason.PREAMBLE_TOO_LARGE, null, null, max, -1);
    }

    public static CoreException session(SessionError e) {
        return new CoreException(Reason.SESSION, null, e, 0, -1);
    }

    /** The wait for pipe ended because this endpoint aborted it. */
    public static CoreException pipeAborted(long pipe) {
        return new CoreException(Reason.PIPE_ABORTED, null, null, 0, pipe);
    }

    /** The peer refused the local join of pipe. */
    public static CoreException joinRefused(long pipe) {
        return new CoreException(Reason.JOIN_REFUSED, null, null, 0, pipe);
    }

    /** What a join-pending session lacks: the end-to-end declaration, the deferred PSK. */
    public static final int MISSING_END_TO_END = 1, MISSING_PSK = 2;

    /** A join pending, missing the parts of {@link #MISSING_END_TO_END} and {@link #MISSING_PSK} set in missing. */
    public static CoreException joinPending(int missing) {
        return new CoreException(Reason.JOIN_PENDING, null, null, missing, -1);
    }

    public static CoreException tooLarge(int max) {
        return new CoreException(Reason.DATAGRAM_TOO_LARGE, null, null, max, -1);
    }

    public static CoreException datagramDropped(DatagramDrop reason) {
        return new CoreException(Reason.DATAGRAM_DROPPED, null, null, 0, -1, reason);
    }

    public DatagramDrop datagramDrop() { return datagramDrop; }

    public Reason reason() { return reason; }
    public StreamError streamError() { return stream; }
    public SessionError sessionError() { return session; }
    /** The largest datagram payload for DATAGRAM_TOO_LARGE; what is missing for JOIN_PENDING. */
    public int max() { return max; }
    /** The pipe of JOIN_REFUSED and PIPE_ABORTED; -1 otherwise. */
    public long pipe() { return pipe; }

    @Override
    public String getMessage() {
        return switch (reason) {
            case STREAM -> stream.toString();
            case SESSION -> session.message();
            case DATAGRAM_DROPPED -> datagramDrop.code();
            case DATAGRAM_TOO_LARGE -> "datagram too large (max " + max + ")";
            default -> reason.name();
        };
    }
}
