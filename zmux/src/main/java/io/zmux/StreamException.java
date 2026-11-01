package io.zmux;

/** A failure of one stream; the session and its other streams go on. */
public abstract sealed class StreamException extends ZmuxException
        permits StreamResetException, StreamStoppedException, StreamClosedException, StreamRejectedException,
        StreamLimitException, GoingAwayException {
    private final long streamId;

    StreamException(String message, long streamId) {
        super(message);
        this.streamId = streamId;
    }

    /** The stream's identifier, or -1 when it was never assigned one (or is not known here). */
    public long streamId() { return streamId; }
}
