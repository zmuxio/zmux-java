package io.zmux;

/** The direction was closed locally (finished, reset or stopped by this endpoint). */
public final class StreamClosedException extends StreamException {
    public StreamClosedException(long streamId, String message) {
        super(message, streamId);
    }
}
