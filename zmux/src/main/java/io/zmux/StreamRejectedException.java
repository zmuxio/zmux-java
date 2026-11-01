package io.zmux;

/** The peer did not process the stream (ZMUX_REJECTED, or at or above a GOAWAY watermark); retryable. */
public final class StreamRejectedException extends StreamException {
    public StreamRejectedException(long streamId) {
        super("stream rejected", streamId);
    }

    @Override
    public boolean isRetryable() { return true; }
}
