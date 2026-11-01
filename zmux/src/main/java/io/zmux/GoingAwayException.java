package io.zmux;

/** Local draining or peer GOAWAY prevents opening a stream; retryable on another session. */
public final class GoingAwayException extends StreamException {
    public GoingAwayException(long streamId) {
        super("going away", streamId);
    }

    @Override
    public boolean isRetryable() { return true; }
}
