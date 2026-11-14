package io.zmux;

/**
 * A read that collects a whole stream ({@link ZmuxReceiveStream#readAll(long)}) found more than
 * its limit; the receiving part was stopped with code 0.
 */
public final class LimitExceededException extends StreamException {
    private final long limit;

    public LimitExceededException(long streamId, long limit) {
        super("stream longer than " + limit + " bytes", streamId);
        this.limit = limit;
    }

    /** The limit the stream exceeded, in bytes. */
    public long limit() { return limit; }
}
