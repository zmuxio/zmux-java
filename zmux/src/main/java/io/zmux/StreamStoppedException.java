package io.zmux;

/** The peer asked to stop sending (STOP_SENDING); writes fail with its code. */
public final class StreamStoppedException extends StreamException {
    private final long code;

    public StreamStoppedException(long streamId, long code) {
        super("stream stopped with code 0x" + Long.toHexString(code), streamId);
        this.code = code;
    }

    public long code() { return code; }
}
