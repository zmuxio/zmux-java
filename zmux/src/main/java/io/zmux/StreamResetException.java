package io.zmux;

/** The peer reset its sending part: reads fail with its code; a reset stream is never complete (SPEC.md 7.5). */
public final class StreamResetException extends StreamException {
    private final long code;
    private final long reliableSize;

    public StreamResetException(long streamId, long code, long reliableSize) {
        super("stream reset with code 0x" + Long.toHexString(code), streamId);
        this.code = code;
        this.reliableSize = reliableSize;
    }

    public long code() { return code; }

    /** The Reliable Size of RESET_STREAM_AT, 0 for RESET_STREAM. */
    public long reliableSize() { return reliableSize; }
}
