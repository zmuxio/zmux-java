package io.zmux;

import java.util.Arrays;
import java.util.Objects;

public final class Frame {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final FrameType type;
    private final int flags;
    private final long streamId;
    private final byte[] payload;

    public Frame(FrameType type, int flags, long streamId, byte[] payload) {
        this.type = Objects.requireNonNull(type, "type");
        if ((flags & ~0xff) != 0) {
            throw new IllegalArgumentException("zmux frame flags must fit in one byte");
        }
        if (streamId < 0L || streamId > Protocol.MAX_VARINT62) {
            throw new IllegalArgumentException("zmux stream id must be within varint62 range");
        }
        this.flags = flags;
        this.streamId = streamId;
        this.payload = payload == null || payload.length == 0 ? EMPTY_BYTES : Arrays.copyOf(payload, payload.length);
    }

    public FrameType type() {
        return type;
    }

    public int flags() {
        return flags;
    }

    public long streamId() {
        return streamId;
    }

    public byte[] payload() {
        return payload.length == 0 ? EMPTY_BYTES : Arrays.copyOf(payload, payload.length);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Frame)) {
            return false;
        }
        Frame that = (Frame) other;
        return type == that.type
                && flags == that.flags
                && streamId == that.streamId
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(type, flags, streamId);
        return 31 * result + Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
        return "Frame[type=" + type
                + ", flags=" + flags
                + ", streamId=" + streamId
                + ", payloadLength=" + payload.length
                + "]";
    }

    byte[] payloadView() {
        return payload;
    }
}
