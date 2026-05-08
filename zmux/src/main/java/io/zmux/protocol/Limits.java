package io.zmux.protocol;

import io.zmux.Settings;
import java.util.Objects;

public final class Limits {
    private final long maxFramePayload;
    private final long maxControlPayloadBytes;
    private final long maxExtensionPayloadBytes;
    private transient volatile Limits normalized;

    public Limits(long maxFramePayload, long maxControlPayloadBytes, long maxExtensionPayloadBytes) {
        requireVarint62(maxFramePayload, "maxFramePayload");
        requireVarint62(maxControlPayloadBytes, "maxControlPayloadBytes");
        requireVarint62(maxExtensionPayloadBytes, "maxExtensionPayloadBytes");
        this.maxFramePayload = maxFramePayload;
        this.maxControlPayloadBytes = maxControlPayloadBytes;
        this.maxExtensionPayloadBytes = maxExtensionPayloadBytes;
        if (maxFramePayload != 0L && maxControlPayloadBytes != 0L && maxExtensionPayloadBytes != 0L) {
            this.normalized = this;
        }
    }

    private static void requireVarint62(long value, String field) {
        if (value < 0L) {
            throw new IllegalArgumentException("zmux limits " + field + " must be >= 0");
        }
        if (value > Protocol.MAX_VARINT62) {
            throw new IllegalArgumentException("zmux limits " + field + " must be within varint62 range");
        }
    }

    public Limits normalize() {
        Limits cached = normalized;
        if (cached != null) {
            return cached;
        }
        Settings defaults = Settings.defaults();
        Limits resolved = new Limits(
                maxFramePayload == 0 ? defaults.maxFramePayload() : maxFramePayload,
                maxControlPayloadBytes == 0 ? defaults.maxControlPayloadBytes() : maxControlPayloadBytes,
                maxExtensionPayloadBytes == 0 ? defaults.maxExtensionPayloadBytes() : maxExtensionPayloadBytes
        );
        normalized = resolved;
        return resolved;
    }

    public long maxFramePayload() {
        return maxFramePayload;
    }

    public long maxControlPayloadBytes() {
        return maxControlPayloadBytes;
    }

    public long maxExtensionPayloadBytes() {
        return maxExtensionPayloadBytes;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Limits)) {
            return false;
        }
        Limits that = (Limits) other;
        return maxFramePayload == that.maxFramePayload
                && maxControlPayloadBytes == that.maxControlPayloadBytes
                && maxExtensionPayloadBytes == that.maxExtensionPayloadBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxFramePayload, maxControlPayloadBytes, maxExtensionPayloadBytes);
    }

    @Override
    public String toString() {
        return "Limits[maxFramePayload=" + maxFramePayload
                + ", maxControlPayloadBytes=" + maxControlPayloadBytes
                + ", maxExtensionPayloadBytes=" + maxExtensionPayloadBytes
                + "]";
    }
}
