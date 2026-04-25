package io.zmux;

import java.net.SocketAddress;
import java.util.Objects;

@SuppressWarnings("MissingSerialAnnotation")
public final class ZmuxSocketAddress extends SocketAddress {
    //noinspection MissingSerialAnnotation,Serial
    private static final long serialVersionUID = 1L;

    private final String endpoint;
    private final long streamId;
    private final boolean streamIdSet;

    private ZmuxSocketAddress(String endpoint, long streamId, boolean streamIdSet) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.streamId = streamId;
        this.streamIdSet = streamIdSet;
    }

    public static ZmuxSocketAddress localPending() {
        return new ZmuxSocketAddress("local", 0L, false);
    }

    public static ZmuxSocketAddress remotePending() {
        return new ZmuxSocketAddress("remote", 0L, false);
    }

    public static ZmuxSocketAddress localStream(long streamId) {
        return new ZmuxSocketAddress("local", streamId, true);
    }

    public static ZmuxSocketAddress remoteStream(long streamId) {
        return new ZmuxSocketAddress("remote", streamId, true);
    }

    public boolean local() {
        return "local".equals(endpoint);
    }

    public boolean hasStreamId() {
        return streamIdSet;
    }

    public long streamId() {
        return streamId;
    }

    @Override
    public String toString() {
        return streamIdSet
                ? endpoint + "/stream/" + streamId
                : endpoint + "/stream/pending";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ZmuxSocketAddress)) {
            return false;
        }
        ZmuxSocketAddress that = (ZmuxSocketAddress) other;
        return streamId == that.streamId
                && streamIdSet == that.streamIdSet
                && endpoint.equals(that.endpoint);
    }

    @Override
    public int hashCode() {
        int result = endpoint.hashCode();
        result = 31 * result + Long.hashCode(streamId);
        result = 31 * result + Boolean.hashCode(streamIdSet);
        return result;
    }
}
