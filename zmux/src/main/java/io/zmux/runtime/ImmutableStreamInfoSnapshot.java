package io.zmux.runtime;

import io.zmux.StreamMetadata;
import io.zmux.ZmuxStreamInfo;

import java.net.SocketAddress;

final class ImmutableStreamInfoSnapshot implements ZmuxStreamInfo {
    private final long streamId;
    private final StreamMetadata metadata;
    private final SocketAddress localAddress;
    private final SocketAddress remoteAddress;

    ImmutableStreamInfoSnapshot(long streamId,
                                StreamMetadata metadata,
                                SocketAddress localAddress,
                                SocketAddress remoteAddress) {
        this.streamId = streamId;
        this.metadata = metadata == null ? StreamMetadata.empty() : metadata;
        this.localAddress = localAddress;
        this.remoteAddress = remoteAddress;
    }

    @Override
    public long streamId() {
        return streamId;
    }

    @Override
    public StreamMetadata metadata() {
        return metadata;
    }

    @Override
    public SocketAddress localAddress() {
        return localAddress;
    }

    @Override
    public SocketAddress remoteAddress() {
        return remoteAddress;
    }

    @Override
    public byte[] openInfo() {
        return metadata.openInfo();
    }
}
