package io.zmux;

import java.net.SocketAddress;

public interface ZmuxStreamInfo {
    long streamId();

    byte[] openInfo();

    default int openInfoLength() {
        return metadata().openInfoLength();
    }

    default boolean hasOpenInfo() {
        return openInfoLength() != 0;
    }

    StreamMetadata metadata();

    SocketAddress localAddress();

    SocketAddress remoteAddress();
}
