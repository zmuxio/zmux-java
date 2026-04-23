package io.zmux;

import java.net.SocketAddress;

public interface ZmuxStreamInfo {
    long streamId();

    byte[] openInfo();

    StreamMetadata metadata();

    SocketAddress localAddress();

    SocketAddress remoteAddress();
}
