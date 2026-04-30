package io.zmux;

import java.net.SocketAddress;

public interface ZmuxStreamInfo {
    long streamId();

    byte[] openInfo();

    default String openInfoUtf8() {
        return TextSupport.utf8String(openInfo());
    }

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
