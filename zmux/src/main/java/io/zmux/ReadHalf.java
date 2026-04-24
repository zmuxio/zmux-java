package io.zmux;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.time.Instant;

public interface ReadHalf extends Closeable {
    int read(byte[] dst, int offset, int length) throws IOException;

    void closeRead() throws IOException;

    void setReadDeadline(Instant deadline) throws IOException;

    SocketAddress localAddress();

    SocketAddress remoteAddress();

    @Override
    default void close() throws IOException {
        closeRead();
    }
}
