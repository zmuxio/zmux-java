package io.zmux.transport;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.nio.channels.GatheringByteChannel;
import java.time.Instant;

public interface DuplexConnection extends Closeable {
    InputStream input();

    OutputStream output();

    default GatheringByteChannel gatheringOutput() {
        return null;
    }

    default SocketAddress localAddress() {
        return null;
    }

    default SocketAddress remoteAddress() {
        return null;
    }

    default boolean supportsReadDeadline() {
        return false;
    }

    default void setReadDeadline(Instant deadline) throws IOException {
    }

    default boolean supportsWriteDeadline() {
        return false;
    }

    default void setWriteDeadline(Instant deadline) throws IOException {
    }

    @Override
    void close() throws IOException;
}
