package io.zmux;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.GatheringByteChannel;
import java.time.Instant;

public interface WriteHalf extends Closeable {
    void write(byte[] src, int offset, int length) throws IOException;

    void closeWrite() throws IOException;

    void setWriteDeadline(Instant deadline) throws IOException;

    default GatheringByteChannel gatheringOutput() {
        return null;
    }

    SocketAddress localAddress();

    SocketAddress remoteAddress();

    @Override
    default void close() throws IOException {
        closeWrite();
    }
}
