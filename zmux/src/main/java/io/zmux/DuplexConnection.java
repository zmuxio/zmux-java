package io.zmux;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.nio.channels.GatheringByteChannel;

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

    @Override
    void close() throws IOException;
}
