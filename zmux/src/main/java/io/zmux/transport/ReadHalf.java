package io.zmux.transport;

import io.zmux.support.DeadlineSupport;
import io.zmux.support.RangeChecks;
import io.zmux.support.StreamIoSupport;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public interface ReadHalf extends Closeable {
    int read(byte[] dst, int offset, int length) throws IOException;

    default int read(byte[] dst) throws IOException {
        Objects.requireNonNull(dst, "dst");
        if (dst.length == 0) {
            return 0;
        }
        return read(dst, 0, dst.length);
    }

    default int read(ByteBuffer dst) throws IOException {
        return StreamIoSupport.readIntoByteBuffer(dst, this);
    }

    default InputStream asInputStream() {
        return new InputStream() {
            private final byte[] singleByte = new byte[1];

            @Override
            public int read() throws IOException {
                int read;
                do {
                    read = ReadHalf.this.read(singleByte, 0, 1);
                    StreamIoSupport.validateReadProgress(read, 1);
                } while (read == 0);
                return read < 0 ? -1 : singleByte[0] & 0xff;
            }

            @Override
            public int read(byte[] buffer, int offset, int length) throws IOException {
                Objects.requireNonNull(buffer, "buffer");
                RangeChecks.checkFromIndexSize(offset, length, buffer.length);
                if (length == 0) {
                    return 0;
                }
                return StreamIoSupport.validateReadProgress(ReadHalf.this.read(buffer, offset, length), length);
            }

            @Override
            public void close() throws IOException {
                ReadHalf.this.closeRead();
            }
        };
    }

    void closeRead() throws IOException;

    void setReadDeadline(Instant deadline) throws IOException;

    default void setReadTimeout(Duration timeout) throws IOException {
        setReadDeadline(DeadlineSupport.after(timeout));
    }

    default void clearReadDeadline() throws IOException {
        setReadDeadline(null);
    }

    default SocketAddress localAddress() {
        return null;
    }

    default SocketAddress remoteAddress() {
        return null;
    }

    @Override
    default void close() throws IOException {
        closeRead();
    }
}
