package io.zmux;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
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
        Objects.requireNonNull(dst, "dst");
        if (!dst.hasRemaining()) {
            return 0;
        }
        if (dst.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }
        if (dst.hasArray()) {
            int position = dst.position();
            int read = read(dst.array(), dst.arrayOffset() + position, dst.remaining());
            if (read > 0) {
                dst.position(position + read);
            }
            return read;
        }

        byte[] buffer = new byte[StreamApiSupport.transientBufferSize(dst.remaining())];
        int read = read(buffer, 0, buffer.length);
        if (read > 0) {
            dst.put(buffer, 0, read);
        }
        return read;
    }

    default InputStream asInputStream() {
        return new InputStream() {
            private final byte[] singleByte = new byte[1];

            @Override
            public int read() throws IOException {
                int read;
                do {
                    read = ReadHalf.this.read(singleByte, 0, 1);
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
                return ReadHalf.this.read(buffer, offset, length);
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
