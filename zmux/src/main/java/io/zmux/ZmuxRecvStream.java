package io.zmux;

import io.zmux.internal.StreamIoSupport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public interface ZmuxRecvStream extends ZmuxStreamInfo, ReadHalf {
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

    default byte[] readAllBytes() throws IOException {
        return readAllBytes(Integer.MAX_VALUE);
    }

    default byte[] readAllBytes(int maxBytes) throws IOException {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must be >= 0");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.min(maxBytes, 8192));
        byte[] buffer = new byte[Math.min(Math.max(maxBytes, 1), 8192)];
        while (true) {
            int read = read(buffer, 0, buffer.length);
            if (read < 0) {
                return out.toByteArray();
            }
            if (read == 0) {
                continue;
            }
            if (out.size() > maxBytes - read) {
                throw StreamApiSupport.readAllBytesTooLarge(maxBytes);
            }
            out.write(buffer, 0, read);
        }
    }

    default InputStream asInputStream() {
        return new InputStream() {
            private final byte[] singleByte = new byte[1];

            @Override
            public int read() throws IOException {
                int read;
                do {
                    read = ZmuxRecvStream.this.read(singleByte, 0, 1);
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
                return ZmuxRecvStream.this.read(buffer, offset, length);
            }

            @Override
            public void close() throws IOException {
                ZmuxRecvStream.this.closeRead();
            }
        };
    }

    void closeRead() throws IOException;

    default void cancelRead(ErrorCode code) throws IOException {
        Objects.requireNonNull(code, "code");
        cancelRead(code.code());
    }

    void cancelRead(long code) throws IOException;

    default void closeWithError(ErrorCode code, String reason) throws IOException {
        Objects.requireNonNull(code, "code");
        closeWithError(code.code(), reason);
    }

    void closeWithError(long code, String reason) throws IOException;

    void setReadDeadline(Instant deadline) throws IOException;

    default void setDeadline(Instant deadline) throws IOException {
        setReadDeadline(deadline);
    }

    default void setReadTimeout(Duration timeout) throws IOException {
        setReadDeadline(DeadlineSupport.after(timeout));
    }

    default void clearReadDeadline() throws IOException {
        setReadDeadline(null);
    }

    @Override
    SocketAddress localAddress();

    @Override
    SocketAddress remoteAddress();

    @Override
    default void close() throws IOException {
        closeRead();
    }

}
