package io.zmux;

import io.zmux.internal.StreamIoSupport;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public interface WriteHalf extends Closeable {
    void write(byte[] src, int offset, int length) throws IOException;

    default void write(byte[] src) throws IOException {
        Objects.requireNonNull(src, "src");
        if (src.length == 0) {
            return;
        }
        write(src, 0, src.length);
    }

    default void writeUtf8(String src) throws IOException {
        write(TextSupport.utf8Bytes(src, "src"));
    }

    default int write(ByteBuffer src) throws IOException {
        return StreamIoSupport.writeFromByteBuffer(src, this);
    }

    default OutputStream asOutputStream() {
        return new OutputStream() {
            private final byte[] singleByte = new byte[1];

            @Override
            public void write(int value) throws IOException {
                singleByte[0] = (byte) value;
                WriteHalf.this.write(singleByte, 0, 1);
            }

            @Override
            public void write(byte[] buffer, int offset, int length) throws IOException {
                Objects.requireNonNull(buffer, "buffer");
                RangeChecks.checkFromIndexSize(offset, length, buffer.length);
                if (length == 0) {
                    return;
                }
                WriteHalf.this.write(buffer, offset, length);
            }

            @Override
            public void close() throws IOException {
                WriteHalf.this.closeWrite();
            }
        };
    }

    void closeWrite() throws IOException;

    void setWriteDeadline(Instant deadline) throws IOException;

    default void setWriteTimeout(Duration timeout) throws IOException {
        setWriteDeadline(DeadlineSupport.after(timeout));
    }

    default void clearWriteDeadline() throws IOException {
        setWriteDeadline(null);
    }

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
    default void close() throws IOException {
        closeWrite();
    }
}
