package io.zmux;

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

    default int write(ByteBuffer src) throws IOException {
        Objects.requireNonNull(src, "src");
        if (!src.hasRemaining()) {
            return 0;
        }
        if (src.hasArray()) {
            int position = src.position();
            int length = src.remaining();
            write(src.array(), src.arrayOffset() + position, length);
            src.position(position + length);
            return length;
        }

        int initialPosition = src.position();
        int total = 0;
        ByteBuffer duplicate = src.duplicate();
        byte[] buffer = new byte[StreamApiSupport.transientBufferSize(duplicate.remaining())];
        while (duplicate.hasRemaining()) {
            int length = Math.min(duplicate.remaining(), buffer.length);
            duplicate.get(buffer, 0, length);
            write(buffer, 0, length);
            total += length;
            src.position(initialPosition + total);
        }
        return total;
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
