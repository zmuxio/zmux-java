package io.zmux;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public interface ZmuxSendStream extends ZmuxStreamInfo, AutoCloseable {
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

    int writeFinal(byte[] src, int offset, int length) throws IOException;

    default int writeFinal(byte[] src) throws IOException {
        Objects.requireNonNull(src, "src");
        return writeFinal(src, 0, src.length);
    }

    default int writeFinal(ByteBuffer src) throws IOException {
        Objects.requireNonNull(src, "src");
        if (!src.hasRemaining()) {
            return writeFinal(DeadlineSupport.EMPTY_BYTES);
        }
        if (src.hasArray()) {
            int position = src.position();
            int length = src.remaining();
            int written = writeFinal(src.array(), src.arrayOffset() + position, length);
            if (written > 0) {
                src.position(position + written);
            }
            return written;
        }

        int initialPosition = src.position();
        int total = 0;
        ByteBuffer duplicate = src.duplicate();
        byte[] buffer = new byte[StreamApiSupport.transientBufferSize(duplicate.remaining())];
        while (duplicate.remaining() > buffer.length) {
            duplicate.get(buffer, 0, buffer.length);
            write(buffer, 0, buffer.length);
            total += buffer.length;
            src.position(initialPosition + total);
        }

        int finalLength = duplicate.remaining();
        duplicate.get(buffer, 0, finalLength);
        int written = writeFinal(buffer, 0, finalLength);
        if (written > 0) {
            total += written;
            src.position(initialPosition + total);
        }
        return total;
    }

    default int writevFinal(byte[]... parts) throws IOException {
        Objects.requireNonNull(parts, "parts");
        if (parts.length == 0) {
            return writeFinal(DeadlineSupport.EMPTY_BYTES);
        }
        int total = StreamApiSupport.checkedWritevTotalLength(parts);
        if (total == 0) {
            return writeFinal(DeadlineSupport.EMPTY_BYTES);
        }
        int lastNonEmpty = StreamApiSupport.lastNonEmptyPart(parts);
        for (int i = 0; i <= lastNonEmpty; i++) {
            byte[] part = parts[i];
            if (part.length == 0) {
                continue;
            }
            if (i < lastNonEmpty) {
                write(part);
            } else {
                writeFinal(part);
            }
        }
        return total;
    }

    default OutputStream asOutputStream() {
        return new OutputStream() {
            private final byte[] singleByte = new byte[1];

            @Override
            public void write(int value) throws IOException {
                singleByte[0] = (byte) value;
                ZmuxSendStream.this.write(singleByte, 0, 1);
            }

            @Override
            public void write(byte[] buffer, int offset, int length) throws IOException {
                Objects.requireNonNull(buffer, "buffer");
                RangeChecks.checkFromIndexSize(offset, length, buffer.length);
                if (length == 0) {
                    return;
                }
                ZmuxSendStream.this.write(buffer, offset, length);
            }

            @Override
            public void close() throws IOException {
                ZmuxSendStream.this.closeWrite();
            }
        };
    }

    void updateMetadata(MetadataUpdate update) throws IOException;

    void closeWrite() throws IOException;

    default void cancelWrite(ErrorCode code) throws IOException {
        Objects.requireNonNull(code, "code");
        cancelWrite(code.code());
    }

    void cancelWrite(long code) throws IOException;

    default void closeWithError(ErrorCode code, String reason) throws IOException {
        Objects.requireNonNull(code, "code");
        closeWithError(code.code(), reason);
    }

    void closeWithError(long code, String reason) throws IOException;

    void setWriteDeadline(Instant deadline) throws IOException;

    default void setDeadline(Instant deadline) throws IOException {
        setWriteDeadline(deadline);
    }

    default void setWriteTimeout(Duration timeout) throws IOException {
        setWriteDeadline(DeadlineSupport.after(timeout));
    }

    default void clearWriteDeadline() throws IOException {
        setWriteDeadline(null);
    }

    @Override
    void close() throws IOException;

}
