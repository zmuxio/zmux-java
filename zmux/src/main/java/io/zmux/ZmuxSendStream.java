package io.zmux;

import io.zmux.support.DeadlineSupport;
import io.zmux.support.RangeChecks;
import io.zmux.support.StreamApiSupport;
import io.zmux.transport.WriteHalf;

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public interface ZmuxSendStream extends ZmuxStreamInfo, WriteHalf {
    void write(byte[] src, int offset, int length) throws IOException;

    default void write(byte[] src) throws IOException {
        Objects.requireNonNull(src, "src");
        if (src.length == 0) {
            return;
        }
        write(src, 0, src.length);
    }

    int writeFinal(byte[] src, int offset, int length) throws IOException;

    default int writeFinal(byte[] src) throws IOException {
        Objects.requireNonNull(src, "src");
        return StreamApiSupport.validateWriteFinalProgress(writeFinal(src, 0, src.length), src.length);
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
            StreamApiSupport.validateWriteFinalProgress(written, length);
            if (written > 0) {
                src.position(position + written);
            }
            return written;
        }

        int total = 0;
        byte[] buffer = StreamApiSupport.transientBuffer();
        while (src.remaining() > buffer.length) {
            int position = src.position();
            src.get(buffer, 0, buffer.length);
            try {
                write(buffer, 0, buffer.length);
                total += buffer.length;
            } catch (IOException error) {
                src.position(position);
                throw error;
            }
        }

        int finalPosition = src.position();
        int finalLength = src.remaining();
        src.get(buffer, 0, finalLength);
        int written;
        try {
            written = writeFinal(buffer, 0, finalLength);
            StreamApiSupport.validateWriteFinalProgress(written, finalLength);
        } catch (IOException error) {
            src.position(finalPosition);
            throw error;
        }
        if (written >= 0 && written < finalLength) {
            src.position(finalPosition + written);
        }
        if (written > 0) {
            total += written;
        }
        return total;
    }

    default int writevFinal(byte[]... parts) throws IOException {
        Objects.requireNonNull(parts, "parts");
        if (parts.length == 1) {
            return writeFinal(Objects.requireNonNull(parts[0], "parts[0]"));
        }
        if (parts.length == 0) {
            return writeFinal(DeadlineSupport.EMPTY_BYTES);
        }
        int total = StreamApiSupport.checkedWritevTotalLength(parts);
        if (total == 0) {
            return writeFinal(DeadlineSupport.EMPTY_BYTES);
        }
        int lastNonEmpty = StreamApiSupport.lastNonEmptyPart(parts);
        int written = 0;
        for (int i = 0; i <= lastNonEmpty; i++) {
            byte[] part = parts[i];
            if (part.length == 0) {
                continue;
            }
            if (i < lastNonEmpty) {
                write(part);
                written += part.length;
            } else {
                written += StreamApiSupport.validateWriteFinalProgress(writeFinal(part), part.length);
            }
        }
        return written;
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
    SocketAddress localAddress();

    @Override
    SocketAddress remoteAddress();

    @Override
    default void close() throws IOException {
        closeWrite();
    }

}
