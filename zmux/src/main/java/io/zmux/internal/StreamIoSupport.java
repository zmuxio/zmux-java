package io.zmux.internal;

import io.zmux.ErrorCode;
import io.zmux.ReadHalf;
import io.zmux.WriteHalf;
import io.zmux.ZmuxErrorDirection;
import io.zmux.ZmuxErrorScope;
import io.zmux.ZmuxErrorSource;
import io.zmux.ZmuxException;
import io.zmux.ZmuxTerminationKind;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.Objects;

public final class StreamIoSupport {
    private static final int TRANSIENT_BUFFER_CAPACITY = 8192;
    private static final ThreadLocal<byte[]> TRANSIENT_BUFFER = new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[TRANSIENT_BUFFER_CAPACITY];
        }
    };

    private StreamIoSupport() {
    }

    public static int readIntoByteBuffer(ByteBuffer dst, ReadHalf reader) throws IOException {
        Objects.requireNonNull(dst, "dst");
        Objects.requireNonNull(reader, "reader");
        if (!dst.hasRemaining()) {
            return 0;
        }
        if (dst.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }
        if (dst.hasArray()) {
            int position = dst.position();
            int read = reader.read(dst.array(), dst.arrayOffset() + position, dst.remaining());
            if (read > 0) {
                dst.position(position + read);
            }
            return read;
        }
        byte[] buffer = TRANSIENT_BUFFER.get();
        int read = reader.read(buffer, 0, transientBufferSize(dst.remaining()));
        if (read > 0) {
            dst.put(buffer, 0, read);
        }
        return read;
    }

    public static int writeFromByteBuffer(ByteBuffer src, WriteHalf writer) throws IOException {
        Objects.requireNonNull(src, "src");
        Objects.requireNonNull(writer, "writer");
        if (!src.hasRemaining()) {
            return 0;
        }
        if (src.hasArray()) {
            int position = src.position();
            int length = src.remaining();
            writer.write(src.array(), src.arrayOffset() + position, length);
            src.position(position + length);
            return length;
        }

        int total = 0;
        byte[] buffer = TRANSIENT_BUFFER.get();
        while (src.hasRemaining()) {
            int position = src.position();
            int length = Math.min(src.remaining(), buffer.length);
            src.get(buffer, 0, length);
            try {
                writer.write(buffer, 0, length);
                total += length;
            } catch (IOException error) {
                src.position(position);
                throw error;
            }
        }
        return total;
    }

    public static int checkedWritevTotalLength(byte[][] parts, String operation) throws IOException {
        Objects.requireNonNull(parts, "parts");
        int total = 0;
        for (int i = 0; i < parts.length; i++) {
            byte[] part = Objects.requireNonNull(parts[i], "parts[" + i + "]");
            if (part.length > Integer.MAX_VALUE - total) {
                throw multipartWriteTooLarge(operation);
            }
            total += part.length;
        }
        return total;
    }

    private static ZmuxException multipartWriteTooLarge(String operation) {
        return new ZmuxException(
                ErrorCode.FRAME_SIZE.code(),
                operation,
                "multipart write exceeds maximum supported size",
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.WRITE,
                ZmuxTerminationKind.UNKNOWN
        );
    }

    private static int transientBufferSize(int remaining) {
        return Math.min(remaining, TRANSIENT_BUFFER_CAPACITY);
    }
}
