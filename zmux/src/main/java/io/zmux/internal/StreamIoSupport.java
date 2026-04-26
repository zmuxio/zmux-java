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
        byte[] buffer = new byte[transientBufferSize(dst.remaining())];
        int read = reader.read(buffer, 0, buffer.length);
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

        int initialPosition = src.position();
        int total = 0;
        ByteBuffer duplicate = src.duplicate();
        byte[] buffer = new byte[transientBufferSize(duplicate.remaining())];
        while (duplicate.hasRemaining()) {
            int length = Math.min(duplicate.remaining(), buffer.length);
            duplicate.get(buffer, 0, length);
            writer.write(buffer, 0, length);
            total += length;
            src.position(initialPosition + total);
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
        return Math.min(remaining, 8192);
    }
}
