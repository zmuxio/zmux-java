package io.zmux;


import java.io.IOException;

final class StreamApiSupport {
    private static final int TRANSIENT_BUFFER_CAPACITY = 8192;
    private static final ThreadLocal<byte[]> TRANSIENT_BUFFER =
            ThreadLocal.withInitial(() -> new byte[TRANSIENT_BUFFER_CAPACITY]);

    private StreamApiSupport() {
    }

    static int transientBufferSize(int remaining) {
        return Math.min(remaining, TRANSIENT_BUFFER_CAPACITY);
    }

    static byte[] transientBuffer() {
        return TRANSIENT_BUFFER.get();
    }

    static int readAllBytesChunkSize(int maxBytes, int currentSize) {
        int remaining = maxBytes - currentSize;
        return transientBufferSize(Math.max(remaining, 1));
    }

    static int checkedWritevTotalLength(byte[][] parts) throws IOException {
        return StreamIoSupport.checkedWritevTotalLength(parts, "writevFinal");
    }

    static int validateWriteFinalProgress(int written, int requested) throws IOException {
        if (written < 0 || written > requested) {
            throw new IOException("writeFinal reported invalid progress");
        }
        return written;
    }

    static int lastNonEmptyPart(byte[][] parts) {
        for (int i = parts.length - 1; i >= 0; i--) {
            if (parts[i].length > 0) {
                return i;
            }
        }
        return -1;
    }

    static ZmuxException readAllBytesTooLarge(int maxBytes) {
        return new ZmuxException(
                ErrorCode.FRAME_SIZE.code(),
                "readAllBytes",
                "stream payload exceeds readAllBytes limit: " + maxBytes,
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.READ,
                ZmuxTerminationKind.UNKNOWN
        );
    }
}
