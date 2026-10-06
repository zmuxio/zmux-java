package io.zmux.support;

import io.zmux.*;

import java.io.IOException;

public final class StreamApiSupport {
    private static final int TRANSIENT_BUFFER_CAPACITY = 8192;
    private static final ThreadLocal<byte[]> TRANSIENT_BUFFER =
            ThreadLocal.withInitial(() -> new byte[TRANSIENT_BUFFER_CAPACITY]);

    private StreamApiSupport() {
    }

    /**
     * Best-effort abort of a stream that an open-and-send helper created but will not return because its initial
     * write failed. The caller never receives the handle, so without this the stream (whose opener may already be
     * peer-visible) would stay open on both sides until the session ends, or its committed ID would be left unused.
     * The abort uses the failure's wire code when it has one and {@code CANCELLED} otherwise; a secondary failure is
     * attached to {@code failure} as suppressed.
     *
     * <p>Internal helper for the {@link ZmuxSession} open-and-send default methods and transport adapters; it is not
     * part of the stable API and may change without notice.
     */
    public static void abortUnreturnedStream(ZmuxSendStream stream, Throwable failure, String reason) {
        if (stream == null) {
            return;
        }
        long code = ZmuxErrors.code(failure, ErrorCode.CANCELLED.code());
        try {
            stream.closeWithError(code, reason);
        } catch (Throwable secondary) {
            if (failure != null && secondary != failure) {
                failure.addSuppressed(secondary);
            }
        }
    }

    public static int transientBufferSize(int remaining) {
        return Math.min(remaining, TRANSIENT_BUFFER_CAPACITY);
    }

    public static byte[] transientBuffer() {
        return TRANSIENT_BUFFER.get();
    }

    public static int readAllBytesChunkSize(int maxBytes, int currentSize) {
        int remaining = maxBytes - currentSize;
        return transientBufferSize(Math.max(remaining, 1));
    }

    public static int checkedWritevTotalLength(byte[][] parts) throws IOException {
        return StreamIoSupport.checkedWritevTotalLength(parts, "writevFinal");
    }

    public static int validateWriteFinalProgress(int written, int requested) throws IOException {
        if (written < 0 || written > requested) {
            throw new IOException("writeFinal reported invalid progress");
        }
        return written;
    }

    public static int lastNonEmptyPart(byte[][] parts) {
        for (int i = parts.length - 1; i >= 0; i--) {
            if (parts[i].length > 0) {
                return i;
            }
        }
        return -1;
    }

    public static ZmuxException readAllBytesTooLarge(int maxBytes) {
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
