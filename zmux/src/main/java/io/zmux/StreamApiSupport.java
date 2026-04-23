package io.zmux;

import java.io.IOException;
import java.util.Objects;

final class StreamApiSupport {
    private StreamApiSupport() {
    }

    static int transientBufferSize(int remaining) {
        return Math.min(remaining, 8192);
    }

    static int checkedWritevTotalLength(byte[][] parts) throws IOException {
        int total = 0;
        for (int i = 0; i < parts.length; i++) {
            byte[] part = Objects.requireNonNull(parts[i], "parts[" + i + "]");
            if (part.length > Integer.MAX_VALUE - total) {
                throw multipartWriteTooLarge();
            }
            total += part.length;
        }
        return total;
    }

    static int lastNonEmptyPart(byte[][] parts) {
        for (int i = parts.length - 1; i >= 0; i--) {
            if (parts[i].length > 0) {
                return i;
            }
        }
        return -1;
    }

    private static ZmuxException multipartWriteTooLarge() {
        return new ZmuxException(
                ErrorCode.FRAME_SIZE.code(),
                "writevFinal",
                "multipart write exceeds maximum supported size",
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.WRITE,
                ZmuxTerminationKind.UNKNOWN
        );
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
