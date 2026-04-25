package io.zmux;

import io.zmux.internal.StreamIoSupport;

import java.io.IOException;

final class StreamApiSupport {
    private StreamApiSupport() {
    }

    static int transientBufferSize(int remaining) {
        return Math.min(remaining, 8192);
    }

    static int checkedWritevTotalLength(byte[][] parts) throws IOException {
        return StreamIoSupport.checkedWritevTotalLength(parts, "writevFinal");
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
