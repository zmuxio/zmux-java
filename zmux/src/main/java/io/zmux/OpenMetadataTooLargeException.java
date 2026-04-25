package io.zmux;

public final class OpenMetadataTooLargeException extends ZmuxException {
    public static final String MESSAGE = "opening metadata exceeds peer max_frame_payload";

    public OpenMetadataTooLargeException() {
        this("build open metadata", ZmuxErrorScope.SESSION, ZmuxErrorSource.LOCAL, ZmuxErrorDirection.WRITE, null);
    }

    public OpenMetadataTooLargeException(String operation,
                                         ZmuxErrorScope scope,
                                         ZmuxErrorSource source,
                                         ZmuxErrorDirection direction,
                                         Throwable cause) {
        super(
                ErrorCode.PROTOCOL.code(),
                operation,
                MESSAGE,
                cause,
                scope,
                source,
                direction,
                ZmuxTerminationKind.UNKNOWN
        );
    }
}
