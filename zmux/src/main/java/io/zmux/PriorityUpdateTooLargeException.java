package io.zmux;

public final class PriorityUpdateTooLargeException extends ZmuxException {
    public static final String MESSAGE = "priority update exceeds peer max_extension_payload_bytes";

    public PriorityUpdateTooLargeException() {
        this("build priority update", ZmuxErrorScope.SESSION, ZmuxErrorSource.LOCAL, ZmuxErrorDirection.WRITE, null);
    }

    public PriorityUpdateTooLargeException(String operation,
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
