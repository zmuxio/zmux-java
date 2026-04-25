package io.zmux;

public final class OpenInfoUnavailableException extends ZmuxException {
    public static final String MESSAGE = "open_info requires negotiated open_metadata";

    public OpenInfoUnavailableException() {
        this("build open metadata", ZmuxErrorScope.SESSION, ZmuxErrorSource.LOCAL, ZmuxErrorDirection.WRITE, null);
    }

    public OpenInfoUnavailableException(String operation,
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
