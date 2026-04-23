package io.zmux;

import java.io.IOException;

public final class GracefulCloseTimeoutException extends IOException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: graceful close drain timed out";

    public GracefulCloseTimeoutException() {
        super(MESSAGE);
    }

    public GracefulCloseTimeoutException(Throwable cause) {
        super(MESSAGE, cause);
    }

    @Override
    public String operation() {
        return "close";
    }

    @Override
    public ZmuxErrorScope scope() {
        return ZmuxErrorScope.SESSION;
    }

    @Override
    public ZmuxErrorSource source() {
        return ZmuxErrorSource.LOCAL;
    }

    @Override
    public ZmuxErrorDirection direction() {
        return ZmuxErrorDirection.BOTH;
    }

    @Override
    public ZmuxTerminationKind terminationKind() {
        return ZmuxTerminationKind.TIMEOUT;
    }
}
