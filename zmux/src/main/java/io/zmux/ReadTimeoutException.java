package io.zmux;

import java.net.SocketTimeoutException;

public final class ReadTimeoutException extends SocketTimeoutException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: read timed out";

    public ReadTimeoutException() {
        super(MESSAGE);
    }

    @Override
    public String operation() {
        return "read";
    }

    @Override
    public ZmuxErrorScope scope() {
        return ZmuxErrorScope.STREAM;
    }

    @Override
    public ZmuxErrorSource source() {
        return ZmuxErrorSource.LOCAL;
    }

    @Override
    public ZmuxErrorDirection direction() {
        return ZmuxErrorDirection.READ;
    }

    @Override
    public ZmuxTerminationKind terminationKind() {
        return ZmuxTerminationKind.TIMEOUT;
    }
}
