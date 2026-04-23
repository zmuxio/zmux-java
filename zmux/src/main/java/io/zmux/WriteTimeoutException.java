package io.zmux;

import java.net.SocketTimeoutException;

public final class WriteTimeoutException extends SocketTimeoutException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: write timed out";

    public WriteTimeoutException() {
        super(MESSAGE);
    }

    @Override
    public String operation() {
        return "write";
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
        return ZmuxErrorDirection.WRITE;
    }

    @Override
    public ZmuxTerminationKind terminationKind() {
        return ZmuxTerminationKind.TIMEOUT;
    }
}
