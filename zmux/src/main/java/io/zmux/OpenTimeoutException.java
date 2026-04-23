package io.zmux;

import java.net.SocketTimeoutException;

public final class OpenTimeoutException extends SocketTimeoutException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: open timed out";

    public OpenTimeoutException() {
        super(MESSAGE);
    }

    @Override
    public String operation() {
        return "open";
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
