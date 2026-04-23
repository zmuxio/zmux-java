package io.zmux;

import java.net.SocketTimeoutException;

public final class SessionWaitTimeoutException extends SocketTimeoutException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: session termination wait timed out";

    public SessionWaitTimeoutException() {
        super(MESSAGE);
    }

    @Override
    public String operation() {
        return "wait";
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
