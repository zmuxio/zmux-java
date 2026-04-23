package io.zmux;

import java.net.SocketTimeoutException;

public final class AcceptTimeoutException extends SocketTimeoutException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: accept timed out";

    public AcceptTimeoutException() {
        super(MESSAGE);
    }

    @Override
    public String operation() {
        return "accept";
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
