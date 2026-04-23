package io.zmux;

import java.net.SocketTimeoutException;

public final class PingTimeoutException extends SocketTimeoutException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: ping timed out";

    public PingTimeoutException() {
        super(MESSAGE);
    }

    @Override
    public String operation() {
        return "ping";
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
