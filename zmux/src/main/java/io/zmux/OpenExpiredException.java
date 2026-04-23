package io.zmux;

import java.io.IOException;

public final class OpenExpiredException extends IOException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: provisional local open expired before first-frame commit";

    public OpenExpiredException() {
        super(MESSAGE);
    }

    public OpenExpiredException(Throwable cause) {
        super(MESSAGE, cause);
    }

    @Override
    public String operation() {
        return "open";
    }

    @Override
    public long code() {
        return ErrorCode.CANCELLED.code();
    }

    @Override
    public String reason() {
        return MESSAGE;
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
        return ZmuxErrorDirection.BOTH;
    }

    @Override
    public ZmuxTerminationKind terminationKind() {
        return ZmuxTerminationKind.ABORT;
    }
}
