package io.zmux;

import java.io.IOException;

public final class StreamNotReadableException extends IOException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: stream is not readable";

    public StreamNotReadableException() {
        super(MESSAGE);
    }

    public StreamNotReadableException(Throwable cause) {
        super(MESSAGE, cause);
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
}
