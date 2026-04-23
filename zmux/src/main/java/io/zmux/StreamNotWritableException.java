package io.zmux;

import java.io.IOException;

public final class StreamNotWritableException extends IOException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: stream is not writable";

    public StreamNotWritableException() {
        super(MESSAGE);
    }

    public StreamNotWritableException(Throwable cause) {
        super(MESSAGE, cause);
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
}
