package io.zmux;

import java.io.IOException;

public final class EmptyMetadataUpdateException extends IOException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: metadata update has no fields";

    public EmptyMetadataUpdateException() {
        super(MESSAGE);
    }

    public EmptyMetadataUpdateException(Throwable cause) {
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
