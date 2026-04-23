package io.zmux;

import java.io.IOException;

public final class SessionClosedException extends IOException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: session closed";
    private final ZmuxErrorSource source;
    private final String operation;

    public SessionClosedException() {
        this(ZmuxErrorSource.UNKNOWN, null);
    }

    public SessionClosedException(Throwable cause) {
        this(ZmuxErrorSource.UNKNOWN, cause);
    }

    public SessionClosedException(ZmuxErrorSource source) {
        this(source, null);
    }

    public SessionClosedException(ZmuxErrorSource source, Throwable cause) {
        this(source, cause, "");
    }

    public SessionClosedException(ZmuxErrorSource source, Throwable cause, String operation) {
        super(MESSAGE, cause);
        this.source = source == null ? ZmuxErrorSource.UNKNOWN : source;
        this.operation = operation == null ? "" : operation;
    }

    @Override
    public String operation() {
        return operation;
    }

    @Override
    public ZmuxErrorScope scope() {
        return ZmuxErrorScope.SESSION;
    }

    @Override
    public ZmuxErrorSource source() {
        return source;
    }

    @Override
    public ZmuxErrorDirection direction() {
        return ZmuxErrorDirection.BOTH;
    }

    @Override
    public ZmuxTerminationKind terminationKind() {
        return ZmuxTerminationKind.SESSION_TERMINATION;
    }
}
