package io.zmux;

import java.io.InterruptedIOException;

public final class ZmuxInterruptedIOException extends InterruptedIOException implements ZmuxErrorDetails {
    private final String operation;
    private final ZmuxErrorScope scope;
    private final ZmuxErrorSource source;
    private final ZmuxErrorDirection direction;

    public ZmuxInterruptedIOException(String message,
                                      String operation,
                                      ZmuxErrorScope scope,
                                      ZmuxErrorSource source,
                                      ZmuxErrorDirection direction,
                                      InterruptedException cause) {
        super(message);
        this.operation = operation == null ? "" : operation;
        this.scope = scope == null ? ZmuxErrorScope.UNKNOWN : scope;
        this.source = source == null ? ZmuxErrorSource.UNKNOWN : source;
        this.direction = direction == null ? ZmuxErrorDirection.UNKNOWN : direction;
        initCause(cause);
    }

    @Override
    public String operation() {
        return operation;
    }

    @Override
    public ZmuxErrorScope scope() {
        return scope;
    }

    @Override
    public ZmuxErrorSource source() {
        return source;
    }

    @Override
    public ZmuxErrorDirection direction() {
        return direction;
    }

    @Override
    public ZmuxTerminationKind terminationKind() {
        return ZmuxTerminationKind.INTERRUPTED;
    }
}
