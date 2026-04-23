package io.zmux;

import java.io.IOException;

public class AdapterUnsupportedException extends IOException implements ZmuxErrorDetails {
    private final String operation;
    private final ZmuxErrorScope scope;
    private final ZmuxErrorDirection direction;
    private final ZmuxTerminationKind terminationKind;

    public AdapterUnsupportedException(String message) {
        this(message, null);
    }

    public AdapterUnsupportedException(String message, Throwable cause) {
        this(message, cause, "", ZmuxErrorScope.UNKNOWN, ZmuxErrorDirection.UNKNOWN, ZmuxTerminationKind.UNKNOWN);
    }

    public AdapterUnsupportedException(String message,
                                       String operation,
                                       ZmuxErrorScope scope,
                                       ZmuxErrorDirection direction) {
        this(message, null, operation, scope, direction, ZmuxTerminationKind.UNKNOWN);
    }

    public AdapterUnsupportedException(String message,
                                       Throwable cause,
                                       String operation,
                                       ZmuxErrorScope scope,
                                       ZmuxErrorDirection direction,
                                       ZmuxTerminationKind terminationKind) {
        super(message, cause);
        this.operation = operation == null ? "" : operation;
        this.scope = scope == null ? ZmuxErrorScope.UNKNOWN : scope;
        this.direction = direction == null ? ZmuxErrorDirection.UNKNOWN : direction;
        this.terminationKind = terminationKind == null ? ZmuxTerminationKind.UNKNOWN : terminationKind;
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
        return ZmuxErrorSource.LOCAL;
    }

    @Override
    public ZmuxErrorDirection direction() {
        return direction;
    }

    @Override
    public ZmuxTerminationKind terminationKind() {
        return terminationKind;
    }
}
