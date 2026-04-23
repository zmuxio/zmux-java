package io.zmux;

import java.io.IOException;

public class ZmuxException extends IOException implements ZmuxErrorDetails {
    private final long code;
    private final String operation;
    private final ZmuxErrorScope scope;
    private final ZmuxErrorSource source;
    private final ZmuxErrorDirection direction;
    private final ZmuxTerminationKind terminationKind;

    public ZmuxException(long code, String operation, String message) {
        this(code, operation, message, null, ZmuxErrorScope.UNKNOWN, ZmuxErrorSource.UNKNOWN, ZmuxErrorDirection.BOTH, ZmuxTerminationKind.UNKNOWN);
    }

    public ZmuxException(long code, String operation, String message, Throwable cause) {
        this(code, operation, message, cause, ZmuxErrorScope.UNKNOWN, ZmuxErrorSource.UNKNOWN, ZmuxErrorDirection.BOTH, ZmuxTerminationKind.UNKNOWN);
    }

    public ZmuxException(long code,
                         String operation,
                         String message,
                         ZmuxErrorScope scope,
                         ZmuxErrorSource source,
                         ZmuxErrorDirection direction,
                         ZmuxTerminationKind terminationKind) {
        this(code, operation, message, null, scope, source, direction, terminationKind);
    }

    public ZmuxException(long code,
                         String operation,
                         String message,
                         Throwable cause,
                         ZmuxErrorScope scope,
                         ZmuxErrorSource source,
                         ZmuxErrorDirection direction,
                         ZmuxTerminationKind terminationKind) {
        super(message, cause);
        this.code = code;
        this.operation = operation;
        this.scope = scope == null ? ZmuxErrorScope.UNKNOWN : scope;
        this.source = source == null ? ZmuxErrorSource.UNKNOWN : source;
        this.direction = direction == null ? ZmuxErrorDirection.UNKNOWN : direction;
        this.terminationKind = terminationKind == null ? ZmuxTerminationKind.UNKNOWN : terminationKind;
    }

    public long code() {
        return code;
    }

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
        return terminationKind;
    }
}
