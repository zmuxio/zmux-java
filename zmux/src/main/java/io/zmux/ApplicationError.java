package io.zmux;

import io.zmux.protocol.Protocol;
import java.io.IOException;
import java.util.Objects;

public final class ApplicationError extends IOException implements ZmuxErrorDetails {
    private final long code;
    private final String reason;
    private final String operation;
    private final ZmuxErrorScope scope;
    private final ZmuxErrorSource source;
    private final ZmuxErrorDirection direction;
    private final ZmuxTerminationKind terminationKind;

    public ApplicationError(ErrorCode code, String reason) {
        this(Objects.requireNonNull(code, "code").code(), reason);
    }

    public ApplicationError(long code, String reason) {
        this(code, reason, ZmuxErrorScope.UNKNOWN, ZmuxErrorSource.UNKNOWN, ZmuxErrorDirection.BOTH, ZmuxTerminationKind.UNKNOWN);
    }

    public ApplicationError(ErrorCode code,
                            String reason,
                            ZmuxErrorScope scope,
                            ZmuxErrorSource source,
                            ZmuxErrorDirection direction,
                            ZmuxTerminationKind terminationKind) {
        this(Objects.requireNonNull(code, "code").code(), reason, scope, source, direction, terminationKind);
    }

    public ApplicationError(long code,
                            String reason,
                            ZmuxErrorScope scope,
                            ZmuxErrorSource source,
                            ZmuxErrorDirection direction,
                            ZmuxTerminationKind terminationKind) {
        this(code, reason, scope, source, direction, terminationKind, "");
    }

    public ApplicationError(ErrorCode code,
                            String reason,
                            ZmuxErrorScope scope,
                            ZmuxErrorSource source,
                            ZmuxErrorDirection direction,
                            ZmuxTerminationKind terminationKind,
                            String operation) {
        this(Objects.requireNonNull(code, "code").code(), reason, scope, source, direction, terminationKind, operation);
    }

    public ApplicationError(long code,
                            String reason,
                            ZmuxErrorScope scope,
                            ZmuxErrorSource source,
                            ZmuxErrorDirection direction,
                            ZmuxTerminationKind terminationKind,
                            String operation) {
        super(reason == null || reason.isEmpty() ? "zmux application error " + code : "zmux application error " + code + ": " + reason);
        if (code < 0L) {
            throw new IllegalArgumentException("zmux application error code must be >= 0");
        }
        if (code > Protocol.MAX_VARINT62) {
            throw new IllegalArgumentException("zmux application error code must be within varint62 range");
        }
        this.code = code;
        this.reason = reason == null ? "" : reason;
        this.operation = operation == null ? "" : operation;
        this.scope = scope == null ? ZmuxErrorScope.UNKNOWN : scope;
        this.source = source == null ? ZmuxErrorSource.UNKNOWN : source;
        this.direction = direction == null ? ZmuxErrorDirection.UNKNOWN : direction;
        this.terminationKind = terminationKind == null ? ZmuxTerminationKind.UNKNOWN : terminationKind;
    }

    public long applicationCode() {
        return code;
    }

    public long code() {
        return code;
    }

    public boolean isCode(ErrorCode code) {
        return code != null && this.code == code.code();
    }

    public String reason() {
        return reason;
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
        return terminationKind;
    }
}
