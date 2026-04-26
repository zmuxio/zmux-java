package io.zmux;

import java.io.IOException;
import java.util.IdentityHashMap;

public final class WriteClosedException extends IOException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: write side closed";
    private static final int MAX_ERROR_UNWRAP_DEPTH = 64;

    private final ZmuxErrorSource source;
    private final ZmuxTerminationKind terminationKind;

    public WriteClosedException() {
        this(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL, null);
    }

    public WriteClosedException(Throwable cause) {
        this(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.GRACEFUL, cause);
    }

    public WriteClosedException(ZmuxErrorSource source, ZmuxTerminationKind terminationKind) {
        this(source, terminationKind, null);
    }

    public WriteClosedException(ZmuxErrorSource source, ZmuxTerminationKind terminationKind, Throwable cause) {
        super(MESSAGE, cause);
        this.source = source == null ? ZmuxErrorSource.UNKNOWN : source;
        this.terminationKind = terminationKind == null ? ZmuxTerminationKind.UNKNOWN : terminationKind;
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
        return source;
    }

    @Override
    public ZmuxErrorDirection direction() {
        return ZmuxErrorDirection.WRITE;
    }

    @Override
    public ZmuxTerminationKind terminationKind() {
        return terminationKind;
    }

    @Override
    public long code() {
        ZmuxErrorDetails details = nestedDetails();
        return details == null ? -1L : details.code();
    }

    @Override
    public String reason() {
        ZmuxErrorDetails details = nestedDetails();
        return details == null ? "" : details.reason();
    }

    private ZmuxErrorDetails nestedDetails() {
        Throwable current = getCause();
        IdentityHashMap<Throwable, Boolean> seen = new IdentityHashMap<>();
        int depth = 0;
        while (current != null && depth <= MAX_ERROR_UNWRAP_DEPTH && seen.put(current, Boolean.TRUE) == null) {
            if (current instanceof ZmuxErrorDetails && !(current instanceof WriteClosedException)) {
                return (ZmuxErrorDetails) current;
            }
            current = current.getCause();
            depth++;
        }
        return null;
    }
}
