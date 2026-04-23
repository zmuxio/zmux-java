package io.zmux;

import java.io.IOException;

public final class ReadClosedException extends IOException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: read side closed";
    private final ZmuxErrorSource source;
    private final ZmuxTerminationKind terminationKind;

    public ReadClosedException() {
        this(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.STOPPED, null);
    }

    public ReadClosedException(Throwable cause) {
        this(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.STOPPED, cause);
    }

    public ReadClosedException(ZmuxErrorSource source, ZmuxTerminationKind terminationKind) {
        this(source, terminationKind, null);
    }

    public ReadClosedException(ZmuxErrorSource source, ZmuxTerminationKind terminationKind, Throwable cause) {
        super(MESSAGE, cause);
        this.source = source == null ? ZmuxErrorSource.UNKNOWN : source;
        this.terminationKind = terminationKind == null ? ZmuxTerminationKind.UNKNOWN : terminationKind;
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
        return source;
    }

    @Override
    public ZmuxErrorDirection direction() {
        return ZmuxErrorDirection.READ;
    }

    @Override
    public ZmuxTerminationKind terminationKind() {
        return terminationKind;
    }
}
