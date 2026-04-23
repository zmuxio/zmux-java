package io.zmux;

public final class PriorityUpdateUnavailableException extends AdapterUnsupportedException {
    public static final String MESSAGE =
            "zmux: metadata update requires negotiated priority_update and matching semantic capability";

    public PriorityUpdateUnavailableException() {
        super(MESSAGE);
    }

    public PriorityUpdateUnavailableException(Throwable cause) {
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
    public ZmuxErrorDirection direction() {
        return ZmuxErrorDirection.WRITE;
    }
}
