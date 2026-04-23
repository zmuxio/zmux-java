package io.zmux;

import java.io.IOException;

public final class OpenLimitedException extends IOException implements ZmuxErrorDetails {
    public static final String MESSAGE = "zmux: too many provisional local opens";
    private final ZmuxErrorSource source;

    public OpenLimitedException() {
        this(null, ZmuxErrorSource.LOCAL);
    }

    public OpenLimitedException(Throwable cause) {
        this(cause, ZmuxErrorSource.LOCAL);
    }

    public OpenLimitedException(Throwable cause, ZmuxErrorSource source) {
        super(MESSAGE, cause);
        this.source = source == null ? ZmuxErrorSource.LOCAL : source;
    }

    @Override
    public String operation() {
        return "open";
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
}
