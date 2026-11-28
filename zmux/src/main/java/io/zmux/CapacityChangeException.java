package io.zmux;

/** A capacity change could not be admitted against the current session state. */
public final class CapacityChangeException extends ZmuxException {
    /** The condition that blocked admission. */
    public enum Reason { STALE_POLICY, BLOCKED_CAPACITY, UNKNOWN_MEMBER, MEMBER_BUSY, EXPIRED }
    private final Reason reason;

    CapacityChangeException(Reason reason) {
        super(reason.name().toLowerCase(java.util.Locale.ROOT));
        this.reason = reason;
    }

    public Reason reason() { return reason; }
}
