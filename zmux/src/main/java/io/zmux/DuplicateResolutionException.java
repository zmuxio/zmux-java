package io.zmux;

/** No safe duplicate-session decision could be made; neither session has been changed. */
public final class DuplicateResolutionException extends ZmuxException {
    public enum Reason { SAME_SESSION, NOT_ESTABLISHED, UNKNOWN_DIALER, SAME_DIALER, NO_COMMON_INPUT, EQUAL_VALUES }
    private final Reason reason;
    public DuplicateResolutionException(Reason reason) { super("duplicate resolution: " + reason); this.reason = reason; }
    public Reason reason() { return reason; }
}
