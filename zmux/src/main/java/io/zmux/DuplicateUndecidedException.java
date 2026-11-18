package io.zmux;

/**
 * {@link Zmux#resolveDuplicate} cannot decide: an argument error, since the caller can supply
 * the identities that decide. Neither session has been changed.
 */
public final class DuplicateUndecidedException extends IllegalArgumentException {
    /** Why no decision could be made. */
    public enum Reason { SAME_SESSION, NOT_ESTABLISHED, UNKNOWN_DIALER, SAME_DIALER, NO_COMMON_INPUT, EQUAL_VALUES }

    private final Reason reason;

    public DuplicateUndecidedException(Reason reason) { super("duplicate sessions not decidable: " + reason); this.reason = reason; }

    public Reason reason() { return reason; }
}
