package io.zmux.seal;

import io.zmux.ZmuxException;
import java.io.Serial;

/**
 * A failure of the protocol layer, by the error classes of the secure carrier: of an Establish, of a Join, or of a
 * Sealed carrier's records.
 */
public final class SecureCarrierException extends ZmuxException {
    @Serial
    private static final long serialVersionUID = 1L;

    /** The error classes of the secure carrier (secure-carrier/SPEC.md Section 9): what happened, not a wire error code. */
    public enum Kind {
        INCOMPLETE,
        MALFORMED,
        TRAILING_BYTES,
        UNKNOWN_PROFILE,
        UNKNOWN_SUITE,
        BAD_FLAGS,
        BAD_HANDLE,
        AUTH,
        /** A key exchange input or output that fails its check: a DH public key or a KEM encapsulation key. */
        KEX,
        PEER_IS_SELF,
        TOO_LARGE,
        EXHAUSTED,
        RANDOM_FAILED,
        TLV_TRUNCATED,
        TLV_NON_MINIMAL,
        TLV_ORDER,
        TLV_UNKNOWN_CRITICAL,
        TLV_CREDENTIALS,
        MISSING_FIELD,
        BAD_FIELD,
        BAD_CONFIRM,
        CREDENTIALS_FAILED,
        INVALID_CONFIG
    }

    private final Kind kind;
    private final int need;
    /** An {@link Kind#INCOMPLETE} of the parsers, ordinary control flow: without a stack trace. */
    private final transient boolean flow;

    public SecureCarrierException(Kind kind) {
        this(kind, kind.name().toLowerCase(java.util.Locale.ROOT), null);
    }

    public SecureCarrierException(Kind kind, String detail) {
        this(kind, kind.name().toLowerCase(java.util.Locale.ROOT) + ": " + detail, null);
    }

    public SecureCarrierException(Kind kind, String detail, Throwable cause) {
        this.kind = kind;
        this.need = 0;
        this.flow = false;
        super(detail, cause);
    }

    private SecureCarrierException(int need) {
        // Set before the superclass constructor asks fillInStackTrace, which then records nothing.
        this.kind = Kind.INCOMPLETE;
        this.need = need;
        this.flow = true;
        super("incomplete", null);
    }

    @Override
    public Throwable fillInStackTrace() {
        return flow ? this : super.fillInStackTrace();
    }

    /** More stream bytes are needed: {@code need} in total. */
    public static SecureCarrierException incomplete(int need) {
        return new SecureCarrierException(need);
    }

    /** The error class. */
    public Kind kind() {
        return kind;
    }

    /** With {@link Kind#INCOMPLETE}, the smallest total length that can make progress. */
    public int need() {
        return need;
    }
}
