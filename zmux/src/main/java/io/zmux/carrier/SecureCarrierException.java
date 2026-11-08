package io.zmux.carrier;

import java.io.Serial;

/** A failure of the protocol layer, by the error classes of the secure carrier. */
public final class SecureCarrierException extends Exception {
    @Serial
    private static final long serialVersionUID = 1L;

    /** The error classes. */
    public enum Code {
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

    private final Code code;
    private final int need;

    public SecureCarrierException(Code code) {
        this(code, code.name().toLowerCase(java.util.Locale.ROOT), null);
    }

    public SecureCarrierException(Code code, String detail) {
        this(code, code.name().toLowerCase(java.util.Locale.ROOT) + ": " + detail, null);
    }

    public SecureCarrierException(Code code, String detail, Throwable cause) {
        super(detail, cause);
        this.code = code;
        this.need = 0;
    }

    private SecureCarrierException(int need) {
        super("incomplete", null, false, false); // ordinary control flow, without a stack trace
        this.code = Code.INCOMPLETE;
        this.need = need;
    }

    /** More stream bytes are needed: {@code need} in total. */
    public static SecureCarrierException incomplete(int need) {
        return new SecureCarrierException(need);
    }

    public Code code() {
        return code;
    }

    /** With {@link Code#INCOMPLETE}, the smallest total length that can make progress. */
    public int need() {
        return need;
    }
}
