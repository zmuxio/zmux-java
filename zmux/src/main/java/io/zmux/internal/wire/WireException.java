package io.zmux.internal.wire;

/**
 * A violation of the protocol found in peer input; it becomes a CONNECTION_CLOSE
 * of type 0x1c. Carries no stack trace: it is control flow on the receive path.
 */
public final class WireException extends Exception {
    private final long code;
    private final long frameType;
    private final String reason;
    private final String details;

    public WireException(long code, long frameType, String reason, String details) {
        super(null, null, false, false);
        this.code = code;
        this.frameType = frameType;
        this.reason = reason;
        this.details = details == null ? "" : details;
    }

    public WireException(long code, String reason) {
        this(code, 0, reason, "");
    }

    public static WireException frame(String reason) {
        return new WireException(TransportError.FRAME_ENCODING_ERROR, reason);
    }

    public static WireException frame(long type, String reason) {
        return new WireException(TransportError.FRAME_ENCODING_ERROR, type, reason, "");
    }

    public static WireException protocol(String reason) {
        return new WireException(TransportError.PROTOCOL_VIOLATION, reason);
    }

    public static WireException param(String reason) {
        return new WireException(TransportError.TRANSPORT_PARAMETER_ERROR, reason);
    }

    /** Builds an error with a diagnostic line of {@code key=value} pairs (SPEC.md Section 4.3). */
    public static WireException withDetails(long code, String reason, Object... kv) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            if (!b.isEmpty()) b.append(' ');
            b.append(kv[i]).append('=').append(kv[i + 1] instanceof Number n ? Long.toUnsignedString(n.longValue()) : "0");
        }
        return new WireException(code, 0, reason, b.toString());
    }

    public long code() { return code; }
    public long frameType() { return frameType; }
    public String reason() { return reason; }
    public String details() { return details; }

    /** The Reason Phrase with its diagnostic line, at most 1024 bytes of UTF-8. */
    public String phrase() {
        String s = details.isEmpty() ? reason : reason + "\n" + details;
        return Utf8.truncate(s, 1024);
    }

    @Override
    public String getMessage() {
        String s = TransportError.name(code);
        if (!reason.isEmpty()) s += ": " + reason;
        if (!details.isEmpty()) s += " (" + details + ")";
        return s;
    }
}
