package io.zmux.internal.core;

import io.zmux.internal.wire.TransportError;

/**
 * The terminal error of a session (SPEC.md Section 6.5, API.md Section 5.3).
 *
 * @param cause the last pipe error for PIPES_LOST and RESUME_TIMEOUT, or null
 */
public record SessionError(Kind kind, long code, long frameType, String reason, String details, Throwable cause,
                           boolean remote) {

    /** Termination kinds. */
    public enum Kind { APPLICATION, TRANSPORT, IDLE, HANDSHAKE_TIMEOUT, EARLY_REJECTED, RESUME_TIMEOUT, PIPES_LOST }

    public static SessionError of(Kind kind, boolean remote) {
        return new SessionError(kind, 0, 0, "", "", null, remote);
    }

    public static SessionError pipesLost(Throwable cause) {
        return new SessionError(Kind.PIPES_LOST, 0, 0, "", "", cause, false);
    }

    public static SessionError application(long code, String reason, boolean remote) {
        return new SessionError(Kind.APPLICATION, code, 0, reason, "", null, remote);
    }

    public static SessionError transport(long code, long frameType, String reason, String details, boolean remote) {
        return new SessionError(Kind.TRANSPORT, code, frameType, reason, details, null, remote);
    }

    public String message() {
        String from = remote ? "remote" : "local";
        return switch (kind) {
            case APPLICATION -> "application close (" + from + ") code 0x" + Long.toHexString(code) + ": " + reason;
            case TRANSPORT -> "transport close (" + from + ") " + TransportError.name(code)
                    + (reason.isEmpty() ? "" : ": " + reason);
            case IDLE -> "idle timeout (" + from + ")";
            case HANDSHAKE_TIMEOUT -> "handshake timeout";
            case EARLY_REJECTED -> "early data rejected";
            case RESUME_TIMEOUT -> "resume timeout";
            case PIPES_LOST -> cause != null ? "pipes lost: " + cause.getMessage() : "pipes lost";
        };
    }
}
