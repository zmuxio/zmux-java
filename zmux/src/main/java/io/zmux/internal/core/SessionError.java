package io.zmux.internal.core;

import io.zmux.internal.wire.TransportError;

/**
 * The terminal error of a session.
 *
 * @param cause the last pipe error for PIPES_LOST and RESUME_TIMEOUT, or null
 */
public record SessionError(Kind kind, long code, long frameType, String reason, String details, Throwable cause,
                           boolean remote) {

    public static final String HANDSHAKE_TIMEOUT_MESSAGE = "handshake timeout";
    public static final String RESUME_TIMEOUT_MESSAGE = "resume timeout";

    /** Termination kinds. */
    public enum Kind { APPLICATION, TRANSPORT, IDLE, HANDSHAKE_TIMEOUT, RESUME_TIMEOUT, PIPES_LOST }

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
        return switch (kind) {
            case APPLICATION -> applicationMessage(code, reason, remote);
            case TRANSPORT -> code == TransportError.RESUME_FAILED ? resumeFailedMessage(reason, remote)
                    : transportMessage(TransportError.name(code), reason, remote);
            case IDLE -> idleMessage(remote);
            case HANDSHAKE_TIMEOUT -> HANDSHAKE_TIMEOUT_MESSAGE;
            case RESUME_TIMEOUT -> RESUME_TIMEOUT_MESSAGE;
            case PIPES_LOST -> pipesLostMessage(cause);
        };
    }

    public static String applicationMessage(long code, String reason, boolean remote) {
        return withReason(from("application close", remote) + " code 0x" + Long.toHexString(code), reason);
    }

    public static String transportMessage(String name, String reason, boolean remote) {
        return withReason(from("transport close", remote) + " " + name, reason);
    }

    public static String idleMessage(boolean remote) { return from("idle timeout", remote); }

    public static String resumeFailedMessage(String reason, boolean remote) { return withReason(from("resume failed", remote), reason); }

    public static String pipesLostMessage(Throwable cause) { return cause == null ? "pipes lost" : "pipes lost: " + cause.getMessage(); }

    private static String from(String message, boolean remote) { return message + (remote ? " (remote)" : " (local)"); }

    private static String withReason(String message, String reason) { return reason.isEmpty() ? message : message + ": " + reason; }
}
