package io.zmux;

import static io.zmux.internal.core.SessionError.*;

/** Why a session ended; the first cause wins. */
public sealed interface SessionError {
    /** Whether the peer caused it. */
    boolean remote();

    /** A short description. */
    String message();

    /** A close by either application, CONNECTION_CLOSE 0x1d. */
    record ApplicationClosed(long code, String reason, boolean remote) implements SessionError {
        @Override public String message() {
            return applicationMessage(code, reason, remote);
        }
    }

    /**
     * A protocol error, CONNECTION_CLOSE 0x1c.
     *
     * @param details the {@code key=value} pairs of the reason's diagnostic line, empty when none
     */
    record TransportClosed(long code, String name, long frameType, String reason, String details, boolean remote)
            implements SessionError {
        /** The listener refused this new session in the protocol. */
        public boolean isConnectionRefused() { return remote && code == io.zmux.internal.wire.TransportError.CONNECTION_REFUSED; }

        @Override public String message() {
            return transportMessage(name, reason, remote);
        }
    }

    /** The idle timeout; remote when the peer's timer expired. */
    record IdleTimeout(boolean remote) implements SessionError {
        @Override public String message() {
            return idleMessage(remote);
        }
    }

    /** No peer parameters within the handshake timeout. */
    record HandshakeTimeout() implements SessionError {
        @Override public boolean remote() { return false; }
        @Override public String message() { return HANDSHAKE_TIMEOUT_MESSAGE; }
    }

    /**
     * Resume could not continue safely: the data in flight is in an unknown state
     * (CONNECTION_CLOSE with ZMUX_RESUME_FAILED); local or remote.
     */
    record ResumeFailed(String reason, boolean remote) implements SessionError {
        public ResumeFailed(boolean remote) {
            this("", remote);
        }

        @Override public String message() {
            return resumeFailedMessage(reason, remote);
        }
    }

    /** The resume grace period expired; lastPipeError is the error of the last pipe lost, or null. */
    record ResumeTimeout(Throwable lastPipeError) implements SessionError {
        @Override public boolean remote() { return false; }
        @Override public String message() { return RESUME_TIMEOUT_MESSAGE; }
    }

    /** Every pipe in a direction was lost, or the single carrier failed; lastPipeError is the error of the last pipe lost, or null. */
    record PipesLost(Throwable lastPipeError) implements SessionError {
        @Override public boolean remote() { return false; }
        @Override public String message() {
            return pipesLostMessage(lastPipeError);
        }
    }
}
