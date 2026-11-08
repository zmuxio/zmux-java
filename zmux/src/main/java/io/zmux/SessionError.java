package io.zmux;

/** Why a session ended; the first cause wins. */
public sealed interface SessionError {
    /** Whether the peer caused it. */
    boolean remote();

    /** A short description. */
    String message();

    /** A close by either application, CONNECTION_CLOSE 0x1d. */
    record ApplicationClosed(long code, String reason, boolean remote) implements SessionError {
        @Override public String message() {
            return "application close (" + (remote ? "remote" : "local") + ") code 0x" + Long.toHexString(code)
                    + (reason.isEmpty() ? "" : ": " + reason);
        }
    }

    /**
     * A protocol error, CONNECTION_CLOSE 0x1c.
     *
     * @param details the {@code key=value} pairs of the reason's diagnostic line, empty when none
     */
    record TransportClosed(long code, String name, long frameType, String reason, String details, boolean remote)
            implements SessionError {
        @Override public String message() {
            return "transport close (" + (remote ? "remote" : "local") + ") " + name + (reason.isEmpty() ? "" : ": " + reason);
        }
    }

    /** The idle timeout; remote when the peer's timer expired. */
    record IdleTimeout(boolean remote) implements SessionError {
        @Override public String message() {
            return "idle timeout (" + (remote ? "remote" : "local") + ")";
        }
    }

    /** No peer parameters within the handshake timeout. */
    record HandshakeTimeout() implements SessionError {
        @Override public boolean remote() { return false; }
        @Override public String message() { return "handshake timeout"; }
    }

    /** The peer cannot take the early data it was sent and processed none of it; retry without early data. */
    record EarlyDataRejected() implements SessionError {
        @Override public boolean remote() { return false; }
        @Override public String message() { return "early data rejected"; }
    }

    /**
     * Resume could not continue safely: the data in flight is in an unknown state
     * (CONNECTION_CLOSE with ZMUX_RESUME_FAILED); local or remote.
     */
    record ResumeFailed(boolean remote) implements SessionError {
        @Override public String message() {
            return "resume failed (" + (remote ? "remote" : "local") + ")";
        }
    }

    /** The resume grace period expired; lastPipeError is the error of the last pipe lost, or null. */
    record ResumeTimeout(Throwable lastPipeError) implements SessionError {
        @Override public boolean remote() { return false; }
        @Override public String message() { return "resume timeout"; }
    }

    /** Every pipe in a direction was lost, or the single carrier failed; lastPipeError is the error of the last pipe lost, or null. */
    record PipesLost(Throwable lastPipeError) implements SessionError {
        @Override public boolean remote() { return false; }
        @Override public String message() {
            return lastPipeError == null ? "pipes lost" : "pipes lost: " + lastPipeError.getMessage();
        }
    }
}
