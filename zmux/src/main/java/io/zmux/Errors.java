package io.zmux;

import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.StreamError;
import io.zmux.internal.wire.TransportError;
import java.io.IOException;
import java.io.InterruptedIOException;

/** Maps core outcomes to the public exceptions. */
final class Errors {
    private Errors() {}

    /**
     * Clears the interrupt a library-owned worker (a splice or pool thread) received as an
     * internal wake-up. Its cause lives in the worker's shared state, so the flag carries
     * nothing to restore, and clearing it lets the teardown that follows block normally.
     */
    static void clearInternalWake() {
        boolean ignored = Thread.interrupted();
    }

    static SessionError session(io.zmux.internal.core.SessionError e) {
        return switch (e.kind()) {
            case APPLICATION -> new SessionError.ApplicationClosed(e.code(), e.reason(), e.remote());
            case TRANSPORT -> new SessionError.TransportClosed(e.code(), TransportError.name(e.code()), e.frameType(),
                    e.reason(), e.details(), e.remote());
            case IDLE -> new SessionError.IdleTimeout(e.remote());
            case HANDSHAKE_TIMEOUT -> new SessionError.HandshakeTimeout();
            case EARLY_REJECTED -> new SessionError.EarlyDataRejected();
            case RESUME_TIMEOUT -> new SessionError.ResumeTimeout(e.cause());
            case PIPES_LOST -> new SessionError.PipesLost(e.cause());
        };
    }

    static IOException io(CoreException e) {
        return switch (e.reason()) {
            case SESSION -> new SessionClosedException(session(e.sessionError()));
            case STREAM -> stream(e.streamError());
            case STREAM_LIMIT -> new StreamLimitException();
            case NOT_NEGOTIATED -> new NotNegotiatedException("feature");
            case NOT_ESTABLISHED -> new NotEstablishedException();
            case TOO_LATE -> throw new IllegalStateException("the setting must precede the stream's first frame");
            case INVALID -> throw new IllegalStateException("not valid in this state");
            case DATAGRAM_TOO_LARGE -> new DatagramTooLargeException(e.max());
            case PIPE_REFUSED -> new PipeRefusedException();
            case PREAMBLE_TOO_LARGE -> throw new PreambleTooLargeException();
        };
    }

    /** Maps the cause of a failed internal future. */
    static IOException failure(Throwable c) {
        return switch (c) {
            case CoreException ce -> io(ce);
            case IOException io -> io;
            case Error error -> throw error;
            default -> new IOException(c);
        };
    }

    static IOException stream(StreamError e) {
        return switch (e.kind()) {
            case RESET -> new StreamResetException(e.streamId(), e.code(), e.reliableSize());
            case STOPPED -> new StreamStoppedException(e.streamId(), e.code());
            case CLOSED_LOCALLY -> new StreamClosedException(e.streamId(), "stream closed locally");
            case REJECTED -> new StreamRejectedException(e.streamId());
            case GOING_AWAY -> new GoingAwayException(e.streamId());
            case PREAMBLE_TOO_LARGE -> throw new PreambleTooLargeException();
        };
    }

    /** Turns an operation's outcome into its exception; bytes is what a partial transfer moved. */
    static IOException outcome(Object r, long bytes) {
        IOException x = switch (r) {
            case CoreException ce -> io(ce);
            case InterruptedIOException ie -> {
                InterruptedIOException c = ie instanceof java.net.SocketTimeoutException
                        ? new java.net.SocketTimeoutException(ie.getMessage()) : new InterruptedIOException(ie.getMessage());
                c.bytesTransferred = (int) Math.min(bytes, Integer.MAX_VALUE);
                yield c;
            }
            case IOException io -> io;
            case RuntimeException re -> throw re;
            case Throwable t -> new IOException(t);
            default -> throw new IllegalStateException("not an outcome: " + r);
        };
        return x;
    }

    /** Throws when r is an outcome (a Throwable); returns r otherwise. */
    static Object check(Object r) throws IOException {
        if (r instanceof Throwable) throw outcome(r, 0);
        return r;
    }
}
