package io.zmux;

import java.io.IOException;

/**
 * Every zmux failure an operation reports, by scope: the session ended
 * ({@link SessionClosedException}), one stream failed ({@link StreamException}), one pipe
 * failed ({@link PipeException}), or a feature or state was missing. Timeouts are
 * {@link java.net.SocketTimeoutException} (with the bytes transferred) and interruptions
 * {@link java.io.InterruptedIOException}; calling a blocking method on an event loop or a
 * completion context throws {@link IllegalStateException}.
 */
public abstract sealed class ZmuxException extends IOException
        permits SessionClosedException, StreamException, PipeException, NotNegotiatedException,
        DatagramTooLargeException, NotEstablishedException, DuplicateResolutionException {

    ZmuxException(String message) {
        super(message);
    }

    /** Whether the operation may succeed on a retry, possibly on another session (rejected, going away). */
    public boolean isRetryable() {
        return false;
    }
}
