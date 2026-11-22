package io.zmux.carrier;

import java.util.OptionalLong;

/**
 * The sending half of a pipe input. zmux has at most one write in flight; a write in
 * progress means "not writable". Closing or aborting must eventually complete any pending
 * write. Both operations must be idempotent. A graceful {@link #close} may be followed by
 * one {@link #abort} at the linger deadline, including after close returned. The default abort
 * calls close again.
 */
public interface PipeWriter {
    /** The outcome of one write. */
    interface Completion {
        void completed();

        void failed(Throwable cause);
    }

    /**
     * Writes batch in order and then reports the outcome; with {@link Framing#MESSAGES} each
     * record of the batch is one message (gather its segments, see
     * {@link OutboundBatch#messageEnd}). Completion reports that the carrier took the bytes;
     * the batch is valid until then, and a writer that still needs the payload afterwards
     * calls {@link OutboundBatch#retain()} first. The completion may be reported on any
     * thread, also from inside this call.
     */
    void write(OutboundBatch batch, Completion done);

    /**
     * The bytes a write took that the carrier has not yet transmitted (for example
     * {@code tcpi_notsent_bytes}, or the adapter's own buffer), or empty when the carrier
     * cannot tell; empty by default. The driver calls it on its own thread, outside the
     * session's lock, while it hands over batches and takes statistics: it must not block and
     * must not call into the session.
     */
    default OptionalLong unsent() {
        return OptionalLong.empty();
    }

    /**
     * Ends the half gracefully (a graceful end of the pipe). It runs on a thread of its own and may overlap a
     * write in progress, whose completion is still reported.
     */
    void close();

    /** Ends the half abortively, so the peer sees a failure; defaults to {@link #close}. */
    default void abort(Throwable cause) {
        close();
    }
}
