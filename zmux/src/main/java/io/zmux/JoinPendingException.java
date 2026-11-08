package io.zmux;

/**
 * A local join of a session that has no join keys yet. A session whose first pipe is not on the
 * secure carrier gets them only once the host declared pipe 0 end to end ({@link
 * ZmuxSession#declareEndToEnd()} or {@link PipeAuth#withEndToEnd()}) and once a {@link
 * PipeAuth#deferred() deferred PSK} was supplied ({@link ZmuxSession#setJoinSecret}); the message
 * names what is still missing. Until then the peer's joins are held.
 */
public final class JoinPendingException extends ZmuxException {
    private final boolean endToEndMissing, pskMissing;

    JoinPendingException(boolean endToEndMissing, boolean pskMissing) {
        super(message(endToEndMissing, pskMissing));
        this.endToEndMissing = endToEndMissing;
        this.pskMissing = pskMissing;
    }

    private static String message(boolean endToEnd, boolean psk) {
        String what = endToEnd && psk ? "the end-to-end declaration (declareEndToEnd or PipeAuth.withEndToEnd) and the deferred PSK (setJoinSecret)"
                : endToEnd ? "the end-to-end declaration (declareEndToEnd or PipeAuth.withEndToEnd)"
                : psk ? "the deferred PSK (setJoinSecret)" : "the join keys";
        return "join pending: " + what + " missing";
    }

    /** Whether the end-to-end declaration is missing. */
    public boolean endToEndMissing() { return endToEndMissing; }

    /** Whether the deferred PSK is missing. */
    public boolean pskMissing() { return pskMissing; }
}
