package io.zmux;

/** The outcome of an {@link Admission} decision, carried by {@link Admission.Decision}. */
public enum AdmissionVerdict {
    /** The carrier goes to the listener without the first {@link Admission.Decision#skip()} prefix bytes. */
    ACCEPT,
    /**
     * The carrier goes, with every byte read replayed, to the fallback target of the entry
     * that took it ({@link ListenerConfig#fallback()} for the listener's sockets, the target
     * given to {@link ZmuxListener#handle}: an HTTP endpoint's host handler, a QUIC
     * server's fallback); without one it is {@link #STALL}.
     */
    FALLBACK,
    /**
     * Reads and discards what arrives, sends nothing, and closes the carrier gracefully when
     * the peer ends it or after a duration that depends on its source alone. A stalled carrier
     * keeps its slots of {@link ListenerConfig#maxPendingPerSource()} and {@link
     * ListenerConfig#maxPending()} until closed, and is the first closed when the listener's
     * slots are all taken; a source has at most one fewer stalled carriers than its bound, the
     * listener at most half of its own, and a further stall is closed at once.
     */
    STALL,
    /** Closes the carrier at once. */
    CLOSE,
    /**
     * Asks for a longer prefix on a byte stream ({@link Admission.Decision#need()}): the listener reads
     * on, within the same peek timeout, and decides again. A need that does not exceed the
     * prefix or exceeds the bound, and this verdict on a message-framed carrier or on a
     * carrier that ended or timed out, are {@link #CLOSE}.
     */
    MORE
}
