package io.zmux;

/** The outcome of an {@link Admission} decision, carried by {@link Admission.Decision}. */
public enum AdmissionVerdict {
    /** The carrier goes to the listener without the first {@link Admission.Decision#skip()} prefix bytes. */
    ACCEPT,
    /**
     * The carrier goes, with every byte or message read replayed, to {@link ListenerConfig#fallback()} as a
     * {@link Takeover}, whatever entry took it; without a fallback it is stalled (counted as a fallback). An HTTP
     * request that is itself the carrier, decided on its head or body, and an upgrade refused on its head, are
     * requests rather than carriers to take over: their HTTP adapter answers them ({@link
     * io.zmux.adapter.ListenerAdapter#handleRequest}). The carrier of an upgrade decided after a {@link #DEFER}
     * is a carrier: it goes to the listener's fallback.
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
     * prefix or exceeds {@link Admission#maxPeek()}, and this verdict on a message-framed carrier, on a
     * request head (an upgrade's included) or on a carrier that ended or timed out, are {@link #CLOSE}.
     */
    MORE,
    /**
     * Completes an HTTP upgrade whose head is being decided ({@link Admission.CarrierInfo#upgrade()}, {@link
     * Admission.Stage#HEAD}) and decides again on the carrier: the decider is called with the same metadata at
     * {@link Admission.Stage#CARRIER} and the carrier's first message as prefix. The peek timeout runs on from the
     * head's decision, and the carrier keeps its slots throughout. On any other decision it is {@link #CLOSE}.
     */
    DEFER
}
