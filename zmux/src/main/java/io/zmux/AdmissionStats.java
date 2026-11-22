package io.zmux;

/**
 * The admission outcomes of a listener ({@link ListenerStats#admission()}), all zero without an
 * {@link Admission}. Each carrier that took its source's slot and whose outcome is known counts
 * once, when the outcome is decided, in exactly one field, so that the fields add up to the number of
 * such carriers. A peek timeout that takes the fallback path uses that path's terminal outcome.
 * Read without a lock; two counts are not a consistent snapshot.
 *
 * @param accepted carriers decided with {@link AdmissionVerdict#ACCEPT}; each is then a pipe
 *     input counted once more in the listener's other fields
 * @param fallback carriers handed to a host under {@link AdmissionVerdict#FALLBACK}
 * @param takeover carriers handed to the takeover target after any acceptance response head was committed
 * @param stalled carriers whose stall began, including fallbacks and takeovers without a target,
 *     and requests answered with the adapter's default not-found response
 * @param closed carriers decided with {@link AdmissionVerdict#CLOSE} (a {@link AdmissionVerdict#MORE}
 *     or {@link AdmissionVerdict#DEFER} where it does not apply included), a decider that failed, a
 *     skip outside the prefix, decisions cancelled when the listener stopped, a first message exceeding the positive peek prefix bound, a message-framed
 *     carrier whose ordinary first-message read failed
 * @param stallOverflow carriers decided with {@link AdmissionVerdict#STALL} that were closed at
 *     once because their source had its share of stalled carriers, or the listener's stalled carriers
 *     held half of {@link ListenerConfig#maxPending()}
 * @param tooLarge message-framed carriers whose first message crossed its limit after deferral,
 *     or crossed a smaller {@link Admission#maxMessage()} limit in ordinary admission
 * @param peekTimedOut carriers whose decision was not taken within the peek timeout, the wait for
 *     a deferred decision's upgrade and first message or a takeover response head included
 * @param handshakeFailed carriers whose outer handshake (TLS, QUIC, an upgrade after a
 *     {@link AdmissionVerdict#DEFER}) failed or timed out before a decision, whose deferred first
 *     input failed before the deadline, or whose takeover response head failed
 */
public record AdmissionStats(long accepted, long fallback, long takeover, long stalled, long closed, long stallOverflow,
                             long tooLarge, long peekTimedOut, long handshakeFailed) {
    /** A snapshot with no carrier takeovers. */
    public AdmissionStats(long accepted, long fallback, long stalled, long closed, long stallOverflow,
                          long tooLarge, long peekTimedOut, long handshakeFailed) {
        this(accepted, fallback, 0, stalled, closed, stallOverflow, tooLarge, peekTimedOut, handshakeFailed);
    }
}
