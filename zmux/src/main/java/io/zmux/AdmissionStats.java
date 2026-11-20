package io.zmux;

/**
 * The admission outcomes of a listener ({@link ListenerStats#admission()}), all zero without an
 * {@link Admission}. Each carrier that took its source's slot and whose outcome is known counts
 * once, when the outcome is decided, in exactly one field, so that the fields add up to the number of
 * such carriers, except that a carrier {@link Admission.Builder#fallbackOnPeekTimeout} takes to the
 * fallback counts in both peekTimedOut and fallback. Read without a lock; two counts are not a
 * consistent snapshot.
 *
 * @param accepted carriers decided with {@link AdmissionVerdict#ACCEPT}; each is then a pipe
 *     input counted once more in the listener's other fields
 * @param fallback carriers decided with {@link AdmissionVerdict#FALLBACK}, stalled for want of a target included
 * @param stalled carriers decided with {@link AdmissionVerdict#STALL} whose stall began (counted when decided)
 * @param closed carriers decided with {@link AdmissionVerdict#CLOSE} (a {@link AdmissionVerdict#MORE}
 *     or {@link AdmissionVerdict#DEFER} where it does not apply included), a decider that failed, a
 *     skip outside the prefix, decisions cancelled when the listener stopped, a message-framed
 *     carrier whose first message failed (it ended, failed or was not binary) and a deferred
 *     decision whose carrier ended before its first message
 * @param stallOverflow carriers decided with {@link AdmissionVerdict#STALL} that were closed at
 *     once because their source had its share of stalled carriers, or the listener's stalled carriers
 *     held half of {@link ListenerConfig#maxPending()}
 * @param tooLarge message-framed carriers closed without a decision because their first message
 *     held more than {@link Admission#maxMessage()} bytes
 * @param peekTimedOut carriers whose decision was not taken within the peek timeout, the wait for
 *     a deferred decision's upgrade and first message included
 * @param handshakeFailed carriers whose outer handshake (TLS, QUIC, an upgrade after a
 *     {@link AdmissionVerdict#DEFER}) failed or timed out before a decision
 */
public record AdmissionStats(long accepted, long fallback, long stalled, long closed, long stallOverflow,
                             long tooLarge, long peekTimedOut, long handshakeFailed) {}
