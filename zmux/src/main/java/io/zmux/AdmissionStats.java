package io.zmux;

/**
 * The admission outcomes of a listener ({@link ListenerStats#admission()}), all zero without an
 * {@link Admission}. Each carrier that took its source's slot and whose outcome is known counts
 * once, when the outcome is decided, in exactly one field. Read without a lock; two counts are
 * not a consistent snapshot.
 *
 * @param accepted carriers decided with {@link Admission.Verdict#ACCEPT}; each is then a pipe
 *     input counted once more in the listener's other fields
 * @param fallback carriers decided with {@link Admission.Verdict#FALLBACK}, stalled for want of a target included
 * @param stalled carriers decided with {@link Admission.Verdict#STALL} whose stall began (counted when decided)
 * @param closed carriers decided with {@link Admission.Verdict#CLOSE}, a decider that failed,
 *     a skip outside the prefix, and decisions cancelled when the listener stopped
 * @param stallOverflow carriers decided with {@link Admission.Verdict#STALL} that were closed at
 *     once because their source had its share of stalled carriers
 * @param peekTimedOut carriers whose decision was not taken within the peek timeout
 * @param handshakeFailed carriers whose outer handshake (TLS, QUIC) failed or timed out before a decision
 */
public record AdmissionStats(long accepted, long fallback, long stalled, long closed, long stallOverflow,
                             long peekTimedOut, long handshakeFailed) {}
