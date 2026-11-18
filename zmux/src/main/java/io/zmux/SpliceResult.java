package io.zmux;

/**
 * How a splice ended, per direction. A queued datagram is not an acknowledgement of peer delivery.
 * Cancelling the stage is the cancellation of the splice: it then has no result; an abort through a
 * {@link SpliceAbort} has one.
 *
 * @param aToB what arrived on a and was sent on b
 * @param bToA what arrived on b and was sent on a
 */
public record SpliceResult(SpliceDirection aToB, SpliceDirection bToA) {}
