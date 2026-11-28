package io.zmux;

/**
 * How a splice ended, per direction. A queued datagram is not an acknowledgement of peer delivery.
 * Cancelling the future returned by {@link Zmux#splice} cancels the relay. A controlled
 * splice exposes a read-only result stage and accepts an explicit abort with an application code.
 *
 * @param aToB what arrived on a and was sent on b
 * @param bToA what arrived on b and was sent on a
 */
public record SpliceResult(SpliceDirection aToB, SpliceDirection bToA) {}
