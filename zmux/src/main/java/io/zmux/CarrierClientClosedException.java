package io.zmux;

/**
 * A carrier dial through a carrier client (such as an HTTP or QUIC client) that was closed: no carrier
 * can be dialled through it any more. Final, unlike a failed dial: a redial hook or a pipe pool whose dial
 * fails with it stops instead of retrying.
 */
public final class CarrierClientClosedException extends ZmuxException {
    public CarrierClientClosedException(String message) {
        super(message);
    }
}
