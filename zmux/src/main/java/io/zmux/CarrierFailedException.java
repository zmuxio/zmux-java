package io.zmux;

/**
 * A carrier failed after it was set up: its framing was violated, its peer forgot it, or one of
 * its own deadlines passed. The session sees the pipe fail; the carrier adapter that reports it
 * names the reason in the message.
 */
public final class CarrierFailedException extends ZmuxException {
    public CarrierFailedException(String message) {
        super(message);
    }

    public CarrierFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
