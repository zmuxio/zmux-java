package io.zmux.carrier;

/**
 * The failure a reader of messages reports for a first message longer than the bound zmux set
 * ({@link PipeReader#limitFirstMessage}): the admission counts it as too large rather than as a
 * failed carrier.
 */
public final class MessageTooLargeException extends java.net.ProtocolException {
    private static final long serialVersionUID = 1L;

    /** A first message above limit bytes. */
    public MessageTooLargeException(int limit) {
        super("a first message above " + limit + " bytes");
    }
}
