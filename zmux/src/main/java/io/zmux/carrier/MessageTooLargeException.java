package io.zmux.carrier;

/**
 * The failure a reader of messages reports for a first message longer than the bound zmux set
 * ({@link PipeReader#limitFirstMessage}). The admission classifies the oversize outcome according
 * to the bound and the admission stage.
 */
public final class MessageTooLargeException extends java.net.ProtocolException {
    @java.io.Serial
    private static final long serialVersionUID = 1L;

    private final long observedBytes;

    /** A first message above limit bytes, with its size unavailable. */
    public MessageTooLargeException(int limit) {
        this(limit, 0);
    }

    /** A first message above limit bytes, with at least observedBytes already seen. */
    public MessageTooLargeException(int limit, long observedBytes) {
        super("a first message above " + limit + " bytes");
        this.observedBytes = observedBytes;
    }

    /** The number of first-message bytes already seen by the reader, if available. */
    public java.util.OptionalLong observedBytes() {
        return observedBytes <= 0 ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(observedBytes);
    }
}
