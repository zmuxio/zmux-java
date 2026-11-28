package io.zmux;

import java.io.IOException;

/** The engine replaced a datagram path and the provider must announce a higher source generation. */
public final class DatagramLossSourceExpiredException extends IOException {
    @java.io.Serial
    private static final long serialVersionUID = 1L;
    private final long sourceGeneration;

    public DatagramLossSourceExpiredException(long sourceGeneration) {
        super("datagram loss source generation " + sourceGeneration + " expired; use a higher generation");
        this.sourceGeneration = sourceGeneration;
    }

    public long sourceGeneration() { return sourceGeneration; }
}
