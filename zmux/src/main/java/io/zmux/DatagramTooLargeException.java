package io.zmux;

/** The datagram does not fit; zmux never fragments. {@link #max()} is the largest payload now. */
public final class DatagramTooLargeException extends ZmuxException {
    private final int max;

    public DatagramTooLargeException(int max) {
        super("datagram too large (max " + max + ")");
        this.max = max;
    }

    public int max() { return max; }
}
