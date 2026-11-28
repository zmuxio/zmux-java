package io.zmux.internal.buffer;

/** Size of the largest controlled receive backing and the smallest byte-stream read. */
public final class ReceiveSizes {
    public static final int MAX_BLOCK = 128 << 10;
    public static final int IDLE_BLOCK = 256;

    private ReceiveSizes() {}
}
