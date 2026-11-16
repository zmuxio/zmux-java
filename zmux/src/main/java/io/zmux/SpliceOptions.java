package io.zmux;

import io.zmux.helper.Limiter;

/**
 * Relay policy: the chunk size, each direction's limiter (null: none) and the control that may end the splice with
 * an application code (null: none).
 */
public record SpliceOptions(int chunkSize, Limiter aToB, Limiter bToA, SpliceControl control) {
    public SpliceOptions {
        if (chunkSize < 1) throw new IllegalArgumentException("chunkSize must be positive");
    }
    /** The chunk size and the limiters, without a control. */
    public SpliceOptions(int chunkSize, Limiter aToB, Limiter bToA) { this(chunkSize, aToB, bToA, null); }
    public static SpliceOptions defaults() { return new SpliceOptions(65536, null, null, null); }
    /**
     * These options with control, which then serves the one splice started with them. A control
     * serves one splice only: a second splice started with it fails at its start
     * ({@link Zmux#splice}) with {@link IllegalStateException}, not here.
     */
    public SpliceOptions withControl(SpliceControl control) { return new SpliceOptions(chunkSize, aToB, bToA, control); }
}
