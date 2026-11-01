package io.zmux;

import io.zmux.helper.Limiter;

/** Relay policy: the chunk size and each direction's limiter (null: none). */
public record SpliceOptions(int chunkSize, Limiter aToB, Limiter bToA) {
    public SpliceOptions {
        if (chunkSize < 1) throw new IllegalArgumentException("chunkSize must be positive");
    }
    public static SpliceOptions defaults() { return new SpliceOptions(65536, null, null); }
}
