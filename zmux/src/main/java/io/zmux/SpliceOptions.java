package io.zmux;

import io.zmux.helper.Limiter;
import java.util.Optional;

/**
 * Relay policy of {@link Zmux#splice} and {@link Zmux#splicePackets}, built with {@link #builder()};
 * {@link #DEFAULT} sets nothing.
 */
public final class SpliceOptions {
    /** The default chunk, 64 KiB. */
    public static final int DEFAULT_CHUNK = 65536;

    /** No limiter, no abort handle and the default chunk. */
    public static final SpliceOptions DEFAULT = builder().build();

    private final int chunk;
    private final Limiter aToB, bToA;
    private final SpliceAbort abort;

    private SpliceOptions(Builder b) {
        chunk = b.chunk; aToB = b.aToB; bToA = b.bToA; abort = b.abort;
    }

    public static Builder builder() { return new Builder(); }

    /** The most bytes a direction holds between reading and writing. */
    public int chunk() { return chunk; }

    /** The limiter of the direction from a to b; empty: none. */
    public Optional<Limiter> aToB() { return Optional.ofNullable(aToB); }

    /** The limiter of the direction from b to a; empty: none. */
    public Optional<Limiter> bToA() { return Optional.ofNullable(bToA); }

    /** The handle that may end the splice with an application code; empty: none. */
    public Optional<SpliceAbort> abort() { return Optional.ofNullable(abort); }

    /** Builds {@link SpliceOptions}; every setting not called keeps its default. */
    public static final class Builder {
        private int chunk = DEFAULT_CHUNK;
        private Limiter aToB, bToA;
        private SpliceAbort abort;

        private Builder() {}

        /** The most bytes a direction holds between reading and writing, at least 1; default {@link #DEFAULT_CHUNK}. */
        public Builder chunk(int n) {
            if (n < 1) throw new IllegalArgumentException("chunk must be positive");
            chunk = n;
            return this;
        }

        /** The limiter of the direction from a to b. */
        public Builder aToB(Limiter l) { aToB = java.util.Objects.requireNonNull(l, "l"); return this; }

        /** The limiter of the direction from b to a. */
        public Builder bToA(Limiter l) { bToA = java.util.Objects.requireNonNull(l, "l"); return this; }

        /** The handle that may end the splice with an application code; one handle may serve several splices. */
        public Builder abort(SpliceAbort a) { abort = java.util.Objects.requireNonNull(a, "a"); return this; }

        public SpliceOptions build() { return new SpliceOptions(this); }
    }
}
