package io.zmux;

/** Hard bounds for optional copies on one session's outbound direction. */
public record DatagramCopyPolicy(int maxCopies, int extraPercent, boolean standby) {
    public static final DatagramCopyPolicy DEFAULT = new DatagramCopyPolicy(2, 20, false);

    public DatagramCopyPolicy {
        if (maxCopies < 1 || extraPercent < 0 || extraPercent > 100)
            throw new IllegalArgumentException("invalid datagram copy bounds");
    }

    /** An unordered pair with independent path evidence supplied by the application. */
    public record IndependentPair(PipeId first, PipeId second) {
        public IndependentPair {
            if (first.equals(second)) throw new IllegalArgumentException("a path cannot be independent of itself");
            if (first.value() > second.value()) {
                PipeId swap = first;
                first = second;
                second = swap;
            }
        }
    }
}
