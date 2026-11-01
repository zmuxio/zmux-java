package io.zmux.internal.core;

/** RFC 9002 Section 5 with a 300 s minimum filter. */
final class RttEstimator {
    private boolean has;
    boolean has() { return has; }
    private long smoothed;
    long smoothed() { return smoothed; }
    private long rttvar;
    long rttvar() { return rttvar; }
    private long min;
    long min() { return min; }
    private long minAt;
    private long latest;
    long latest() { return latest; }

    /** Sets the estimate before any sample (RFC 9002 Section 5.3). */
    void initial(long d) {
        smoothed = d;
        rttvar = d / 2;
    }

    /**
     * Takes one sample and returns the adjusted value. A quiet sample (taken while a multipath session is quiet)
     * enters the minimum only when there is none yet (SCHEDULER.md 2.1).
     */
    long update(long sample, long ackDelay, long maxAckDelay, long now, boolean quiet) {
        latest = sample;
        if (!has) {
            has = true;
            min = sample;
            minAt = now;
            smoothed = sample;
            rttvar = sample / 2;
            return sample;
        }
        if (!quiet && (sample <= min || Time.sub(now, minAt) > 300 * Time.SECOND)) {
            min = sample;
            minAt = now;
        }
        long adj = sample;
        long ad = Math.min(ackDelay, maxAckDelay);
        if (sample - ad >= min) adj = sample - ad;
        long d = Math.abs(smoothed - adj);
        rttvar = (3 * rttvar + d) / 4;
        smoothed = (7 * smoothed + adj) / 8;
        return adj;
    }
}
