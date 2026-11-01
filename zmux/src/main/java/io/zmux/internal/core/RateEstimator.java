package io.zmux.internal.core;

import java.util.Arrays;

/**
 * R_p of one outbound pipe (SCHEDULER.md Section 2.3): delivery-rate samples, a
 * windowed maximum over the last 10 round trips within 10 s, and the
 * application-limited rules (the bottleneck observation and the bubble).
 */
final class RateEstimator {
    static final int ROUNDS = 10;
    static final long WINDOW = 10 * Time.SECOND;
    static final double INITIAL = 1e6;
    static final int KEEP = 64;

    private double r;
    /** R_lo, the limiter's rate bound; 0: none. */
    private double lo;
    private double[] sr = new double[0];
    private long[] sRound = new long[0];
    private long[] sAt = new long[0];
    private int sn;
    private long round;
    long round() { return round; }
    private long roundEnd;
    private long bubble;
    private long blockEpoch;
    long blockEpoch() { return blockEpoch; }
    private boolean blockedNow;
    /** An accepted rate sample exists; distinct from a bottleneck observation. */
    private boolean hasSample;
    boolean hasSample() { return hasSample; }
    /** Placement probing ends only after a bottleneck observation. */
    private boolean measured;
    boolean measured() { return measured; }

    /** R_lo, the limiter's rate bound; 0: none. */
    double bound() { return lo; }

    /** Sets R_lo (written by the pipe's limiter only); 0 removes the bound. */
    void bound(double v) { lo = v; }

    /** The windowed maximum without the bound. */
    double raw() { return r; }

    /** R_p, the initial 1 MB/s before any sample. */
    double value() {
        double v = r <= 0 ? INITIAL : r;
        return lo > 0 && lo < v ? lo : v;
    }

    void blocked() { blockEpoch++; }

    /** Whether the pipe's carrier is not writable now (a sample taken now is a bottleneck sample). */
    void blockedNow(boolean v) { blockedNow = v; }

    /** Opportunistic penalization (MULTIPATH.md 7.5.1): R_p halves and its samples are discarded. */
    void penalize() {
        double v = value();
        sn = 0;
        r = v / 2;
        measured = false;
    }

    void appLimited(long so) { bubble = so; }

    /** Whether a bubble is in flight (cum has not passed the recorded position). */
    boolean bubbleInFlight(long cum) { return cum <= bubble; }

    /** Moves the round count with the cumulative point; reports whether a round ended. */
    boolean advance(long cum, long so) {
        if (cum > roundEnd) {
            round++;
            roundEnd = so;
            return true;
        }
        return false;
    }

    /** Feeds one sample; returns its value when it is a bottleneck sample, else 0. */
    double sample(double rate, boolean bubbleFlag, long epoch, long now) {
        boolean bottleneck = !bubbleFlag && (epoch != blockEpoch || blockedNow);
        if (!bottleneck && rate <= value()) return 0;
        hasSample = true;
        if (bottleneck) measured = true;
        if (sn == sr.length) {
            int c = Math.max(4, sn * 2);
            sr = Arrays.copyOf(sr, c);
            sRound = Arrays.copyOf(sRound, c);
            sAt = Arrays.copyOf(sAt, c);
        }
        sr[sn] = rate;
        sRound[sn] = round;
        sAt[sn] = now;
        sn++;
        int cut = 0;
        while (cut < sn && !(sRound[cut] + ROUNDS >= round && Time.sub(now, sAt[cut]) <= WINDOW)) cut++;
        if (sn - cut > KEEP) cut = sn - KEEP;
        if (cut > 0) {
            System.arraycopy(sr, cut, sr, 0, sn - cut);
            System.arraycopy(sRound, cut, sRound, 0, sn - cut);
            System.arraycopy(sAt, cut, sAt, 0, sn - cut);
            sn -= cut;
        }
        double m = 0;
        for (int i = 0; i < sn; i++) m = Math.max(m, sr[i]);
        if (m > 0) r = m;
        return bottleneck ? rate : 0;
    }
}
