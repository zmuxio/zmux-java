package io.zmux.internal.core;

/**
 * Receive-window auto-tuning state of one scope (SPEC.md Section 8.3 rule 6): the
 * arrival rate in the last intervals and the credit-loop samples awaiting their
 * first byte beyond the old limit.
 */
final class Tuner {
    static final int K = 4;
    static final long RATE_FLOOR = Time.MILLISECOND;
    static final int PENDING_MAX = 17;

    private final double[] rates = new double[4];
    private final long[] rttSeen = new long[4];
    private int idx;
    private int n;
    private long t0;
    private long off0;
    private boolean started;
    private final long[] pendLimit = new long[PENDING_MAX];
    private final long[] pendSentAt = new long[PENDING_MAX];
    private final boolean[] pendBlocked = new boolean[PENDING_MAX];
    private int ph;
    private int pn;

    /** Number of rate intervals measured. */
    int intervals() { return n; }

    /** The highest rate of the last intervals. */
    double maxRate() {
        double r = 0;
        for (double x : rates) r = Math.max(r, x);
        return r;
    }

    /** The highest input-pipe RTT seen in the last intervals; -1 when none. */
    long maxRttSeen() {
        long m = -1;
        for (long v : rttSeen) if (v > 0) m = Math.max(m, v);
        return m;
    }

    /** Multipath: the minimum RTT of the input pipes during the current interval. */
    void seenRtt(long d) {
        if (d > rttSeen[idx]) rttSeen[idx] = d;
    }

    /**
     * Data up to off arrived: settles the credit-loop samples it passed, feeding loopMin (null:
     * not measured, multipath) from blocked ones, then measures the arrival rate over intervals
     * of at least iv.
     */
    void arrived(long off, long now, MinFilter loopMin, long iv) {
        while (pn > 0) {
            int k = ph;
            if (off < pendLimit[k]) break;
            if (off == pendLimit[k]) {
                pendBlocked[k] = true;
                break;
            }
            if (pendBlocked[k] && loopMin != null) loopMin.update(Time.sub(now, pendSentAt[k]), now);
            ph = (ph + 1) % PENDING_MAX;
            pn--;
        }
        if (!started) {
            started = true;
            t0 = now;
            off0 = off;
            return;
        }
        long el = Time.sub(now, t0);
        if (el >= iv) {
            idx = (idx + 1) % rates.length;
            rates[idx] = (off - off0) * 1e9 / el;
            rttSeen[idx] = 0;
            n++;
            t0 = now;
            off0 = off;
        }
    }

    /** Records a BLOCKED frame at limit. */
    void blockedAt(long limit) {
        for (int i = 0; i < pn; i++) {
            int k = (ph + i) % PENDING_MAX;
            if (pendLimit[k] == limit) pendBlocked[k] = true;
        }
    }

    /** Records an update from oldLimit (credit-loop sample). */
    void sent(long oldLimit, long now, boolean blocked) {
        if (pn == PENDING_MAX) {
            ph = (ph + 1) % PENDING_MAX;
            pn--;
        }
        int k = (ph + pn) % PENDING_MAX;
        pendLimit[k] = oldLimit;
        pendSentAt[k] = now;
        pendBlocked[k] = blocked;
        pn++;
    }

    /** A 300 s windowed minimum that keeps its value while no sample arrives. */
    static final class MinFilter {
        private long v;
        private long at;
        private boolean has;

        long value() { return v; }

        boolean has() { return has; }

        void update(long d, long now) {
            if (!has || d <= v || Time.sub(now, at) > 300 * Time.SECOND) {
                v = d;
                at = now;
                has = true;
            }
        }
    }
}
