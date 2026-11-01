package io.zmux.internal.core;

/**
 * Quality events (API.md Section 4.5): a pipe's smoothed RTT or delivery rate leaving a band
 * around the last reported values, and the outbound capacity crossing a floor. Allocated only
 * when the application sets a band or a floor; evaluated where the estimates change.
 */
final class QualityMonitor {
    /** The band of PIPE_QUALITY: +-rtt nanoseconds, [last / ratio, last x ratio]; zero turns a half off. */
    record Band(long rtt, double rateRatio, long minInterval) {}

    private final Session s;
    private Band band;
    private double floorRate;
    private double floorRatio;
    private boolean floorSet;
    boolean floorSet() { return floorSet; }
    private boolean below;

    QualityMonitor(Session s) {
        this.s = s;
    }

    /** The smoothed RTT of p, or -1 before a sample; [rtt, rate] via the fields below. */
    private long estRtt;
    private double estRate;

    private boolean estimates(Pipe p) {
        RttEstimator r = p.mp() != null ? p.mp().rtt : p.out().snd.rtt();
        if (!r.has()) return false;
        estRtt = r.smoothed();
        estRate = p.out().snd.re().value();
        return true;
    }

    /** Sets the quality band of one pipe, or (p null) the default of the session's pipes. */
    void band(Pipe p, Band b) {
        if (p == null) band = b;
        else p.out().q.band = b;
    }

    /** Judges p's estimates after they changed. */
    void sample(Pipe p) {
        if (p.out() == null) return;
        QualityMonitor.PipeQ q = p.out().q;
        Band b = q.band != null ? q.band : band;
        if (b != null && estimates(p)) {
            if (!q.has) {
                q.has = true;
                q.rtt = estRtt;
                q.rate = estRate;
                q.lastAt = s.now();
            } else if (outOfBand(b, q, estRtt, estRate)) {
                if (b.minInterval() > 0 && s.now() < Time.add(q.lastAt, b.minInterval())) {
                    q.pending = true;
                    q.pendTime = Time.add(q.lastAt, b.minInterval());
                } else {
                    report(p, q, estRtt, estRate);
                }
            }
        }
        if (floorSet) checkFloor();
    }

    private static boolean outOfBand(Band b, QualityMonitor.PipeQ q, long rtt, double rate) {
        if (b.rtt() > 0 && (rtt > Time.add(q.rtt, b.rtt()) || Time.add(rtt, b.rtt()) < q.rtt)) return true;
        return b.rateRatio() > 0 && q.rate > 0 && (rate > q.rate * b.rateRatio() || rate * b.rateRatio() < q.rate);
    }

    private void report(Pipe p, QualityMonitor.PipeQ q, long rtt, double rate) {
        q.rtt = rtt;
        q.rate = rate;
        q.lastAt = s.now();
        q.pending = false;
        s.emit(Event.pipeQuality(p.id, p.carrier(), rtt, rate));
    }

    /** Emits the reports held back by minInterval; returns the next such deadline. */
    long timers(long now, boolean apply) {
        long next = Time.NEVER;
        for (Pipe p : s.pipes.all()) {
            if (p.out() == null || !p.out().q.pending) continue;
            QualityMonitor.PipeQ q = p.out().q;
            if (now >= q.pendTime) {
                if (apply) {
                    if (estimates(p)) report(p, q, estRtt, estRate);
                    else q.pending = false;
                } else {
                    next = Math.min(next, q.pendTime); // Overdue: pending until applied.
                }
            } else {
                next = Math.min(next, q.pendTime);
            }
        }
        return next;
    }

    void setFloor(double rate, double ratio) {
        floorRate = rate;
        floorRatio = ratio <= 0 ? 1.25 : ratio;
        below = false;
        floorSet = rate > 0;
        if (floorSet) checkFloor();
    }

    /** Compares the measured rates of the usable, non-standby outbound pipes with the floor. */
    void checkFloor() {
        double sum = 0;
        PathSession mp = s.path() instanceof PathSession m ? m : null;
        boolean probationFallback = mp != null && (mp.tiers.usableClass() & 2) != 0;
        for (Pipe p : s.pipes.all()) {
            if (p.out() == null || !p.out().snd.re().hasSample()) continue;
            // Only a multipath session attaches pipes, so an attached pipe implies mp.
            if (p.mp() != null && (p.mp().effStandby()
                    || !java.util.Objects.requireNonNull(mp).metrics.pipeUsable(p, probationFallback))) continue;
            sum += p.out().snd.re().value();
        }
        if (!below && sum < floorRate) {
            below = true;
            s.emit(Event.capacity(true, sum));
        } else if (below && sum >= floorRate * floorRatio) {
            below = false;
            s.emit(Event.capacity(false, sum));
        }
    }

    /** A pipe's quality band state (API.md Section 4.5). */
    static final class PipeQ {
        private Band band;
        private long rtt;
        private double rate;
        private long lastAt;
        private long pendTime;
        private boolean has;
        private boolean pending;

        PipeQ() {
        }

        Band band() {
            return band;
        }

        long rtt() {
            return rtt;
        }

        double rate() {
            return rate;
        }

        long lastAt() {
            return lastAt;
        }

        long pendTime() {
            return pendTime;
        }

        boolean has() {
            return has;
        }

        boolean pending() {
            return pending;
        }
    }
}
