package io.zmux.internal.core;

/**
 * The carrier queue limiter of one outbound pipe (SCHEDULER.md Section 9.1): a pipe
 * whose carrier has neither a low-water mark nor an unsent count is paced from what
 * the peer acknowledges. Every event costs constant amortised work.
 */
final class Limiter {
    static final long GRANULARITY = Time.MILLISECOND;
    static final double STARTUP_GAIN = 2.885;
    static final int DOWN_PHASE = 1;
    static final int DOWN_PHASES = 4;
    static final double BUCKET_QUANTA = 2.0;
    static final double FULL_GROWTH = 1.25;
    static final int FULL_ROUNDS = 3;
    static final long BASE_SPAN = 10 * Time.SECOND;
    static final int BASE_TOLERANCE = 8;
    static final long EXPLAINED_SPAN = Time.SECOND;
    static final int EXPLAINED_BASES = 8;
    static final long PROBE_MIN = 200 * Time.MILLISECOND;
    static final long RQ_SPAN = 500 * Time.MILLISECOND;
    static final int RQ_BASES = 4;
    static final long RATE_BOUND_LIFE = 10 * Time.SECOND;
    static final double[] GAIN_CYCLE = {1.25, 0.75, 1, 1, 1, 1, 1, 1};

    static final int STARTUP = 0, DRAIN = 1, CRUISE = 2, PROBE = 3;

    /** Session values the limiter uses. */
    record Params(long budget, long threshold, long ackDelay, long chunk) {
        /** A = min(threshold, R x max_ack_delay) + spread_chunk. */
        long slack(double rate) {
            long d = (long) (rate * ackDelay / 1e9);
            return Math.min(threshold, d) + chunk;
        }
    }

    /**
     * One acknowledgement that advanced the pipe's position: the RTT sample (0: none) and when its
     * record was sent, the delivery-rate sample (0: none), whether a round ended, and the
     * acknowledged, sent and held-back positions.
     */
    record Ack(long rtt, long rttSentAt, double rate, boolean roundEnd, long acked, long sent, long held) {}

    private int mode = STARTUP;
    private double full;
    private int flat;
    private int phase;
    private long phaseSince;
    // Probe.
    private long prSince;
    private long prLength;
    private long prStartSent;
    private long prDrainedAt;
    private long prOldBase;
    private long prAfterDrain;
    private double prRq;
    private boolean prDrained;
    private boolean prHasAfter;
    private boolean prHasRq;

    private long nearAt;
    private long explainedAt;
    private long baseD;
    private boolean baseHas;
    final Windowed recent = new Windowed(false);
    private long lastSample;
    private boolean hasLastSample;
    final Windowed bottleneck = new Windowed(true);
    private double roundMax;
    private long boundSet;
    private boolean boundHas;

    private double tokens;
    private boolean hasTokens;
    private long refilled;
    private long inFlight;
    private boolean held;
    boolean held() { return held; }
    private long release = Time.NEVER;
    long release() { return release; }
    final Windowed heldMin = new Windowed(false);

    Limiter(long now) {
        nearAt = now;
        explainedAt = now;
        refilled = now;
    }

    static long timeAt(long bytes, double rate) {
        return (long) (bytes / Math.max(rate, 1) * 1e9);
    }

    long baseOr(long srtt) { return baseHas ? baseD : srtt; }

    long unit(long srtt) { return Math.max(baseOr(srtt), GRANULARITY); }

    boolean downDone(double rate, long srtt, Params p) {
        long bdp = (long) (rate * baseOr(srtt) / 1e9);
        return inFlight <= bdp + p.slack(rate);
    }

    private long plen(int k, long u, boolean downDone) {
        if (k == DOWN_PHASE) return downDone ? 0 : u * DOWN_PHASES;
        return u;
    }

    /** Runs the cycle on to t: [phase, phase start, gain integral in seconds]. */
    private long walkPhase;
    private long walkStart;

    private double walk(long t, long unit, boolean downDone) {
        long u = Math.max(unit, 1);
        long left = Math.max(Time.sub(t, phaseSince), 0);
        int k = phase;
        long start = 0;
        double gain = 0;
        long n0 = plen(k, u, downDone);
        if (left >= n0) {
            gain += GAIN_CYCLE[k] * n0;
            start += n0;
            left -= n0;
            k = (k + 1) % GAIN_CYCLE.length;
            long period = 0;
            double pg = 0;
            for (int i = 0; i < GAIN_CYCLE.length; i++) {
                period += plen(i, u, downDone);
                pg += GAIN_CYCLE[i] * plen(i, u, downDone);
            }
            if (period > 0) {
                long n = left / period;
                gain += n * pg;
                start += n * period;
                left -= n * period;
            }
            while (true) {
                long nk = plen(k, u, downDone);
                if (left < nk) break;
                gain += GAIN_CYCLE[k] * nk;
                start += nk;
                left -= nk;
                k = (k + 1) % GAIN_CYCLE.length;
            }
        }
        gain += GAIN_CYCLE[k] * left;
        walkPhase = k;
        walkStart = Time.add(phaseSince, start);
        return gain / 1e9;
    }

    double gain(long now, double rate, long srtt, Params p) {
        return switch (mode) {
            case STARTUP -> STARTUP_GAIN;
            case DRAIN -> 1 / STARTUP_GAIN;
            case CRUISE -> {
                walk(now, unit(srtt), downDone(rate, srtt, p));
                yield GAIN_CYCLE[(int) walkPhase];
            }
            default -> 1;
        };
    }

    double paceRate(long now, double rate, long srtt, Params p) {
        return gain(now, rate, srtt, p) * Math.max(rate, 1);
    }

    double quantum(double paceRate, Params p) {
        return Math.max(2.0 * p.chunk, paceRate * GRANULARITY / 1e9);
    }

    double gainTime(long from, long to, double rate, long srtt, Params p) {
        if (mode != CRUISE) return gain(to, rate, srtt, p) * Math.max(Time.sub(to, from), 0) / 1e9;
        long u = unit(srtt);
        boolean done = downDone(rate, srtt, p);
        double g1 = walk(to, u, done);
        double g0 = walk(from, u, done);
        return Math.max(g1 - g0, 0);
    }

    double bucket(long now, double rate, long srtt, Params p) {
        double q = quantum(paceRate(now, rate, srtt, p), p);
        if (!hasTokens) return q;
        return Math.min(tokens + Math.max(rate, 1) * gainTime(refilled, now, rate, srtt, p), BUCKET_QUANTA * q);
    }

    void settle(long now, long bytes, double rate, long srtt, Params p) {
        tokens = bucket(now, rate, srtt, p) - bytes;
        hasTokens = true;
        refilled = now;
    }

    void advance(long now, long inFlightNow, double rate, long srtt, Params p) {
        settle(now, 0, rate, srtt, p);
        if (mode == CRUISE) {
            walk(now, unit(srtt), downDone(rate, srtt, p));
            phase = (int) walkPhase;
            phaseSince = walkStart;
        }
        inFlight = inFlightNow;
        if (mode == CRUISE && phase == DOWN_PHASE && downDone(rate, srtt, p)) {
            phase = DOWN_PHASE + 1;
            phaseSince = now;
        }
    }

    /** 2 x R x (RTT_base + B) + A, or A during a probe. */
    long cap(double rate, long srtt, Params p) {
        long a = p.slack(rate);
        if (mode == PROBE) return a;
        return (long) (2 * rate * (baseOr(srtt) + p.budget()) / 1e9) + a;
    }

    /** Stream data the pipe may take now, rounded up to whole chunks. */
    long room(long now, long inFlightNow, double rate, long srtt, Params p) {
        double t = bucket(now, rate, srtt, p);
        long c = cap(rate, srtt, p);
        if (t < 1 || inFlightNow >= c) return 0;
        long r = Math.clamp((long) t, 1, c - inFlightNow);
        long ch = Math.max(p.chunk(), 1);
        return (r + ch - 1) / ch * ch;
    }

    /** Decides whether the pipe is held and until when. */
    void refresh(long now, long inFlightNow, double rate, long srtt, Params p) {
        advance(now, inFlightNow, rate, srtt, p);
        held = room(now, inFlightNow, rate, srtt, p) == 0;
        release = Time.NEVER;
        double t = bucket(now, rate, srtt, p);
        if (held && t < 1) {
            long d = GRANULARITY;
            double pr = paceRate(now, rate, srtt, p);
            if (pr > 0) d = (long) Math.min((1 - t) / pr * 1e9, Long.MAX_VALUE / 2.0) + Time.MICROSECOND;
            release = Time.add(now, d);
        }
    }

    long standing(long srtt) {
        return recent.has() ? Math.max((long) recent.get() - baseOr(srtt), 0) : 0;
    }

    void onAck(long now, Ack ev, RateEstimator e, long srtt, Params p) {
        double rate = e.value();
        advance(now, inFlight, rate, srtt, p);
        if (mode == DRAIN || mode == CRUISE) maybeProbe(now, ev, srtt);
        long aTime = timeAt(p.slack(rate), rate);
        if (ev.rtt() > 0) rttSample(now, ev.rtt(), ev.rttSentAt(), srtt, p, aTime);
        long base = baseOr(srtt);
        if (ev.rate() > 0) {
            roundMax = Math.max(roundMax, ev.rate());
            bottleneck.update(now, ev.rate(), Math.max(RQ_SPAN, RQ_BASES * base));
        }
        if (ev.roundEnd()) roundEnded(e, srtt, p, aTime);
        if (e.bound() > 0 && (e.raw() <= e.bound() || !boundHas || Time.sub(now, boundSet) >= RATE_BOUND_LIFE)) {
            e.bound(0);
            boundHas = false;
        }
        long inf = ev.sent() - Math.min(ev.sent(), ev.acked());
        inf -= Math.min(inf, ev.held());
        switch (mode) {
            case DRAIN -> {
                long bdp = (long) (rate * base / 1e9);
                if (standing(srtt) <= p.budget() || inf <= Math.max(bdp, p.slack(rate))) {
                    mode = CRUISE;
                    phase = 0;
                    phaseSince = now;
                }
            }
            case PROBE -> {
                if (!prDrained && ev.acked() >= prStartSent) {
                    prDrained = true;
                    prDrainedAt = now;
                }
                boolean measuredNow = prHasAfter || ev.acked() >= ev.sent();
                if (now >= Time.add(prSince, prLength) && measuredNow && prDrained) endProbe(now, e, p);
            }
            default -> { }
        }
        refresh(now, inf, e.value(), srtt, p);
    }

    /** The QX_PING round trip of a single pipe's first record. */
    void pingSample(long now, long rtt, RateEstimator e, long srtt, Params p) {
        double rate = e.value();
        rttSample(now, rtt, now - rtt, srtt, p, timeAt(p.slack(rate), rate));
    }

    void rttSample(long now, long u, long sentAt, long srtt, Params p, long aTime) {
        long gap = hasLastSample ? Time.sub(now, lastSample) : 0;
        recent.update(now, u, Math.max(srtt / 2, gap));
        lastSample = now;
        hasLastSample = true;
        if (mode == PROBE && prDrained && sentAt >= prDrainedAt) {
            if (!prHasAfter || u < prAfterDrain) {
                prAfterDrain = u;
                prHasAfter = true;
            }
        }
        if (!baseHas || u < baseD) {
            baseD = u;
            baseHas = true;
        }
        long base = baseOr(srtt);
        if (u <= base + p.budget() + aTime + base / BASE_TOLERANCE) nearAt = now;
        if (u <= 2 * (base + p.budget()) + aTime) explainedAt = now;
    }

    void roundEnded(RateEstimator e, long srtt, Params p, long aTime) {
        double sampled = roundMax;
        roundMax = 0;
        if (mode == STARTUP && sampled > 0) {
            if (e.value() >= full * FULL_GROWTH) {
                full = e.value();
                flat = 0;
            } else if (++flat >= FULL_ROUNDS) {
                mode = DRAIN;
            }
        }
        if (e.bound() > 0 && sampled > e.bound() && standing(srtt) <= p.budget() + aTime) e.bound(sampled);
    }

    void maybeProbe(long now, Ack ev, long srtt) {
        long base = baseOr(srtt);
        boolean far = Time.sub(now, nearAt) >= BASE_SPAN;
        boolean unexplained = Time.sub(now, explainedAt) >= Math.max(EXPLAINED_SPAN, EXPLAINED_BASES * base);
        if (!far && !unexplained) return;
        mode = PROBE;
        bottleneck.prune(now, Math.max(RQ_SPAN, RQ_BASES * base));
        prSince = now;
        prLength = Math.max(PROBE_MIN, srtt);
        prStartSent = ev.sent();
        prDrained = ev.acked() >= ev.sent();
        prDrainedAt = now;
        prOldBase = base;
        prHasRq = bottleneck.has();
        prRq = prHasRq ? bottleneck.get() : 0;
        prHasAfter = false;
        prAfterDrain = 0;
    }

    void endProbe(long now, RateEstimator e, Params p) {
        boolean unchanged = prHasAfter && prHasRq
                && prAfterDrain <= prOldBase + p.budget() + timeAt(p.slack(prRq), prRq);
        if (unchanged && prRq < e.value()) {
            e.bound(prRq);
            boundSet = now;
            boundHas = true;
            baseD = prOldBase;
            baseHas = true;
        } else if (prHasAfter) {
            baseD = prAfterDrain;
            baseHas = true;
        }
        nearAt = now;
        explainedAt = now;
        mode = CRUISE;
        phase = 0;
        phaseSince = now;
    }

    /** Held bytes left out of the bytes in flight: all during start-up, their windowed minimum afterwards. */
    long leftOut(long heldBytes) {
        if (mode == STARTUP) return heldBytes;
        return heldMin.has() ? Math.min(heldBytes, (long) heldMin.get()) : heldBytes;
    }
}
