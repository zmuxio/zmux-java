package io.zmux.internal.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Scheduling decisions of a multipath session: the per-opportunity decision, ECF/BLEST spread, weighted
 * round-robin, affinity bindings, redundancy and custom policy requests (MULTIPATH.md 11, SCHEDULER.md 3-8,
 * IMPLEMENTATION.md 2.5.2).
 */
public final class PathScheduler {
    /** What PathScheduler asks of the rest of the path layer and the session. */
    interface Port {
        /** The session's current scheduling policy. */
        CoreConfig.Policy policy();

        /** See {@link Tiers}. */
        boolean admits(Tiers.Gate g, Pipe p);

        /** See {@link Tiers}. */
        Tiers.Gate buildGate();

        /** See {@link Retention}. */
        void copyChunk(PathScheduler.Decision d, RecordWriter w, Stream st, long off, long n, boolean fin);

        /** See {@link Tiers}. */
        boolean gateUsable(Tiers.Gate g, Pipe p);

        /** The session clock. */
        long now();

        /** See {@link Resume}. */
        boolean reinjectRoom(Pipe p);

        /** The session's effective send buffer. */
        long sessionSendBuffer();

        /** See {@link Tiers}. */
        boolean takes(Tiers.Gate g, Pipe p);
    }

    static final double ECF_BETA = 0.25;
    static final long SMALL_STREAM = 256 << 10;
    static final int PLACEMENT_ROUNDS = 4;
    static final long PICK_WAIT = Time.MILLISECOND;

    private final Port port;
    private final CoreConfig cfg;
    private final ConnFlow conn;
    private final StreamScheduler sched;
    private final Transmitter tx;
    private final Lanes lanes;
    private final PipeTable pipes;
    private final StreamsView streams;
    private final PathMetrics metrics;
    private final Decision dec = new Decision();
    // Affinity re-evaluation.
    private final ArrayList<Long> affSeen = new ArrayList<>(0);
    private long affEpoch;
    private boolean affProvisional;
    private boolean affStale;
    // Custom policy.
    private long pickGen;
    private boolean pickWanted;
    private final ArrayList<Stream> pickQ = new ArrayList<>(0);
    private long inputEpoch;
    private final SessionStats stats;
    private boolean pickTimer;
    private long pickDeadline;

    PathScheduler(CoreConfig cfg, SessionStats stats, ConnFlow conn, StreamScheduler sched, Transmitter tx, Lanes lanes, PipeTable pipes, StreamsView streams,
                  PathMetrics metrics, Port port) {
        this.port = port;
        this.cfg = cfg;
        this.stats = stats;
        this.conn = conn;
        this.sched = sched;
        this.tx = tx;
        this.lanes = lanes;
        this.pipes = pipes;
        this.streams = streams;
        this.metrics = metrics;
    }


    /** Whether some stream direction would put data on lane ol of p now. */
    boolean anyStreamOn(Pipe p, Pipe.OutLane ol) {
        Decision d = dec;
        newDecision(d, p);
        StreamScheduler.Iter it = new StreamScheduler.Iter().reset(sched);
        for (Stream st = it.next(); st != null; st = it.next()) {
            if (!tx.canSendNow(st) || !onPipe(d, st)) continue;
            if (p.out().lo.offered() && lanes.laneFor(p, st) != ol) continue;
            return true;
        }
        return false;
    }

    /** What every stream's choice at one write opportunity of a pipe shares. */
    static final class Decision {
        private Pipe p;
        Pipe p() { return p; }
        private Tiers.Gate g;
        Tiers.Gate g() { return g; }
        private Pipe fastest;
        Pipe fastest() { return fastest; }
        private LevelStats lv;
        private int tier;
        int tier() { return tier; }
        private boolean multiTier;
        private boolean open;
        private boolean usable;
        private boolean takes;
        boolean takes() { return takes; }
        private boolean joiningTakes;
    }

    /** Ready amounts of the stream directions by urgency level, for ECF's backlog and BLEST's share. */
    static final class LevelStats {
        private boolean valid;
        final long[] total = new long[8];
        final long[] nonIncr = new long[8];
        final long[][] incr = new long[8][];
        final int[] incrN = new int[8];
        final long[][] prefix = new long[8][];

        long cappedSum(int u, long r) {
            long[] x = incr[u];
            int n = incrN[u];
            int lo = 0, hi = n;
            while (lo < hi) {
                int m = (lo + hi) >>> 1;
                if (x[m] <= r) lo = m + 1;
                else hi = m;
            }
            long sum = lo > 0 ? prefix[u][lo - 1] : 0;
            return sum + (long) (n - lo) * r;
        }
    }

    /** Prepares the decision for pipe p now. */
    void newDecision(Decision d, Pipe p) {
        LevelStats lv = d.lv;
        d.p = p;
        if (lv != null) lv.valid = false;
        d.fastest = null;
        d.tier = -1;
        d.multiTier = d.usable = d.takes = false;
        d.open = metrics.open(p);
        d.g = port.buildGate();
        d.joiningTakes = p.mp().health.out() == Attach.OUT_JOINING;
        if (d.g == null) return;
        d.multiTier = d.g.tiers.size() > 1;
        d.usable = port.gateUsable(d.g, p);
        d.takes = port.takes(d.g, p);
        d.tier = d.g.tierOf(p);
        if (d.tier >= 0) d.fastest = fastestIn(d.g, d.tier, cfg.spreadChunk(), null);
    }

    /** Updates the decision after a chunk: only a multi-tier gate depends on the data the chunk took. */
    void renewDecision(Decision d) {
        if (d.lv != null) d.lv.valid = false;
        if (d.multiTier) newDecision(d, d.p);
    }

    CoreConfig.Policy policyOf(Stream st) {
        CoreConfig.Policy own = ownPolicy(st);
        return own != null ? own : port.policy();
    }

    /** Whether the next chunk of st may go on the decision's pipe. */
    boolean onPipe(Decision d, Stream st) {
        if (!d.open) return false;
        Pipe p = d.p;
        StreamSend.SendPart sp = st.snd;
        if (d.g == null) return d.joiningTakes;
        PathStreamState m = sp.mps();
        if (m != null && (m.ret.resend().size() > 0 || m.ret.finResend())) return d.takes && d.fastest == p && port.reinjectRoom(p);
        if (d.takes && redundant(st)) {
            long l = Math.clamp(metrics.readyBytes(st), 1, cfg.spreadChunk());
            Pipe[] two = earliestTwo(d.g, d.tier, l, null);
            if (two[1] != null && (two[0] == p || two[1] == p)) return true;
        }
        return switch (policyOf(st)) {
            case AFFINITY -> d.usable && chosen(p, affinityTarget(d, st));
            case CUSTOM -> customOk(d, st);
            case SPREAD_ETA -> {
                if (!d.takes) yield false;
                if (spreadEtaOk(d, st)) yield true;
                chosen(p, d.fastest);
                yield false;
            }
            case SPREAD_ROUND_ROBIN, SPREAD_RATE_WEIGHTED -> d.takes && chosen(p, rrPick(d.g, d.tier, st));
            case SESSION_AFFINITY -> d.takes && chosen(p, d.g.tiers.get(d.tier).active());
        };
    }

    /** Whether the policy chose p; another chosen pipe's writer is woken to take the chunk. */
    boolean chosen(Pipe p, Pipe q) {
        if (q == p) return true;
        if (q != null && q.out() != null) pipes.wake(q);
        return false;
    }

    /** New stream data of st, [off, off+n), went on the decision's pipe. */
    void chunkSent(Decision d, RecordWriter w, Stream st, long off, long n, boolean fin) {
        Pipe p = d.p;
        switch (policyOf(st)) {
            case AFFINITY -> bindOnSend(d, st);
            case SPREAD_ROUND_ROBIN, SPREAD_RATE_WEIGHTED -> {
                if (d.g != null) rrCommit(d.g, d.tier, p, st);
            }
            case CUSTOM -> customUsed(st, n);
            default -> { }
        }
        if (d.g != null) port.copyChunk(d, w, st, off, n, fin);
    }

    /** The placement rate: R_p, or for a usable pipe without a bottleneck sample the fastest measured of its tier. */
    double placementRate(Pipe p) {
        RateEstimator e = p.out().snd.re();
        double own = e.value();
        Attach a = p.mp();
        if (e.measured() || a.sched.placedSet && e.round() >= a.sched.placedFrom + PLACEMENT_ROUNDS || !metrics.pipeUsable(p, false)) return own;
        long pr = a.effPrio();
        boolean sb = a.effStandby();
        double fastest = 0;
        for (Pipe q : pipes.all()) {
            if (q == p || q.mp() == null || q.out() == null || !q.out().snd.re().measured() || !metrics.pipeUsable(q, false)) continue;
            if (q.mp().effPrio() == pr && q.mp().effStandby() == sb) fastest = Math.max(fastest, q.out().snd.re().value());
        }
        if (fastest <= own) return own;
        if (!a.sched.placedSet) {
            a.sched.placedSet = true;
            a.sched.placedFrom = e.round();
        }
        return fastest;
    }

    /** ETA_p(L) in seconds. */
    double eta(Pipe p, long l) {
        double r = Math.max(placementRate(p), 1);
        return (metrics.queued(p) + l) / r + metrics.owd(p) / 1e9;
    }

    Pipe fastestIn(Tiers.Gate g, int ti, long l, Pipe except) {
        return earliestTwo(g, ti, l, except)[0];
    }

    /** The two pipes of tier ti with the smallest ETA for l bytes, earliest first (ties to the lower ID). */
    Pipe[] earliestTwo(Tiers.Gate g, int ti, long l, Pipe except) {
        Pipe a = null, b = null;
        double ea = 0, eb = 0;
        if (ti >= 0 && ti < g.tiers.size()) {
            for (Pipe q : g.tierPipes(ti)) {
                if (q == except) continue;
                double e = eta(q, l);
                if (a == null || e < ea || e == ea && q.id < a.id) {
                    b = a;
                    eb = ea;
                    a = q;
                    ea = e;
                } else if (b == null || e < eb || e == eb && q.id < b.id) {
                    b = q;
                    eb = e;
                }
            }
        }
        return new Pipe[] {a, b};
    }

    LevelStats levels(Decision d) {
        if (d.lv == null) d.lv = new LevelStats();
        LevelStats lv = d.lv;
        if (lv.valid) return lv;
        Arrays.fill(lv.total, 0);
        Arrays.fill(lv.nonIncr, 0);
        Arrays.fill(lv.incrN, 0);
        StreamScheduler.Iter it = new StreamScheduler.Iter().reset(sched);
        for (Stream st = it.next(); st != null; st = it.next()) {
            long r = metrics.readyBytes(st);
            if (r == 0) continue;
            int u = st.tab.urgency() & 7;
            lv.total[u] += r;
            if (st.tab.incremental()) {
                if (lv.incr[u] == null || lv.incrN[u] == lv.incr[u].length) {
                    lv.incr[u] = Arrays.copyOf(lv.incr[u] == null ? new long[0] : lv.incr[u], Math.max(4, lv.incrN[u] * 2));
                }
                lv.incr[u][lv.incrN[u]++] = r;
            } else {
                lv.nonIncr[u] += r;
            }
        }
        for (int u = 0; u < 8; u++) {
            int n = lv.incrN[u];
            if (n == 0) continue;
            Arrays.sort(lv.incr[u], 0, n);
            if (lv.prefix[u] == null || lv.prefix[u].length < n) lv.prefix[u] = new long[lv.incr[u].length];
            long sum = 0;
            for (int k = 0; k < n; k++) {
                sum += lv.incr[u][k];
                lv.prefix[u][k] = sum;
            }
        }
        lv.valid = true;
        return lv;
    }

    /** spread-eta: earliest arrival with the ECF backlog and the BLEST credit test (SCHEDULER.md 5). */
    boolean spreadEtaOk(Decision d, Stream st) {
        Tiers.Gate g = d.g;
        Pipe p = d.p;
        long ready = metrics.readyBytes(st);
        long whole = cfg.spreadChunk();
        long chunk = Math.clamp(ready, 1, whole);
        Pipe f = d.fastest;
        if (chunk != whole) f = fastestIn(g, d.tier, chunk, null);
        if (f == null) return false;
        if (f == p) return true;
        double etaP = eta(p, chunk);
        if (etaP <= eta(f, chunk)) return true;
        int u = st.tab.urgency() & 7;
        LevelStats lv = levels(d);
        long more = 0;
        for (int v = 0; v < u; v++) more += lv.total[v];
        long room = conn.room();
        long backlog;
        double beta = ECF_BETA;
        if (u <= 2) {
            backlog = Math.min(room, more);
            beta = 0;
        } else {
            long level = st.tab.incremental() ? lv.nonIncr[u] + lv.cappedSum(u, ready) : lv.total[u];
            backlog = Math.min(room, more + level);
            backlog -= Math.min(backlog, chunk);
        }
        if (etaP > (1 + beta) * eta(f, backlog + chunk)) return false;
        double need = metrics.rateOf(f) * Math.max(0, etaP - metrics.owd(f) / 1e9);
        long w = Math.min(room, reorderBudget());
        if (need > w) return false;
        int n = st.tab.incremental() ? Math.max(1, lv.incrN[u]) : 1;
        long sroom = st.snd.maxData() - Math.min(st.snd.maxData(), st.snd.sent());
        return need / n <= sroom;
    }

    /** Twice the summed delivery rate times the one-way-delay spread of the usable pipes, plus one chunk. */
    long reorderBudget() {
        long room = conn.room();
        double sum = 0;
        long lo = 0, hi = 0;
        boolean first = true;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a == null || p.out() == null || !a.liveOut()) continue;
            sum += metrics.rateOf(p);
            if (metrics.pipeUsable(p, true)) {
                long o = metrics.owd(p);
                if (first || o < lo) lo = o;
                if (first || o > hi) hi = o;
                first = false;
            }
        }
        double b = 2 * sum * (hi - lo) / 1e9 + cfg.spreadChunk();
        if (b >= 9.2e18) return room;
        return Math.min(room, (long) b);
    }

    private interface WeightVisitor {
        void visit(Pipe p, double w);
    }

    void rrWeights(Tiers.Gate g, int ti, boolean byRate, WeightVisitor f) {
        boolean allZero = true;
        for (Pipe q : g.tierPipes(ti)) if (metrics.open(q) && q.mp().tier.weight() > 0) allZero = false;
        for (Pipe q : g.tierPipes(ti)) {
            if (!metrics.open(q)) continue;
            double w = allZero ? 1 : q.mp().tier.weight();
            if (w == 0) continue;
            if (byRate) w *= Math.max(placementRate(q), 1);
            f.visit(q, w);
        }
    }

    double rrCurrent(Stream st, boolean own, Pipe p) {
        if (!own) return p.mp().sched.rrCur;
        return st.snd.mps() == null ? 0 : st.snd.mps().sched.rr(p.id);
    }

    Pipe rrPick(Tiers.Gate g, int ti, Stream st) {
        boolean own = ownPolicy(st) != null;
        boolean byRate = policyOf(st) == CoreConfig.Policy.SPREAD_RATE_WEIGHTED;
        Pipe[] best = {null};
        double[] bv = {0};
        rrWeights(g, ti, byRate, (q, w) -> {
            double v = rrCurrent(st, own, q) + w;
            if (best[0] == null || v > bv[0] || v == bv[0] && q.id < best[0].id) {
                best[0] = q;
                bv[0] = v;
            }
        });
        return best[0];
    }

    /** A chunk given to p: every candidate gains its weight, the chosen one gives up the sum. */
    void rrCommit(Tiers.Gate g, int ti, Pipe p, Stream st) {
        boolean own = ownPolicy(st) != null;
        boolean byRate = policyOf(st) == CoreConfig.Policy.SPREAD_RATE_WEIGHTED;
        double[] sum = {0};
        rrWeights(g, ti, byRate, (q, w) -> sum[0] += w);
        rrWeights(g, ti, byRate, (q, w) -> {
            double dv = q == p ? w - sum[0] : w;
            if (!own) q.mp().sched.rrCur += dv;
            else st.snd.mp().sched.rrAdd(q.id, dv);
        });
        for (Pipe q : pipes.all()) {
            if (q.mp() == null || port.admits(g, q)) continue;
            if (!own) q.mp().sched.rrCur = 0;
            else if (st.snd.mps() != null) st.snd.mps().sched.rrZero(q.id);
        }
    }

    long expectedBytes(Stream st) {
        long e = st.unsent();
        if (st.snd.wwait()) {
            long lim = st.snd.bufLimit() > 0 ? st.snd.bufLimit() : port.sessionSendBuffer();
            e = Math.max(e, lim);
        }
        return e;
    }

    /** A_p: the expected bytes of the other directions bound to p. */
    long affinityLoad(Pipe p, Stream except) {
        long a = 0;
        StreamScheduler.Iter it = new StreamScheduler.Iter().reset(sched);
        for (Stream st = it.next(); st != null; st = it.next()) {
            if (st == except || st.snd.mps() == null) continue;
            PathStreamState m = st.snd.mps();
            long target = m.sched.moving ? m.sched.moveTo : m.sched.bindPipe;
            if (m.sched.bindSet && target == p.id) a += expectedBytes(st);
        }
        return a;
    }

    boolean affinityIdle(Stream st) {
        PathStreamState m = st.snd.mps();
        return m == null || m.ret.acked().prefix() >= st.snd.sent() && m.ret.resend().size() == 0;
    }

    /** The pipe an affinity direction sends on now. */
    Pipe affinityTarget(Decision d, Stream st) {
        Tiers.Gate g = d.g;
        PathStreamState m = st.snd.mps();
        boolean set = m != null && m.sched.bindSet;
        boolean idle = affinityIdle(st);
        if (set && !idle) {
            Pipe q = pipes.get(m.sched.bindPipe);
            if (q != null && port.gateUsable(g, q)) return m.sched.moving ? null : q;
        }
        if (set && m.sched.moving) {
            Pipe q = pipes.get(m.sched.moveTo);
            if (q != null && port.gateUsable(g, q)) return q;
        }
        return bindingChoice(g, st);
    }

    /** The most-preferred tier whose binding wait is at most D. */
    int passingTier(Tiers.Gate g, Stream st) {
        double d = cfg.overflowDelay() / 1e9;
        for (int i = 0; i < g.tiers.size(); i++) {
            double wait = 0, rate = 0;
            for (Pipe q : g.tierPipes(i)) {
                wait += metrics.queued(q) + affinityLoad(q, st);
                rate += Math.max(metrics.rateOf(q), 1);
            }
            if (wait / rate <= d) return i;
        }
        return -1;
    }

    /** The pipe an affinity direction binds to. */
    Pipe bindingChoice(Tiers.Gate g, Stream st) {
        long e = expectedBytes(st);
        boolean small = e < SMALL_STREAM || st.tab.urgency() <= 2;
        int ti = passingTier(g, st);
        List<Pipe> within = ti >= 0 ? g.tierPipes(ti) : small ? g.tierPipes(0) : g.pipes;
        Pipe best = null;
        double bs = 0;
        for (Pipe q : within) {
            double score = metrics.owd(q) / 1e9;
            if (!small) score += (metrics.queued(q) + affinityLoad(q, st) + e) / Math.max(metrics.rateOf(q), 1);
            if (best == null || score < bs || score == bs && q.id < best.id) {
                best = q;
                bs = score;
            }
        }
        return best;
    }

    boolean allSampled(Tiers.Gate g) {
        for (Pipe q : g.pipes) if (!q.mp().rtt.has()) return false;
        return true;
    }

    void bindOnSend(Decision d, Stream st) {
        PathStreamState m = st.snd.mp();
        Pipe p = d.p;
        if (m.sched.bindSet && m.sched.bindPipe == p.id && !m.sched.moving) return;
        boolean sampled = d.g != null && allSampled(d.g);
        m.sched.clearBinding();
        m.sched.bindPipe = p.id;
        m.sched.bindSet = true;
        m.sched.provisional = !sampled;
        m.sched.bindEpoch = affEpoch;
        if (!sampled) affProvisional = true;
    }

    /** Re-evaluates affinity bindings once per batch (SCHEDULER.md 3). */
    void refreshBindings(Tiers.Gate g) {
        if (g == null) return;
        boolean sampled = allSampled(g);
        boolean prov = affProvisional && sampled;
        if (prov) affProvisional = false;
        boolean grew = false;
        for (Pipe q : g.pipes) if (!affSeen.contains(q.id)) { grew = true; break; }
        affSeen.clear();
        for (Pipe q : g.pipes) affSeen.add(q.id);
        if (grew) {
            affEpoch++;
            affStale = true;
        }
        boolean staleNow = sampled && affStale;
        boolean topOnly = true;
        for (Pipe q : pipes.all()) {
            if (q.mp() != null && q.out() != null && Long.compareUnsigned(q.mp().effPrio(), g.tiers.getFirst().prio()) > 0) topOnly = false;
        }
        if (!prov && !staleNow && topOnly) return;
        if (sampled) affStale = false;
        StreamScheduler.Iter it = new StreamScheduler.Iter().reset(sched);
        for (Stream st = it.next(); st != null; st = it.next()) {
            if (policyOf(st) != CoreConfig.Policy.AFFINITY || st.snd.mps() == null) continue;
            PathStreamState b = st.snd.mps();
            if (!b.sched.bindSet || b.sched.moving) continue;
            Pipe target = null;
            boolean old = b.sched.bindEpoch < affEpoch && sampled;
            if (prov && b.sched.provisional || old) {
                target = bindingChoice(g, st);
            } else {
                Pipe q = pipes.get(b.sched.bindPipe);
                if (q == null || q.mp() == null) continue;
                int ti = g.tierOf(q);
                if (ti == 0) continue;
                int pt = passingTier(g, st);
                if (pt >= 0 && (ti < 0 || pt < ti)) target = bindingChoice(g, st);
            }
            b.sched.provisional = false;
            if (old) b.sched.bindEpoch = affEpoch;
            if (target != null && target.id != b.sched.bindPipe) {
                b.sched.moving = true;
                b.sched.moveTo = target.id;
            }
        }
    }

    boolean customOk(Decision d, Stream st) {
        PathStreamState m = st.snd.mp();
        if (m.sched.pickSet && m.sched.pickGen == pickGen) {
            if (m.sched.pickWait) {
                if (m.sched.pickEpoch == inputEpoch) return false;
            } else {
                return m.sched.pickPipe == d.p.id && pipeTakesAny(d.p);
            }
        }
        if (!m.sched.pickAsked || m.sched.pickGen != pickGen) {
            m.sched.clearPick();
            m.sched.pickAsked = true;
            m.sched.pickGen = pickGen;
            m.sched.pickAskedAt = port.now();
            if (!pickTimer || Time.add(port.now(), PICK_WAIT) < pickDeadline) {
                pickTimer = true;
                pickDeadline = Time.add(port.now(), PICK_WAIT);
            }
            if (!m.sched.pickQueued) {
                m.sched.pickQueued = true;
                pickQ.add(st);
            }
            pickWanted = true;
            return false;
        }
        if (Time.sub(port.now(), m.sched.pickAskedAt) < PICK_WAIT) return false;
        stats.schedulerFallback();
        return d.usable && affinityTarget(d, st) == d.p;
    }

    boolean pipeTakesAny(Pipe p) {
        return p.mp().health.out() == Attach.OUT_ACTIVE && p.out() != null && !p.out().failed();
    }

    void customUsed(Stream st, long n) {
        PathStreamState m = st.snd.mps();
        if (m == null || !m.sched.pickSet || m.sched.pickWait || m.sched.pickLeft == 0) return;
        if (n >= m.sched.pickLeft) m.sched.clearPick();
        else m.sched.pickLeft -= n;
    }

    void pickInput() {
        inputEpoch++;
    }

    void pickChanged() {
        pickGen++;
    }

    /** The session's policy changed: the previous policy's state is discarded. */
    void policyChanged() {
        for (Pipe q : pipes.all()) if (q.mp() != null) q.mp().sched.rrCur = 0;
        for (Stream st : streams.live()) {
            if (ownPolicy(st) == null && st.snd.mps() != null) {
                st.snd.mps().sched.clearBinding();
                st.snd.mps().sched.clearPick();
            }
        }
    }

    boolean redundant(Stream st) {
        if (policyOf(st) == CoreConfig.Policy.SESSION_AFFINITY) return false;
        int r = st.snd.mps() == null ? 0 : st.snd.mps().sched.redundant;
        if (r != 0) return r > 0;
        return cfg.redundancy() && st.tab.urgency() <= 2;
    }

    boolean idleCopied(Stream st) {
        if (policyOf(st) == CoreConfig.Policy.SESSION_AFFINITY) return false;
        int r = st.snd.mps() == null ? 0 : st.snd.mps().sched.redundant;
        if (r != 0) return r > 0;
        return cfg.redundancy();
    }

    /** A stream direction waiting for a custom decision. */
    public record PickFrame(long stream, long len, long eligibleTier, boolean hasTier, long lastPipe, boolean hasLastPipe,
                            int urgency) {}

    /** The value-only view of a pipe for a custom policy. */
    public record PickPipe(long pipe, PipeInfo.Out state, long srtt, long rttvar, long rate, long unsent, long inFlight,
                           long priority, long weight, boolean writable, boolean standby, boolean saturated,
                           boolean probation, boolean eligible, boolean shaped, double shapingWeight) {}

    /** One answer: a pipe index of the view and the bytes it covers (0: until invalidated), or wait. */
    public record PickAnswer(int index, long bytes, boolean waitAnswer) {}

    /** One call of the custom policy. */
    public static final class PickRequest {
        private long gen;
        public final ArrayList<PickFrame> frames = new ArrayList<>();
        public final ArrayList<PickPipe> pipes = new ArrayList<>();
        final ArrayList<Stream> keys = new ArrayList<>();

        /** The core stream of a request, for driver callback dispatch only. */
        public Stream stream(int index) { return keys.get(index); }

        /** Releases the snapshot references after its answers were applied. */
        public void clear() { frames.clear(); pipes.clear(); keys.clear(); }
    }

    boolean wantsPick() {
        return pickWanted && !pickQ.isEmpty();
    }

    boolean takePick(PickRequest req) {
        req.frames.clear();
        req.pipes.clear();
        req.keys.clear();
        pickWanted = false;
        req.gen = pickGen;
        Tiers.Gate g = port.buildGate();
        for (Pipe p : pipes.all()) {
            if (p.mp() == null || p.out() == null || !p.mp().liveOut()) continue;
            Attach a = p.mp();
            boolean eligible = false, sat = false;
            if (g != null) {
                eligible = port.admits(g, p);
                int ti = g.tierOf(p);
                if (ti >= 0 && ti <= g.last()) sat = g.tiers.get(ti).sat();
            }
            long inFlight = p.out().snd.so() - Math.min(p.out().snd.so(), p.out().snd.cum());
            req.pipes.add(new PickPipe(p.id, Attach.OUT_STATES[a.health.out()], a.rtt.smoothed(), a.rtt.rttvar(), (long) metrics.rateOf(p),
                    0, inFlight, a.effPrio(), a.tier.weight(), metrics.open(p), a.effStandby(), sat, a.prob.probOn(), eligible,
                    p.shape() != null && p.shape().active(), p.shape() != null && p.shape().active() ? p.shape().weight() : 0));
        }
        for (Stream st : pickQ) {
            st.snd.mps().sched.pickQueued = false;
            if (st.tab.discarded() || !st.sendable()) continue;
            PathStreamState m = st.snd.mps();
            req.frames.add(new PickFrame(st.tab.id(), metrics.readyBytes(st), g != null ? g.tiers.getFirst().prio() : 0, g != null,
                    m.sched.bindPipe, m.sched.bindSet, st.tab.urgency()));
            req.keys.add(st);
        }
        pickQ.clear();
        return !req.frames.isEmpty();
    }

    /** Applies the answers when their generation is still current; an invalid answer is a miss. */
    void answerPick(PickRequest req, List<PickAnswer> answers, boolean failed) {
        for (int i = 0; i < req.keys.size(); i++) {
            Stream st = req.keys.get(i);
            if (st.tab.discarded() || st.snd.mps() == null) continue;
            PathStreamState m = st.snd.mps();
            m.sched.clearPick();
            m.sched.pickAsked = true;
            if (failed || req.gen != pickGen || i >= answers.size()) {
                stats.schedulerFallback();
                m.sched.pickGen = pickGen;
                continue;
            }
            PickAnswer a = answers.get(i);
            if (a.waitAnswer()) {
                // A Wait names no pipe: scheduler_answers counts only the answers that do.
                m.sched.pickSet = true;
                m.sched.pickWait = true;
                m.sched.pickGen = req.gen;
                m.sched.pickEpoch = inputEpoch;
            } else if (a.index() < 0 || a.index() >= req.pipes.size()) {
                stats.schedulerFallback();
                m.sched.pickGen = pickGen;
            } else {
                stats.schedulerAnswer();
                m.sched.pickSet = true;
                m.sched.pickPipe = req.pipes.get(a.index()).pipe();
                m.sched.pickLeft = a.bytes();
                m.sched.pickGen = req.gen;
            }
        }
        pipes.wakeWriters();
    }

    /** The decision reused at every write opportunity. */
    Decision decision() {
        return dec;
    }

    /** The custom policy's pick deadline; with apply an expired one wakes the writers. Time.NEVER when none. */
    long pickTimer(long now, boolean apply) {
        if (!pickTimer) return Time.NEVER;
        if (now < pickDeadline) return pickDeadline;
        if (apply) {
            pickTimer = false;
            pipes.wakeWriters();
        }
        return Time.NEVER;
    }

    /** The scheduler's per-attachment state. */
    static final class Slot {
        private double rrCur;
        private boolean placedSet;
        private long placedFrom;

        Slot() {
        }

        double rrCur() {
            return rrCur;
        }

        boolean placedSet() {
            return placedSet;
        }

        long placedFrom() {
            return placedFrom;
        }
    }
    /** A stream direction's own policy, or null for the session's. */
    private static CoreConfig.Policy ownPolicy(Stream st) {
        return st.snd.mps() == null ? null : st.snd.mps().sched.policy;
    }

    /** Sets a stream direction's own policy (null: the session's); the previous policy's state is discarded. */
    static void streamPolicy(Stream st, CoreConfig.Policy p) {
        StreamSlot m = st.snd.mp().sched;
        m.policy = p;
        m.clearBinding();
        m.clearPick();
        m.rrPipes.clear();
        m.rrVals.clear();
    }

    /** Overrides the session's redundancy for a stream direction: 1 on, -1 off, 0 the default. */
    static void streamRedundant(Stream st, int r) {
        st.snd.mp().sched.redundant = r;
    }


    /** A stream's scheduling state in the path layer: its own policy, affinity binding, round-robin credit and custom-pick answer. */
    static final class StreamSlot {
        /** The pipe the direction is bound to under affinity, or -1. */
        long bound() {
            return bindSet ? bindPipe : -1;
        }

        private CoreConfig.Policy policy;
        private int redundant;
        private long bindPipe;
        private long bindEpoch;
        private long moveTo;
        private boolean bindSet;
        private boolean provisional;
        private boolean moving;
        private final ArrayList<long[]> rrPipes = new ArrayList<>(0);
        private final ArrayList<double[]> rrVals = new ArrayList<>(0);
        private long pickPipe;
        private long pickLeft;
        private long pickGen;
        private long pickEpoch;
        private long pickAskedAt;
        private boolean pickSet;
        private boolean pickWait;
        private boolean pickAsked;
        private boolean pickQueued;

        StreamSlot() {
        }

        CoreConfig.Policy policy() {
            return policy;
        }

        int redundant() {
            return redundant;
        }

        long bindPipe() {
            return bindPipe;
        }

        long bindEpoch() {
            return bindEpoch;
        }

        long moveTo() {
            return moveTo;
        }

        boolean bindSet() {
            return bindSet;
        }

        boolean provisional() {
            return provisional;
        }

        boolean moving() {
            return moving;
        }

        ArrayList<long[]> rrPipes() {
            return rrPipes;
        }

        ArrayList<double[]> rrVals() {
            return rrVals;
        }

        long pickPipe() {
            return pickPipe;
        }

        long pickLeft() {
            return pickLeft;
        }

        long pickGen() {
            return pickGen;
        }

        long pickEpoch() {
            return pickEpoch;
        }

        long pickAskedAt() {
            return pickAskedAt;
        }

        boolean pickSet() {
            return pickSet;
        }

        boolean pickWait() {
            return pickWait;
        }

        boolean pickAsked() {
            return pickAsked;
        }

        boolean pickQueued() {
            return pickQueued;
        }

        void clearBinding() {
            bindSet = provisional = moving = false;
            bindPipe = bindEpoch = moveTo = 0;
        }

        void clearPick() {
            pickSet = pickWait = pickAsked = false;
            pickPipe = pickLeft = pickGen = pickEpoch = pickAskedAt = 0;
        }

        double rr(long pipe) {
            for (int i = 0; i < rrPipes.size(); i++) if (rrPipes.get(i)[0] == pipe) return rrVals.get(i)[0];
            return 0;
        }

        void rrAdd(long pipe, double dv) {
            for (int i = 0; i < rrPipes.size(); i++) {
                if (rrPipes.get(i)[0] == pipe) {
                    rrVals.get(i)[0] += dv;
                    return;
                }
            }
            rrPipes.add(new long[] {pipe});
            rrVals.add(new double[] {dv});
        }

        void rrZero(long pipe) {
            for (int i = 0; i < rrPipes.size(); i++) if (rrPipes.get(i)[0] == pipe) rrVals.get(i)[0] = 0;
        }
    }
}
