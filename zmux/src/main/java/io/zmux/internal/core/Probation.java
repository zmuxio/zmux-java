package io.zmux.internal.core;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Probation of new and recovered pipes, its history per pipe or fate group, revalidation and the busy mark that
 * paces standby probes (SCHEDULER.md 11).
 */
final class Probation {
    /** What Probation asks of the rest of the path layer and the session. */
    interface Port {
        /** See {@link PathHealth}. */
        void requestPing(Pipe p, long now);

        /** The usable capacity may have changed (quality floor). */
        void capacityChanged();

        /** See {@link Tiers}. */
        void checkTakeover();

        /** The session clock. */
        long now();
    }

    static final long PROBATION_FLOOR = Time.SECOND;
    static final long PROBATION_PROBE = 200 * Time.MILLISECOND;

    private final Port port;
    private final CoreConfig cfg;
    private final PipeTable pipes;
    private final PathMetrics metrics;
    private final HashMap<Long, ProbHistory> fateHist = new HashMap<>();
    private long busyUntil;

    Probation(CoreConfig cfg, PipeTable pipes, PathMetrics metrics, long now, Port port) {
        this.port = port;
        this.cfg = cfg;
        this.pipes = pipes;
        this.metrics = metrics;
        busyUntil = Time.add(now, 15 * Time.SECOND);
    }

    ProbHistory hist(Pipe p) {
        Attach a = p.mp();
        if (a.fate == 0) return a.prob.hist;
        long key = a.fate * 2 + (a.joinerLocal ? 1 : 0);
        return fateHist.computeIfAbsent(key, k -> new ProbHistory());
    }

    /** The probation base now; negative when probation is disabled. */
    long probationBase(Pipe p) {
        Attach a = p.mp();
        long opt = a.tier.probOpt() < 0 ? cfg.probation() : a.tier.probOpt();
        if (opt == 0) return -1;
        if (opt > 0) return opt;
        long srtt4 = Time.mul(a.rtt.smoothed(), 4);
        return hist(p).trouble() ? Math.max(PROBATION_FLOOR, srtt4) : srtt4;
    }

    long backoffBase(Pipe p) {
        Attach a = p.mp();
        long opt = a.tier.probOpt() < 0 ? cfg.probation() : a.tier.probOpt();
        return opt > 0 ? opt : Math.max(PROBATION_FLOOR, Time.mul(a.rtt.smoothed(), 4));
    }

    /** start + min(base x 2^k, max(base, 2 x U_max)), or Long.MIN_VALUE when not in probation. */
    long probationEnd(Pipe p) {
        Attach a = p.mp();
        if (!a.prob.probOn) return Long.MIN_VALUE;
        long base = probationBase(p);
        if (base < 0) return Long.MIN_VALUE;
        long l = base;
        for (int i = 0; i < a.prob.probK && l < Time.NEVER / 4; i++) l *= 2;
        ProbHistory h = hist(p);
        if (h.hasUMax()) l = Math.clamp(Time.mul(h.uMax(), 2), base, l); // l >= base: it only doubles
        return Time.add(a.prob.probStart, l);
    }

    /** The pipe joined, or returned from SUSPECT to ACTIVE. */
    void startProbation(Pipe p) {
        Attach a = p.mp();
        if (p.out() == null || p.id == 0 && !a.prob.wasSuspect) return;
        ProbHistory h = hist(p);
        int k = h.nextK();
        a.prob.probOn = probationBase(p) >= 0;
        a.prob.probStart = port.now();
        a.prob.probK = k;
        a.prob.probUpCounted = h.trouble();
        a.prob.probUpSince = port.now();
        a.prob.probPinged = false;
        a.prob.probPing = 0;
        if (a.prob.probOn && a.liveOut()) {
            a.prob.probPing = port.now();
            a.prob.probPinged = true;
            if (!a.health.needPing()) {
                port.requestPing(p, port.now());
            }
            pipes.wake(p);
        }
        port.capacityChanged();
    }

    void probationSuspect(Pipe p) {
        Attach a = p.mp();
        a.prob.wasSuspect = true;
        boolean inProb = a.prob.probOn;
        a.prob.probOn = false;
        boolean counted = a.prob.probUpCounted;
        long since = a.prob.probUpSince;
        a.prob.probUpCounted = false;
        if (a.health.draining() || a.health.resLimited()) return;
        hist(p).troubleAt(inProb, counted, since, port.now());
    }

    void probationFailed(Pipe p) {
        Attach a = p.mp();
        if (a.fate == 0 || a.health.draining() || a.health.resLimited()) return;
        boolean inProb = a.prob.probOn;
        boolean counted = a.prob.probUpCounted;
        long since = a.prob.probUpSince;
        a.prob.probUpCounted = false;
        hist(p).troubleAt(inProb, counted, since, port.now());
    }

    long probationTimers(long now, boolean apply) {
        long next = Time.NEVER;
        boolean changed = false;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a == null || p.out() == null || a.health.retired()) continue;
            long end = probationEnd(p);
            if (end != Long.MIN_VALUE) {
                if (now >= end) {
                    if (apply) {
                        a.prob.probOn = false;
                        changed = true;
                        ProbHistory h = hist(p);
                        h.end(end, backoffBase(p));
                        port.checkTakeover();
                        pipes.wakeWriters();
                    } else {
                        // Retain overdue work until the timer applies it; a
                        // first probation can have a zero-RTT deadline.
                        next = Math.min(next, end);
                    }
                } else {
                    next = Math.min(next, end);
                    long every = Math.max(a.rtt.smoothed(), PROBATION_PROBE);
                    long due = Time.add(a.prob.probPing, every);
                    if (!a.prob.probPinged || now >= due) {
                        if (!apply) {
                            if (a.liveOut()) next = Math.min(next, a.prob.probPinged ? due : now);
                        } else if (a.liveOut()) {
                            a.prob.probPing = now;
                            a.prob.probPinged = true;
                            if (!a.health.needPing()) {
                                port.requestPing(p, now);
                            }
                            pipes.wakeWriters();
                            next = Math.min(next, Time.add(now, every));
                        }
                    } else {
                        next = Math.min(next, due);
                    }
                }
            }
            if (a.fate == 0 && apply) a.prob.hist.decay(!(a.prob.probOn || a.health.out() == Attach.OUT_SUSPECT), now);
        }
        if (apply) {
            for (Iterator<java.util.Map.Entry<Long, ProbHistory>> it = fateHist.entrySet().iterator(); it.hasNext(); ) {
                var e = it.next();
                long group = e.getKey() / 2;
                boolean local = (e.getKey() & 1) == 1;
                ProbHistory h = e.getValue();
                boolean busy = false, used = false;
                for (Pipe p : pipes.all()) {
                    Attach a = p.mp();
                    if (a != null && a.fate == group && a.joinerLocal == local && a.health.out() != Attach.OUT_FAILED) {
                        used = true;
                        if (a.prob.probOn || a.health.out() == Attach.OUT_SUSPECT) busy = true;
                    }
                }
                h.decay(!busy, now);
                if (!used && h.k() == 0 && !h.pending() && !h.trouble() && !h.recent(now)) it.remove();
            }
        }
        if (changed) port.capacityChanged();
        return next;
    }

    /** Revalidation of one pipe (all false) or of every live outbound pipe (API.md 4.2); false when unknown. */
    boolean revalidate(long id, boolean all) {
        if (!all && pipes.get(id) == null) return false;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a == null || p.out() == null || !all && p.id != id || a.prob.reval) continue;
            if (a.health.out() != Attach.OUT_ACTIVE && a.health.out() != Attach.OUT_JOINING) continue;
            a.prob.reval = true;
            a.prob.revalPing = true;
            if (!a.health.needPing()) {
                port.requestPing(p, port.now());
            }
        }
        port.capacityChanged();
        port.checkTakeover();
        pipes.wakeWriters();
        return true;
    }

    void revalPingWritten(Pipe p) {
        p.mp().prob.revalPing = false;
        p.mp().prob.revalEnd = p.out().snd.so();
    }

    void revalAcked(Pipe p) {
        Attach a = p.mp();
        if (a.prob.reval && !a.prob.revalPing && p.out().snd.cum() >= a.prob.revalEnd) endRevalidation(p);
    }

    void endRevalidation(Pipe p) {
        Attach a = p.mp();
        if (a == null || !a.prob.reval) return;
        a.prob.reval = false;
        a.prob.revalPing = false;
        port.capacityChanged();
        port.checkTakeover();
        pipes.wakeWriters();
    }

    /** A pipe probed every standby_probe while the session is busy: standby, or of a less-preferred tier. */
    boolean backup(Pipe p) {
        if (p.mp().effStandby()) return true;
        long pr = p.mp().effPrio();
        long best = -1;
        boolean ok = false;
        for (Pipe q : pipes.all()) {
            if (q.mp() == null || q.out() == null || !metrics.pipeUsable(q, true)) continue;
            if (!q.mp().effStandby() && (!ok || Long.compareUnsigned(q.mp().effPrio(), best) < 0)) {
                best = q.mp().effPrio();
                ok = true;
            }
        }
        return ok && Long.compareUnsigned(pr, best) > 0;
    }

    /** A traffic frame; when the session was quiet, backup pipes unprobed for standby_probe get a PIPE_PING. */
    void markBusy() {
        boolean quiet = port.now() >= busyUntil;
        busyUntil = Time.add(port.now(), 15 * Time.SECOND);
        if (!quiet || cfg.standbyProbe() <= 0) return;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a == null || p.out() == null || a.health.needPing() || a.link.unacked() || a.health.out() != Attach.OUT_ACTIVE && a.health.out() != Attach.OUT_SUSPECT
                    || !backup(p)) {
                continue;
            }
            if (port.now() >= Time.add(Math.max(a.link.lastElicit(), a.link.liveAt()), cfg.standbyProbe())) {
                port.requestPing(p, port.now());
                pipes.wakeWriters();
            }
        }
    }

    /** Whether the session is busy: a traffic frame within the busy window, or the window after establishment. */
    boolean busy(long now) {
        return now < busyUntil;
    }

    /** A backup pipe while the session is busy (probed every standby_probe). */
    boolean busyBackup(Pipe p, long now) {
        return now < busyUntil && backup(p);
    }

    /** The probation option was turned off: an attachment on probation leaves it. */
    void dropProbation(Pipe p) {
        p.mp().prob.probOn = false;
    }

    /** The probation and revalidation state of one attachment (SCHEDULER.md). */
    static final class Slot {
        private long probStart;
        private int probK;
        private long probPing;
        private long probUpSince;
        private boolean probOn;
        private boolean probPinged;
        private boolean probUpCounted;
        private final ProbHistory hist = new ProbHistory();
        private boolean wasSuspect;
        private boolean reval;
        private boolean revalPing;
        private long revalEnd;

        Slot() {
        }

        long probStart() {
            return probStart;
        }

        int probK() {
            return probK;
        }

        long probPing() {
            return probPing;
        }

        long probUpSince() {
            return probUpSince;
        }

        boolean probOn() {
            return probOn;
        }

        boolean probPinged() {
            return probPinged;
        }

        boolean probUpCounted() {
            return probUpCounted;
        }

        ProbHistory hist() {
            return hist;
        }

        boolean wasSuspect() {
            return wasSuspect;
        }

        boolean reval() {
            return reval;
        }

        boolean revalPing() {
            return revalPing;
        }

        long revalEnd() {
            return revalEnd;
        }
    }
}
