package io.zmux.internal.core;

import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.TransportError;
import io.zmux.carrier.VarInt;
import io.zmux.internal.wire.WireException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tiers, usability and the tier gate, saturation reports, the standby takeover and PIPE_STATUS settings
 * (MULTIPATH.md 9, SCHEDULER.md 10).
 */
public final class Tiers {
    /** What Tiers asks of the rest of the path layer and the session. */
    interface Port {
        /** See {@link Probation}. */
        void dropProbation(Pipe p);

        /** The usable capacity may have changed (quality floor). */
        void capacityChanged();

        /** Queues an event (with the session's quality floor check). */
        void emit(Event e);

        /** The session clock. */
        long now();

        /** The session's current scheduling policy. */
        CoreConfig.Policy policy();

        /** See {@link PathScheduler}. */
        void pickChanged();

        /** See {@link Probation}. */
        long probationBase(Pipe p);

        /** See {@link Joins}. */
        boolean untaken(long id);
    }

    static final long SATURATION_EVERY = Time.SECOND;
    static final int STATUS_OWED_MAX = 8;

    private final Port port;
    private final CoreConfig cfg;
    private final ConnFlow conn;
    private final StreamScheduler sched;
    private final PipeTable pipes;
    private final PathMetrics metrics;
    private final StreamScheduler.Iter iter = new StreamScheduler.Iter();
    private final ArrayList<long[]> tierSince = new ArrayList<>(0);
    private final ArrayList<SatReport> satRep = new ArrayList<>(0);
    private final Gate gateBuf = new Gate();

    Tiers(CoreConfig cfg, ConnFlow conn, StreamScheduler sched, PipeTable pipes, PathMetrics metrics, Port port) {
        this.port = port;
        this.cfg = cfg;
        this.conn = conn;
        this.sched = sched;
        this.pipes = pipes;
        this.metrics = metrics;
    }

    static final class SatReport {
        final long prio;
        private boolean sat;
        private long at;

        SatReport(long prio, boolean sat, long at) {
            this.prio = prio;
            this.sat = sat;
            this.at = at;
        }
    }

    /** One usable tier of a gate. */
    static final class TierInfo {
        private long prio;
        long prio() { return prio; }
        private int lo;
        private int hi;
        private Pipe active;
        Pipe active() { return active; }
        private double q;
        private double r;
        private boolean sat;
        boolean sat() { return sat; }
    }

    /** The tier gate at one decision; reused scratch storage. */
    static final class Gate {
        private boolean standby;
        private boolean probOk;
        final ArrayList<Pipe> pipes = new ArrayList<>(4);
        final ArrayList<TierInfo> tiers = new ArrayList<>(2);
        private final ArrayList<TierInfo> spare = new ArrayList<>(2);
        private int last;
        int last() { return last; }
        private int fed;

        TierInfo newTier() {
            TierInfo t = spare.isEmpty() ? new TierInfo() : spare.removeLast();
            tiers.add(t);
            return t;
        }

        void clear() {
            pipes.clear();
            spare.addAll(tiers);
            tiers.clear();
        }

        int tierOf(Pipe p) {
            long pr = p.mp().effPrio();
            for (int i = 0; i < tiers.size(); i++) if (tiers.get(i).prio == pr) return i;
            return -1;
        }

        List<Pipe> tierPipes(int i) {
            TierInfo t = tiers.get(i);
            return pipes.subList(t.lo, t.hi);
        }
    }

    boolean gateUsable(Gate g, Pipe p) {
        return classUsable(g.standby, g.probOk, p);
    }

    /** Eligible: usable, in an eligible tier. */
    boolean admits(Gate g, Pipe p) {
        if (!gateUsable(g, p)) return false;
        int i = g.tierOf(p);
        return i >= 0 && i <= g.last;
    }

    boolean classUsable(boolean standby, boolean probOk, Pipe p) {
        return p.mp() != null && p.mp().effStandby() == standby && metrics.pipeUsable(p, probOk);
    }

    private boolean hasClass(boolean standby, boolean probOk) {
        for (Pipe p : pipes.all()) if (classUsable(standby, probOk, p)) return true;
        return false;
    }

    /** The usable class of this moment as [standby, probOk, ok] bits: 1, 2, 4. */
    int usableClass() {
        boolean settled = hasClass(false, false) || hasClass(true, false);
        boolean probOk = !settled;
        boolean standby = !hasClass(false, probOk);
        boolean ok = hasClass(standby, probOk);
        return (standby ? 1 : 0) | (probOk ? 2 : 0) | (ok ? 4 : 0);
    }

    /** Works out the gate now; null when no pipe is usable. */
    Gate buildGate() {
        int c = usableClass();
        if ((c & 4) == 0) return null;
        Gate g = gateBuf;
        g.clear();
        g.standby = (c & 1) != 0;
        g.probOk = (c & 2) != 0;
        for (Pipe p : pipes.all()) if (classUsable(g.standby, g.probOk, p)) g.pipes.add(p);
        g.pipes.sort((a, b) -> Long.compareUnsigned(a.mp().effPrio(), b.mp().effPrio()));
        boolean sa = port.policy() == CoreConfig.Policy.SESSION_AFFINITY;
        for (int lo = 0; lo < g.pipes.size(); ) {
            long pr = g.pipes.get(lo).mp().effPrio();
            Pipe active = g.pipes.get(lo);
            long aOwd = metrics.owd(active);
            int hi = lo + 1;
            while (hi < g.pipes.size() && g.pipes.get(hi).mp().effPrio() == pr) {
                long o = metrics.owd(g.pipes.get(hi));
                if (o < aOwd) {
                    active = g.pipes.get(hi);
                    aOwd = o;
                }
                hi++;
            }
            TierInfo t = g.newTier();
            t.prio = pr;
            t.lo = lo;
            t.hi = hi;
            t.active = active;
            t.q = t.r = 0;
            t.sat = false;
            if (sa) {
                t.q = metrics.queueBytes(active);
                t.r = metrics.rateOf(active);
            } else {
                for (int k = lo; k < hi; k++) {
                    t.q += metrics.queueBytes(g.pipes.get(k));
                    t.r += metrics.rateOf(g.pipes.get(k));
                }
            }
            lo = hi;
        }
        g.last = g.fed = 0;
        if (g.tiers.size() == 1) return g;
        waitingData();
        double offered = wdX;
        long d = cfg.overflowDelay();
        for (int i = 0; i < g.tiers.size(); i++) {
            TierInfo t = g.tiers.get(i);
            boolean drain = (t.q + offered) > t.r * d / 1e9;
            boolean aged = false;
            if (wdHas) {
                long since = wdOldest;
                if (i > 0) {
                    long e = eligibleSince(t.prio);
                    if (e == Long.MIN_VALUE) since = port.now();
                    else if (e > since) since = e;
                }
                aged = Time.sub(port.now(), since) >= d;
            }
            t.sat = drain || aged;
            g.last = i;
            if (offered > 0) g.fed = i;
            double room = t.r * d / 1e9 - t.q;
            if (room > 0) offered = Math.max(0, offered - room);
            if (!t.sat) break;
        }
        return g;
    }

    /** Whether an eligible pipe takes new data now (a tier eligible only by age takes data while no better pipe can). */
    boolean takes(Gate g, Pipe p) {
        if (!admits(g, p)) return false;
        int ti = g.tierOf(p);
        if (ti <= g.fed) return true;
        for (int i = 0; i < ti; i++) {
            if (port.policy() == CoreConfig.Policy.SESSION_AFFINITY) {
                if (metrics.open(g.tiers.get(i).active)) return false;
                continue;
            }
            for (Pipe q : g.tierPipes(i)) if (metrics.open(q)) return false;
        }
        return true;
    }

    long eligibleSince(long prio) {
        for (long[] t : tierSince) if (t[0] == prio) return t[1];
        return Long.MIN_VALUE;
    }

    /** Records since when each eligible tier is eligible and reports saturation changes. */
    void refreshTiers() {
        Gate g = buildGate();
        ArrayList<long[]> keep = new ArrayList<>(2);
        if (g != null) {
            for (int i = 0; i <= g.last; i++) {
                long pr = g.tiers.get(i).prio;
                long since = eligibleSince(pr);
                keep.add(new long[] {pr, since == Long.MIN_VALUE ? port.now() : since});
            }
        }
        tierSince.clear();
        tierSince.addAll(keep);
        reportSaturation(g);
        checkTakeover();
    }

    void reportSaturation(Gate g) {
        if (g != null) {
            for (int i = 0; i <= g.last; i++) {
                TierInfo t = g.tiers.get(i);
                boolean found = false;
                for (SatReport r : satRep) {
                    if (r.prio != t.prio) continue;
                    found = true;
                    if (r.sat != t.sat && port.now() >= Time.add(r.at, SATURATION_EVERY)) {
                        r.sat = t.sat;
                        r.at = port.now();
                        port.emit(Event.tierSaturation(t.prio, t.sat));
                    }
                }
                if (!found && t.sat) {
                    satRep.add(new SatReport(t.prio, true, port.now()));
                    port.emit(Event.tierSaturation(t.prio, true));
                }
            }
        }
        for (Iterator<SatReport> it = satRep.iterator(); it.hasNext(); ) {
            SatReport r = it.next();
            boolean seen = false;
            if (g != null) for (int i = 0; i <= g.last && i < g.tiers.size(); i++) if (g.tiers.get(i).prio == r.prio) { seen = true; break; }
            if (seen) continue;
            if (r.sat) port.emit(Event.tierSaturation(r.prio, false));
            it.remove();
        }
    }

    /** The next moment the age test can change a decision. */
    long tierDeadline() {
        if (tierSince.isEmpty() && satRep.isEmpty()) return Time.NEVER;
        long next = Time.NEVER;
        waitingData();
        if (wdHas) {
            long d = cfg.overflowDelay();
            next = Math.min(next, Time.add(wdOldest, d));
            for (long[] t : tierSince) next = Math.min(next, Time.add(Math.max(wdOldest, t[1]), d));
        }
        for (SatReport r : satRep) next = Math.min(next, Time.add(r.at, SATURATION_EVERY));
        return next <= port.now() ? Time.NEVER : next;
    }

    private long wdX;

    private long wdOldest;

    private boolean wdHas;

    /** X of the most-preferred tier and the age of its oldest byte (into wdX, wdOldest, wdHas). */
    void waitingData() {
        long fresh = 0, lost = 0, oldest = 0;
        boolean has = false;
        StreamScheduler.Iter it = iter.reset(sched);
        for (Stream st = it.next(); st != null; st = it.next()) {
            StreamSend.SendPart sp = st.snd;
            if (sp.mps() != null) for (int i = 0; i < sp.mps().ret.resend().size(); i++) lost += sp.mps().ret.resend().hi(i) - sp.mps().ret.resend().lo(i);
            long r = metrics.readyBytes(st);
            if (r == 0 && (sp.mps() == null || sp.mps().ret.resend().size() == 0)) continue;
            fresh += r;
            long w = sp.waitAt();
            if (sp.wwait() && sp.wwaitAt() < w) w = sp.wwaitAt();
            if (sp.creditAt() > w) w = sp.creditAt();
            if (conn.creditAt() > w) w = conn.creditAt();
            if (!has || w < oldest) {
                oldest = w;
                has = true;
            }
        }
        long room = conn.room();
        wdX = Math.min(fresh, room) + lost;
        wdOldest = oldest;
        wdHas = has;
    }

    /** Tier settings of an attachment; null fields stay. */
    public record PipeSettings(Long sendPriority, Boolean sendStandby, boolean clearSendPriority, boolean clearSendStandby, Long recvPriority, Boolean recvStandby, Long weight, Long probation) {
        public static Builder builder() { return new Builder(); }

        /** A builder holding these values. */
        public Builder toBuilder() {
            Builder b = new Builder();
            b.sendPriority = sendPriority;
            b.sendStandby = sendStandby;
            b.clearSendPriority = clearSendPriority;
            b.clearSendStandby = clearSendStandby;
            b.recvPriority = recvPriority;
            b.recvStandby = recvStandby;
            b.weight = weight;
            b.probation = probation;
            return b;
        }

        /** Collects the values of a {@link PipeSettings}; every setter returns the builder. */
        public static final class Builder {
            private Long sendPriority;
            private Boolean sendStandby;
            private boolean clearSendPriority;
            private boolean clearSendStandby;
            private Long recvPriority;
            private Boolean recvStandby;
            private Long weight;
            private Long probation;

            private Builder() { }

            public PipeSettings build() { return new PipeSettings(sendPriority, sendStandby, clearSendPriority, clearSendStandby, recvPriority, recvStandby, weight, probation); }

            public Builder sendPriority(Long v) { sendPriority = v; return this; }

            public Builder sendStandby(Boolean v) { sendStandby = v; return this; }

            public Builder clearSendPriority(boolean v) { clearSendPriority = v; return this; }

            public Builder clearSendStandby(boolean v) { clearSendStandby = v; return this; }

            public Builder recvPriority(Long v) { recvPriority = v; return this; }

            public Builder recvStandby(Boolean v) { recvStandby = v; return this; }

            public Builder weight(Long v) { weight = v; return this; }

            /** Negative: the session default; 0 disables. */
            public Builder probation(Long v) { probation = v; return this; }
        }
    }

    /** Applies tier settings (MULTIPATH.md 9); false when the pipe is unknown. */
    boolean setPipe(long id, PipeSettings o) {
        Pipe p = pipes.get(id);
        if (p == null || p.mp() == null || p.mp().health.retired()) return false;
        Attach a = p.mp();
        long bp = a.effPrio();
        boolean bs = a.effStandby();
        long bw = a.tier.weight;
        boolean wasProbation = a.prob.probOn();
        if (o.sendPriority != null) a.tier.sendPrio = o.sendPriority;
        else if (o.clearSendPriority) a.tier.sendPrio = null;
        if (o.sendStandby != null) a.tier.sendStandby = o.sendStandby;
        else if (o.clearSendStandby) a.tier.sendStandby = null;
        if (o.weight != null) a.tier.weight = o.weight;
        boolean req = false;
        if (o.recvPriority != null) {
            a.tier.recvPrio = o.recvPriority;
            req = true;
        }
        if (o.recvStandby != null) {
            a.tier.recvStandby = o.recvStandby;
            req = true;
        }
        if (o.probation != null) {
            a.tier.probOpt = o.probation;
            if (port.probationBase(p) < 0) port.dropProbation(p);
        }
        if (req) requestStatus(p);
        if (wasProbation != a.prob.probOn()) port.capacityChanged();
        if (a.effPrio() != bp || a.effStandby() != bs || a.tier.weight != bw) {
            emitPriority(p);
            port.pickChanged();
        }
        checkTakeover();
        pipes.wakeWriters();
        return true;
    }

    void emitPriority(Pipe p) {
        if (p.out() == null) return;
        port.emit(Event.pipePriority(p.id, p.carrier(), p.mp().effPrio(), p.mp().effStandby(), p.mp().tier.weight));
    }

    /** Queues the current request for the peer's outbound direction as a new PIPE_STATUS. */
    void requestStatus(Pipe p) {
        Attach a = p.mp();
        if (p.in() == null) return;
        a.tier.statusSeq++;
        if (a.tier.statusQ.size() >= STATUS_OWED_MAX) a.tier.statusQ.removeFirst();
        a.tier.statusQ.add(new long[] {a.tier.statusSeq, a.tier.recvStandby && !a.tier.takeover ? 1 : 0, a.tier.recvPrio});
        pipes.wakeWriters();
    }

    /** Re-queues the latest request after its frame was lost. */
    void statusLost(Pipe p, long seq) {
        Attach a = p.mp();
        if (seq != a.tier.statusSeq) return;
        for (long[] r : a.tier.statusQ) if (r[0] == seq) return;
        a.tier.statusQ.add(new long[] {seq, a.tier.recvStandby && !a.tier.takeover ? 1 : 0, a.tier.recvPrio});
        pipes.wakeWriters();
    }

    /** The peer's request for the local outbound direction (MULTIPATH.md 9). */
    void onPipeStatus(io.zmux.internal.wire.Frame f) throws WireException {
        Pipe q = pipes.get(f.pipeId);
        if (q == null) {
            if (port.untaken(f.pipeId)) {
                throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "PIPE_STATUS for an untaken pipe ID", "pipe", f.pipeId);
            }
            return;
        }
        Attach a = q.mp();
        if (a == null || a.health.retired() || q.out() == null || f.seq <= a.tier.peerSeq) return;
        long bp = a.effPrio();
        boolean bs = a.effStandby();
        a.tier.peerSeq = f.seq;
        a.tier.peerPrio = f.priority;
        a.tier.peerStandby = f.status == 1;
        if (a.effPrio() != bp || a.effStandby() != bs) emitPriority(q);
        checkTakeover();
        pipes.wakeWriters();
    }

    /** The standby takeover (MULTIPATH.md 9). */
    void checkTakeover() {
        int c = usableClass();
        boolean ok = (c & 4) != 0, standby = (c & 1) != 0, probOk = (c & 2) != 0;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a == null || a.health.retired()) continue;
            boolean want = a.tier.recvStandby && a.effStandby() && ok && standby && metrics.pipeUsable(p, probOk);
            if (want != a.tier.takeover) {
                a.tier.takeover = want;
                requestStatus(p);
            }
        }
    }

    boolean writeStatus(RecordWriter w, Pipe q) {
        Attach qa = q.mp();
        while (!qa.tier.statusQ.isEmpty()) {
            long[] r = qa.tier.statusQ.getFirst();
            long id = q.id, seq = r[0], st = r[1], prio = r[2];
            int n = 4 + VarInt.length(id) + VarInt.length(seq) + VarInt.length(st) + VarInt.length(prio);
            if (!w.frame(n, true, bb -> FrameEncoder.pipeStatus(bb, id, seq, st, prio))) return false;
            w.logFrame(new FrameDesc(FrameDesc.Kind.STATUS, false, false, id, 0, 0, seq));
            qa.tier.statusQ.removeFirst();
        }
        return true;
    }

    /** Whether q's PIPE_STATUS may go out: the attachment is ACTIVE. */
    static boolean statusReady(Pipe q) {
        Attach qa = q.mp();
        return qa != null && !qa.tier.statusQ.isEmpty() && !qa.health.retired() && q.in() != null && qa.health.in() == Attach.IN_OPEN
                && qa.health.out() != Attach.OUT_JOINING;
    }

    /** The number of tiers currently eligible. */
    int eligibleTiers() {
        return tierSince.size();
    }

    /** The tier options of a local join. */
    static void initLocal(Attach a, Joins.JoinOptions o) {
        Slot t = a.tier;
        if (o.probation() != null) t.probOpt = o.probation();
        t.weight = o.weight();
        t.sendPrio = o.sendPriority();
        t.sendStandby = o.sendStandby();
        t.recvPrio = o.recvPriority();
        t.recvStandby = o.recvStandby();
    }

    /** The tier the peer asked for in its join. */
    static void initPeer(Attach a, long prio, boolean standby) {
        a.tier.peerPrio = prio;
        a.tier.peerStandby = standby;
    }

    /** The tier options and PIPE_STATUS state of one attachment (MULTIPATH.md 9). */
    static final class Slot {
        private Long sendPrio;
        private Boolean sendStandby;
        private long peerPrio;
        private boolean peerStandby;
        private long peerSeq;
        private long recvPrio;
        private boolean recvStandby;
        private long statusSeq;
        private final ArrayList<long[]> statusQ = new ArrayList<>(0);
        private long weight = 1;
        private boolean takeover;
        private long probOpt = -1;

        Slot() {
        }

        Long sendPrio() {
            return sendPrio;
        }

        Boolean sendStandby() {
            return sendStandby;
        }

        long peerPrio() {
            return peerPrio;
        }

        boolean peerStandby() {
            return peerStandby;
        }

        long peerSeq() {
            return peerSeq;
        }

        long recvPrio() {
            return recvPrio;
        }

        boolean recvStandby() {
            return recvStandby;
        }

        long statusSeq() {
            return statusSeq;
        }

        ArrayList<long[]> statusQ() {
            return statusQ;
        }

        long weight() {
            return weight;
        }

        boolean takeover() {
            return takeover;
        }

        long probOpt() {
            return probOpt;
        }
    }
}
