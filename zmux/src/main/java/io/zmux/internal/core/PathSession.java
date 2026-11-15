package io.zmux.internal.core;

import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.FrameType;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.VarInt;
import io.zmux.internal.wire.WireException;
import java.util.ArrayList;
import java.util.List;

/**
 * The path layer of a multipath session: the composition root of its
 * components. It exists only once multipath is in use; every pipe of such a session carries an {@link Attach}.
 * Single-pipe sessions never allocate any of it. It writes the path frames of a
 * batch, keeps the control order and dispatches {@link PathLayer} calls to {@link Joins}, {@link PathHealth},
 * {@link Probation}, {@link Tiers}, {@link PathScheduler}, {@link Retention} and {@link Resume}, which reach each
 * other only through their ports, implemented here. Owned by the session's thread like the session itself.
 */
final class PathSession implements PathLayer {
    /** What the path layer asks of the session. */
    interface Port {
        long now();

        /** See {@link Joins.Port#firstBatch}. */
        byte[] firstBatch(int limit);

        /** See {@link Joins.Port#joinPadding}. */
        void joinPadding(long n);

        /** See {@link Joins.Port#applyBatch}. */
        void applyBatch(byte[] frames);

        /** Queues an event (with the session's quality floor check). */
        void emit(Event e);

        void failLocal(WireException e);

        void closeWith(SessionError e, Lifecycle.CloseFrame fr);

        void checkLingerDone();

        boolean isClosing();

        long lingerTimeout();

        long sendBufferLimit();

        /** The usable capacity may have changed (quality floor). */
        void capacityChanged();

        SessionState state();

        /** The session enters SUSPENDED with the grace deadline. */
        void suspended(long graceDeadline);

        /** The session leaves SUSPENDED. */
        void resumed();

        /** The session's current scheduling policy. */
        CoreConfig.Policy policy();
    }

    private final Port port;
    private final CoreConfig cfg;
    private final Features feat;
    private final PipeTable pipes;
    private final AckEngine ack;
    private final ControlQueue ctl;
    private final Transmitter tx;
    final PathMetrics metrics;
    final Joins joins;
    final PathHealth health;
    final Probation probation;
    final Tiers tiers;
    final PathScheduler scheduler;
    final Retention retention;
    final Resume resume;
    private long redundantAt;
    private boolean ackChanged;
    private final Outbox outbox;
    /** Operations waiting for the peer to attach a pipe ({@link #peerAttached}). */
    private final WaitList attachW = new WaitList();
    private final WaitList epochW = new WaitList();

    /** Allocates the path layer at establishment. */
    PathSession(CoreConfig cfg, TransportParams peer, boolean initiator, Features feat, SessionStats stats, PipeTable pipes, Lanes lanes,
              StreamsView streams, StreamScheduler sched, AckEngine ack, ControlQueue ctl, ConnFlow conn, Transmitter tx, KeepAlive ka,
              Lifecycle life, Outbox outbox, long now, Port port, byte[] exporter, byte[] psk) {
        this.port = port;
        this.cfg = cfg;
        this.feat = feat;
        this.pipes = pipes;
        this.ack = ack;
        this.ctl = ctl;
        this.tx = tx;
        this.outbox = outbox;
        Wiring w = new Wiring();
        metrics = new PathMetrics(pipes, ack, feat);
        joins = new Joins(cfg, stats, peer, initiator, pipes, outbox, w, exporter, psk);
        health = new PathHealth(cfg, stats, feat, pipes, streams, ack, ka, outbox, metrics, w);
        probation = new Probation(cfg, pipes, metrics, now, w);
        tiers = new Tiers(cfg, conn, sched, pipes, metrics, w);
        scheduler = new PathScheduler(cfg, stats, conn, sched, tx, lanes, pipes, streams, metrics, w);
        retention = new Retention(cfg, stats, ack, conn, ctl, sched, lanes, life, pipes, streams, metrics, w);
        resume = new Resume(cfg, peer, pipes, w);
        for (Pipe pp : pipes.all()) {
            Attach a = new Attach(initiator, 2, 0, 0, 0, -1, Double.NaN, Attach.OUT_ACTIVE, Attach.IN_OPEN, false, now);
            if (cfg.firstPipeMode() != null) a.mode(cfg.firstPipeMode());
            joins.initialRtt(a, pp.id);
            pp.attach(a);
        }
    }

    /** The states (out, in) of a pipe. */
    @Override
    public PipeInfo.States pipeState(Pipe p) {
        Attach a = p.mp();
        if (a == null) return PipeInfo.States.SINGLE;
        if (a.health.retired()) return PipeInfo.States.RETIRED;
        return new PipeInfo.States(Attach.OUT_STATES[a.health.out()], Attach.IN_STATES[a.health.in()]);
    }

    /** One PIPE_ACK entry of a multipath session. */
    void ackEntry(long pipeId, long lane, long position, long delay) throws WireException {
        Pipe p = pipes.get(pipeId);
        if (p == null) {
            // The bootstrap pipe always existed: once retired, its entries are ignored.
            if (pipeId == 0) return;
            if (joins.untaken(pipeId)) {
                throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "PIPE_ACK for an untaken pipe ID", "pipe", pipeId);
            }
            if (!joins.ownParity(pipeId) && position > 0 && !joins.idRetired(pipeId)) throw ackBeyondSent(pipeId, position);
            return;
        }
        Attach a = p.mp();
        if (a.health.retired() || position == 0) return;
        Pipe.OutLane ol = p.out() != null ? p.out().lane(lane) : null;
        if (ol == null) throw ackBeyondSent(pipeId, position);
        if (ol.st.refused()) return;
        if (position > ol.tx.pos()) throw ackBeyondSent(pipeId, position);
        if (position <= ol.ack.acked()) return;
        ack.acked(ol, position);
        joins.laneAcked(ol, position);
        if (!attachW.isEmpty()) attachW.wakeEvery(outbox);
        health.laneAcked(p, ol);
        retention.ackLog(ol, position);
        ack.advanced(p, ol, position, delay, port.now());
    }

    WireException ackBeyondSent(long pipe, long pos) {
        long code = feat.resume() ? TransportError.RESUME_FAILED : TransportError.PROTOCOL_VIOLATION;
        return WireException.withDetails(code, "PIPE_ACK position beyond the bytes sent", "pipe", pipe, "pos", pos);
    }

    /** The pipe's cumulative acknowledgement point moved. */
    void advanced(Pipe p, Pipe.OutLane ol) {
        Attach a = p.mp();
        AckEngine.Send ps = p.out().snd;
        a.link.lastAdvance = port.now();
        if (ps.samples().isEmpty()) {
            a.link.unacked = false;
            a.link.oldestQueue = 0;
        } else {
            a.link.unacked = true;
            a.link.oldestAt = ps.samples().getFirst().sent();
            a.link.oldestQueue = ps.samples().getFirst().queue();
        }
        switch (a.health.out()) {
            case Attach.OUT_SUSPECT -> {
                if (health.writeStall(p) == PathHealth.NO_STALL) {
                    health.activated(p);
                    probation.startProbation(p);
                    emitPipeState(p);
                    tiers.checkTakeover();
                    resume.checkResumed();
                }
            }
            default -> { }
        }
        probation.revalAcked(p);
        retention.penaltyAcked(p);
        laneAdvanced(p);
    }

    /** Any lane of the pipe was acknowledged further. */
    void laneAdvanced(Pipe p) {
        if (p.mp().health.out() == Attach.OUT_DRAINING) health.checkDrain(p);
        pipes.wakeWriters();
    }

    /** A verified Reply with decision 0: the attachment is ACTIVE and its clocks start. */
    private void joinProven(Pipe p) {
        Attach a = p.mp();
        long now = port.now();
        a.link.lastAdvance = a.link.lastRecord = a.link.liveAt = now;
        if (p.in() != null) health.opened(p);
        if (p.out() != null) {
            health.activated(p);
            health.requestPing(p, now);
            resume.startReinjectHold(p);
        }
        joinConfirmed(p);
        if (p.out() != null) pipes.wake(p);
    }

    /** A verified Reply with decision 1: retired at once, no PIPE_ABANDON. */
    private void declined(Pipe p) {
        health.declined(p);
    }

    /** A local join became ACTIVE. */
    void joinConfirmed(Pipe p) {
        Attach a = p.mp();
        a.link.liveAt = port.now();
        joins.confirmed();
        joins.epochPipeAdded(a.mode());
        probation.startProbation(p);
        if (a.tier.statusQ().size() > 1) {
            long[] last = a.tier.statusQ().getLast();
            a.tier.statusQ().clear();
            a.tier.statusQ().add(last);
        }
        tiers.checkTakeover();
        emitPipeState(p);
        resume.checkResumed();
    }

    /** PIPE_PING, PIPE_ABANDON and PIPE_STATUS. */
    @Override
    public void onPathFrame(Pipe p, io.zmux.internal.wire.Frame f) throws WireException {
        switch (f.kind) {
            case PIPE_PING -> {
                // Answered on its own pipe pair; the record decides whether the primary pipe answers too.
                if (pairAnswers(p)) {
                    Link l = p.mp().link;
                    if (!l.ackPair) l.ackPairAt = port.now();
                    l.ackPair = true;
                }
                pipes.wakeWriters();
            }
            case PIPE_ABANDON -> {
                Pipe q = pipes.get(f.pipeId);
                if (q == null) {
                    joins.abandonUnknown(f.pipeId);
                    return;
                }
                health.peerAbandon(q, f.code);
            }
            case PIPE_STATUS -> tiers.onPipeStatus(f);
            case SESSION_EPOCH -> joins.onSessionEpoch(p, f);
            default -> { }
        }
    }

    /** Whether the outbound direction of p's pipe pair takes the answer to a PIPE_PING: it exists and is neither suspect nor failed. */
    boolean pairAnswers(Pipe p) {
        if (p.mp() == null || p.out() == null || p.out().failed()) return false;
        int o = p.mp().health.out();
        return o == Attach.OUT_ACTIVE || o == Attach.OUT_JOINING || o == Attach.OUT_DRAINING;
    }

    /** Whether the session is quiet: no traffic frame within the busy window. */
    boolean quiet() {
        return !probation.busy(port.now());
    }

    /**
     * Whether a record of p whose only ack-eliciting frame is a PIPE_PING is answered on p's pipe pair alone: in a
     * quiet session when that direction takes it; otherwise the primary pipe answers too.
     */
    boolean pingAnsweredOnPair(Pipe p) {
        return quiet() && pairAnswers(p);
    }

    /**
     * Whether p is unproven (MULTIPATH.md Section 6.4), so that an acknowledgement on it carries a PIPE_PING: for
     * R_p = max(1 s, T_suspect_p) no ack-eliciting byte was sent on it, its acknowledged position did not advance and
     * it was not made live. An advance proves the direction as a sent byte does, so an answer that acknowledges p's
     * own PIPE_PING carries none, and PIPE_PING frames never answer each other however long an exchange takes.
     */
    private boolean idleForReturnPing(Pipe p, long now) {
        Attach a = p.mp();
        long since = Math.max(Math.max(a.link.lastElicit, a.link.liveAt), a.link.lastAdvance);
        return !a.health.needPing() && now >= Time.add(since, Math.max(Time.SECOND, health.tSuspect(p)));
    }

    /**
     * When a shaping delay of p's next record ends because the record carries a pending PIPE_ACK: zmux_max_ack_delay
     * after the first ack-eliciting byte it reports (MULTIPATH.md Section 6.3). The first record's snapshot and the
     * primary acknowledgement report the session's pending bytes; the answer to a PIPE_PING on p's pipe pair reports
     * that PIPE_PING too, which a quiet session does not count toward the primary acknowledgement.
     * {@link Time#NEVER} when p carries none.
     */
    long ackRelease(Pipe p) {
        Attach a = p.mp();
        long t = ack.release();
        if (a == null) return t;
        if (t != Time.NEVER && !a.link.firstRecord && !a.link.ackPair) {
            Pipe prim = primary();
            if (p != prim && prim != null) t = Time.NEVER;
        }
        return a.link.ackPair ? Math.min(t, Time.add(a.link.ackPairAt, ack.delay())) : t;
    }

    void emitPipeState(Pipe p) {
        health.stateChanged();
        scheduler.pickChanged();
        PipeInfo.States st = pipeState(p);
        if (p.out() != null) port.emit(Event.pipeState(p.id, p.carrier(), Action.DIR_OUT, st.out()));
        if (p.in() != null) port.emit(Event.pipeState(p.id, p.carrier(), Action.DIR_IN, st.in()));
        reportPipeEvents(p, st);
    }

    /**
     * The application's pipe events: an outbound direction is added when
     * first seen, then joined, suspect, recovered (active after suspect), failed or closed as
     * it settles in each state (joining and draining have no event of their own); an inbound
     * direction is closed once it ended.
     */
    private void reportPipeEvents(Pipe p, PipeInfo.States st) {
        Link l = p.mp().link;
        if (p.out() != null && st.out() != PipeInfo.Out.NONE && st.out() != PipeInfo.Out.RETIRED && st.out() != l.reportedOut) {
            PipeInfo.Out prev = l.reportedOut;
            l.reportedOut = st.out();
            if (prev == PipeInfo.Out.NONE) port.emit(Event.pipe(Event.Kind.PIPE_ADDED, p.id, p.carrier(), Action.DIR_OUT));
            Event.Kind k = switch (st.out()) {
                case ACTIVE -> prev == PipeInfo.Out.SUSPECT ? Event.Kind.PIPE_RECOVERED : Event.Kind.PIPE_JOINED;
                case SUSPECT -> Event.Kind.PIPE_SUSPECT;
                case FAILED -> Event.Kind.PIPE_FAILED;
                case CLOSED -> Event.Kind.PIPE_CLOSED;
                default -> null;
            };
            if (k != null) port.emit(Event.pipe(k, p.id, p.carrier(), Action.DIR_OUT));
        }
        if (p.in() != null && st.in() == PipeInfo.In.ENDED && !l.reportedInEnded) {
            l.reportedInEnded = true;
            port.emit(Event.pipe(Event.Kind.PIPE_CLOSED, p.id, p.carrier(), Action.DIR_IN));
        }
    }

    private static int controlRank(Attach a) {
        return switch (a.health.out()) {
            case Attach.OUT_ACTIVE -> 0;
            case Attach.OUT_JOINING -> 1;
            case Attach.OUT_SUSPECT -> 2;
            case Attach.OUT_DRAINING -> a.health.drainSuspect() ? 4 : 3;
            default -> 4;
        };
    }

    private int compareControl(Pipe x, Pipe y) {
        Attach a = x.mp(), b = y.mp();
        int c = Integer.compare(controlRank(a), controlRank(b));
        if (c != 0) return c;
        c = Boolean.compare(a.effStandby(), b.effStandby());
        if (c != 0) return c;
        c = Long.compareUnsigned(a.effPrio(), b.effPrio());
        if (c != 0) return c;
        c = Long.compare(metrics.owd(x), metrics.owd(y));
        if (c != 0) return c;
        return Long.compare(x.id, y.id);
    }

    private final ArrayList<Pipe> orderBuf = new ArrayList<>(4);

    /** The live outbound pipes in the control order. */
    List<Pipe> controlOrder() {
        ArrayList<Pipe> d = orderBuf;
        d.clear();
        for (Pipe p : pipes.all()) {
            if (p.mp() != null && p.out() != null && !p.out().failed() && p.mp().liveOut() && !p.mp().handshaking() && !p.mp().health.retired()) {
                if (p.mp().health.out() == Attach.OUT_DRAINING && p.mp().health.abandonSent() && p.mp().health.abandonRecv()) continue;
                d.add(p);
            }
        }
        d.sort(this::compareControl);
        return d;
    }

    Pipe primary() {
        List<Pipe> o = controlOrder();
        return o.isEmpty() ? null : o.getFirst();
    }

    Pipe second() {
        List<Pipe> o = controlOrder();
        return o.size() > 1 ? o.get(1) : null;
    }

    /** Builds a batch for one outbound lane of a multipath session. */
    @Override
    public void write(RecordWriter w, Pipe p, Pipe.OutLane ol) {
        w.retainFor(p);
        Attach a = p.mp();
        if (a.handshaking()) return;
        if (ol.lane == 0) {
            tiers.refreshTiers();
            joins.epochProgress();
            pipeFrames(w, p);
            Pipe prim = primary();
            if (prim == p || prim == null) tx.writeControl(w, p);
            else tx.writeDatagrams(w, p);
        }
        PathScheduler.Decision d = scheduler.decision();
        if (ol.lane == 0) scheduler.refreshBindings(tiers.buildGate());
        scheduler.newDecision(d, p);
        if (ol.lane == 0) {
            retention.writeCopies(d, w, a.ret.copyQ());
            retention.opportunistic(d, w);
        } else if (ol == p.mp().ret.copyLane()) {
            retention.writeCopies(d, w, a.ret.selfCopyQ());
        }
        tx.writeData(w, p, ol);
    }

    /** PIPE_ACK, PIPE_PING, PIPE_ABANDON and PIPE_STATUS for the pipe. */
    void pipeFrames(RecordWriter w, Pipe p) {
        Attach a = p.mp();
        boolean first = a.link.firstRecord;
        // A delayed first record is delayed whole: its snapshot and PIPE_PING wait with it.
        if (first && w.room() <= 0) return;
        Pipe prim = primary(), second = second();
        boolean snapped = true;
        if (ack != null) {
            long now = port.now();
            boolean onPrim = p == prim || prim == null;
            if (onPrim && (ack.due(now) || (ack.unreported() && ctl.creditPending()))) {
                // The primary acknowledgement; in a busy session with return-pipe liveness.
                if ((!quiet() || a.link.ackPair) && idleForReturnPing(p, now)) health.requestPing(p, now);
                snapped = ack.snapshot(w, now);
                ackChanged = true;
                redundantAt = Time.add(now, 50 * Time.MILLISECOND);
            } else if (first || a.link.ackPair) {
                // The first record's snapshot, or the answer to a PIPE_PING on its pipe pair; the answer carries a
                // PIPE_PING of its own once the pipe has been idle for R_p.
                if (a.link.ackPair && idleForReturnPing(p, now)) health.requestPing(p, now);
                snapped = ack.snapshot(w, now, false);
            } else if (ackChanged && p == second && now >= redundantAt) {
                if (!quiet() && idleForReturnPing(p, now)) health.requestPing(p, now);
                ack.snapshot(w, now);
                ackChanged = false;
                retention.writeCreditCopies(w);
            }
            // A snapshot the record refused (a planned delay holds it, or it did not fit) stays owed: the answer to a
            // PIPE_PING on the pipe pair goes with the record that takes it.
            if (!snapped) return;
            a.link.ackPair = false;
        }
        a.link.firstRecord = false;
        if (a.health.needPing() && a.health.out() != Attach.OUT_FAILED && health.pingAllowed(port.now()) && health.oldestPingWaiter(p)) {
            if (w.frame(VarInt.length(FrameType.PIPE_PING), true, FrameEncoder::pipePing)) {
                w.pingWritten(health.pingWritten(p, port.now()));
                if (a.prob.revalPing()) w.revalPingWritten();
            }
        }
        for (Pipe q : pipes.all()) {
            Attach qa = q.mp();
            if (qa == null || !qa.health.abandonQueued() || qa.health.retired()) continue;
            if (!abandonGoesOn(q, p, prim)) continue;
            long code = qa.health.abandonCode(), id = q.id;
            int n = VarInt.length(FrameType.PIPE_ABANDON) + VarInt.length(id) + VarInt.length(code);
            if (!w.frame(n, true, bb -> FrameEncoder.pipeAbandon(bb, id, code))) return;
            w.logFrame(FrameDesc.id(FrameDesc.Kind.ABANDON, id));
            if (health.abandonWritten(q)) w.abandonsSent.add(q);
        }
        if (!joins.writeOwedAbandons(w)) return;
        for (Pipe q : pipes.all()) if (Tiers.statusReady(q, p) && !tiers.writeStatus(w, q)) return;
        joins.writeEpoch(w, p, epochRepeat(p));
    }

    /** How often SESSION_EPOCH repeats on p: its probe interval, or 15 s where its probes are off. */
    private long epochRepeat(Pipe p) {
        long k = health.probeInterval(p);
        return k > 0 ? k : PathHealth.UNPAIRED_PROBE;
    }

    /** Whether write() would produce bytes now on the lane (W8). */
    @Override
    public boolean wantsWrite(Pipe p, Pipe.OutLane ol) {
        Attach a = p.mp();
        if (a.health.retired() || a.health.out() == Attach.OUT_FAILED || a.health.out() == Attach.OUT_CLOSED || a.handshaking()) return false;
        if (ol.lane == 0) {
            if ((a.link.firstRecord && ack != null && hasEntries()) || a.link.ackPair) {
                return true;
            }
            if (a.health.needPing() && health.pingAllowed(port.now()) && health.oldestPingWaiter(p)) return true;
            Pipe prim = primary();
            if (ack != null && ack.due(port.now()) && (p == prim || prim == null)) return true;
            if (ackChanged && p == second() && port.now() >= redundantAt) return true;
            for (Pipe q : pipes.all()) {
                if (q.mp() != null && !q.mp().health.retired() && ((q.mp().health.abandonQueued() && abandonGoesOn(q, p, prim)) || Tiers.statusReady(q, p))) return true;
            }
            if (joins.owesAbandons() || joins.epochWanted(p)) return true;
            if ((p == prim || prim == null) && tx.controlPending()) return true;
            if (tx.datagramsPending(p, ol)) return true;
        }
        if (ol.lane == 0 && !a.ret.copyQ().isEmpty() && metrics.open(p)) return true;
        if (ol == p.mp().ret.copyLane() && !a.ret.selfCopyQ().isEmpty() && metrics.laneOpen(p, ol)) return true;
        return tx.dataPendingOn(p, ol) && scheduler.anyStreamOn(p, ol) && (pipes.shaping() == null || !Shaping.held(p, port.now()));
    }

    /** Accounts a produced batch on a pipe of a multipath session. */
    void afterBatch(RecordWriter w, Pipe p, Pipe.OutLane ol, long start, long queue) {
        Attach a = p.mp();
        retention.retainBatch(w, ol, start);
        if (w.anyElicit()) {
            if (!a.link.unacked) {
                a.link.unacked = true;
                a.link.oldestAt = port.now();
                a.link.oldestQueue = queue;
            }
            a.link.lastElicit = port.now();
        }
        for (Pipe q : w.abandonsSent) health.abandonHanded(q);
        w.abandonsSent.clear();
        if (w.pingSent()) health.pingRecorded(p, ol.tx.pos());
        if (w.revalPing()) probation.revalPingWritten(p);
        if (w.epochAnswer() != 0) joins.answerSent(ol, ol.tx.pos(), w.epochAnswer());
        if (w.activity()) probation.markBusy();
    }

    boolean abandonGoesOn(Pipe q, Pipe p, Pipe prim) {
        if (q.mp().selfGraceful(q.out() != null)) return q == p;
        return q != p || prim == p || prim == null;
    }

    boolean hasEntries() {
        for (Pipe p : pipes.all()) {
            if (p.in() == null || (p.mp() != null && p.mp().health.retired())) continue;
            for (Pipe.InLane il : p.in().lanes) if (il.rx.pos() > 0) return true;
        }
        return false;
    }

    /** A record arrived on p: attachment liveness and the busy mark. */
    void recorded(Pipe p, Pipe.InLane il, boolean traffic) {
        Attach a = p.mp();
        if (traffic) probation.markBusy();
        long now = port.now();
        long gap = Time.sub(now, a.link.lastRecord);
        if (il.rx.recs() > 1 && gap > a.link.maxGap) a.link.maxGap = gap;
        a.link.lastRecord = now;
        if (il.rx.recs() == 1 && !attachW.isEmpty()) attachW.wakeEvery(outbox);
    }

    /** Evaluates every path-layer deadline; with apply it acts on the due ones. Returns the earliest pending deadline. */
    long timers(long now, boolean apply) {
        long n = health.pipeTimers(now, apply);
        n = Math.min(n, joins.backoffTimer(now, apply));
        n = Math.min(n, joins.epochTimers(now, apply));
        if (ackChanged) {
            if (now < redundantAt) n = Math.min(n, redundantAt);
            else if (apply) pipes.wakeWriters();
        }
        n = Math.min(n, health.pingTimer(now, apply));
        n = Math.min(n, scheduler.pickTimer(now, apply));
        n = Math.min(n, probation.probationTimers(now, apply));
        if (apply) {
            tiers.refreshTiers();
            if (tiers.eligibleTiers() > 1) pipes.wakeWriters();
        }
        return Math.min(n, tiers.tierDeadline());
    }

    /** A pipe became SUSPECT: a changed acknowledgement waiting for its redundant copy goes now. */
    void suspectedRedundantAck() {
        if (primary() != null && ackChanged) redundantAt = port.now();
    }

    // ---- PathLayer ----

    @Override public boolean acksInControl() { return false; }
    @Override public void maxDataSent(boolean peerWaiting) { retention.maxDataSent(peerWaiting); }
    @Override public void maxStreamDataSent(Stream st, boolean peerWaiting) { retention.maxStreamDataSent(st, peerWaiting); }
    @Override public void creditBlocked(Stream st) { retention.noteBlocked(st); }
    @Override public boolean urgentLane(Stream st) { return scheduler.redundant(st); }
    @Override public long sendBuffer(long configured) { return Math.max(configured, retention.retransmitBudget()); }
    @Override public long retainRoom() { return retention.retainRoom(); }
    @Override public void batchCarried(Stream st, long off, long n) { retention.lost(FrameDesc.stream(st.tab.id(), off, n, false)); }
    @Override public void retain(long n) { retention.retain(n); }
    @Override public void unretain(long n) { retention.unretain(n); }
    @Override public boolean retainsSent() { return true; }
    @Override public long retained() { return retention.retained(); }

    @Override
    public void onPipeAck(io.zmux.internal.wire.Frame.AckEntries it) throws WireException {
        resume.endReinjectHolds();
        while (it.next()) ackEntry(it.pipeId, it.lane, it.position, it.delay);
    }

    @Override
    public void inboundEnded(Pipe p, boolean failed, boolean allEnded, Throwable cause) {
        if (failed || allEnded) health.inboundEnded(p, !failed, cause);
    }

    @Override public void outboundFailed(long id, Throwable cause) { health.outboundFailed(id, cause); }
    @Override public long srttMax() { return health.srttMax(); }
    @Override public long deadline(long now) { return timers(now, false); }
    /**
     * Acts on the due deadlines. A pipe state change can make other limits due at once (a suspicion whose T_dead is
     * T_suspect, a failure that turns another pipe into the last of its direction), so passes repeat until one
     * changes no pipe state.
     */
    @Override
    public void onTimeout(long t) {
        for (int i = 0; i < 16; i++) {
            long g = health.stateGen();
            timers(t, true);
            if (health.stateGen() == g || port.state() == SessionState.CLOSED) return;
        }
    }
    @Override public boolean joinReady() { return joins.joinReady(); }

    /** The running pools that dial for this session. */
    private int attendants;

    @Override public void attend(int delta) { attendants = Math.max(0, attendants + delta); }
    @Override public void pickInput() { scheduler.pickInput(); }
    @Override public void policyChanged() { scheduler.policyChanged(); }
    @Override public boolean wantsPick() { return scheduler.wantsPick(); }
    @Override public boolean takePick(PathScheduler.PickRequest req) { return scheduler.takePick(req); }

    @Override
    public void answerPick(PathScheduler.PickRequest req, List<PathScheduler.PickAnswer> answers, boolean failed) {
        scheduler.answerPick(req, answers, failed);
    }

    @Override
    public Session.JoinProgress joinProgress(Pipe p) {
        Attach a = p.mp();
        if (a == null) return Session.JoinProgress.CONFIRMED;
        if (a.health.retired()) return Session.JoinProgress.GONE;
        if (a.health.out() == Attach.OUT_ACTIVE || a.health.out() == Attach.OUT_SUSPECT || (a.health.out() == Attach.OUT_NONE && a.health.in() == Attach.IN_OPEN))
            return Session.JoinProgress.CONFIRMED;
        if (a.health.out() == Attach.OUT_JOINING || (a.health.out() == Attach.OUT_NONE && a.health.in() == Attach.IN_JOINING)) return Session.JoinProgress.PENDING;
        return Session.JoinProgress.GONE;
    }

    @Override
    public boolean pipeUsable(Pipe p) {
        if (p.out() != null && !metrics.pipeUsable(p, false)) return false;
        return !p.mp().health.retired() && (p.in() == null || p.mp().health.in() == Attach.IN_OPEN);
    }

    /**
     * Whether the peer is known to hold the attachment. A joiner is Proven by the
     * Reply, but the acceptor attaches only once it read the Confirm, which may still
     * be in flight. A locally joined pipe with an outbound direction counts as attached once any
     * of its lanes received a PIPE_ACK covering more than zero bytes; a receive-only one once it
     * received a record. Pipe 0 and pipes the peer joined are attached from the start. Until then,
     * removing another pipe for this one may leave the acceptor without a live pipe (8.5).
     */
    @Override
    public boolean peerAttached(Pipe p) {
        Attach a = p.mp();
        if (a == null || !a.joinerLocal || p.id == 0) return true;
        if (p.out() != null) {
            for (Pipe.OutLane l : p.out().lanes) if (l.ack.acked() > 0) return true;
            return false;
        }
        if (p.in() != null) for (Pipe.InLane l : p.in().lanes) if (l.rx.recs() > 0) return true;
        return false;
    }

    @Override public void waitPeerAttached(Waiter w) { attachW.add(w); }
    @Override public void cancelPeerAttachedWait(Waiter w) { attachW.remove(w); }
    @Override public void wakePeerAttachedWaiters() { if (!attachW.isEmpty()) attachW.wakeEvery(outbox); }

    /** Registers w until new local joins use a later epoch or the session ends. */
    void waitEpoch(Waiter w) { epochW.add(w); }
    void cancelEpochWait(Waiter w) { epochW.remove(w); }
    void wakeEpochWaiters() { if (!epochW.isEmpty()) epochW.wakeEvery(outbox); }

    @Override public long nextPipeId() { return joins.nextId(); }

    @Override
    public boolean[] liveDirections(boolean closing) {
        int[] c = metrics.liveCounts();
        return new boolean[] {c[0] > 0, c[1] > 0};
    }

    /** The ports of the components: each forwards to the one owner of the effect. */
    private final class Wiring implements Joins.Port, PathHealth.Port, Resume.Port, Tiers.Port, PathScheduler.Port, Probation.Port,
            Retention.Port {
        @Override public boolean admits(Tiers.Gate g, Pipe p) { return tiers.admits(g, p); }
        @Override public void backoffCleared() { joins.backoffCleared(); }
        @Override public void joinFailed() { joins.joinFailed(); }
        @Override public Tiers.Gate buildGate() { return tiers.buildGate(); }
        @Override public boolean busyBackup(Pipe p, long now) { return probation.busyBackup(p, now); }
        @Override public void capacityChanged() { port.capacityChanged(); }
        @Override public void checkLingerDone() { port.checkLingerDone(); }
        @Override public void checkResumed() { resume.checkResumed(); }
        @Override public void epochInUse() { wakeEpochWaiters(); }
        @Override public boolean attended() { return cfg.redial() || attendants > 0; }
        @Override public byte[] firstBatch(int limit) { return port.firstBatch(limit); }
        @Override public void applyBatch(byte[] frames) { port.applyBatch(frames); }
        @Override public void joinPadding(long n) { port.joinPadding(n); }
        @Override public void proven(Pipe p) { joinProven(p); }
        @Override public void retireDeclined(Pipe p) { declined(p); }
        @Override public void checkTakeover() { tiers.checkTakeover(); }
        @Override public void closeWith(SessionError e, Lifecycle.CloseFrame fr) { port.closeWith(e, fr); }
        @Override public void copyChunk(PathScheduler.Decision d, RecordWriter w, Stream st, long off, long n, boolean fin) { retention.copyChunk(d, w, st, off, n, fin); }
        @Override public void emit(Event e) { port.emit(e); }
        @Override public void emitPipeState(Pipe p) { PathSession.this.emitPipeState(p); }
        @Override public void endRevalidation(Pipe p) { probation.endRevalidation(p); }
        @Override public void failLocal(WireException e) { port.failLocal(e); }
        @Override public void failPipe(Pipe p, long code, Throwable cause) { health.failPipe(p, code, cause); }
        @Override public Pipe fastestIn(Tiers.Gate g, int ti, long l, Pipe except) { return scheduler.fastestIn(g, ti, l, except); }
        @Override public boolean gateUsable(Tiers.Gate g, Pipe p) { return tiers.gateUsable(g, p); }
        @Override public boolean idleCopied(Stream st) { return scheduler.idleCopied(st); }
        @Override public boolean isClosing() { return port.isClosing(); }
        @Override public long lingerTimeout() { return port.lingerTimeout(); }
        @Override public void maybeRetire(Pipe p) { health.maybeRetire(p); }
        @Override public void abandonAcked(Pipe p) { health.abandonAcked(p); }
        @Override public void abandonLost(Pipe p) { health.abandonLost(p); }
        @Override public void dropProbation(Pipe p) { probation.dropProbation(p); }
        @Override public void requestPing(Pipe p, long now) { health.requestPing(p, now); }
        @Override public void failWithoutAbandon(Pipe p, long code) { health.failWithoutAbandon(p, code); }
        @Override public long now() { return port.now(); }
        @Override public CoreConfig.Policy policy() { return port.policy(); }
        @Override public void peerIdRetired(long id) { joins.peerIdRetired(id); }
        @Override public void pickChanged() { scheduler.pickChanged(); }
        @Override public long probationBase(Pipe p) { return probation.probationBase(p); }
        @Override public void probationFailed(Pipe p) { probation.probationFailed(p); }
        @Override public void probationSuspect(Pipe p) { probation.probationSuspect(p); }
        @Override public boolean redundant(Stream st) { return scheduler.redundant(st); }
        @Override public void refusedForResources() { joins.refusedForResources(); }
        @Override public void reinjectPipe(Pipe p) { retention.reinjectPipe(p); }
        @Override public void heldBack(Stream st) { retention.noteBlocked(st); }
        @Override public boolean reinjectRoom(Pipe p) { return resume.reinjectRoom(p); }
        @Override public void resumed() {
            port.resumed();
            joins.epochResumed();
        }
        @Override public void retire(Pipe p) { health.retire(p); }
        @Override public long sendBufferLimit() { return port.sendBufferLimit(); }
        @Override public long sessionSendBuffer() { return Math.max(cfg.sendBuffer(), retention.retransmitBudget()); }
        @Override public void startProbation(Pipe p) { probation.startProbation(p); }
        @Override public void startReinjectHold(Pipe p) { resume.startReinjectHold(p); }
        @Override public SessionState state() { return port.state(); }
        @Override public void statusLost(Pipe p, long seq) { tiers.statusLost(p, seq); }
        @Override public void suspectedRedundantAck() { PathSession.this.suspectedRedundantAck(); }
        @Override public void suspend(Throwable cause) { resume.suspend(cause); }
        @Override public void suspended(long graceDeadline) { port.suspended(graceDeadline); }
        @Override public long tSuspect(Pipe p) { return health.tSuspect(p); }
        @Override public boolean takes(Tiers.Gate g, Pipe p) { return tiers.takes(g, p); }
        @Override public void unsettle(Pipe p) { retention.unsettle(p); }
        @Override public boolean untaken(long id) { return joins.untaken(id); }
        @Override public boolean waitsFor(Pipe p, Pipe.OutLane ol) {
            metrics.openAs(p);
            try {
                return PathSession.this.wantsWrite(p, ol);
            } finally {
                metrics.openAs(null);
            }
        }
    }

    /** The sender view and record liveness of one attachment. */
    static final class Link {
        private long lastAdvance;
        private long liveAt;
        private boolean unacked;
        private long oldestAt;
        /** queue_p: unsent_ahead over R_p, both taken when the oldest unacknowledged ack-eliciting batch was handed over. */
        private long oldestQueue;
        private long lastElicit;
        private long lastRecord;
        private long maxGap;
        private boolean ackPair;
        /** When the PIPE_PING that set ackPair arrived. */
        private long ackPairAt;
        private boolean firstRecord;
        /** The outbound state last reported as an application event; whether the inbound end was. */
        private PipeInfo.Out reportedOut = PipeInfo.Out.NONE;
        private boolean reportedInEnded;

        Link(long now, boolean firstRecord) {
            liveAt = lastAdvance = lastRecord = now;
            this.firstRecord = firstRecord;
        }

        long lastAdvance() {
            return lastAdvance;
        }

        long liveAt() {
            return liveAt;
        }

        boolean unacked() {
            return unacked;
        }

        long oldestAt() {
            return oldestAt;
        }

        long oldestQueue() {
            return oldestQueue;
        }

        long lastElicit() {
            return lastElicit;
        }

        long lastRecord() {
            return lastRecord;
        }

        long maxGap() {
            return maxGap;
        }

        boolean ackPair() {
            return ackPair;
        }

        boolean firstRecord() {
            return firstRecord;
        }
    }
}
