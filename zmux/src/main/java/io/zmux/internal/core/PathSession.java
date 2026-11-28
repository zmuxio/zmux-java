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
        CoreSessionPolicy sessionPolicy();
        long now();

        /** See {@link Joins.Port#firstBatch}. */
        StreamSend.FirstBatch firstBatch(int limit, Batch receipt);
        int firstBatchStreamCount();
        void firstBatchAccepted(long own);

        long joinPaddingRoom();
        boolean profileBounded();

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
        void memberHealth(Pipe pipe, io.zmux.PipeHealth before, io.zmux.PipeHealth after);

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
    private final HealthProjection projection;
    final Probation probation;
    final Tiers tiers;
    final PathScheduler scheduler;
    final Retention retention;
    final Resume resume;
    private long redundantAt = Time.NEVER;
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

        projection = new HealthProjection(cfg.settleFor(), this::syncHealth);
        this.feat = feat;
        this.pipes = pipes;
        this.ack = ack;
        this.ctl = ctl;
        this.tx = tx;
        this.outbox = outbox;
        Wiring w = new Wiring();
        metrics = new PathMetrics(pipes, ack, feat);
        joins = new Joins(cfg, stats, peer, initiator, pipes, w, exporter, psk, conn);
        health = new PathHealth(cfg, stats, feat, pipes, streams, ack, ka, outbox, metrics, w);
        probation = new Probation(cfg, pipes, metrics, now, w);
        tiers = new Tiers(cfg, conn, sched, pipes, metrics, w);
        scheduler = new PathScheduler(cfg, stats, conn, sched, tx, lanes, pipes, streams, metrics, w);
        retention = new Retention(cfg, stats, ack, conn, ctl, sched, lanes, life, pipes, streams, metrics, tx.copyBudget(), health, w);
        resume = new Resume(cfg, peer, pipes, w);
        for (Pipe pp : pipes.all()) {
            Attach a = new Attach(initiator, 2, 0, 0, 0, -1, Double.NaN, Attach.OUT_ACTIVE, Attach.IN_OPEN, false, now);
            if (cfg.firstPipeMode() != null) a.mode(cfg.firstPipeMode());
            a.ownFailure(pp.ownFailure());
            joins.initialRtt(a, pp.id);
            pp.attach(a);
            health.refreshSilence(pp);
            syncHealth(pp);
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
    void ackEntry(Pipe received, long pipeId, long lane, long position, long delay) throws WireException {
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
        boolean advanced = position > ol.ack.acked();
        if (advanced) {
            ack.acked(ol, position);
            joins.laneAcked(ol, position);
            if (!attachW.isEmpty()) attachW.wakeEvery(outbox);
            health.laneAcked(p, ol);
            retention.ackLog(ol, position);
            var send = p.out().snd;
            long previous = send.ackedElicitEnd();
            ack.advanced(p, ol, position, delay, port.now());
            if (send.ackedElicitEnd() > previous) {
                health.ackDelivered(p, send.ackedElicitEnd(), send.rttSampleEnd(), port.now());
            }
        }
        // A redundant ACK still proves the receive half if it returned on the pair.
        boolean proved = ol.lane == 0 && a.projection.acknowledged(position, received == p);
        if (advanced || proved) syncHealth(p);
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
                    boolean probeAnswered = health.recoveryProbeAcknowledged(p);
                    if (probeAnswered) a.projection.recoveredByProbe();
                    health.activated(p);
                    a.link.liveAt = port.now();
                    probation.startProbation(p, probeAnswered);
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
            if (pipes.shaping() != null) pipes.shaping().of(p);
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
        probation.startProbation(p, false);
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
        if (port.state() != SessionState.CLOSED) health.refreshSilence(p);
        scheduler.pickChanged();
        PipeInfo.States st = pipeState(p);
        if (p.out() != null) port.emit(Event.pipeState(p.id, p.carrier(), Action.DIR_OUT, st.out()));
        if (p.in() != null) port.emit(Event.pipeState(p.id, p.carrier(), Action.DIR_IN, st.in()));
        reportPipeEvents(p, st);
        syncHealth(p);
    }

    io.zmux.PipeHealth syncHealth(Pipe p) { return syncHealth(p, false); }

    private io.zmux.PipeHealth syncHealth(Pipe p, boolean verifiedReceiveRecord) {
        var slot = p.mp().projection;
        var old = slot.snapshot();
        boolean held = slot.outboundHeld();
        boolean attached = peerAttached(p);
        if (attached) p.member().confirmed(port.now());
        var result = projection.sync(p, port.now(), attached, port.isClosing(), metrics.pipeUsable(p, false), verifiedReceiveRecord, port::emit);
        projection.registerDeadline(p);
        if (held != slot.outboundHeld()) scheduler.pickChanged();
        if (slot.outboundHeld()) observeStability(p);
        if (old != result) {
            pipes.changed();
            port.memberHealth(p, old, result);
        }
        return result;
    }

    void observeStability(Pipe pipe) {
        Attach attachment = pipe.mp();
        if (attachment.projection.needsProbe() && !attachment.health.needPing()) {
            health.requestPing(pipe, port.now());
            pipes.wake(pipe);
        }
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

    private boolean routeBuilt;
    private Tiers.Gate routeGate;
    private Pipe.OutLane routeLane;
    private Stream routeStream;

    void beginRoute() {
        routeBuilt = false;
        routeLane = null;
        routeStream = null;
        routeGate = null;
    }

    boolean routeStream(Pipe pipe, Pipe.OutLane lane, Stream stream) {
        if (!routeBuilt) {
            routeGate = tiers.buildGate();
            tiers.refreshTiers(routeGate);
            scheduler.refreshBindings(routeGate);
            routeBuilt = true;
        }
        var decision = scheduler.decision();
        scheduler.newDecision(decision, pipe, routeGate);
        if (!scheduler.onPipe(decision, stream)) return false;
        routeLane = lane;
        routeStream = stream;
        return true;
    }

    /** Builds a batch for one outbound lane of a multipath session. */
    @Override
    public void write(RecordWriter w, Pipe p, Pipe.OutLane ol) {
        boolean ownsRoute = routeLane == ol;
        boolean mayTakeData = ownsRoute || routeLane == null;
        Stream reserved = ownsRoute ? routeStream : null;
        Tiers.Gate prepared = ownsRoute ? routeGate : null;
        if (ownsRoute) beginRoute();
        w.retainFor(p, ol.log.room());
        Attach a = p.mp();
        if (a.handshaking()) return;
        if (ol.lane == 0) {
            joins.epochProgress();
            pipeFrames(w, p);
            Pipe prim = primary();
            if (prim == p || prim == null) tx.writeControl(w, p);
            else tx.writeDatagrams(w, p);
        }
        if ((a.topologyExcluded() & Action.DIR_OUT) != 0) return;
        boolean copies = ol.lane == 0 ? !a.ret.copyQ().isEmpty() || retention.prefixReady(p)
                : ol == a.ret.copyLane() && !a.ret.selfCopyQ().isEmpty();
        if (!w.historyRoom() || (!copies && !(mayTakeData && tx.dataPendingOn(p, ol)))) return;
        Tiers.Gate gate = reserved == null ? tiers.buildGate() : prepared;
        if (reserved == null) {
            tiers.refreshTiers(gate);
            if (ol.lane == 0) scheduler.refreshBindings(gate);
        }
        PathScheduler.Decision d = scheduler.decision();
        scheduler.newDecision(d, p, gate);
        if (ol.lane == 0) {
            retention.writeCopies(d, w, a.ret.copyQ());
            if (retention.prefixReady(p) && health.pipeTimers(port.now(), false) > port.now()) {
                retention.opportunistic(gate, w, p);
            }
        } else if (ol == p.mp().ret.copyLane()) {
            retention.writeCopies(d, w, a.ret.selfCopyQ());
        }
        if (mayTakeData) tx.writeData(w, p, ol, reserved);
    }

    boolean priorityControl(Pipe p) {
        Attach a = p.mp();
        return a.health.needPing() || a.link.firstRecord || a.link.ackPair || ackRelease(p) <= port.now();
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
            } else if (redundantAckReady() && p == second) {
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
            if (!w.trackedFrame(n, qa.health.abandonSent(), bb -> FrameEncoder.pipeAbandon(bb, id, code),
                    FrameDesc.id(FrameDesc.Kind.ABANDON, id))) return;
            if (health.abandonWritten(q)) w.abandonsSent.add(q);
        }
        if (!joins.writeOwedAbandons(w)) return;
        for (Pipe q : pipes.all()) if (Tiers.statusReady(q, p) && !tiers.writeStatus(w, q)) return;
        // A closing session exchanges no epoch: no join follows.
        if (!port.isClosing()) joins.writeEpoch(w, p, epochRepeat(p));
    }

    /** How often SESSION_EPOCH repeats on p: its probe interval, or 15 s where its probes are off. */
    private long epochRepeat(Pipe p) {
        long k = health.probeInterval(p);
        return k > 0 ? k : PathHealth.UNPAIRED_PROBE;
    }

    private static boolean writableControl(Pipe p) {
        Attach a = p.mp();
        return !a.health.retired() && a.health.out() != Attach.OUT_FAILED
                && a.health.out() != Attach.OUT_CLOSED && !a.handshaking();
    }

    boolean wantsPipeControl(Pipe p, Pipe.OutLane ol) {
        if (!writableControl(p)) return false;
        Attach a = p.mp();
        if ((a.link.firstRecord && ack != null && hasEntries()) || a.link.ackPair) return true;
        if (a.health.needPing() && health.pingAllowed(port.now()) && health.oldestPingWaiter(p)) return true;
        Pipe primary = primary();
        if (ack != null && ack.due(port.now()) && (p == primary || primary == null)) return true;
        if (redundantAckReady() && p == second()) return true;
        if (ol.log.room() != 0) {
            for (Pipe q : pipes.all()) {
                if (q.mp() == null || q.mp().health.retired()) continue;
                if ((q.mp().health.abandonQueued() && abandonGoesOn(q, p, primary)) || Tiers.statusReady(q, p)) return true;
            }
        }
        return joins.owesAbandons() || (!port.isClosing() && joins.epochWanted(p));
    }

    @Override
    public boolean wantsControl(Pipe p, Pipe.OutLane ol) {
        return wantsControl(p, ol, true);
    }

    private boolean wantsControl(Pipe p, Pipe.OutLane ol, boolean checkPrefix) {
        if (!writableControl(p)) return false;
        Attach a = p.mp();
        if (ol.lane == 0) {
            if (wantsPipeControl(p, ol) || tx.datagramsPending(p, ol)) return true;
            Pipe primary = primary();
            if ((p == primary || primary == null) && tx.controlPending(ol.log.room() != 0)) return true;
        }
        if (ol.log.room() == 0) return false;
        if (ol.lane == 0 && (!a.ret.copyQ().isEmpty() || (checkPrefix && retention.prefixReady(p))) && metrics.open(p)) return true;
        return ol == a.ret.copyLane() && !a.ret.selfCopyQ().isEmpty() && metrics.laneOpen(p, ol);
    }

    private final Tiers.Gate stallGate = new Tiers.Gate();

    private boolean waitsFor(Pipe p, Pipe.OutLane ol) {
        return wantsControl(p, ol, false)
                || (tx.dataPendingOn(p, ol) && scheduler.anyStreamOn(p, ol, tiers.buildGate(stallGate)));
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
        if (w.suspPing()) health.recoveryProbeRecorded(p);
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
        boolean receiveProof = p.out() == null && a.projection.needsReceiveRecord();
        if (il.rx.recs() == 1 || receiveProof) syncHealth(p, receiveProof);
        if (il.rx.recs() == 1 && !attachW.isEmpty()) attachW.wakeEvery(outbox);
    }

    long protocolDeadline(ProtocolTimeline.Reason reason, long now) {
        return switch (reason) {
            case HEALTH -> health.pipeTimers(now, false);
            case PREFIX_REPAIR -> port.isClosing() || !retention.hasPrefixWork() ? Time.NEVER
                    : retention.prefixTimers(tiers.buildGate(), false);
            case JOIN_BACKOFF -> joins.backoffTimer(now, false);
            case EPOCH -> joins.epochTimers(now, false);
            case ACK_COPY -> ackChanged ? redundantAt : Time.NEVER;
            case PING -> health.pingTimer(now, false);
            case CUSTOM_PICK -> scheduler.pickDeadline();
            case PROBATION -> probation.probationTimers(now, false);
            case HEALTH_PROJECTION -> projection.nextDeadline();
            case TIER -> tiers.tierDeadline();
            default -> Time.NEVER;
        };
    }

    private void runDeadlines(long now) {
        probation.probationTimers(now, true);
        projection.onTimeout(now);
        health.pipeTimers(now, true);
        joins.backoffTimer(now, true);
        joins.epochTimers(now, true);
        if (ackChanged && redundantAt != Time.NEVER && now >= redundantAt) {
            redundantAt = Time.NEVER;
            pipes.wakeWriters();
        }
        health.pingTimer(now, true);
        scheduler.expirePick(now);
        if (tiers.onTimeout(now)) pipes.wakeWriters();
    }

    private boolean redundantAckReady() {
        return ackChanged && (redundantAt == Time.NEVER || port.now() >= redundantAt);
    }

    /** A pipe became SUSPECT: a changed acknowledgement waiting for its redundant copy goes now. */
    void suspectedRedundantAck() {
        if (primary() != null && ackChanged) redundantAt = port.now();
    }

    // ---- PathLayer ----

    @Override public boolean acksInControl() { return false; }
    @Override public void maxDataSent(boolean peerWaiting) { retention.maxDataSent(peerWaiting); }
    @Override public void maxStreamDataSent(Stream st, boolean peerWaiting) { retention.maxStreamDataSent(st, peerWaiting); }
    @Override public boolean urgentLane(Stream st) { return scheduler.redundant(st); }
    @Override public long sendBuffer(long configured) { return Math.max(configured, retention.retransmitBudget()); }
    @Override public long retainRoom() { return retention.retainRoom(); }
    @Override public void batchCarried(Stream st, long off, long n) { retention.lost(FrameDesc.stream(st.tab.id(), off, n, false)); }
    @Override public void retain(long n) { retention.retain(n); }
    @Override public void unretain(long n) { retention.unretain(n); }
    @Override public boolean retainsSent() { return true; }
    @Override public long retained() { return retention.retained(); }

    @Override
    public void onPipeAck(Pipe received, io.zmux.internal.wire.Frame.AckEntries it) throws WireException {
        resume.endReinjectHolds();
        while (it.next()) ackEntry(received, it.pipeId, it.lane, it.position, it.delay);
    }

    @Override
    public void inboundEnded(Pipe p, boolean failed, boolean allEnded, Throwable cause) {
        if (failed || allEnded) health.inboundEnded(p, !failed, cause);
    }

    @Override public void outboundFailed(long id, Throwable cause) { health.outboundFailed(id, cause); }
    @Override public long srttMax() { return health.srttMax(); }
    @Override public void closeTimers() {
        beginRoute();
        health.closeTimers();
        projection.closeTimers();
    }
    /**
     * Acts on the due deadlines. A pipe state change can make other limits due at once (a suspicion whose T_dead is
     * T_suspect, a failure that turns another pipe into the last of its direction), so passes repeat until one
     * changes no pipe state.
     */
    @Override
    public void onTimeout(long t) {
        long generation;
        do {
            generation = health.stateGen();
            runDeadlines(t);
        } while (health.stateGen() != generation && port.state() != SessionState.CLOSED);
        if (!port.isClosing() && retention.hasPrefixWork()) retention.prefixTimers(tiers.buildGate(), true);
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
        @Override public CoreSessionPolicy sessionPolicy() { return port.sessionPolicy(); }
        @Override public boolean peerAttached(Pipe p) { return PathSession.this.peerAttached(p); }
        @Override public boolean admits(Tiers.Gate g, Pipe p) { return tiers.admits(g, p); }
        @Override public void backoffCleared() { joins.backoffCleared(); }
        @Override public void joinFailed() { joins.joinFailed(); }
        @Override public Tiers.Gate buildGate() { return tiers.buildGate(); }
        @Override public boolean busyBackup(Pipe p, long now) { return probation.busyBackup(p, now); }
        @Override public void capacityChanged() { port.capacityChanged(); }
        @Override public void healthChanged(Pipe p) { syncHealth(p); }
        @Override public void checkLingerDone() { port.checkLingerDone(); }
        @Override public void checkResumed() { resume.checkResumed(); }
        @Override public void epochInUse() { wakeEpochWaiters(); }
        @Override public boolean attended() { return attendants > 0; }
        @Override public StreamSend.FirstBatch firstBatch(int limit, Batch receipt) { return port.firstBatch(limit, receipt); }
        @Override public int firstBatchStreamCount() { return port.firstBatchStreamCount(); }
        @Override public void firstBatchAccepted(long own) { port.firstBatchAccepted(own); }
        @Override public void applyBatch(byte[] frames) { port.applyBatch(frames); }
        @Override public long joinPaddingRoom() { return port.joinPaddingRoom(); }
        @Override public boolean profileBounded() { return port.profileBounded(); }
        @Override public void proven(Pipe p) { joinProven(p); }
        @Override public void retireDeclined(Pipe p) { declined(p); }
        @Override public void checkTakeover() { tiers.checkTakeover(); }
        @Override public void closeWith(SessionError e, Lifecycle.CloseFrame fr) { port.closeWith(e, fr); }
        @Override public void copyChunk(PathScheduler.Decision d, RecordWriter w, Stream st, long off, long n, boolean fin) { retention.copyChunk(d, w, st, off, n, fin); }
        @Override public void emit(Event e) { port.emit(e); }
        @Override public void emitPipeState(Pipe p) { PathSession.this.emitPipeState(p); }
        @Override public void emitRetired(Pipe p) {
            if (port.state() != SessionState.CLOSED) health.refreshSilence(p);
            tx.datagramPathRetired(p);
            scheduler.pickChanged();
            syncHealth(p);
            port.emit(Event.pipeRetired(p.id, p.carrier(), p.out() != null && p.mp().out() == Attach.OUT_FAILED));
        }
        @Override public void endRevalidation(Pipe p) { probation.endRevalidation(p); }
        @Override public void failLocal(WireException e) { port.failLocal(e); }
        @Override public void failPipe(Pipe p, long code, Throwable cause) { health.failPipe(p, code, cause); }
        @Override public long eta(Pipe p, long bytes) { return scheduler.repairEta(p, bytes); }
        @Override public PathScheduler.Arrival arrival(Pipe p) { return scheduler.arrival(p); }
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
        @Override public boolean reinjectRoom(Pipe p) { return resume.reinjectRoom(p); }
        @Override public long reinjectAllowance(Pipe p) { return resume.reinjectAllowance(p); }
        @Override public void prefixResent(Pipe p, long bytes) { resume.resent(p, bytes); }
        @Override public void resumed() {
            port.resumed();
            joins.epochResumed();
        }
        @Override public void retire(Pipe p) { health.retire(p); }
        @Override public long sendBufferLimit() { return port.sendBufferLimit(); }
        @Override public long sessionSendBuffer() { return Math.max(cfg.sendBuffer(), retention.retransmitBudget()); }
        @Override public void startProbation(Pipe p) { probation.startProbation(p, false); }
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
                return PathSession.this.waitsFor(p, ol);
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
