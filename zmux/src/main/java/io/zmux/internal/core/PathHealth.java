package io.zmux.internal.core;

import io.zmux.internal.wire.TransportError;
import java.util.ArrayList;

/**
 * Failure detection of the attachments: PTO, SUSPECT, failure and dead timeouts, PIPE_PING pacing, drains,
 * PIPE_ABANDON exchange, retirement, fate groups and the last-pipe rule (MULTIPATH.md 6.4, 7, 8).
 */
final class PathHealth {
    /** What PathHealth asks of the rest of the path layer and the session. */
    interface Port {
        /** See {@link Joins}. */
        void backoffCleared();

        /** See {@link Probation}. */
        boolean busyBackup(Pipe p, long now);

        /** Re-checks whether a closing session finished lingering. */
        void checkLingerDone();

        /** See {@link Tiers}. */
        void checkTakeover();

        /** Closes the session. */
        void closeWith(SessionError e, Lifecycle.CloseFrame fr);

        /** Queues an event (with the session's quality floor check). */
        void emit(Event e);

        /** See {@link PathSession}. */
        void emitPipeState(Pipe p);

        /** Whether the session is closing. */
        boolean isClosing();

        /** The inbound linger after a graceful close. */
        long lingerTimeout();

        /** The session clock. */
        long now();

        /** See {@link Joins}. */
        void peerIdRetired(long id);

        /** See {@link Probation}. */
        void probationFailed(Pipe p);

        /** See {@link Probation}. */
        void probationSuspect(Pipe p);

        /** See {@link Joins}. */
        void refusedForResources();

        /** See {@link Retention}. */
        void reinjectPipe(Pipe p);

        /** The session state. */
        SessionState state();

        /** See {@link PathSession}. */
        void suspectedRedundantAck();

        /** See {@link Resume}. */
        void suspend(Throwable cause);

        /** See {@link Retention}. */
        void unsettle(Pipe p);

        /** See {@link PathSession}. */
        boolean wantsWrite(Pipe p, Pipe.OutLane ol);
    }

    static final long NO_STALL = Long.MIN_VALUE;

    private final Port port;
    private final CoreConfig cfg;
    private final SessionStats stats;
    private final Features feat;
    private final PipeTable pipes;
    private final StreamsView streams;
    private final AckEngine ack;
    private final KeepAlive ka;
    private final Outbox outbox;
    private final PathMetrics metrics;
    private final long[] pingTimes = new long[8];
    private int pingIdx;
    private int pingCount;

    PathHealth(CoreConfig cfg, SessionStats stats, Features feat, PipeTable pipes, StreamsView streams, AckEngine ack, KeepAlive ka, Outbox outbox, PathMetrics metrics,
               Port port) {
        this.port = port;
        this.cfg = cfg;
        this.stats = stats;
        this.feat = feat;
        this.pipes = pipes;
        this.streams = streams;
        this.ack = ack;
        this.ka = ka;
        this.outbox = outbox;
        this.metrics = metrics;
    }

    boolean pingAllowed(long now) {
        return pingCount < 8 || now >= Time.add(pingTimes[pingIdx], Time.SECOND);
    }

    void pingSent(long now) {
        pingTimes[pingIdx] = now;
        pingIdx = (pingIdx + 1) % pingTimes.length;
        if (pingCount < 8) pingCount++;
    }

    long pingNext() {
        return pingCount < 8 ? 0 : Time.add(pingTimes[pingIdx], Time.SECOND);
    }

    boolean oldestPingWaiter(Pipe p) {
        for (Pipe q : pipes.all()) {
            if (q.mp() != null && q != p && q.mp().health.needPing && q.out() != null && !q.out().failed() && q.mp().health.pingSince < p.mp().health.pingSince) {
                return false;
            }
        }
        return true;
    }

    long tSuspect(Pipe p) {
        Attach a = p.mp();
        long f = a.suspectFloor > 0 ? a.suspectFloor : cfg.suspectFloor();
        return Math.max(f, Time.mul(Time.add(metrics.pto(a), queueDelay(p)), 2));
    }

    long tFail(Pipe p) {
        Attach a = p.mp();
        return Math.max(2 * Time.SECOND, Time.mul(Time.add(metrics.pto(a), queueDelay(p)), 8));
    }

    /**
     * queue_p = min(1 s, unsent_ahead_p / R_p) (MULTIPATH.md 7.1), both snapshotted when the oldest unacknowledged
     * ack-eliciting batch was handed over; 0 while nothing is unacknowledged.
     */
    long queueDelay(Pipe p) {
        Attach a = p.mp();
        return a.link.unacked() ? a.link.oldestQueue() : 0;
    }

    static long queueDelay(long ahead, double rate) {
        if (ahead <= 0) return 0;
        if (!(rate > 0)) return Time.SECOND;
        return (long) Math.min(Time.SECOND, ahead / rate * Time.SECOND);
    }

    /** The protocol's probe interval for an attachment without an outbound direction at the peer (MULTIPATH.md 7.1). */
    static final long UNPAIRED_PROBE = 15 * Time.SECOND;

    /** The probe interval k of the pipe: its own (-1: none), else the session's; at most 15 s, and never 0, without a peer outbound direction. */
    long probeInterval(Pipe p) {
        return probeInterval(p.mp().probeInterval, cfg.probeInterval(), p.in() == null);
    }

    static long probeInterval(long own, long session, boolean unpaired) {
        long k = own >= 0 ? own : session;
        return unpaired ? (k == 0 ? UNPAIRED_PROBE : Math.min(k, UNPAIRED_PROBE)) : k;
    }

    /**
     * The pipe's current probe wait, drawn once per wait (MULTIPATH.md 7.1): in [k_eff(1 - j), k_eff] with k_eff =
     * min(k, max(1 s, k - H_p)), so that the probe reaches the carrier within k of the previous ack-eliciting byte however
     * long shaping holds it; the first wait of the bootstrap pipe in [0, k_eff] when j > 0 (a joined pipe carried
     * the joiner's PIPE_PING, so both of its ends draw as usual). 0 when the idle probes are off.
     */
    long probeWait(Pipe p) {
        Attach a = p.mp();
        long k = probeInterval(p);
        if (k == 0) return 0;
        Slot h = a.health;
        if (h.probeHold < 0) { // the wait starts: H_p as bounded now
            Shaping sh = pipes.shaping();
            h.probeHold = sh == null ? 0 : sh.maxHold(p);
        }
        long kEff = effectiveInterval(k, h.probeHold);
        double j = Double.isNaN(a.probeJitter) ? cfg.probeJitter() : a.probeJitter;
        if (j <= 0) return kEff;
        if (Double.isNaN(h.probeDraw)) h.probeDraw = cfg.random().nextDouble();
        return probeWait(kEff, drawJitter(p.id, h.pingsSent, j), h.probeDraw);
    }

    /** The jitter a wait is drawn with: 1 (the whole interval) for the bootstrap pipe's first wait only (MULTIPATH.md 7.1). */
    static double drawJitter(long pipeId, long pingsSent, double j) {
        return pipeId == 0 && pingsSent == 0 ? 1 : j;
    }

    /** k_eff = min(k, max(1 s, k - H_p)): a k below 1 s is never raised. */
    static long effectiveInterval(long k, long hold) {
        if (k <= Time.SECOND) return k;
        return Math.max(Time.SECOND, k - Math.max(hold, 0));
    }

    /** The wait k(1 - j u) for a uniform draw u in [0, 1); j = 1 draws from the whole interval. */
    static long probeWait(long k, double j, double u) {
        return Math.max(1, (long) (k * (1 - j * u)));
    }

    long tDead(Pipe p) {
        Attach a = p.mp();
        long dt = a.deadTimeout > 0 ? a.deadTimeout : cfg.deadTimeout();
        if (dt == 0) {
            if (feat.resume()) {
                dt = tFail(p);
            } else {
                long idle = ka.idleTimeout();
                if (idle == 0) idle = 30 * Time.SECOND;
                dt = Math.max(tFail(p), idle / 2);
            }
        }
        return Math.max(tSuspect(p), dt);
    }

    /** The start of the oldest write stall on p (a lane whose carrier takes no bytes while records wait), or NO_STALL. */
    long writeStall(Pipe p) {
        if (p.out() == null) return NO_STALL;
        long t = NO_STALL;
        for (Pipe.OutLane ol : p.out().lanes) {
            if (!(ol.tx.writing() || ol.tx.notWritable()) || ol.st.shut()) continue;
            if (!ol.tx.writing() && !port.wantsWrite(p, ol)) continue;
            if (t == NO_STALL || ol.tx.stallAt() < t) t = ol.tx.stallAt();
        }
        if (t != NO_STALL && p.mp() != null && p.mp().link.liveAt() > t) t = p.mp().link.liveAt();
        return t;
    }

    boolean otherActive(Pipe p) {
        for (Pipe q : pipes.all()) if (q != p && q.mp() != null && q.out() != null && q.mp().health.out == Attach.OUT_ACTIVE) return true;
        return false;
    }

    private long nextT;

    /** Counts pipe state changes, so that a timer pass can tell whether it made other deadlines due. */
    private long stateGen;

    long stateGen() { return stateGen; }

    void stateChanged() { stateGen++; }

    private long timerNow;
    private boolean timerApply;

    /**
     * Whether deadline t is due. Every deadline here changes a pipe's state when processed: one
     * that passed but was not processed yet stays pending without apply, so that a driver asking
     * for the next deadline between its passing and the timer pass still arms a timer for it.
     */
    private boolean at(long t) {
        if (timerNow >= t) {
            if (!timerApply) nextT = Math.min(nextT, t);
            return true;
        }
        nextT = Math.min(nextT, t);
        return false;
    }

    /** Evaluates the per-pipe deadlines; with apply it acts on the due ones. Returns the earliest pending one. */
    long pipeTimers(long now, boolean apply) {
        nextT = Time.NEVER;
        timerNow = now;
        timerApply = apply;
        boolean liveIn = metrics.liveCounts()[1] > 0;
        for (int i = 0; i < pipes.all().size(); i++) {
            Pipe p = pipes.all().get(i);
            Attach a = p.mp();
            if (a == null || a.health.retired) continue;
            if (a.joinerLocal && (a.health.out == Attach.OUT_JOINING || a.health.out == Attach.OUT_NONE && a.health.in == Attach.IN_JOINING) && p.id != 0) {
                if (at(Time.add(a.joining.attachedAt(), cfg.handshakeTimeout())) && apply) {
                    failPipe(p, TransportError.PIPE_FAILED, null);
                    continue;
                }
            }
            long ws = writeStall(p);
            boolean wstall = ws != NO_STALL;
            if (p.out() != null && a.liveOut() && (a.link.unacked() && liveIn || wstall)) {
                long stall = a.tref();
                if (wstall && (!a.link.unacked() || !liveIn || ws < stall)) stall = ws;
                switch (a.health.out) {
                    case Attach.OUT_ACTIVE, Attach.OUT_JOINING -> {
                        // Only a deadline whose pass acts is due: an unconfirmed join waits for its timeout.
                        if ((a.health.out != Attach.OUT_JOINING || confirmedOnce(p)) && at(Time.add(stall, tSuspect(p))) && apply) {
                            makeSuspect(p);
                        }
                    }
                    case Attach.OUT_SUSPECT -> {
                        long lim = otherActive(p) ? tFail(p) : tDead(p);
                        if (at(Time.add(stall, lim)) && apply) {
                            failPipe(p, TransportError.PIPE_FAILED, null);
                            continue;
                        }
                    }
                    case Attach.OUT_DRAINING -> {
                        if (!a.health.drainSuspect && at(Time.add(stall, tSuspect(p))) && apply) {
                            a.health.drainSuspect = true;
                            port.reinjectPipe(p);
                        }
                    }
                    default -> { }
                }
            }
            if (a.health.out == Attach.OUT_DRAINING && at(Time.add(a.health.drainStart, tFail(p))) && apply) {
                failPipe(p, TransportError.PIPE_FAILED, null);
                continue;
            }
            if (p.out() != null && (a.health.out == Attach.OUT_ACTIVE || a.health.out == Attach.OUT_SUSPECT) && !a.link.unacked() && !a.health.needPing) {
                long iv = probeWait(p);
                long sp = cfg.standbyProbe();
                if (sp > 0 && port.busyBackup(p, now) && (iv == 0 || sp < iv)) iv = sp;
                if (iv > 0) {
                    long ref = Math.max(a.link.lastElicit(), a.link.liveAt());
                    if (at(Time.add(ref, iv)) && apply) {
                        a.health.needPing = true;
                        a.health.pingSince = now;
                        pipes.wakeWriters();
                    }
                }
            }
            if (a.health.abandonAcked && !a.health.abandonRecv && !a.health.unilateral) {
                if (at(Time.add(a.health.ackedAbandonAt, Math.max(2 * Time.SECOND, Time.mul(maxPto(), 3)))) && apply) {
                    a.health.unilateral = true;
                    outbox.action(new Action(Action.Kind.ABORT, p.id, 0, Action.DIR_BIDI));
                    if (p.out() != null && a.health.out != Attach.OUT_FAILED) a.health.out = Attach.OUT_CLOSED;
                    if (p.in() != null) a.health.in = Attach.IN_ENDED;
                    maybeRetire(p);
                    continue;
                }
            }
            if (a.health.lingering && a.health.in == Attach.IN_OPEN && at(a.health.lingerUntil) && apply) {
                a.health.in = Attach.IN_ENDED;
                outbox.action(new Action(Action.Kind.CLOSE, p.id, 0, Action.DIR_IN));
                maybeRetire(p);
                continue;
            }
            if (p.out() == null && a.health.in == Attach.IN_OPEN) {
                int nIn = metrics.liveCounts()[1];
                long lim = 15 * Time.SECOND + nIn * Time.SECOND / 8
                        + Math.max(2 * Time.SECOND, Math.max(Time.mul(srttMaxOut(), 2), a.link.maxGap() - 15 * Time.SECOND));
                if (at(Time.add(a.link.lastRecord(), lim)) && apply) {
                    failPipe(p, TransportError.PIPE_FAILED, null);
                }
            }
        }
        return nextT;
    }

    /** The PIPE_PING rate limit deadline while a pipe waits to ping; Time.NEVER when none. */
    long pingTimer(long now, boolean apply) {
        for (Pipe p : pipes.all()) {
            if (p.mp() != null && p.mp().health.needPing) {
                long t = pingNext();
                if (t > now) return t;
                if (apply) pipes.wakeWriters();
                break;
            }
        }
        return Time.NEVER;
    }

    boolean confirmedOnce(Pipe p) {
        return p.out() != null && p.out().lanes.getFirst().ack.acked() > 0 || !p.mp().joinerLocal || p.id == 0;
    }

    long maxPto() {
        long m = 0;
        for (Pipe p : pipes.all()) if (p.mp() != null && p.out() != null && p.mp().liveOut()) m = Math.max(m, metrics.pto(p.mp()));
        return m == 0 ? 333 * Time.MILLISECOND + 4 * 166 * Time.MILLISECOND + 25 * Time.MILLISECOND : m;
    }

    long srttMaxOut() {
        long m = 0;
        for (Pipe p : pipes.all()) if (p.mp() != null && p.out() != null && p.mp().liveOut()) m = Math.max(m, p.mp().rtt.smoothed());
        return m == 0 ? 333 * Time.MILLISECOND : m;
    }

    /** The largest smoothed RTT of the live outbound pipes (SPEC.md 1.3), 0 when none. */
    long srttMax() {
        long m = 0;
        for (Pipe p : pipes.all()) {
            if (p.mp() != null && p.out() != null && p.mp().liveOut() && p.mp().rtt.smoothed() > m) m = p.mp().rtt.smoothed();
        }
        return m;
    }

    /** Fails an attachment: reinjection, carrier closed, PIPE_ABANDON unless one was sent, then the last-pipe rule. */
    void failPipe(Pipe p, long code, Throwable cause) {
        Attach a = p.mp();
        if (a == null || a.health.retired) return;
        boolean wasDraining = a.health.out == Attach.OUT_DRAINING || a.health.draining;
        if (p.out() != null && a.health.out != Attach.OUT_FAILED && a.health.out != Attach.OUT_CLOSED) {
            a.health.out = Attach.OUT_FAILED;
            a.health.failures++;
            a.health.failCode = code;
            p.out().lost();
            stats.pipeFailed();
            p.out().fail();
            Shaping.pipeDown(p);
            port.probationFailed(p);
            port.unsettle(p);
            port.reinjectPipe(p);
        }
        if (p.in() != null && a.health.in != Attach.IN_ENDED) {
            a.health.in = Attach.IN_ENDED;
            p.in().end();
        }
        if (a.health.failCause == null) a.health.failCause = cause;
        ka.pipeFailed(p.id);
        outbox.action(new Action(Action.Kind.ABORT, p.id, 0, Action.DIR_BIDI));
        if (!a.health.abandonSent && !a.health.abandonQueued) {
            a.health.abandonQueued = true;
            a.health.abandonCode = code;
        }
        port.emitPipeState(p);
        port.checkTakeover();
        if (!wasDraining && code != TransportError.PIPE_RESOURCE_LIMIT && a.fate != 0) fateGroupHit(p);
        maybeRetire(p);
        checkLastPipe(cause);
        port.checkLingerDone();
        pipes.wakeWriters();
    }

    /** Marks the other attachments of the same joiner and fate group suspect or probes them (7.1). */
    void fateGroupHit(Pipe p) {
        Attach a = p.mp();
        for (Pipe q : new ArrayList<>(pipes.all())) {
            Attach qa = q.mp();
            if (q == p || qa == null || qa.health.retired || qa.fate != a.fate || qa.joinerLocal != a.joinerLocal) continue;
            if (qa.health.draining || qa.health.abandonRecv && qa.health.abandonCodeRecv == TransportError.NO_ERROR) continue;
            if (qa.health.out == Attach.OUT_ACTIVE) {
                if (qa.link.unacked()) {
                    makeSuspect(q);
                } else {
                    qa.health.needPing = true;
                    qa.health.pingSince = port.now();
                }
            }
        }
    }

    /** Moves an outbound pipe to SUSPECT (MULTIPATH.md 7.2). */
    void makeSuspect(Pipe p) {
        Attach a = p.mp();
        if (a.health.out != Attach.OUT_ACTIVE && a.health.out != Attach.OUT_JOINING) return;
        a.health.out = Attach.OUT_SUSPECT;
        a.health.suspects++;
        a.health.suspectAt = port.now();
        a.health.noSampleAt = port.now();
        a.health.noSample = true;
        port.probationSuspect(p);
        port.unsettle(p);
        port.checkTakeover();
        port.reinjectPipe(p);
        a.health.needPing = true;
        a.health.pingSince = port.now();
        port.emitPipeState(p);
        port.suspectedRedundantAck();
    }

    /** Removes an attachment gracefully (drain) or abruptly; false when unknown. */
    boolean removePipe(long id, boolean graceful) {
        Pipe p = pipes.get(id);
        if (p == null || p.mp() == null || p.mp().health.retired) return false;
        if (!graceful) failPipe(p, TransportError.PIPE_FAILED, null);
        else startDrain(p);
        return true;
    }

    /** Begins a graceful removal (MULTIPATH.md 8.2). */
    void startDrain(Pipe p) {
        Attach a = p.mp();
        if (a.health.draining) return;
        a.health.draining = true;
        a.health.drainStart = port.now();
        if (p.out() != null && a.liveOut()) a.health.out = Attach.OUT_DRAINING;
        port.unsettle(p);
        if (!a.health.abandonSent && !a.health.abandonQueued) {
            a.health.abandonQueued = true;
            a.health.abandonCode = TransportError.NO_ERROR;
        }
        port.emitPipeState(p);
        checkDrain(p);
        checkLastPipe(null);
        pipes.wakeWriters();
    }

    /** A received PIPE_ABANDON (MULTIPATH.md 8.2, 8.3). */
    void peerAbandon(Pipe p, long code) {
        Attach a = p.mp();
        if (a == null || a.health.retired || a.health.abandonRecv) return;
        a.health.abandonRecv = true;
        a.health.abandonRecvAt = port.now();
        a.health.abandonCodeRecv = code;
        if (a.joinerLocal && code == TransportError.PIPE_RESOURCE_LIMIT) {
            a.health.resLimited = true;
            port.refusedForResources();
        }
        if (code == TransportError.NO_ERROR) {
            if (!a.health.draining) startDrain(p);
            checkDrain(p);
        } else {
            if (code != TransportError.PIPE_RESOURCE_LIMIT) code = TransportError.PIPE_FAILED;
            failPipe(p, code, null);
        }
        maybeRetire(p);
    }

    static boolean unackedStream(Pipe p) {
        if (p.out() == null) return false;
        for (Pipe.OutLane l : p.out().lanes) {
            for (Retained e : l.log) if (e.d.kind() == FrameDesc.Kind.STREAM && !e.reinjected()) return true;
        }
        return false;
    }

    /** Completes a drain (MULTIPATH.md 8.2 step 3). */
    void checkDrain(Pipe p) {
        Attach a = p.mp();
        if (a.health.out != Attach.OUT_DRAINING || !a.health.abandonSent || !a.health.abandonRecv || unackedStream(p)) return;
        a.health.out = Attach.OUT_CLOSED;
        port.reinjectPipe(p);
        for (Pipe.OutLane l : p.out().lanes) {
            if (Lanes.shut(l)) {
                outbox.action(new Action(Action.Kind.SHUTDOWN, p.id, l.lane, Action.DIR_OUT));
            }
        }
        if (p.in() != null && a.health.in == Attach.IN_OPEN) {
            a.health.lingerUntil = Time.add(port.now(), port.lingerTimeout());
            a.health.lingering = true;
        }
        port.emitPipeState(p);
        maybeRetire(p);
        checkLastPipe(null);
    }

    void abandonHanded(Pipe p) {
        checkDrain(p);
        maybeRetire(p);
    }

    /** Retires an attachment at the first moment every condition holds (MULTIPATH.md 8.4). */
    void maybeRetire(Pipe p) {
        Attach a = p.mp();
        if (a == null || a.health.retired) return;
        if (!a.health.abandonSent || !a.health.abandonAcked) return;
        if (!a.health.abandonRecv && !a.health.unilateral) return;
        if (p.out() != null && a.health.out != Attach.OUT_FAILED && a.health.out != Attach.OUT_CLOSED) return;
        if (p.in() != null && a.health.in != Attach.IN_ENDED) return;
        retire(p);
    }

    /** Frees the attachment's state; its ID stays used. */
    void retire(Pipe p) {
        Attach a = p.mp();
        a.health.retired = true;
        stateGen++;
        if (!a.joinerLocal && p.id != 0) port.peerIdRetired(p.id);
        outbox.action(new Action(Action.Kind.ABORT, p.id, 0, Action.DIR_BIDI));
        pipes.remove(p);
        for (Stream st : streams.live()) {
            if (st.snd.bind().laneBound() && st.snd.bind().lanePipe() == p.id) Lanes.pipeGone(st);
        }
        port.emit(Event.pipeState(p.id, p.carrier(), Action.DIR_BIDI, PipeInfo.Out.RETIRED));
        checkLastPipe(null);
        if (a.joinerLocal && !a.health.resLimited) port.backoffCleared();
    }

    /** MULTIPATH.md 8.5. */
    void checkLastPipe(Throwable cause) {
        if (port.isClosing() || port.state() == SessionState.SUSPENDED) return;
        int[] lc = metrics.liveCounts();
        if (lc[0] > 0 && lc[1] > 0) return;
        if (feat.resume()) {
            port.suspend(cause);
            return;
        }
        SessionError e = SessionError.pipesLost(cause);
        if (lc[0] > 0) port.closeWith(e, Lifecycle.CloseFrame.of(false, TransportError.NO_VIABLE_PATH, 0, null));
        else port.closeWith(e, null);
    }

    /** The end of an inbound pipe (MULTIPATH.md 7.3). */
    void inboundEnded(Pipe p, boolean graceful, Throwable cause) {
        Attach a = p.mp();
        if (a == null || a.health.retired) return;
        boolean expected = graceful && (a.health.abandonRecv || a.health.abandonSent && a.health.abandonCode == TransportError.NO_ERROR || a.health.draining
                || port.isClosing());
        ack.ackNow();
        if (expected) {
            a.health.in = Attach.IN_ENDED;
            port.emitPipeState(p);
            maybeRetire(p);
            checkLastPipe(null);
            pipes.wakeWriters();
            return;
        }
        failPipe(p, TransportError.PIPE_FAILED, cause);
    }

    /** A failed write on an outbound pipe. */
    void outboundFailed(long id, Throwable cause) {
        Pipe p = pipes.get(id);
        if (p != null && p.mp() != null) failPipe(p, TransportError.PIPE_FAILED, cause);
    }

    /** A joining or SUSPECT outbound attachment became ACTIVE. */
    void activated(Pipe p) {
        p.mp().health.out = Attach.OUT_ACTIVE;
    }

    /** The first record arrived on a joining inbound attachment: it is OPEN, and a joining outbound side ACTIVE. */
    void inboundOpened(Pipe p) {
        Slot h = p.mp().health;
        h.in = Attach.IN_OPEN;
        if (h.out == Attach.OUT_JOINING) h.out = Attach.OUT_ACTIVE;
    }

    /** A PIPE_PING is owed on the pipe from now on. */
    void requestPing(Pipe p, long now) {
        Slot h = p.mp().health;
        h.needPing = true;
        h.pingSince = now;
    }

    /** A batch that holds a PIPE_PING went out on lane 0, ending at lane position end. */
    void pingRecorded(Pipe p, long end) {
        p.mp().health.pingEnds.addLast(end);
    }

    /**
     * Lane ol's acknowledged position advanced: every PIPE_PING whose record it now covers was answered, each once.
     * PIPE_PING goes on lane 0 only.
     */
    void laneAcked(Pipe p, Pipe.OutLane ol) {
        if (ol.lane != 0) return;
        Slot h = p.mp().health;
        while (!h.pingEnds.isEmpty() && ol.ack.acked() >= h.pingEnds.peekFirst()) {
            h.pingEnds.pollFirst();
            h.pingsAnswered++;
        }
    }

    /** The owed PIPE_PING went out on the pipe. */
    void pingWritten(Pipe p, long now) {
        Slot h = p.mp().health;
        h.needPing = false;
        h.pingsSent++;
        h.probeDraw = Double.NaN; // the next wait is drawn anew
        h.probeHold = -1;
        pingSent(now);
    }

    /** The queued PIPE_ABANDON about q was written; returns whether it is the first one sent. */
    boolean abandonWritten(Pipe q) {
        Slot h = q.mp().health;
        h.abandonQueued = false;
        if (h.abandonSent) return false;
        h.abandonSent = true;
        return true;
    }

    /** The PIPE_ABANDON about p was acknowledged. */
    void abandonAcked(Pipe p) {
        Slot h = p.mp().health;
        h.abandonAcked = true;
        h.ackedAbandonAt = port.now();
        maybeRetire(p);
    }

    /** The PIPE_ABANDON about p was lost: it is queued again unless already acknowledged. */
    void abandonLost(Pipe p) {
        Slot h = p.mp().health;
        if (!h.abandonAcked) h.abandonQueued = true;
    }

    /** Fails the pipe without owing a PIPE_ABANDON for it (displacement while suspended, RESUME.md 4.3). */
    void failWithoutAbandon(Pipe p, long code) {
        p.mp().health.abandonSent = true;
        failPipe(p, code, null);
    }

    /** The failure-detection and removal state of one attachment (MULTIPATH.md 6.4, 7, 8). */
    static final class Slot {
        private int out;
        private int in;
        private boolean retired;
        private boolean draining;
        private boolean drainSuspect;
        private long drainStart;
        private boolean lingering;
        private long lingerUntil;
        private long suspectAt;
        private Throwable failCause;
        private boolean noSample;
        private long noSampleAt;
        private boolean resLimited;
        private boolean abandonSent;
        private boolean abandonQueued;
        private long abandonCode;
        private boolean abandonAcked;
        private boolean abandonRecv;
        private long abandonRecvAt;
        private long abandonCodeRecv;
        private boolean unilateral;
        private long ackedAbandonAt;
        private long suspects;
        private long failures;
        private boolean needPing;
        private long pingSince;
        private long pingsSent;
        private long pingsAnswered;
        /** The lane 0 positions at the end of the batches that hold unanswered PIPE_PINGs, in send order. */
        private final java.util.ArrayDeque<Long> pingEnds = new java.util.ArrayDeque<>(2);
        /** Why the attachment failed: the transport error code, -1 before. */
        private long failCode = -1;
        /** The uniform draw of the current probe wait; NaN: not drawn yet. */
        private double probeDraw = Double.NaN;
        /** H_p of the current probe wait, taken when the wait started; -1: not taken yet. */
        private long probeHold = -1;

        Slot(int out, int in) {
            this.out = out;
            this.in = in;
        }

        int out() {
            return out;
        }

        int in() {
            return in;
        }

        boolean retired() {
            return retired;
        }

        boolean draining() {
            return draining;
        }

        boolean drainSuspect() {
            return drainSuspect;
        }

        long drainStart() {
            return drainStart;
        }

        boolean lingering() {
            return lingering;
        }

        long lingerUntil() {
            return lingerUntil;
        }

        long suspectAt() {
            return suspectAt;
        }

        Throwable failCause() {
            return failCause;
        }

        boolean noSample() {
            return noSample;
        }

        long noSampleAt() {
            return noSampleAt;
        }

        boolean resLimited() {
            return resLimited;
        }

        boolean abandonSent() {
            return abandonSent;
        }

        boolean abandonQueued() {
            return abandonQueued;
        }

        long abandonCode() {
            return abandonCode;
        }

        boolean abandonAcked() {
            return abandonAcked;
        }

        boolean abandonRecv() {
            return abandonRecv;
        }

        long abandonRecvAt() {
            return abandonRecvAt;
        }

        long abandonCodeRecv() {
            return abandonCodeRecv;
        }

        boolean unilateral() {
            return unilateral;
        }

        long ackedAbandonAt() {
            return ackedAbandonAt;
        }

        long suspects() {
            return suspects;
        }

        long failures() {
            return failures;
        }

        boolean needPing() {
            return needPing;
        }

        long pingSince() {
            return pingSince;
        }

        long pingsAnswered() {
            return pingsAnswered;
        }

        long failCode() {
            return failCode;
        }

        long pingsSent() {
            return pingsSent;
        }
    }
}
