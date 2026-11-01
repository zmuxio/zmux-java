package io.zmux.internal.core;

import io.zmux.internal.core.CoreConfig.Role;
import io.zmux.internal.core.Lifecycle.CloseFrame;
import io.zmux.internal.wire.Gates;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.WireException;
import io.zmux.internal.wire.ChannelDatagram;
import io.zmux.internal.wire.FrameDecoder;
import java.util.List;

/**
 * The sans-IO state of one zmux session (IMPLEMENTATION.md Section 2.1). It is owned
 * by one thread, the session's event loop: nothing here is thread-safe, blocks,
 * performs I/O or calls application code. Inputs are records, carrier events,
 * API commands and timeouts; outputs are batches, events, carrier actions, wake-ups
 * and the next deadline.
 */
public final class Session {
    static final long MAX_62 = (1L << 62) - 1;

    final CoreConfig cfg;
    private long now;
    final long start;
    private SessionState state = SessionState.ESTABLISHING;

    private Role role;
    /** The scheduling policy; starts from the configuration and changes with {@link #setPolicy}. */
    private CoreConfig.Policy policy;
    private boolean roleKnown;
    private TransportParams peer = new TransportParams();
    TransportParams peer() { return peer; }
    private boolean peerSeen;
    private Features feat = Features.NONE;
    private Gates gates;

    final StreamTable streams;
    final StreamSend send;
    final StreamRecv recv;
    /** What the path layer sees of the streams. */
    final StreamsView streamsView;
    final StreamScheduler sched = new StreamScheduler();
    final Outbox outbox = new Outbox();
    /** The pipes by ID and in attachment order. */
    final PipeTable pipes = new PipeTable(outbox);
    final ControlQueue ctl = new ControlQueue(pipes::wakeWriters);
    final ConnFlow conn;
    final KeepAlive ka;
    final EarlyData early;
    /** Pipe acknowledgement and pacing; null without acknowledgement. */
    private AckEngine ack;
    private Pacer pacer;
    /** The resume grace deadline while SUSPENDED (RESUME.md 8). */
    private long grace = Time.NEVER;
    private volatile SessionError err;
    final Lifecycle life;
    final SessionStats stats = new SessionStats();
    final Datagrams dg;
    final WaitList closedW = new WaitList();
    final WaitList estW = new WaitList();
    private Waiter bcast;
    final MemoryCharge mem;
    final WindowTuner tuner;
    /** The path layer (IMPLEMENTATION.md 2.6): single pipe until multipath is negotiated. */
    private PathLayer path = singlePath();
    /** Quality events; null until a band or floor is set. */
    private QualityMonitor quality;
    QualityMonitor quality() { return quality; }
    private SessionState preSuspend = SessionState.ACTIVE;
    final Lanes lanes;
    final RecordReceiver rx;
    final Transmitter tx;

    /** Creates a session; the bootstrap pipe is attached with {@link #attachBootstrap}. */
    public Session(CoreConfig cfg, long now) {
        this.cfg = cfg;
        this.policy = cfg.policy();
        this.now = now;
        this.start = now;
        dg = new Datagrams(cfg, stats, outbox);
        TransportParams l = cfg.local();
        if (l.maxRecordSize == 0) l.maxRecordSize = TransportParams.DEFAULT_MAX_RECORD_SIZE;
        conn = new ConnFlow(l.initialMaxData, this::maxDataDue);
        mem = new MemoryCharge(cfg, conn, dg);
        tuner = new WindowTuner(cfg, conn, new TunerPort());
        if (cfg.role() != Role.AUTO) {
            role = cfg.role();
            roleKnown = true;
        } else if (!l.has(TransportParams.ROLE)) {
            role = cfg.transportRole();
            roleKnown = true;
        }
        gates = new Gates(l.has(TransportParams.DATAGRAM) ? l.maxDatagramFrameSize : 0,
                l.has(TransportParams.RESET_AT), false, false, false, false, false);
        ka = new KeepAlive(cfg, new KeepAlivePort(), outbox, now);
        life = new Lifecycle(Time.add(now, cfg.handshakeTimeout()));
        early = new EarlyData(cfg);
        StreamsPort sp = new StreamsPort();
        streamsView = sp;
        streams = new StreamTable(cfg, stats, outbox, life, early, dg, conn, mem, sched, ctl, sp);
        send = new StreamSend(stats, outbox, conn, sched, ctl, streams, sp);
        recv = new StreamRecv(cfg, stats, outbox, conn, tuner, ctl, streams, sp);
        IoPort io = new IoPort();
        lanes = new Lanes(cfg, pipes, outbox, life, io);
        rx = new RecordReceiver(cfg, stats, life, io);
        tx = new Transmitter(cfg, stats, pipes, lanes, streams, send, recv, sched, ctl, conn, tuner, ka, life, early, dg, io);
        if (early.active()) assume(early.assumed(), early.floor());
        dg.channels(new ChannelsPort());
        if (cfg.shaper() != null) setShaper(cfg.shaper());
    }

    /** Advances the session clock for API calls; the driver calls it before every operation. */
    public void setNow(long t) {
        if (t > now) now = t;
    }

    public long now() { return now; }
    public SessionState state() { return state; }
    /** The terminal error, null while open. */
    public SessionError error() { return err; }
    public Features features() { return feat; }
    public boolean initiator() { return role == Role.INITIATOR; }
    public boolean roleKnown() { return roleKnown; }
    /** The peer's parameters once established, else null. */
    public TransportParams peerParams() { return peerSeen ? peer : null; }
    public TransportParams localParams() { return cfg.local(); }
    /** Bootstrap transport direction, independent of negotiated protocol role. */
    public Role transportRole() { return cfg.transportRole(); }
    public CoreConfig config() { return cfg; }
    public SessionStats stats() { return stats; }

    /**
     * The session's statistics now, gauges included (API.md Section 7.2): RTTs of pipe 0
     * outside multipath, of the primary pipe in a multipath session.
     */
    public SessionStats.Snapshot statsSnapshot() {
        Pipe rp = path instanceof PathSession ps ? ps.primary() : pipes.get(0);
        long[] rtt = rp == null ? null : rttOf(rp, new io.zmux.PipeStats.RttSource[1]);
        long active = 0;
        for (Pipe p : pipes.all()) {
            PipeInfo.States s = pipeState(p.id);
            if (s != null && s.out() == PipeInfo.Out.ACTIVE) active++;
        }
        long retained = path.retained();
        var g = new SessionStats.Gauges(streams.count(), Math.max(0, conn.sendMemory() - retained), conn.buffered(), retained,
                active, rtt == null ? -1 : rtt[0], rtt == null ? -1 : rtt[1], rtt == null ? -1 : rtt[3], rtt == null ? -1 : rtt[2]);
        return stats.snapshot(ka.budgetTokens(), g);
    }
    public boolean established() { return peerSeen; }
    boolean isClosing() { return state.compareTo(SessionState.CLOSING) >= 0; }
    /** The session's streams: opening, accepting, identifiers, priorities, release. */
    public StreamTable streams() { return streams; }
    /** The sending parts of the session's streams. */
    public StreamSend sending() { return send; }
    /** The receiving parts of the session's streams. */
    public StreamRecv receiving() { return recv; }
    /** The session's pipes. */
    public PipeTable pipes() { return pipes; }
    public Outbox outbox() { return outbox; }
    public Transmitter transmitter() { return tx; }
    public WindowTuner tuner() { return tuner; }
    public MemoryCharge memory() { return mem; }
    public KeepAlive keepAlive() { return ka; }
    PathLayer path() { return path; }

    /** Whether the path layer has a custom pick request waiting for the application picker. */
    public boolean wantsPick() { return path.wantsPick(); }

    /** Fills req with the waiting pick request; false when none is waiting. */
    public boolean takePick(PathScheduler.PickRequest req) { return path.takePick(req); }

    /** Hands the application's answers (or its failure) for req back to the path layer. */
    public void answerPick(PathScheduler.PickRequest req, List<PathScheduler.PickAnswer> answers, boolean failed) {
        path.answerPick(req, answers, failed);
    }
    /** The lanes of the session's pipes. */
    public Lanes lanes() { return lanes; }

    /**
     * Attaches the bootstrap pipe (both directions); the driver then polls it for the first
     * record. With datagrams enabled, the session offers zmux_carrier_datagrams when the
     * carrier has a datagram channel, or a joined pipe may have one (EXTENSIONS.md 6.8).
     */
    public void attachBootstrap(CarrierInfo in, CarrierInfo out, Waiter writerWaiter) {
        TransportParams l = cfg.local();
        if (l.has(TransportParams.DATAGRAM) && (out != null && out.channel() != null || l.has(TransportParams.MULTIPATH))) {
            l.present |= TransportParams.CARRIER_DATAGRAMS;
        }
        pipes.attachBootstrap(in, out, writerWaiter);
        stats.pipeAdded();
    }

    // ---- establishment ----

    /** Runs when the first frame of the inbound bootstrap pipe decoded (SPEC.md Sections 5.1 and 6.1). */
    void processPeerParams(TransportParams p) throws WireException {
        peer = p;
        peerSeen = true;
        TransportParams l = cfg.local();
        boolean fRole = false;
        if (l.has(TransportParams.ROLE) && p.has(TransportParams.ROLE)) {
            fRole = true;
            int lr = l.role, pr = p.role;
            if (lr == TransportParams.ROLE_AUTO && pr == TransportParams.ROLE_AUTO) {
                int c = Long.compareUnsigned(l.roleNonce, p.roleNonce);
                if (c == 0) throw new WireException(TransportError.ROLE_CONFLICT, "equal role nonces");
                role = c > 0 ? Role.INITIATOR : Role.RESPONDER;
            } else if (lr == TransportParams.ROLE_AUTO) {
                role = pr == TransportParams.ROLE_INITIATOR ? Role.RESPONDER : Role.INITIATOR;
            } else if (pr == TransportParams.ROLE_AUTO || lr != pr) {
                role = lr == TransportParams.ROLE_INITIATOR ? Role.INITIATOR : Role.RESPONDER;
            } else {
                throw new WireException(TransportError.ROLE_CONFLICT, "both endpoints claim the same role");
            }
        } else {
            Role tr = cfg.transportRole();
            if (tr == Role.AUTO || (cfg.role() != Role.AUTO && cfg.role() != tr)) {
                throw new WireException(TransportError.ROLE_CONFLICT, "no transport role");
            }
            role = tr;
        }
        roleKnown = true;
        boolean goAway = both(l, p, TransportParams.GOAWAY);
        boolean preamble = both(l, p, TransportParams.PREAMBLE);
        boolean priority = both(l, p, TransportParams.PRIORITY);
        boolean flows = both(l, p, TransportParams.FLOWS) && l.has(TransportParams.DATAGRAM);
        boolean carrierDatagrams = both(l, p, TransportParams.CARRIER_DATAGRAMS) && l.has(TransportParams.DATAGRAM);
        boolean multipath = false, jsu = false;
        if (both(l, p, TransportParams.MULTIPATH)) {
            int sel = l.secretFlags & p.secretFlags;
            if ((sel & (TransportParams.SECRET_EXPORTER | TransportParams.SECRET_PSK | TransportParams.SECRET_INSECURE)) != 0) {
                multipath = true;
            } else {
                jsu = true;
            }
        }
        boolean pipeAck = both(l, p, TransportParams.PIPE_ACK) && !multipath;
        boolean resume = multipath && both(l, p, TransportParams.RESUME) && (l.locator.length > 0 || p.locator.length > 0);
        boolean acked = multipath || pipeAck;
        boolean lanesOut = p.has(TransportParams.LANES) && p.maxLanes > 1 && acked;
        boolean lanesIn = l.has(TransportParams.LANES) && l.maxLanes > 1 && acked;
        feat = new Features(fRole, goAway, preamble, priority, flows, multipath, pipeAck, resume, lanesOut, lanesIn, jsu, carrierDatagrams);
        gates = new Gates(gates.maxDatagram(), gates.resetStreamAt(), goAway, priority, multipath, pipeAck, lanesIn);
        tx.peerRecordLimit(p.maxRecordSize);
        dg.peer(p.has(TransportParams.DATAGRAM) ? p.maxDatagramFrameSize : 0, p.maxRecordSize, flows);
        conn.sendLimit(p.initialMaxData);
        streams.peerLimits(p.maxStreamsBidi, p.maxStreamsUni);
        earlyOutcome();
        streams.applyPeerParamsAll();
        if (acked) {
            pacer = new Pacer(cfg, p.has(TransportParams.ACK_THRESHOLD) ? p.ackThreshold : 65536, AckEngine.peerDelay(p), pipes.all(),
                    new PacerPort());
            ack = new AckEngine(l, p, pipes.all(), pacer, new AckPort());
        }
        if (multipath) path = multiPath();
        if (state == SessionState.ESTABLISHING) state = SessionState.ACTIVE;
        life.established();
        ka.start(idleTimeout(), now);
        emit(Event.established(feat, role == Role.INITIATOR));
        if (jsu) emit(Event.simple(Event.Kind.JOIN_SECURITY_UNAVAILABLE));
        estW.wakeAll(outbox);
        pipes.wakeWriters();
        streams.wakeOpeners();
    }

    private static boolean both(TransportParams l, TransportParams p, int bit) {
        return l.has(bit) && p.has(bit);
    }

    /** Registers sig until establishment or close; true when the condition holds now. */
    public boolean waitEstablished(Waiter sig) {
        if (peerSeen || isClosing()) return true;
        estW.add(sig);
        return false;
    }

    /** Registers sig until CLOSED; true when the session is closed now. */
    public boolean waitClosed(Waiter sig) {
        if (state == SessionState.CLOSED) return true;
        closedW.add(sig);
        return false;
    }

    public void cancelClosedWait(Waiter sig) { closedW.remove(sig); }

    /** The negotiated idle timeout (SPEC.md Section 6.4), 0 when none. */
    private long idleTimeout() {
        long a = cfg.local().maxIdleTimeoutMillis, b = peer.maxIdleTimeoutMillis;
        if (a == 0 && b == 0) return 0;
        if (a == 0) return Time.millis(b);
        if (b == 0 || a < b) return Time.millis(a);
        return Time.millis(b);
    }

    /** The session SRTT of SPEC.md Section 1.3. */
    long srtt() {
        if (cfg.test().rtt() > 0 && ack == null) return cfg.test().rtt();
        if (ack != null) {
            long r = srttMax();
            if (r > 0) return r;
        }
        if (cfg.test().rtt() > 0) return cfg.test().rtt();
        return 333 * Time.MILLISECOND;
    }

    /** min(2 s, max(250 ms, 3 x SRTT)) (SPEC.md Section 6.5). */
    long lingerTimeout() {
        return Math.clamp(Time.mul(srtt(), 3), 250 * Time.MILLISECOND, 2 * Time.SECOND);
    }

    // ---- closing ----

    /**
     * Enters CLOSING and queues CONNECTION_CLOSE fr on every live outbound pipe; with fr
     * null, or without a live outbound pipe, closes the carriers at once. The first error
     * wins (SPEC.md Section 6.5).
     */
    void closeWith(SessionError e, CloseFrame fr) {
        if (isClosing()) return;
        err = e;
        state = SessionState.CLOSING;
        failAllStreams();
        wakeAll();
        if (fr == null || !pipes.hasLiveOutbound()) {
            finishClose();
            return;
        }
        boolean holdForLanes = !feat.multipath() && !e.remote() && (fr.app() || fr.code() == TransportError.NO_ERROR) && !lanes.covered();
        life.closeQueued(fr, holdForLanes ? Time.add(now, lingerTimeout()) : Time.NEVER);
        pipes.wakeWriters();
    }

    /**
     * Ends a session whose peer has not been heard from without CONNECTION_CLOSE: its carriers
     * close at once, as when the handshake times out. False when the peer was heard from or the
     * session is closing already.
     */
    public boolean abandon() {
        if (peerSeen || isClosing()) return false;
        closeWith(SessionError.of(SessionError.Kind.HANDSHAKE_TIMEOUT, false), null);
        return true;
    }

    /** Closes the session with an application error (SPEC.md Section 6.5). */
    public void close(long code, byte[] reason) {
        closeWith(SessionError.application(code, reason == null ? "" : new String(reason, java.nio.charset.StandardCharsets.UTF_8), false),
                CloseFrame.of(true, code, 0, reason));
    }

    /** Closes after a failure inside the endpoint (IMPLEMENTATION.md Section 2.5.6): INTERNAL_ERROR. */
    public void forceClose(Throwable cause) {
        closeTransport(SessionError.transport(TransportError.INTERNAL_ERROR, 0, "internal error", "", false));
    }

    /** Closes with a locally detected transport error. */
    void failLocal(WireException e) {
        closeTransport(SessionError.transport(e.code(), e.frameType(), e.reason(), e.details(), false));
    }

    void closeTransport(SessionError e) {
        String rs = e.details().isEmpty() ? e.reason() : e.reason() + "\n" + e.details();
        byte[] b = rs.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (b.length > 1024) b = java.util.Arrays.copyOf(b, 1024);
        closeWith(e, CloseFrame.of(false, e.code(), e.frameType(), b));
    }

    /** Closes without CONNECTION_CLOSE; graceful shuts down the write sides and lingers. */
    void closeSilently(SessionError e, boolean graceful) {
        if (isClosing()) return;
        err = e;
        state = SessionState.CLOSING;
        failAllStreams();
        wakeAll();
        if (graceful) {
            pipes.shutdownOut();
            life.linger(Time.add(now, lingerTimeout()));
            emitClosedEvent();
            return;
        }
        finishClose();
    }

    /** Releases everything and enters CLOSED. */
    void finishClose() {
        if (state == SessionState.CLOSED) return;
        if (err != null && state != SessionState.CLOSING) failAllStreams();
        state = SessionState.CLOSED;
        mem.release();
        pipes.abortAll();
        life.clearTimers();
        grace = Time.NEVER;
        emitClosedEvent();
        closedW.wakeAll(outbox);
        releaseAll();
        wakeAll();
    }

    void emitClosedEvent() {
        if (!life.markClosedEmitted()) return;
        emit(Event.closed(err));
    }

    // ---- events, actions, wake-ups ----

    void emit(Event e) {
        if (quality != null && quality.floorSet() && (e.kind() == Event.Kind.PIPE_STATE || e.kind() == Event.Kind.PIPE_PRIORITY)) quality.checkFloor();
        outbox.event(e);
    }


    void act(Action a) {
        outbox.action(a);
    }


    /** Queues sig for delivery at the end of the driver's round. */
    void wake(Waiter w) {
        outbox.wake(w);
    }


    /** Queues every waiter of the session (close, GOAWAY). */
    void wakeAll() {
        streams.wakeObservers();
        streams.wakeWaiters();
        estW.wakeAll(outbox);
        ka.wakeAll();
        dg.wakeAll();
        wake(bcast);
    }

    /** Sets the session-level broadcast signal (waits that watch state). */
    public void setBroadcast(Waiter sig) {
        bcast = sig;
    }

    // ---- streams ----

    /** Remaining adapter lane allowance; application openers always take precedence. */
    public long nestedLaneAllowance() {
        if (!peerSeen || isClosing() || streams.openersWaiting(true)) return 0;
        return pipes.spreadable() ? peer.maxStreamsBidi / 2 : 0;
    }

    void failAllStreams() {
        streams.wakeObservers();
        send.wakeCapacity();
    }

    /** Drops every buffer of the session (CLOSED). */
    void releaseAll() {
        streams.releaseBuffers();
        dg.releaseAll();
        conn.clearBuffers();
        sched.clear();
        ctl.clear();
    }

    // ---- flow control ----

    /** Session credit is due: MAX_DATA goes out with the next control. */
    private void maxDataDue() {
        ctl.wantMaxData();
        pipes.wakeWriters();
    }

    /** Absolute send-buffer max, using the existing memory account including this round's uncharged growth. */
    long sendBufferLimit() {
        return mem.sendBufferLimit();
    }

    /** The single effective buffer used for fair sharing and affinity load. */
    long sessionSendBuffer() {
        return path.sendBuffer(cfg.sendBuffer());
    }

    // ---- input ----

    /**
     * Processes the complete records in data[pos, limit), read from an inbound lane, and
     * returns how many bytes it consumed; the caller keeps the rest (a partial record).
     * buf owns data; pieces of stream data may keep a reference to it.
     */
    public int receive(long pipeId, long lane, ReadBuf buf, java.nio.ByteBuffer data, int pos, int limit, long t) {
        setNow(t);
        path.pickInput();
        return rx.receive(pipes.get(pipeId), lane, buf, data, pos, limit, now);
    }

    /** Processes one message of a message-framed carrier (SPEC.md Section 3.4). */
    public void receiveMessage(long pipeId, long lane, ReadBuf buf, java.nio.ByteBuffer data, int pos, int limit, long t) {
        setNow(t);
        rx.receiveMessage(pipes.get(pipeId), lane, buf, data, pos, limit, now);
    }

    /** A record was applied: acknowledgement, attachment liveness and the idle timer. */
    private void recorded(Pipe p, Pipe.InLane il, long posBytes, boolean elicit, boolean pipePing, boolean pingOnly, boolean traffic,
                          boolean activity) {
        // A PIPE_PING alone in a quiet session is answered on its pipe pair only (MULTIPATH.md 6.5).
        boolean pairOnly = pingOnly && p.mp() != null && mp().pingAnsweredOnPair(p);
        if (ack != null && ack.onRecord(il, posBytes, elicit && !pairOnly, pipePing && !pairOnly, now)) pipes.wakeWriters();
        if (traffic) p.traffic(now);
        if (p.mp() != null) mp().recorded(p, il, traffic);
        if (!feat.acked() || activity) ka.activity(now);
    }

    /** Applies one decoded frame. */
    void dispatch(Pipe p, io.zmux.internal.wire.Frame f, ReadBuf buf) throws WireException {
        boolean multi = feat.multipath();
        switch (f.kind) {
            case PADDING, TRANSPORT_PARAMETERS -> { }
            case STREAM -> {
                if (!recv.onStream(f, buf) && !multi) charge();
            }
            case RESET_STREAM, RESET_STREAM_AT -> {
                if (!recv.onReset(f, f.kind == io.zmux.internal.wire.FrameKind.RESET_STREAM_AT) && !multi) charge();
            }
            case STOP_SENDING -> {
                Stream st = streams.lookup(f.streamId, Stream.isBidiId(f.streamId));
                if (st == null) return;
                if (!st.hasSend) {
                    throw WireException.withDetails(TransportError.STREAM_STATE_ERROR, "STOP_SENDING for a receive-only stream", "stream", st.tab.id());
                }
                if (st.snd.stopped() && !multi && st.snd.state() != Stream.SEND_DATA_RECVD && st.snd.state() != Stream.SEND_RESET_RECVD) charge();
                send.onStopSending(st, f.code);
            }
            case MAX_DATA -> {
                if (conn.onMaxData(f.value, now)) {
                    pipes.wakeWriters();
                    send.wakeCapacity();
                } else if (!multi) {
                    charge();
                }
            }
            case MAX_STREAM_DATA -> {
                Stream st = streams.lookup(f.streamId, Stream.isBidiId(f.streamId));
                if (st == null) return;
                if (!st.hasSend) {
                    throw WireException.withDetails(TransportError.STREAM_STATE_ERROR, "MAX_STREAM_DATA for a receive-only stream", "stream", st.tab.id());
                }
                if (!send.onSendCredit(st, f.value) && !multi && st.snd.state() < Stream.SEND_DATA_RECVD) charge();
            }
            case MAX_STREAMS -> {
                if (!streams.onMaxStreams(f.bidi, f.value) && !multi) charge();
            }
            case DATA_BLOCKED -> {
                if (f.value > conn.advertised()) {
                    throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "DATA_BLOCKED above the advertised limit", "value", f.value);
                }
                if (conn.onDataBlocked(f.value) && !multi) charge();
            }
            case STREAM_DATA_BLOCKED -> {
                if (!recv.onDataBlocked(f) && !multi) charge();
            }
            case STREAMS_BLOCKED -> {
                if (!streams.onStreamsBlocked(f.bidi, f.value) && !multi) charge();
            }
            case CONNECTION_CLOSE -> onClose(f);
            case DATAGRAM -> dg.onFrame(f, flows, false);
            case PING -> ka.onPing(f, feat.multipath(), now);
            case GOAWAY -> onGoAway(f);
            case PRIORITY_UPDATE -> onPriorityUpdate(f);
            case PIPE_JOIN -> throw WireException.protocol("PIPE_JOIN on a pipe of the session");
            case PIPE_ACK -> onPipeAck(f);
            case PIPE_LANE -> throw WireException.protocol("PIPE_LANE out of place");
            case PIPE_PING -> {
                charge(); // Every PIPE_PING (SPEC.md Section 9.4.1, NE-2).
                path.onPathFrame(p, f);
            }
            case PIPE_ABANDON, PIPE_STATUS -> path.onPathFrame(p, f);
        }
    }

    /** A received CONNECTION_CLOSE (SPEC.md Section 6.5). */
    void onClose(io.zmux.internal.wire.Frame f) {
        String reason = new String(f.dataBytes(), java.nio.charset.StandardCharsets.UTF_8);
        SessionError e;
        if (f.application) {
            e = SessionError.application(f.code, reason, true);
        } else if (f.code == TransportError.IDLE_TIMEOUT) {
            e = new SessionError(SessionError.Kind.IDLE, f.code, f.errorFrameType, "", "", null, true);
        } else {
            int i = reason.lastIndexOf('\n');
            e = i < 0 ? SessionError.transport(f.code, f.errorFrameType, reason, "", true)
                    : SessionError.transport(f.code, f.errorFrameType, reason.substring(0, i), reason.substring(i + 1), true);
        }
        closeWith(e, CloseFrame.of(false, TransportError.NO_ERROR, 0, null));
    }

    /**
     * Reports the end of an inbound lane: graceful (eof) or failed; partial when it ended
     * inside a record. The driver calls it after feeding every complete record it read.
     */
    public void pipeEnded(long pipeId, long lane, boolean graceful, boolean partial, Throwable cause, long t) {
        setNow(t);
        Pipe p = pipes.get(pipeId);
        if (p == null || p.in() == null) return;
        Pipe.InLane il = p.in().lane(lane);
        if (il == null || !rx.laneEnded(il)) return;
        boolean earlyEnd = il.lane > 0 && il.rx.recs() <= 1;
        boolean allEnded = true;
        for (Pipe.InLane l : p.in().lanes) if (!l.rx.ended()) { allEnded = false; break; }
        if (allEnded) p.in().end();
        if (isClosing()) {
            checkLingerDone();
            return;
        }
        path.inboundEnded(p, (!graceful || partial) && !earlyEnd, allEnded, cause);
    }

    /** The single pipe's inbound side ended. */
    private void singleInboundEnded(boolean failed, boolean allEnded, Throwable cause) {
        if (failed) {
            closeWith(SessionError.pipesLost(cause), null);
            return;
        }
        if (!allEnded) return;
        if (peerSeen && !feat.acked()) {
            closeSilently(SessionError.of(SessionError.Kind.IDLE, true), true);
            return;
        }
        closeWith(SessionError.pipesLost(null), null);
    }

    /** Reports a failed write on an outbound lane. */
    void pipeWriteFailed(long pipeId, long lane, Throwable cause) {
        if (isClosing()) {
            if (state == SessionState.CLOSING) {
                Pipe p = pipes.get(pipeId);
                if (p != null && p.out() != null) p.out().fail();
                checkLingerDone();
            }
            return;
        }
        if (lane > 0) {
            Pipe p = pipes.get(pipeId);
            if (p != null && p.out() != null) {
                Pipe.OutLane ol = p.out().lane(lane);
                if (ol != null && !ol.st.confirmed()) {
                    lanes.refused(p, ol);
                    return;
                }
            }
        }
        path.outboundFailed(pipeId, cause);
    }

    // ---- output ----

    /** The write batch. */
    public static final int DEFAULT_BATCH = 256 << 10;


    /**
     * Fills b with records for the outbound lane, at most max bytes; false when nothing
     * is due. The driver writes the batch and reports it with {@link #writeDone}.
     */
    public boolean pollTransmit(long pipeId, long lane, Batch b, int max, long t) {
        setNow(t);
        return tx.poll(pipeId, lane, b, max, t);
    }

    /** Reports that a batch was written (cause null) or failed; releases the batch's references. */
    public void writeDone(Batch b, Throwable cause, long t) {
        setNow(t);
        tx.written(b, t);
        if (cause != null) pipeWriteFailed(b.pipe(), b.lane(), cause);
    }

    /**
     * The carrier's unsent count of an outbound lane as the driver queried it (outside the
     * core), -1 when the adapter reports none.
     */
    public void unsentReported(long pipeId, long lane, long unsent) {
        tx.setUnsent(pipeId, lane, unsent);
    }

    /**
     * records &times; lost / sent rounded up for non-negative counts and sent &gt; 0, exact and
     * without overflowing the product; Long.MAX_VALUE when the quotient does not fit.
     */
    static long lostRecords(long lost, long records, long sent) {
        long n = lost * records;
        if (Math.multiplyHigh(lost, records) == 0 && n >= 0) return Math.ceilDiv(n, sent);
        java.math.BigInteger q = java.math.BigInteger.valueOf(lost).multiply(java.math.BigInteger.valueOf(records))
                .add(java.math.BigInteger.valueOf(sent - 1)).divide(java.math.BigInteger.valueOf(sent));
        return q.bitLength() < 64 ? q.longValue() : Long.MAX_VALUE;
    }

    /** The shortest time between two samples of a pipe's smoothed loss rate. */
    static final long LOSS_SAMPLE_GAP = Time.SECOND;

    /** Whether pipe's carrier retransmission count is due for a query (at most once per {@link #LOSS_SAMPLE_GAP}). */
    public boolean retransmittedDue(long pipeId, long t) {
        Pipe p = pipes.get(pipeId);
        return p != null && p.out() != null && p.out().retransmittedDue(t, LOSS_SAMPLE_GAP);
    }

    /**
     * Records the bytes the carrier of a pipe retransmitted, as its adapter reports them (-1:
     * it reports none), and samples the pipe's smoothed loss rate (PipeStats.lossRate).
     */
    public void retransmittedReported(long pipeId, long n, long t) {
        Pipe p = pipes.get(pipeId);
        if (p != null && p.out() != null) p.out().retransmitted(n, t, LOSS_SAMPLE_GAP);
    }

    /** Reports a failed outbound lane without a batch (the carrier failed). */
    public void writeFailed(long pipeId, long lane, Throwable cause, long t) {
        setNow(t);
        pipeWriteFailed(pipeId, lane, cause);
    }

    /** Starts the linger timer once CONNECTION_CLOSE was handed to every live outbound pipe (SPEC.md 6.5 step 3). */
    void closeSentOn() {
        if (!pipes.closeSentEverywhere()) return;
        pipes.shutdownOut();
        life.closeSent(Time.add(now, lingerTimeout()));
        emitClosedEvent();
        checkLingerDone();
    }

    /** Enters CLOSED once every inbound pipe ended. */
    void checkLingerDone() {
        if (state != SessionState.CLOSING) return;
        if (life.closePending()) {
            if (!life.waitingLanes()) closeSentOn();
            return;
        }
        if (pipes.inboundEnded()) finishClose();
    }

    // ---- pipes ----

    /** Records whether a carrier half accepts bytes now. */
    public void setWritable(long pipeId, long lane, boolean writable, long t) {
        setNow(t);
        tx.setWritable(pipeId, lane, writable, t);
    }

    /** The pipe state names of STATE_MACHINE.md Section 3 (out, in), or null when unknown. */
    public PipeInfo.States pipeState(long id) {
        Pipe p = pipes.get(id);
        if (p == null) return null;
        return path.pipeState(p);
    }

    /** Where a local join stands. */
    public enum JoinProgress { CONFIRMED, PENDING, GONE }

    /** Whether the join of pipe id is confirmed, still pending, or ended without confirmation. */
    public JoinProgress joinProgress(long id) {
        Pipe p = pipes.get(id);
        return p == null ? JoinProgress.GONE : path.joinProgress(p);
    }

    /** Physical readiness, independent of tier/standby selection by the traffic scheduler. */
    public boolean pipeUsable(long id) {
        Pipe p = pipes.get(id);
        return p != null && !isClosing() && path.pipeUsable(p);
    }

    private static io.zmux.PipeStats.SettingSource settingSource(Attach a, boolean local, boolean peerRequest) {
        if (a == null) return io.zmux.PipeStats.SettingSource.DEFAULT;
        return local ? io.zmux.PipeStats.SettingSource.LOCAL
                : peerRequest ? io.zmux.PipeStats.SettingSource.PEER : io.zmux.PipeStats.SettingSource.DEFAULT;
    }

    private static java.time.Duration nanos(long n) {
        return java.time.Duration.ofNanos(Math.max(0, n));
    }

    /** The RTT estimate of pipe p's outbound direction: {smoothed, min, variation, latest}, null without a sample; src[0] its source. */
    private long[] rttOf(Pipe p, io.zmux.PipeStats.RttSource[] src) {
        if (p.out() == null) return null;
        RttEstimator r = p.mp() == null ? p.out().snd.rtt() : p.mp().rtt;
        long[] v = r.has() ? new long[] {r.smoothed(), r.min(), r.rttvar(), r.latest()} : null;
        src[0] = v != null ? io.zmux.PipeStats.RttSource.ACK : null;
        if (p.mp() == null && tuner.carrierMinRtt() > 0) {
            if (v == null) v = new long[] {tuner.carrierMinRtt(), tuner.carrierMinRtt(), -1, tuner.carrierMinRtt()};
            v[1] = tuner.carrierMinRtt();
            src[0] = io.zmux.PipeStats.RttSource.KERNEL;
        }
        if (p.mp() == null && v == null && ka.lastRtt() > 0) {
            v = new long[] {ka.lastRtt(), ka.lastRtt(), -1, ka.lastRtt()};
            src[0] = io.zmux.PipeStats.RttSource.PING;
        }
        return v;
    }

    /**
     * The statistics of attachment id (API.md Section 7.2), at snapshotTime of the core's clock,
     * whose {@link System#nanoTime()} reading is nanoTime; null once it is missing or retired.
     */
    public io.zmux.PipeStats pipeStats(long id, long snapshotTime, long nanoTime) {
        Pipe p = pipes.get(id);
        if (p == null) return null;
        PipeInfo.States state = path.pipeState(p);
        Attach a = p.mp();
        boolean acks = a != null || feat.pipeAck();
        long sent = 0, received = 0, recordsIn = 0, acked = 0, recordsOut = 0, inflight = 0, lost = 0;
        double rate = 0;
        long unsent = -1;
        boolean writable = false;
        int lanesOut = 0, lanesIn = 0;
        long lanesRefused = 0;
        var lanes = new java.util.ArrayList<io.zmux.PipeStats.LaneStats>(0);
        if (p.out() != null) {
            for (Pipe.OutLane l : p.out().lanes) {
                sent += l.tx.pos();
                if (l.tx.unsent() >= 0) unsent = Math.max(unsent, 0) + l.tx.unsent();
            }
            lanesOut = p.out().lanes.size();
            lanesRefused = p.out().lanesRefused();
            acked = p.out().snd.cum();
            lost = p.out().bytesLost();
            recordsOut = p.out().records();
            inflight = p.out().snd.so() - Math.min(p.out().snd.so(), acked);
            rate = p.out().snd.re().value();
            Pipe.OutLane l = p.out().lane(0);
            writable = l != null && !l.tx.notWritable() && !l.tx.writing() && !Pacer.held(p);
        }
        if (p.in() != null) {
            for (Pipe.InLane l : p.in().lanes) { received += l.rx.pos(); recordsIn += l.rx.recs(); }
            lanesIn = p.in().lanes.size();
        }
        if (lanesOut > 1 || lanesIn > 1) {
            for (int i = 0; i < Math.max(lanesOut, lanesIn); i++) {
                Pipe.OutLane ol = p.out() != null ? p.out().lane(i) : null;
                Pipe.InLane il = p.in() != null && i < p.in().lanes.size() ? p.in().lanes.get(i) : null;
                lanes.add(new io.zmux.PipeStats.LaneStats(i,
                        ol == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(ol.tx.pos()),
                        ol == null || !acks ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(ol.ack.acked()),
                        il == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(il.rx.pos())));
            }
        }
        var src = new io.zmux.PipeStats.RttSource[1];
        long[] rtt = rttOf(p, src);
        boolean eligible = false, saturated = false;
        if (a != null && p.out() != null) {
            Tiers.Gate g = mp().tiers.buildGate();
            if (g != null) {
                eligible = mp().tiers.admits(g, p);
                int tier = g.tierOf(p);
                saturated = tier >= 0 && tier <= g.last() && g.tiers.get(tier).sat();
            }
        }
        long until = a != null ? mp().probation.probationEnd(p) : Long.MIN_VALUE;
        io.zmux.PipeStats.PeerRequest peer = a != null && (a.tier.peerSeq() > 0 || p.id != 0 && !a.joinerLocal)
                ? new io.zmux.PipeStats.PeerRequest(a.tier.peerPrio(), a.tier.peerStandby()) : null;
        boolean peerRequest = peer != null;
        boolean outLive = acks && p.out() != null;
        boolean live = state.out() == PipeInfo.Out.ACTIVE || state.out() == PipeInfo.Out.SUSPECT || state.in() == PipeInfo.In.OPEN;
        return new io.zmux.PipeStats(id, a == null ? initiator() : a.joinerLocal, a == null ? 0 : a.fate, p.label(), p.carrier(),
                state.out().toPublic(), state.in().toPublic(),
                nanos(a == null ? Time.sub(snapshotTime, start) : Time.sub(snapshotTime, a.joining.attachedAt())),
                a == null ? 0 : a.health.suspects(), a == null ? 0 : a.health.failures(),
                a == null || a.health.failCode() < 0 ? null : new io.zmux.PipeStats.Failure(a.health.failCode(), a.health.failCause()),
                a == null ? 0 : a.effPrio(), settingSource(a, a != null && a.tier.sendPrio() != null, peerRequest),
                a != null && a.effStandby(), settingSource(a, a != null && a.tier.sendStandby() != null, peerRequest),
                a == null ? 0 : a.tier.recvPrio(), a != null && a.tier.recvStandby(), a == null ? 0 : a.tier.statusSeq(),
                peer,
                a == null ? 1 : a.tier.weight(),
                until == Long.MIN_VALUE || until >= Time.NEVER / 2 ? java.util.OptionalLong.empty()
                        : java.util.OptionalLong.of(nanoTime + Time.sub(until, snapshotTime)),
                a != null && a.prob.probOn(), a != null && a.prob.reval(), eligible, writable, saturated,
                src[0], rtt == null ? null : nanos(rtt[0]), rtt == null ? null : nanos(rtt[1]),
                rtt == null || rtt[2] < 0 ? null : nanos(rtt[2]),
                a == null || p.out() == null ? null : nanos(mp().metrics.owd(p)),
                outLive ? java.util.OptionalLong.of((long) rate) : java.util.OptionalLong.empty(),
                // queued_bytes: the carrier's part of Q_p only, absent where no lane's adapter reports a count.
                unsent < 0 ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(unsent),
                unsent < 0 || !outLive || !(rate > 0) ? null : nanos((long) Math.min(Time.NEVER / 2.0, unsent / rate * Time.SECOND)),
                outLive ? java.util.OptionalLong.of(inflight) : java.util.OptionalLong.empty(), sent, received,
                outLive ? java.util.OptionalLong.of(acked) : java.util.OptionalLong.empty(), recordsOut, recordsIn,
                a == null ? 0 : a.ret.reinjectedBytes(),
                outLive ? java.util.OptionalLong.of(lost) : java.util.OptionalLong.empty(),
                outLive ? java.util.OptionalLong.of(sent == 0 ? 0 : lostRecords(lost, recordsOut, sent)) : java.util.OptionalLong.empty(),
                p.out() == null || p.out().retransmitted() < 0 ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(p.out().retransmitted()),
                p.out() == null || !p.out().lossKnown() ? java.util.OptionalDouble.empty() : java.util.OptionalDouble.of(p.out().lossRate()),
                a == null ? 0 : a.ret.oppBytes(), a == null ? 0 : a.ret.redundantBytes(),
                a == null ? 0 : a.ret.duplicateBytes(), a == null ? 0 : a.health.pingsSent(), a == null ? 0 : a.health.pingsAnswered(),
                a == null || p.out() == null ? null : nanos(Time.sub(snapshotTime, a.link.lastAdvance())),
                a == null || p.in() == null ? null : nanos(Time.sub(snapshotTime, a.link.lastRecord())),
                live ? nanos(Time.sub(snapshotTime, Math.max(p.lastTraffic(), a == null ? start : a.link.liveAt()))) : null,
                lanesOut, lanesIn, lanesRefused, lanes);
    }

    // ---- timers ----

    /** The earliest deadline, {@link Time#NEVER} when none (T2: every deadline source is here). */
    public long nextTimeout() {
        if (state == SessionState.CLOSED) return Time.NEVER;
        long t = life.deadline();
        if (isClosing()) return t;
        t = Math.min(t, lanes.deadline());
        if (state == SessionState.SUSPENDED) t = Math.min(t, grace);
        t = Math.min(t, ka.deadline(peerSeen && state != SessionState.SUSPENDED));
        if (ack != null) {
            t = Math.min(t, ack.deadline());
            t = Math.min(t, pacer.deadline(!sched.isEmpty(), now));
            if (quality != null) t = Math.min(t, quality.timers(now, false));
        }
        t = Math.min(t, path.deadline(now));
        if (pipes.shaping() != null) t = Math.min(t, pipes.shaping().deadline());
        return Math.min(t, dg.heldDeadline());
    }

    /** Runs every timer that is due at t. */
    public void handleTimeout(long t) {
        setNow(t);
        path.pickInput();
        if (state == SessionState.CLOSED) return;
        if (life.handshakeDue(t)) {
            if (!peerSeen) {
                closeWith(SessionError.of(SessionError.Kind.HANDSHAKE_TIMEOUT, false), null);
                return;
            }
        }
        if (life.lingerDue(t)) {
            finishClose();
            return;
        }
        if (life.laneWaitDue(t)) {
            pipes.wakeWriters();
        }
        if (isClosing()) return;
        lanes.onTimeout(t);
        if (state == SessionState.SUSPENDED && t >= grace) {
            // Grace expiry (RESUME.md 8): CONNECTION_CLOSE on any live outbound pipe, otherwise a local close.
            grace = Time.NEVER;
            closeWith(new SessionError(SessionError.Kind.RESUME_TIMEOUT, TransportError.RESUME_TIMEOUT, 0, "", "",
                    mp().resume.cause(), false), CloseFrame.of(false, TransportError.RESUME_TIMEOUT, 0, null));
            return;
        }
        if (ka.onTimeout(t, peerSeen && state != SessionState.SUSPENDED)) {
            idleExpired();
            return;
        }
        if (ack != null) {
            if (ack.onTimeout(now)) pipes.wakeWriters();
            pacer.onTimeout(t);
        }
        path.onTimeout(t);
        dg.expireHeld(t);
        if (ack != null && quality != null) quality.timers(t, true);
        if (pipes.shaping() != null) pipes.shaping().timeout(t);
    }

    /** Closes on the idle timeout (SPEC.md Section 6.4). */
    void idleExpired() {
        SessionError e = SessionError.of(SessionError.Kind.IDLE, false);
        if (feat.acked()) closeWith(e, CloseFrame.of(false, TransportError.IDLE_TIMEOUT, 0, null));
        else closeSilently(e, true);
    }



    // ---- pipe acknowledgement (MULTIPATH.md Sections 3.5 and 6) and pacing (SCHEDULER.md Section 9.1) ----

    /** The largest smoothed RTT over outbound pipes with a sample. */
    long srttMax() {
        return path.srttMax();
    }

    /** Processes a PIPE_ACK (MULTIPATH.md Sections 3.5 and 6.2). */
    private final io.zmux.internal.wire.Frame.AckEntries ackIt = new io.zmux.internal.wire.Frame.AckEntries();

    void onPipeAck(io.zmux.internal.wire.Frame f) throws WireException {
        path.onPipeAck(f.entries(ackIt));
    }

    private final class AckPort implements AckEngine.Port {
        @Override public void activity() { ka.activity(now); }
        @Override public void rttSampled(long min) { tuner.loopRtt(min, now); }
        @Override public boolean quiet() { return path instanceof PathSession ps && ps.quiet(); }

        @Override public void advanced(Pipe p, Pipe.OutLane ol, boolean cum) {
            if (cum) {
                if (quality != null) quality.sample(p);
                if (p.mp() != null) mp().advanced(p, ol);
            } else if (p.mp() != null) {
                mp().laneAdvanced(p);
            }
            lanes.acked(p, ol);
        }
    }

    private final class PacerPort implements Pacer.Port {
        @Override public void wakeAllWriters() { pipes.wakeWriters(); }
        @Override public void wakePipe(Pipe p) { pipes.wake(p); }
    }

    // ---- receive-window auto-tuning (SPEC.md Section 8.3 rule 6) and the memory budget ----





    /** The largest session window the memory budget allows. */
    long connAllowance() {
        return mem.windowAllowance(path.retained());
    }

    private final class TunerPort implements WindowTuner.Port {
        @Override public boolean multipath() { return path instanceof PathSession; }
        @Override public long inputPipeMinRtt() { return pipes.minRtt(rx.rxPipe()); }
        @Override public long bufferCap() { return streams.bufferCap(); }
        @Override public long allowance() { return connAllowance(); }
        @Override public boolean pressure() { return underPressure(); }
        @Override public boolean closing() { return isClosing(); }
    }


    /** The listener's shared budget, or null for a standalone session. */
    public SharedMemory sharedBudget() { return cfg.budget(); }



    /** Listener memory pressure: the session behaves as at its buffer cap and refuses new peer streams. */
    boolean underPressure() {
        return mem.pressure();
    }

    /** Closes a session the listener does not take (CONNECTION_REFUSED after its own parameters). */
    public void refuse() {
        closeTransport(SessionError.transport(TransportError.CONNECTION_REFUSED, 0, "memory budget", "", false));
    }

    // ---- early data (SPEC.md Section 6.1.2) ----

    /** Whether sent stream data must be kept: carrier early data not yet accepted. */
    boolean holdSends() {
        return early.holdSends();
    }

    /** The carrier accepted its early data: the early sends are final. */
    public void carrierEarlyAccepted() {
        early.carrierAccepted();
        releaseProvisional();
    }

    void releaseProvisional() {
        if (holdSends() || path.retainsSent()) return;
        for (Stream st : streams.snapshot()) {
            if (!st.local || !st.hasSend) continue;
            StreamSend.SendPart sp = st.snd;
            send.release(st, Math.min(sp.sent(), sp.q().end()));
            if (sp.finSent() && sp.state() == Stream.SEND_DATA_SENT && !sp.reset()) send.sendTerminal(st, Stream.SEND_DATA_RECVD);
        }
    }

    /**
     * Restarts the bootstrap pipe after the carrier rejected its early data, on the byte
     * stream the restart hook returned; false, changing nothing, when the session cannot
     * restart (the peer's parameters already arrived, or the session is closing).
     */
    public boolean restartEarly(CarrierInfo info, long t) {
        setNow(t);
        Pipe p = pipes.get(0);
        if (p == null || peerSeen || isClosing()) return false;
        pipes.restartBootstrap(p, info);
        conn.restartSend();
        early.restarted();
        peer = new TransportParams();
        for (Stream st : streams.live()) {
            if (!st.local || !st.hasSend) continue;
            if (StreamSend.restart(st)) ctl.stream(st, ControlQueue.RESET);
            streams.restartOpen(st);
            sched.ensure(st);
        }
        pipes.wake(p);
        return true;
    }

    /** Sends early under the assumed parameters until the peer's arrive. */
    private void assume(TransportParams assumed, boolean floor) {
        peer = assumed;
        boolean pre = cfg.local().has(TransportParams.PREAMBLE) && assumed.has(TransportParams.PREAMBLE);
        feat = new Features(false, false, pre, false, false, false, false, false, false, false, false, false);
        conn.sendLimit(assumed.initialMaxData);
        streams.assumedLimits(assumed.maxStreamsBidi, assumed.maxStreamsUni, floor);
    }

    boolean earlyActive() {
        return early.active() && !peerSeen;
    }

    /** Decides the fate of early data when the peer's parameters arrive (SPEC.md Section 6.1.2, Outcome). */
    void earlyOutcome() throws WireException {
        switch (early.outcome(peer, feat.acked())) {
            case NONE -> {}
            case ADOPT -> earlyAdopt();
            case REJECTED -> closeWith(SessionError.of(SessionError.Kind.EARLY_REJECTED, false), CloseFrame.of(false, TransportError.NO_ERROR, 0, null));
            case BELOW -> throw WireException.param("peer parameters below the assumed set");
        }
    }

    /** Raises the early streams' credit to the peer's values. */
    void earlyAdopt() {
        for (Stream st : streams.live()) {
            if (st.local && st.hasSend) {
                long v = st.bidi ? peer.maxStreamDataBidiRemote : peer.maxStreamDataUni;
                if (StreamSend.raiseCredit(st, v)) sched.ensure(st);
            }
        }
    }


    public void carrierHandshakeDone() {
        early.carrierHandshakeDone();
        for (Stream st : streams.live()) st.replayable(false);
    }

    // ---- QX_PING (SPEC.md Section 4.5) and the no-effect budget (Section 9.4) ----

    /** Asks for a QX_PING request, or joins the outstanding one; returns a ticket. */
    public long ping(Waiter w) throws CoreException {
        if (err != null) throw CoreException.session(err);
        return ka.ping(w);
    }

    /** The RTT of a completed ticket, or -1 while it is pending. */
    public long pingResult(long ticket) throws CoreException {
        long r = ka.result(ticket);
        if (r < 0 && err != null) throw CoreException.session(err);
        return r;
    }


    /** Charges one no-effect frame (SPEC.md Section 9.4.2). */
    void charge() {
        ka.charge(now);
    }


    private final class KeepAlivePort implements KeepAlive.Port {
        @Override public void wantWrite() { pipes.wakeWriters(); }
        @Override public void budgetExhausted() { failLocal(new WireException(TransportError.EXCESSIVE_LOAD, "no-effect budget exhausted")); }
        @Override public void pingMeasured(long rtt) { tuner.loopRtt(rtt, now); if (pacer != null) pacer.onPing(pipes.get(0), rtt, now); }
        @Override public void fenceReached() { fenceDone(); }
    }

    // ---- GOAWAY (EXTENSIONS.md Section 3) and PRIORITY_UPDATE (Section 5) ----

    /** Starts a graceful close: the two-phase GOAWAY when it is in use, then CONNECTION_CLOSE once no stream remains. */
    public void shutdown(long code, byte[] reason) {
        if (isClosing()) return;
        life.shutdownRequested(code, reason);
        streams.wakeOpenWaiters();
        if (!feat.goAway() || !peerSeen) {
            checkShutdownDone();
            return;
        }
        if (life.beginGoAway(streams.remoteLimit(0), streams.remoteLimit(1))) {
            ctl.wantGoAway();
            ka.startFence();
            if (state == SessionState.ACTIVE) state = SessionState.DRAINING;
            pipes.wakeWriters();
        }
        checkShutdownDone();
    }

    /** The final GOAWAY (EXTENSIONS.md Section 3.3 phase 2). */
    void fenceDone() {
        ka.clearFence();
        boolean[] lowered = {false};
        if (!life.finalGoAway(lowered)) return;
        if (lowered[0]) {
            ctl.wantGoAway();
            pipes.wakeWriters();
        }
        streams.rejectAboveWater();
        checkShutdownDone();
    }

    /** Completes what an operation left pending; the driver calls it at the end of every round. */
    public void settle() {
        if (life.takeRecheck()) checkShutdownDone();
    }

    void checkShutdownDone() {
        if (!life.shutdownReady() || isClosing() || !streams.isEmpty()) return;
        close(life.shutdownCode(), life.shutdownReason());
    }

    void onGoAway(io.zmux.internal.wire.Frame f) {
        boolean lowered = life.onGoAway(f);
        if (!lowered) {
            if (!feat.multipath()) charge();
            return;
        }
        if (state == SessionState.ACTIVE) state = SessionState.DRAINING;
        for (Stream st : streams.locals()) {
            if (!st.tab.idSet()) {
                StreamError e = StreamError.of(StreamError.Kind.GOING_AWAY, 0);
                StreamSend.error(st, e, false);
                StreamRecv.error(st, e, false);
                send.cancelUnopened(st);
                wake(st.snd.wsig());
                wake(st.rcv.rsig());
                continue;
            }
            if ((st.tab.id() >>> 2) >= life.recvWater(Stream.typeIndex(st.bidi))) {
                StreamError e = StreamError.of(StreamError.Kind.REJECTED, st.tab.id());
                if (st.hasSend) StreamSend.error(st, e, true);
                if (st.hasRecv) StreamRecv.error(st, e, true);
                wake(st.snd.wsig());
                wake(st.rcv.rsig());
            }
        }
        streams.wakeOpenWaiters();
        emit(Event.goAway(life.recvWater(0), life.recvWater(1), f.code));
    }

    void onPriorityUpdate(io.zmux.internal.wire.Frame f) throws WireException {
        long id = f.streamId;
        if (!Stream.isBidiId(id) && !StreamTable.isLocalId(id, role == Role.RESPONDER)) {
            throw WireException.withDetails(TransportError.STREAM_STATE_ERROR, "PRIORITY_UPDATE for the sender's unidirectional stream", "stream", id);
        }
        Stream st = streams.lookup(id, Stream.isBidiId(id));
        if (st == null || !st.hasSend) return;
        if (st.snd.state() == Stream.SEND_DATA_RECVD || st.snd.state() == Stream.SEND_RESET_RECVD) return;
        if (!streams.applyPeerPriority(st, f.seq, f.urgency, f.incremental) && !feat.multipath()) charge();
    }


    // ---- datagrams (EXTENSIONS.md Section 6) ----

    /** The session's datagram queues. */
    public Datagrams datagrams() {
        return dg;
    }

    /** Queues a datagram; a full queue drops its oldest entry. st is null for a session datagram. */
    public void sendDatagram(Stream st, long context, byte[] payload) throws CoreException {
        if (err != null) throw CoreException.session(err);
        if (!cfg.datagrams()) throw CoreException.of(CoreException.Reason.NOT_NEGOTIATED);
        if (st != null) {
            if (peerSeen && !feat.flows() || !st.bidi) throw CoreException.of(CoreException.Reason.NOT_NEGOTIATED);
            if (!st.tab.idSet()) streams.pinId(st);
        }
        dg.send(st, context, payload, feat.flows() || !peerSeen && cfg.local().has(TransportParams.FLOWS));
        pipes.wakeWriters();
    }

    // ---- datagram channels (EXTENSIONS.md Section 6.8) ----

    private final Datagrams.FlowLookup flows = new Datagrams.FlowLookup() {
        @Override public Stream flow(long id) throws WireException { return streams.flow(id); }
        @Override public boolean unopened(long id) { return streams.unopenedPeer(id); }
        @Override public long holdUntil() { return Time.add(now, srtt()); }
    };

    /** The pipe DATAGRAM frames go to now: the primary pipe of a multipath session, else the outbound pipe. */
    Pipe datagramPipe() {
        if (path instanceof PathSession mp) return mp.primary();
        for (Pipe p : pipes.all()) if (p.out() != null && !p.out().failed()) return p;
        return null;
    }

    /**
     * How DATAGRAM frames for p may use its carrier's datagram channel: when both endpoints sent
     * zmux_carrier_datagrams, the carrier has a channel that takes datagrams now, and p's
     * records are not shaped.
     */
    Datagrams.ChannelUse channelOf(Pipe p) {
        if (!feat.carrierDatagrams() || p == null || p.out() == null || p.out().failed()) return Datagrams.ChannelUse.NONE;
        Pipe.OutLane l0 = p.out().lane(0);
        DatagramChannel ch = l0 == null ? null : l0.st.info().channel();
        if (ch == null || pipes.shaping() != null && pipes.shaping().of(p) != null) return Datagrams.ChannelUse.NONE;
        int room = ch.maxChannelDatagram();
        return room <= 0 ? Datagrams.ChannelUse.NONE : new Datagrams.ChannelUse(room, ch.channelStreamId());
    }

    private final class ChannelsPort implements Datagrams.Channels {
        @Override public Pipe datagramPipe() { return Session.this.datagramPipe(); }
        @Override public Datagrams.ChannelUse of(Pipe p) { return channelOf(p); }
    }

    /**
     * Appends to dst the channel datagrams due on the datagram channel of pipe, in order; the
     * driver sends each at once, never behind a batch. They count as sent;
     * {@link #channelSendFailed} counts those the carrier refused as dropped instead.
     */
    public void pollChannel(long pipeId, List<byte[]> dst, List<Datagrams.ChannelSent> sent, long t) {
        setNow(t);
        if (!peerSeen || isClosing()) return;
        Pipe p = pipes.get(pipeId);
        if (p == null || p != datagramPipe()) return;
        dg.pollChannel(channelOf(p), dst, sent);
    }

    /** A channel datagram that {@link #pollChannel} returned was refused by the carrier. */
    public void channelSendFailed(Datagrams.ChannelSent c) {
        dg.channelRefused(c);
    }

    private final FrameDecoder channelDecoder = new FrameDecoder();
    private final io.zmux.internal.wire.Frame channelFrame = new io.zmux.internal.wire.Frame();

    /**
     * Processes a channel datagram received for pipe, b[pos, end) without its Pipe Stream ID
     * (EXTENSIONS.md 6.8). It is not a record: it advances no PIPE_ACK position, is not
     * charged to the no-effect budget and restarts no idle timer. The payload is copied.
     */
    public void receiveChannelDatagram(long pipeId, java.nio.ByteBuffer b, int pos, int end, long t) {
        setNow(t);
        if (isClosing()) return;
        if (pipes.get(pipeId) == null || !peerSeen || !feat.carrierDatagrams()) {
            dg.dropReceived();
            return;
        }
        io.zmux.internal.wire.Frame f = channelFrame;
        try {
            ChannelDatagram.decodeFrame(channelDecoder, b, pos, end, f);
            gates.check(f);
            dg.onFrame(f, flows, true);
        } catch (WireException e) {
            failLocal(e);
        } finally {
            f.src = null;
        }
    }

    /** Applies a receive-limiter decision once; true when the datagram reached the application's queue. */
    public boolean deliverDatagram(Datagrams.Limited token, boolean drop) {
        return dg.deliver(token, drop, isClosing());
    }

    // ---- multipath API (MULTIPATH.md, API.md Section 4) ----

    private PathSession requireMultipath() throws CoreException {
        if (!(path instanceof PathSession mp)) throw CoreException.of(CoreException.Reason.NOT_NEGOTIATED);
        if (err != null) throw CoreException.session(err);
        return mp;
    }

    /** The handshake timeout of the session's carriers. */
    public boolean preambleInUse() { return peerSeen ? feat.preamble() : cfg.local().has(TransportParams.PREAMBLE); }

    public long handshakeTimeout() {
        return cfg.handshakeTimeout();
    }

    /** The configured attachment limit (MULTIPATH.md max_pipes). */
    public int maxPipes() {
        return cfg.maxPipes();
    }


    /** Starts a local join; returns the pipe ID, or -id - 1 when the cap or the refusal backoff holds it. */
    public long addPipe(Joins.JoinOptions o) throws CoreException {
        return requireMultipath().joins.addPipe(o);
    }

    /** Reserves protocol identity and credentials, without polling or fabricating carrier writes. */
    public Joins.Reservation reservePipe(Joins.JoinOptions options) throws CoreException {
        return requireMultipath().joins.reservePipe(options);
    }

    /** Steps 4 to 9 of MULTIPATH.md 5.4 for a PIPE_JOIN that named this session (decoded in f). */
    public Joins.JoinResult acceptJoin(io.zmux.internal.wire.Frame f, byte[] macInput, byte[] binding,
                                           Joins.JoinCarrier c) {
        if (isClosing()) return Joins.JoinResult.CLOSE;
        if (!peerSeen) return Joins.JoinResult.HOLD;
        return path instanceof PathSession mp ? mp.joins.acceptJoin(f, macInput, binding, c) : Joins.JoinResult.CLOSE;
    }

    /** The session's own locator (joins name it). */
    public byte[] locator() {
        return cfg.local().locator;
    }

    public void removePipe(long id, boolean graceful) throws CoreException {
        if (!requireMultipath().health.removePipe(id, graceful)) throw CoreException.of(CoreException.Reason.INVALID);
    }

    public void setPipe(long id, Tiers.PipeSettings o) throws CoreException {
        if (!requireMultipath().tiers.setPipe(id, o)) throw CoreException.of(CoreException.Reason.INVALID);
    }

    public void revalidatePipes(long id, boolean all) throws CoreException {
        if (!requireMultipath().probation.revalidate(id, all)) throw CoreException.of(CoreException.Reason.INVALID);
    }

    /** Changes the session's scheduling policy; the previous policy's state is discarded. */
    public void setPolicy(CoreConfig.Policy p) {
        policy = p;
        path.policyChanged();
        pipes.wakeWriters();
    }

    /** Sets a stream direction's own policy; null returns it to the session's. */
    public void setStreamPolicy(Stream st, CoreConfig.Policy p) {
        PathScheduler.streamPolicy(st, p);
        pipes.wakeWriters();
    }

    /** Overrides the session's redundancy for a stream direction; null returns it to the default. */
    public void setRedundant(Stream st, Boolean on) {
        PathScheduler.streamRedundant(st, on == null ? 0 : on ? 1 : -1);
        pipes.wakeWriters();
    }

    /** Whether maintenance has target room and a local join may start now. */
    public boolean joinReady() {
        return !isClosing() && path.joinReady();
    }

    /** Closes a suspended session with ZMUX_RESUME_TIMEOUT (a listener's limit, RESUME.md 9). */
    public void closeResumeTimeout() {
        if (state != SessionState.SUSPENDED) return;
        closeWith(new SessionError(SessionError.Kind.RESUME_TIMEOUT, TransportError.RESUME_TIMEOUT, 0, "", "",
                mp().resume.cause(), false), CloseFrame.of(false, TransportError.RESUME_TIMEOUT, 0, null));
    }

    /** [live outbound, live inbound] directions. */
    public boolean[] liveDirections() {
        return path.liveDirections(isClosing());
    }

    // ---- record shaping ----

    private Shaping shaping() {
        if (pipes.shaping() == null) {
            pipes.shaping(new Shaping(new Shaping.Port() {
                @Override public long now() { return now; }
                @Override public int peerRecordLimit() { return tx.peerRecordLimit(); }
                @Override public boolean peerZmux() { TransportParams p = peerParams(); return p != null && p.isZmux(); }
                @Override public long bytesSent() { return stats.bytesSent(); }
                @Override public boolean dataWaiting() { return !sched.isEmpty(); }
            }, pipes, cfg.shapingLimits(), ka.outstandingSentAt()));
        }
        return pipes.shaping();
    }

    /** Installs f on every outbound pipe, replacing earlier instances; null: no shaping. */
    public void setShaper(io.zmux.shaping.ShaperFactory f) {
        shaping().setFactory(f);
    }

    /** Installs x on one outbound pipe until the next setShaper; false when the pipe has no outbound direction. */
    public boolean setPipeShaper(long pipe, io.zmux.shaping.Shaper x) {
        return shaping().setPipe(pipe, x);
    }

    /** A value that grows whenever a shaping counter changes; -1 when no shaper was ever set. */
    public long shapingSignature() {
        return pipes.shaping() == null ? -1 : pipes.shaping().signature();
    }

    /** The shaping counters; null when no shaper was ever set. */
    public io.zmux.shaping.ShapingStats shapingStats() {
        return pipes.shaping() == null ? null : pipes.shaping().stats();
    }

    /** Sets the quality band of one pipe, or (pipe negative) the default of the session's pipes. */
    public void setQualityBand(long pipe, long rtt, double rateRatio, long minInterval) throws CoreException {
        QualityMonitor.Band b = new QualityMonitor.Band(rtt, rateRatio, minInterval); // minInterval 0: no floor
        if (quality == null) quality = new QualityMonitor(this);
        Pipe p = null;
        if (pipe >= 0) {
            p = pipes.get(pipe);
            if (p == null || p.out() == null) throw CoreException.of(CoreException.Reason.INVALID);
        }
        quality.band(p, b);
    }

    /** Sets the capacity floor; it reports at once when the capacity is below it. */
    public void setCapacityFloor(double rate, double ratio) {
        if (quality == null) quality = new QualityMonitor(this);
        quality.setFloor(rate, ratio);
    }




    /** The multipath layer; only where multipath state (an {@link Attach}, a decision) shows it is in use. */
    PathSession mp() {
        return (PathSession) path;
    }

    /** A multipath layer of this session over its current pipes. */
    PathSession multiPath() {
        return new PathSession(cfg, peer, role == Role.INITIATOR, feat, stats, pipes, lanes, streamsView, sched, ack, ctl, conn, tx, ka, life,
                outbox, now, new PathPort());
    }

    /** A single-pipe path layer of this session. */
    PathLayer singlePath() {
        return new SinglePath(new SinglePort());
    }

    /** The single-pipe behaviour behind {@link SinglePath}. */
    private final class SinglePort implements SinglePath.Port {
        @Override public boolean wantsWrite(Pipe p, Pipe.OutLane ol) { return tx.wantsWriteSingle(p, ol); }
        @Override public void write(RecordWriter w, Pipe p, Pipe.OutLane ol) { tx.writeSingle(w, p, ol); }

        @Override public void onPipeAck(io.zmux.internal.wire.Frame.AckEntries it) throws WireException { ack.onSinglePipeAck(it, pipes.get(0), now); }
        @Override public void inboundEnded(boolean failed, boolean allEnded, Throwable cause) { singleInboundEnded(failed, allEnded, cause); }
        @Override public void outboundFailed(Throwable cause) { closeWith(SessionError.pipesLost(cause), null); }
        @Override public long srttMax() { return pipes.srttMax(); }
    }

    /** The session behind the multipath layer. */
    private final class PathPort implements PathSession.Port {
        @Override public long now() { return now; }
        @Override public CoreConfig.Policy policy() { return policy; }
        @Override public void emit(Event e) { Session.this.emit(e); }
        @Override public void failLocal(WireException e) { Session.this.failLocal(e); }
        @Override public void closeWith(SessionError e, CloseFrame fr) { Session.this.closeWith(e, fr); }
        @Override public void checkLingerDone() { Session.this.checkLingerDone(); }
        @Override public boolean isClosing() { return Session.this.isClosing(); }
        @Override public long lingerTimeout() { return Session.this.lingerTimeout(); }
        @Override public long sendBufferLimit() { return Session.this.sendBufferLimit(); }
        @Override public SessionState state() { return state; }

        @Override
        public void capacityChanged() {
            if (quality != null && quality.floorSet()) quality.checkFloor();
        }

        @Override
        public void suspended(long graceDeadline) {
            preSuspend = state;
            state = SessionState.SUSPENDED;
            stats.suspended(now);
            grace = graceDeadline;
        }

        @Override
        public void resumed() {
            stats.resumed(now);
            state = preSuspend;
            if (life.goAwaySent() || life.goAwayReceived()) state = SessionState.DRAINING;
            grace = Time.NEVER;
            ka.activity(now);
        }
    }

    /** The session behind the stream components and the path layer's view of the streams. */
    private final class StreamsPort implements StreamTable.Port, StreamSend.Port, StreamRecv.Port, StreamsView {
        @Override public long now() { return now; }
        @Override public void wantWrite() { pipes.wakeWriters(); }
        @Override public SessionError error() { return err; }
        @Override public boolean established() { return peerSeen; }
        @Override public boolean earlyActive() { return Session.this.earlyActive(); }
        @Override public TransportParams peer() { return peer; }
        @Override public Features features() { return feat; }
        @Override public boolean responder() { return role == Role.RESPONDER; }
        @Override public void reset(Stream st, long code) { send.reset(st, code); }
        @Override public void rejectSend(Stream st) { send.reject(st); }
        @Override public void stopRecv(Stream st, long code) { recv.stopRecv(st, code); }
        @Override public void duplicateReceived(long n) {
            stats.duplicateReceived(n);
            Pipe p = rx.rxPipe();
            if (p != null && p.mp() != null) p.mp().ret.duplicate(n);
        }
        @Override public void discarded(Stream st) {
            lanes.unbind(st);
            if (pipes.shaping() != null) pipes.shaping().stream(io.zmux.shaping.EventKind.STREAM_END, st.tab.id());
        }
        @Override public void opened(Stream st) {
            dg.releaseHeld(st);
            if (pipes.shaping() != null) pipes.shaping().stream(io.zmux.shaping.EventKind.STREAM_OPEN, st.tab.id());
        }
        @Override public long sendBuffer() { return sessionSendBuffer(); }
        @Override public void unretain(long n) { path.unretain(n); }

        @Override public Stream get(long id) { return streams.get(id); }
        @Override public Iterable<Stream> live() { return streams.live(); }
        @Override public boolean streamsBlockedAtLimit(int t) { return streams.streamsBlockedAtLimit(t); }
        @Override public void releaseAcked(Stream st, long upTo) { send.release(st, upTo); }
        @Override public void sendTerminal(Stream st, int sendState) { send.sendTerminal(st, sendState); }
    }

    /** The session behind the lanes, the record receiver and the transmitter. */
    private final class IoPort implements Lanes.Port, RecordReceiver.Port, Transmitter.Port {
        @Override public long now() { return now; }
        @Override public SessionState state() { return state; }
        @Override public boolean established() { return peerSeen; }
        @Override public boolean closing() { return isClosing(); }
        @Override public Features features() { return feat; }
        @Override public TransportParams peer() { return peer; }
        @Override public boolean urgentLane(Stream st) { return path.urgentLane(st); }
        @Override public void failLocal(WireException e) { Session.this.failLocal(e); }

        @Override public Gates gates() { return gates; }
        @Override public void peerParams(TransportParams tp) throws WireException { processPeerParams(tp); }
        @Override public void dispatch(Pipe p, io.zmux.internal.wire.Frame f, ReadBuf buf) throws WireException { Session.this.dispatch(p, f, buf); }

        @Override
        public boolean unconfirmedJoin(Pipe p) {
            Attach a = p.mp();
            return a != null && p.id != 0 && a.joinerLocal && a.health.in() == Attach.IN_JOINING
                    && (a.health.out() == Attach.OUT_JOINING || a.health.out() == Attach.OUT_NONE);
        }

        @Override
        public void joinAnswered(Pipe p, boolean application, long code, String reason) {
            boolean resumeFailed = !application && code == TransportError.RESUME_FAILED;
            if (resumeFailed && state == SessionState.SUSPENDED) {
                // A resume join the peer has no session for: the session ends now, with no pipe to send a close on.
                closeWith(SessionError.transport(TransportError.RESUME_FAILED, 0, "peer has no session", "", true),
                        CloseFrame.of(false, TransportError.RESUME_FAILED, 0, null));
                return;
            }
            mp().joinAnswered(p, resumeFailed, new java.io.IOException("join answered with CONNECTION_CLOSE "
                    + (application ? "application error " + code : TransportError.name(code)) + (reason.isEmpty() ? "" : ": " + reason)));
        }

        @Override
        public void closingAck(io.zmux.internal.wire.Frame f) {
            try {
                onPipeAck(f);
            } catch (WireException ignored) {
                // The session is closing already.
            }
        }

        @Override public void charge() { Session.this.charge(); }

        @Override
        public void recorded(Pipe p, Pipe.InLane il, long posBytes, boolean elicit, boolean pipePing, boolean pingOnly, boolean traffic,
                             boolean activity) {
            Session.this.recorded(p, il, posBytes, elicit, pipePing, pingOnly, traffic, activity);
        }

        @Override public long srtt() { return Session.this.srtt(); }
        @Override public AckEngine ack() { return ack; }
        @Override public Pacer pacer() { return pacer; }
        @Override public PathLayer path() { return path; }
        @Override public void closeSent() { closeSentOn(); }
    }
}
