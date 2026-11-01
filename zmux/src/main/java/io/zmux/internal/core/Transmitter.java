package io.zmux.internal.core;

import io.zmux.internal.wire.FrameEncoder;
import io.zmux.shaping.RecordPlan;
import io.zmux.internal.wire.FrameType;
import io.zmux.internal.wire.TransportParams;
import io.zmux.carrier.VarInt;

/**
 * Builds the batches of outbound lanes (SPEC.md Sections 3 and 9.2): the first record of the
 * bootstrap pipe, PIPE_LANE records, CONNECTION_CLOSE, coalesced control ahead of stream
 * data in priority order, the plan of each STREAM frame, pacing room and lane turns, and the
 * accounting of a produced batch. The path layer decides what a pipe carries; owned by the
 * session's loop.
 */
public final class Transmitter {
    /** What the transmitter asks of the session. */
    interface Port {
        long now();

        SessionState state();

        boolean established();

        Features features();

        /** The session SRTT of SPEC.md Section 1.3. */
        long srtt();

        /** Pipe acknowledgement, or null without it. */
        AckEngine ack();

        /** Pacing, or null without acknowledgement. */
        Pacer pacer();

        PathLayer path();

        /** CONNECTION_CLOSE was handed to an outbound lane. */
        void closeSent();
    }

    static final int MAX_STREAM_FRAME = 16384;
    static final int CHUNK_SENT = 0, CHUNK_SKIP = 1, CHUNK_FULL = 2;

    private final CoreConfig cfg;
    private final SessionStats stats;
    private final PipeTable pipes;
    private final Lanes lanes;
    private final StreamTable streams;
    private final StreamSend send;
    private final StreamRecv recv;
    private final StreamScheduler sched;
    private final ControlQueue ctl;
    private final ConnFlow conn;
    private final WindowTuner tuner;
    private final KeepAlive ka;
    private final Lifecycle life;
    private final EarlyData early;
    private final Datagrams dg;
    private final Port port;
    private final RecordWriter writer = new RecordWriter();
    private final StreamScheduler.Iter schedIter = new StreamScheduler.Iter();
    private final ChunkPlan plan = new ChunkPlan();
    /** The peer's max_record_size once established. */
    private long peerMaxRecord = TransportParams.DEFAULT_MAX_RECORD_SIZE;

    Transmitter(CoreConfig cfg, SessionStats stats, PipeTable pipes, Lanes lanes, StreamTable streams, StreamSend send, StreamRecv recv,
                StreamScheduler sched, ControlQueue ctl, ConnFlow conn, WindowTuner tuner, KeepAlive ka, Lifecycle life,
                EarlyData early, Datagrams dg, Port port) {
        this.cfg = cfg;
        this.recv = recv;
        this.stats = stats;
        this.pipes = pipes;
        this.lanes = lanes;
        this.streams = streams;
        this.send = send;
        this.sched = sched;
        this.ctl = ctl;
        this.conn = conn;
        this.tuner = tuner;
        this.ka = ka;
        this.life = life;
        this.early = early;
        this.dg = dg;
        this.port = port;
    }

    /** The peer's max_record_size, at most 65535. */
    int peerRecordLimit() {
        return (int) Math.min(peerMaxRecord, 65535);
    }

    /** The peer's max_record_size arrived. */
    void peerRecordLimit(long v) {
        peerMaxRecord = v;
    }

    private boolean earlyActive() {
        return early.active() && !port.established();
    }

    private PathSession mp() {
        return (PathSession) port.path();
    }

    /** The largest Size of records on an outbound lane. */
    private int recMax(Pipe.OutLane ol) {
        int m = (int) Math.min(peerMaxRecord, 65535);
        if (!port.established()) m = TransportParams.DEFAULT_MAX_RECORD_SIZE;
        if (ol.st.info().tls() && m > RecordPlan.MAX_RECORD_LENGTH) m = RecordPlan.MAX_RECORD_LENGTH;
        return m;
    }

    // ---- interest ----

    /** Whether {@link #poll} for the lane would produce bytes now (W8). */
    public boolean wantsWrite(long pipeId, long lane) {
        Pipe p = pipes.get(pipeId);
        if (p == null || p.out() == null || p.out().failed()) return false;
        Pipe.OutLane ol = p.out().lane(lane);
        SessionState state = port.state();
        if (ol == null || ol.st.shut() || ol.tx.closeSent || state == SessionState.CLOSED) return false;
        if (pipeId == 0 && lane == 0 && !ol.tx.tpSent) {
            // The parameters lead the pipe: a planned delay of that first record holds them like
            // any other record, unless CONNECTION_CLOSE is owed.
            return life.closePending() || pipes.shaping() == null || !Shaping.delaying(p, port.now());
        }
        if (life.closePending()) return lane == 0 && !life.waitingLanes();
        if (ol.st.laneFirst()) {
            // The lane's PIPE_LANE record is a record of the pipe: a planned delay holds it too (MULTIPATH.md 5.8).
            return state.compareTo(SessionState.CLOSING) < 0 && (pipes.shaping() == null || !Shaping.delaying(p, port.now()));
        }
        if (!ol.st.confirmed() || state == SessionState.CLOSING) return false;
        if (!port.established()) return ka.wants() || earlyActive() && dataPendingOn(null, null);
        if (pipes.shaping() != null) {
            if (Shaping.delaying(p, port.now())) return false;
            if (Shaping.wants(p, lane)) return true;
        }
        return port.path().wantsWrite(p, ol);
    }

    /** The single pipe's interest in writing on ol. */
    boolean wantsWriteSingle(Pipe p, Pipe.OutLane ol) {
        if (ol.lane == 0 && controlPending()) return true;
        return dataPendingOn(p, ol) && !Pacer.held(p) && (pipes.shaping() == null || !Shaping.held(p, port.now()));
    }

    /** Stream data that would go on the lane now. */
    boolean dataPendingOn(Pipe p, Pipe.OutLane ol) {
        if (sched.isEmpty()) return false;
        StreamScheduler.Iter it = new StreamScheduler.Iter().reset(sched);
        for (Stream st = it.next(); st != null; st = it.next()) {
            if (!canSendNow(st)) continue;
            if (p == null || !p.out().lo.offered()) return true;
            if (lanes.laneFor(p, st) == ol) return true;
        }
        return false;
    }

    /** Whether control frames are due. */
    boolean controlPending() {
        if (ctl.pending()) return true;
        if (ka.wants()) return true;
        AckEngine ack = port.ack();
        if (ack != null && port.path().acksInControl() && ack.due(port.now())) return true;
        return dg.sendable();
    }

    /** Whether the stream could put a frame out now; the same plan as sendChunk, so interest never spins (W8). */
    boolean canSendNow(Stream st) {
        if (!st.tab.idSet()) {
            int t = Stream.typeIndex(st.bidi);
            if (!port.established() && !earlyActive() || !streams.idAvailable(t)) return false;
            long pre = port.features().preamble() ? st.preSize() : 0;
            return pre == 0 || conn.room() >= pre;
        }
        ChunkPlan pl = planChunk(st, plan);
        return pl.n > 0 || pl.fin || pl.open;
    }

    // ---- batches ----

    /** The lane's frames after the leading parameters: lane-first setup, CONNECTION_CLOSE, or regular content. */
    private void writeContent(RecordWriter w, Pipe p, Pipe.OutLane ol, long lane, SessionState state,
                              boolean established, Shaping.PipeShape ps, long now) {
        if (ol.st.laneFirst()) {
            if (state.compareTo(SessionState.CLOSING) < 0) lanes.writeFirst(w, p, ol);
            return;
        }
        if (lane > 0 && !ol.st.confirmed()) return; // nothing until confirmed
        if (life.closePending() && !ol.tx.closeSent && lane == 0) {
            w.neverHold(); // a planned delay never holds CONNECTION_CLOSE
            if (!life.waitingLanes() && life.writeClose(w)) ol.tx.closeSent = true;
            return;
        }
        if (state == SessionState.CLOSING || ol.tx.closeSent) return; // nothing after CONNECTION_CLOSE
        if (!established) {
            ka.write(w, now, port.srtt());
            if (earlyActive()) writeEarly(w);
            return;
        }
        if (ps != null && Shaping.deferred(ps)) pipes.shaping().inject(w);
        port.path().write(w, p, ol);
        if (ps != null) pipes.shaping().inject(w);
    }

    boolean poll(long pipeId, long lane, Batch b, int max, long t) {
        b.reset(pipeId, lane);
        Pipe p = pipes.get(pipeId);
        SessionState state = port.state();
        if (p == null || p.out() == null || p.out().failed() || state == SessionState.CLOSED) return false;
        Pipe.OutLane ol = p.out().lane(lane);
        if (ol == null || ol.st.shut()) return false;
        RecordWriter w = writer;
        w.init(b, ol.st.info().framing(), recMax(ol), max);
        AckEngine ack = port.ack();
        boolean established = port.established();
        long now = port.now();
        if (ack != null && established) {
            long room = port.pacer().room(p, now);
            w.limitData(room > 0 && !lanes.dataTurn(p, ol) ? 0 : room);
        }
        Shaping.PipeShape ps = null;
        if (pipes.shaping() != null && (ps = pipes.shaping().of(p)) != null) {
            w.shape(ps);
            w.limitData(ps.room(w, now, w.dataRoom()));
        }
        if (pipeId == 0 && lane == 0 && !ol.tx.tpSent) {
            // A planned delay never holds CONNECTION_CLOSE, nor the parameters that lead it.
            if (life.closePending() && !ol.tx.closeSent) w.neverHold();
            writeParams(w, ol);
        }
        writeContent(w, p, ol, lane, state, established, ps, now);
        w.end();
        if (ps != null) ps.spent(w.dataSpent());
        if (pipes.shaping() != null) for (int i = w.pingRequests(); i > 0; i--) pipes.shaping().mark(now);
        if (w.dataSpent() > 0) lanes.tookTurn(p, ol);
        if (b.n() == 0) {
            if (ack != null && !ol.tx.notWritable) p.out().snd.re().appLimited(p.out().snd.so());
            return false;
        }
        afterBatch(w, p, ol);
        if (ack != null) {
            if ((w.budget() < 1024 || w.dataRoom() == 0) && dataPendingOn(null, null)) p.out().snd.re().blocked();
            else p.out().snd.re().appLimited(p.out().snd.so());
        }
        ol.tx.writing = true;
        ol.tx.inCall = b.bytes();
        if (!ol.tx.notWritable) ol.tx.stallAt = t;
        return true;
    }

    /** Accounts positions and statistics of a produced batch. */
    void afterBatch(RecordWriter w, Pipe p, Pipe.OutLane ol) {
        // queue_p as this batch is handed over: unsent_ahead (Q_p without the batch) over R_p, both taken
        // now (MULTIPATH.md 7.1).
        long queue = p.mp() != null ? PathHealth.queueDelay(mp().metrics.queueBytes(p), mp().metrics.rateOf(p)) : 0;
        long pos = 0;
        Batch b = w.b();
        for (int i = 0; i < b.messageCount(); i++) pos += Batch.positionBytes(b.recordSize(i));
        long startPos = ol.tx.pos;
        ol.tx.pos += pos;
        for (Stream st : w.touched) Lanes.boundEnd(st, ol.tx.pos);
        stats.sent(pos, w.recs());
        p.out().recordsWritten(w.recs());
        AckEngine ack = port.ack();
        long now = port.now();
        if (w.traffic()) p.traffic(now);
        if (ack != null) ack.onSent(p, ol, startPos, w, now, queue);
        if (p.mp() != null) mp().afterBatch(w, p, ol, startPos, queue);
        if (w.closed() && !w.laneOnly()) {
            b.shutdownAfter();
            port.closeSent();
        }
        // Without acknowledgement a record passed to the carrier restarts the idle timer unless it
        // carries only QX_PING requests and PADDING (SPEC.md Section 6.4).
        if (!port.features().acked() && !w.onlyPing()) ka.activity(now);
        if (ack != null) port.pacer().onSent(p, pos, now);
    }

    /** A batch was written or failed: releases its references and the lane's writing state. */
    void written(Batch b, long t) {
        b.release();
        Pipe p = pipes.get(b.pipe());
        if (p != null && p.out() != null) {
            Pipe.OutLane ol = p.out().lane(b.lane());
            if (ol != null) {
                ol.tx.writing = false;
                ol.tx.inCall = 0;
                if (ol.tx.notWritable) ol.tx.stallAt = t;
            }
        }
    }

    /** Records the carrier's unsent count of a lane, -1 when it reports none. */
    void setUnsent(long pipeId, long lane, long unsent) {
        Pipe p = pipes.get(pipeId);
        if (p == null || p.out() == null) return;
        Pipe.OutLane ol = p.out().lane(lane);
        if (ol != null) ol.tx.unsent = unsent < 0 ? -1 : unsent;
    }

    /** Records whether a carrier half accepts bytes now. */
    void setWritable(long pipeId, long lane, boolean writable, long t) {
        Pipe p = pipes.get(pipeId);
        if (p == null || p.out() == null) return;
        Pipe.OutLane ol = p.out().lane(lane);
        if (ol == null || ol.tx.notWritable == !writable) return;
        ol.tx.notWritable = !writable;
        if (!writable && !ol.tx.writing) ol.tx.stallAt = t;
        if (port.ack() != null) {
            RateEstimator e = p.out().snd.re();
            e.blockedNow(!writable);
            if (!writable && dataPendingOn(null, null)) e.blocked();
        }
        if (writable) pipes.wake(p);
    }

    /** Writes the first record of the bootstrap pipe (SPEC.md Section 5.1). */
    private void writeParams(RecordWriter w, Pipe.OutLane ol) {
        byte[] block = cfg.local().toBytes();
        int n = VarInt.length(FrameType.TRANSPORT_PARAMETERS) + VarInt.length(block.length) + block.length;
        // Held by a planned delay of the first record: they still lead the pipe once it ends.
        if (!w.frame(n, true, bb -> FrameEncoder.transportParameters(bb, block))) return;
        ol.tx.tpSent = true;
        if (!ol.st.info().minRtt() && !cfg.test().enabled()) ka.requestFirst();
    }

    /** Writes the fence and the early STREAM frames. */
    private void writeEarly(RecordWriter w) {
        if (!dataPendingOn(null, null)) return;
        if (!early.writeFence(w)) return;
        writeData(w, null, null);
    }

    /** The single pipe's records: control ahead of data on lane 0, data only on the other lanes. */
    void writeSingle(RecordWriter w, Pipe p, Pipe.OutLane ol) {
        boolean lane0 = ol.lane == 0;
        if (lane0) writeControl(w, p);
        writeData(w, p, ol);
        if (lane0 && ctl.pendingAfterData()) writeControl(w, p);
    }

    // ---- control ----

    /** Writes the coalesced control state (SPEC.md Section 9.2) ahead of data. */
    void writeControl(RecordWriter w, Pipe p) {
        AckEngine ack = port.ack();
        PathLayer path = port.path();
        long now = port.now();
        if (ack != null && path.acksInControl()) ack.write(w, now);
        ka.writeResponse(w);
        if (ctl.maxData()) {
            tuner.connUpdate();
            long v = conn.maxDataValue();
            if (conn.maxDataWanted(v, ctl.maxDataForced())) {
                if (!w.frame(1 + VarInt.length(v), true, bb -> FrameEncoder.maxData(bb, v))) return;
                w.logFrame(FrameDesc.of(FrameDesc.Kind.MAX_DATA));
                ctl.forceMaxDataDone();
                path.maxDataSent(conn.peerBlocked() || conn.received() >= conn.advertised());
                conn.maxDataSent(v, now);
            }
            ctl.maxDataDone();
        }
        for (int t = 0; t < 2; t++) {
            if (ctl.maxStreams(t)) {
                long v = streams.remoteLimit(t);
                boolean bidi = t == 0;
                if (!w.frame(1 + VarInt.length(v), true, bb -> FrameEncoder.maxStreams(bb, bidi, v))) return;
                w.logFrame(new FrameDesc(FrameDesc.Kind.MAX_STREAMS, bidi, false, 0, 0, 0, 0));
                ctl.maxStreamsDone(t);
            }
        }
        for (Stream st = ctl.first(); st != null; ) {
            Stream next = ControlQueue.next(st);
            if (!writeStreamCtl(w, st, path, now)) return;
            st = next;
        }
        if (ctl.dataBlocked() && conn.blockedAtLimit()) {
            long v = conn.sendMax();
            if (!w.frame(1 + VarInt.length(v), true, bb -> FrameEncoder.dataBlocked(bb, v))) return;
            w.logFrame(FrameDesc.of(FrameDesc.Kind.DATA_BLOCKED));
        }
        ctl.dataBlockedDone();
        for (int t = 0; t < 2; t++) {
            if (ctl.streamsBlocked(t) && streams.streamsBlockedAtLimit(t)) {
                long v = streams.localLimit(t);
                boolean bidi = t == 0;
                if (!w.frame(1 + VarInt.length(v), true, bb -> FrameEncoder.streamsBlocked(bb, bidi, v))) return;
                w.logFrame(new FrameDesc(FrameDesc.Kind.STREAMS_BLOCKED, bidi, false, 0, 0, 0, 0));
            }
            ctl.streamsBlockedDone(t);
        }
        if (ctl.goAway()) {
            if (!life.writeGoAway(w)) return;
            ctl.goAwayDone();
        }
        ka.write(w, now, port.srtt());
        dg.write(w, p);
    }

    /** Writes the pending control frames of one stream; false when the batch is full. */
    private boolean writeStreamCtl(RecordWriter w, Stream st, PathLayer path, long now) {
        if (ControlQueue.has(st, ControlQueue.MAX_STREAM_DATA)) {
            if (!recv.writeMaxStreamData(w, st, path, now)) return false;
            ControlQueue.done(st, ControlQueue.MAX_STREAM_DATA);
        }
        if (ControlQueue.has(st, ControlQueue.STOP)) {
            if (!recv.writeStop(w, st)) return false;
            ControlQueue.done(st, ControlQueue.STOP);
        }
        if (ControlQueue.has(st, ControlQueue.RESET) && !send.writeReset(w, st, path.retainsSent())) return false;
        if (ControlQueue.has(st, ControlQueue.PRIORITY)) {
            if (!send.writePriority(w, st)) return false;
            ControlQueue.done(st, ControlQueue.PRIORITY);
        }
        if (ControlQueue.has(st, ControlQueue.BLOCKED) && !send.writeBlocked(w, st)) return false;
        ControlQueue.done(st, ControlQueue.BLOCKED);
        ctl.settle(st);
        return true;
    }

    // ---- stream data ----

    /** What a stream could send now, before record room. */
    static final class ChunkPlan {
        private long off;
        private long n;
        private long avail;
        private long credit;
        private long connCredit;
        private boolean fin;
        private boolean open;
        private boolean preBlocked;
        private boolean resend;
    }

    /** The next frame of a stream that has an identifier. */
    private ChunkPlan planChunk(Stream st, ChunkPlan p) {
        StreamSend.SendPart sp = st.snd;
        p.n = 0;
        p.avail = 0;
        p.fin = p.open = p.preBlocked = p.resend = false;
        PathStreamState m = sp.mps();
        if (m != null && (m.ret.resend().size() > 0 || m.ret.finResend()) && planResend(st, p)) return p;
        p.off = Math.max(sp.sent(), sp.q().base());
        long end = sp.sendEnd();
        if (end > p.off) p.avail = end - p.off;
        p.credit = sp.maxData() - Math.min(sp.maxData(), p.off);
        p.connCredit = conn.room();
        p.n = Math.min(p.avail, Math.min(p.credit, p.connCredit));
        if (p.n > MAX_STREAM_FRAME) p.n = MAX_STREAM_FRAME;
        if (p.off >= sp.sent()) {
            p.n = Math.min(p.n, port.path().retainRoom());
        }
        if (p.off == 0 && sp.preLen() > 0 && p.n < sp.preLen()) {
            p.n = 0;
            p.preBlocked = true;
            return p;
        }
        if (p.off + p.n == sp.q().end() && sp.finWritten() && !sp.finSent() && !sp.reset()) p.fin = true;
        p.open = p.n == 0 && !p.fin && !st.tab.opened() && st.tab.pinned();
        return p;
    }

    /** Plans a frame of reinjected stream data (MULTIPATH.md 7.4); false when nothing is left to resend. */
    private static boolean planResend(Stream st, ChunkPlan p) {
        StreamSend.SendPart sp = st.snd;
        PathStreamState m = sp.mps();
        p.resend = true;
        while (m.ret.resend().size() > 0) {
            long lo = Math.max(m.ret.resend().lo(0), sp.q().base()), hi = Math.min(m.ret.resend().hi(0), sp.q().end());
            if (sp.reset()) hi = Math.min(hi, sp.resetRel());
            if (lo >= hi) {
                m.ret.dropFirstResend();
                continue;
            }
            p.off = lo;
            p.n = Math.min(hi - lo, MAX_STREAM_FRAME);
            if (lo == 0 && sp.preLen() > p.n) p.n = Math.min(sp.preLen(), sp.q().end());
            p.fin = m.ret.finResend() && p.off + p.n == sp.q().end() && sp.finSent() && !sp.reset();
            return true;
        }
        if (m.ret.finResend() && !sp.reset() && sp.finSent()) {
            p.off = sp.q().end();
            p.fin = true;
            return true;
        }
        m.ret.finResendDone();
        p.resend = false;
        return false;
    }

    /** Writes stream data in priority order; with lanes, only the directions bound to ol. */
    void writeData(RecordWriter w, Pipe p, Pipe.OutLane ol) {
        Pipe lp = p != null && p.out().lo.offered() ? p : null;
        PathScheduler.Decision d = p != null && p.mp() != null ? mp().scheduler.decision() : null;
        while (w.room() > 0 && !sched.isEmpty()) {
            boolean progressed = false;
            StreamScheduler.Iter it = schedIter.reset(sched);
            for (Stream st = it.next(); st != null; st = it.next()) {
                if (d != null && (!canSendNow(st) || !mp().scheduler.onPipe(d, st))) continue;
                if (lp != null) {
                    if (!canSendNow(st)) continue;
                    boolean elsewhere = lanes.laneFor(lp, st) != ol;
                    lanes.bind(lp, st);
                    if (elsewhere) continue;
                }
                int r = sendChunk(w, st, d);
                if (r == CHUNK_FULL) return;
                if (r == CHUNK_SENT) {
                    if (lp != null) {
                        // This direction already owns bytes in the batch being built. Its
                        // final lane position is known only in afterBatch; do not rebind
                        // another chunk meanwhile using the previous batch's ACK point.
                        Lanes.boundEnd(st, Long.MAX_VALUE);
                        w.touched.add(st);
                    }
                    progressed = true;
                    break;
                }
            }
            if (!progressed) return;
        }
    }

    /** Puts the next frame of st into the record; d is the multipath decision, or null. */
    private int sendChunk(RecordWriter w, Stream st, PathScheduler.Decision d) {
        StreamSend.SendPart sp = st.snd;
        boolean established = port.established();
        if (!st.tab.idSet()) {
            if (!canSendNow(st)) {
                int t = Stream.typeIndex(st.bidi);
                if (established && !streams.idAvailable(t)) streams.markStreamsBlocked(t);
                if ((established || earlyActive()) && port.features().preamble() && streams.idAvailable(t)) {
                    if (!streams.preambleAllowed(st, send)) return CHUNK_SKIP;
                    // The preamble does not fit the session credit: allocate and report the block (SPEC.md 8.4).
                    streams.allocId(st);
                    blockedOn(st, false, true);
                }
                return CHUNK_SKIP;
            }
            if (!streams.preambleAllowed(st, send)) return CHUNK_SKIP;
            streams.allocId(st);
        }
        ChunkPlan pl = planChunk(st, plan);
        if (pl.preBlocked) {
            if (pl.avail >= sp.preLen() || sp.finWritten()) blockedOn(st, pl.credit < sp.preLen(), pl.connCredit < sp.preLen());
            return CHUNK_SKIP;
        }
        if (pl.n == 0 && !pl.fin && !pl.open) {
            if (pl.avail > 0) blockedOn(st, pl.credit == 0, pl.connCredit == 0);
            if (!st.sendable() || pl.credit == 0 && pl.avail > 0) sched.remove(st);
            return CHUNK_SKIP;
        }
        if (w.dataRoom() == 0 && (pl.n > 0 || pl.fin)) return CHUNK_FULL;
        long off = pl.off, n = pl.n;
        boolean fin = pl.fin;
        boolean resend = pl.resend;
        if (d != null && !resend && n > cfg.spreadChunk()) {
            // One scheduling decision per chunk.
            n = cfg.spreadChunk();
            fin = false;
            if (off == 0 && sp.preLen() > n) n = sp.preLen();
        }
        int room = w.dataFit(w.room() - FrameEncoder.streamHeaderLength(st.tab.id(), off, (int) n, true)); // plans a record when shaped
        if (room <= 0) return CHUNK_FULL;
        if (n > room) {
            if (off == 0 && sp.preLen() > 0 && room < sp.preLen()) return CHUNK_FULL;
            n = room;
            fin = false;
        }
        long want = n;
        boolean wantFin = fin;
        java.nio.ByteBuffer data = null;
        Pin pin = Pin.NONE;
        if (n > 0) {
            // The chunk plan only offers queued bytes, so off lies within the queue.
            SendQueue.Peek pk = java.util.Objects.requireNonNull(sp.q().peek(off, (int) n), "chunk outside the send queue");
            data = pk.data();
            pin = pk.pin();
            if (data.remaining() < n) {
                n = data.remaining();
                fin = false;
            }
        }
        if (!w.stream(st.tab.id(), off, data, pin, fin)) return CHUNK_FULL;
        while (n < want) {
            SendQueue.Peek more = sp.q().peek(off + n, (int) (want - n));
            if (more == null || !more.data().hasRemaining()) break;
            boolean f2 = wantFin && n + more.data().remaining() == want;
            if (!w.stream(st.tab.id(), off + n, more.data(), more.pin(), f2)) break;
            n += more.data().remaining();
            fin = f2;
        }
        w.logFrame(FrameDesc.stream(st.tab.id(), off, n, fin));
        st.payloadSent(off, n);
        w.spendData(n);
        if (resend) {
            if (d != null) mp().resume.resent(d.p(), n);
            PathStreamState m = sp.mps();
            m.ret.resent(off, n, fin);
            if (!st.sendable()) sched.remove(st);
            return CHUNK_SENT;
        }
        send.writing(st);
        if (dg.queued()) pipes.wakeWriters();
        if (!established) early.sent();
        if (d != null) mp().scheduler.chunkSent(d, w, st, off, n, fin);
        PathLayer path = port.path();
        long grown = send.dataWritten(st, off + n, fin);
        if (grown > 0) {
            conn.consumed(grown);
            path.retain(grown);
        }
        send.checkCapacity(st);
        if (path.retainsSent()) {
            sp.mp();
        } else if (!early.holdSends()) {
            send.release(st, sp.sent());
            if (fin) send.sendTerminal(st, Stream.SEND_DATA_RECVD);
        }
        sched.served(st, n);
        if (!st.sendable()) sched.remove(st);
        if (d != null) mp().scheduler.renewDecision(d);
        return CHUNK_SENT;
    }

    /** Queues STREAM_DATA_BLOCKED / DATA_BLOCKED once per limit (SPEC.md Section 8.4). */
    private void blockedOn(Stream st, boolean stream, boolean connBlocked) {
        if (stream && send.markBlocked(st)) ctl.stream(st, ControlQueue.BLOCKED);
        if (stream) port.path().creditBlocked(st);
        if (connBlocked && conn.markBlocked()) {
            ctl.wantDataBlocked();
        }
    }

    /** The writer's state of one outbound lane. */
    static final class LaneTx {
        private long pos;
        private boolean tpSent;
        private boolean closeSent;
        private boolean writing;
        private boolean notWritable;
        private long stallAt;
        /** The bytes of the batch whose write call is in progress (part of Q_p). */
        private long inCall;
        /** The carrier's unsent count as the driver last queried it, -1 when it reports none. */
        private long unsent = -1;

        LaneTx() {
        }

        long inCall() {
            return inCall;
        }

        long unsent() {
            return unsent;
        }

        /** Q_p's part of this lane: the write call in progress plus the carrier's unsent bytes. */
        long queued() {
            return inCall + Math.max(0, unsent);
        }

        long pos() {
            return pos;
        }

        boolean tpSent() {
            return tpSent;
        }

        boolean closeSent() {
            return closeSent;
        }

        boolean writing() {
            return writing;
        }

        boolean notWritable() {
            return notWritable;
        }

        long stallAt() {
            return stallAt;
        }
    }
}
