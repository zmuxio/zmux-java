package io.zmux.internal.core;

import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.Preamble;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.WireException;
import java.nio.ByteBuffer;

/**
 * The receiving parts of a session's streams (SPEC.md Sections 4.2, 7.1 and 8.3,
 * EXTENSIONS.md Section 4): STREAM and reset frames, the receive buffer and its
 * session accounting, the opener's preamble, application reads, STOP_SENDING and
 * stream credit. Effects on the session go through {@link Port}.
 */
public final class StreamRecv {
    /** What the receiving parts ask of the session. */
    interface Port {
        long now();

        /** The session's terminal error, or null. */
        SessionError error();

        Features features();

        /** n stream bytes received again in a multipath session, on the pipe being read. */
        void duplicateReceived(long n);
    }

    /** Result of a read: bytes copied and whether the end of the stream was reached. */
    public record ReadResult(int n, boolean fin) {
        public static final ReadResult WAIT = new ReadResult(0, false);
    }

    /** A chunk taken without copying; buf, when not null, holds a reference the caller releases. */
    public record Chunk(ByteBuffer data, ReadBuf buf, boolean fin) {}

    private final CoreConfig cfg;
    private final SessionStats stats;
    private final Outbox outbox;
    private final ConnFlow conn;
    private final WindowTuner tuner;
    private final ControlQueue ctl;
    private final StreamTable table;
    private final Port port;

    StreamRecv(CoreConfig cfg, SessionStats stats, Outbox outbox, ConnFlow conn, WindowTuner tuner, ControlQueue ctl,
               StreamTable table, Port port) {
        this.cfg = cfg;
        this.stats = stats;
        this.outbox = outbox;
        this.conn = conn;
        this.tuner = tuner;
        this.ctl = ctl;
        this.table = table;
        this.port = port;
    }

    // ---- input ----

    /** Processes a STREAM frame; returns whether it had an effect (NE-5). */
    boolean onStream(Frame f, ReadBuf buf) throws WireException {
        Stream st = table.lookup(f.streamId, true);
        if (st == null) return true;
        if (!st.hasRecv) throw WireException.withDetails(TransportError.STREAM_STATE_ERROR, "STREAM on a send-only stream", "stream", st.tab.id());
        Features feat = port.features();
        StreamRecv.RecvPart r = st.rcv;
        long off = f.offset;
        long end = off + f.dataLen;
        if (r.finKnown && (end > r.finalSize || f.fin && end != r.finalSize)) {
            throw WireException.withDetails(TransportError.FINAL_SIZE_ERROR, "data beyond the final size", "stream", st.tab.id(),
                    "offset", off, "length", f.dataLen, "limit", r.finalSize);
        }
        if (f.fin && end < r.highest) {
            throw WireException.withDetails(TransportError.FINAL_SIZE_ERROR, "final size below received data", "stream", st.tab.id());
        }
        if (end > r.adv) {
            throw WireException.withDetails(TransportError.FLOW_CONTROL_ERROR, "stream data beyond the limit", "stream", st.tab.id(),
                    "offset", off, "length", f.dataLen, "limit", r.adv);
        }
        if (!feat.multipath() && off != r.highest && (!r.reset || off < r.highest)) {
            throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "out-of-order stream data", "stream", st.tab.id(),
                    "offset", off, "length", f.dataLen);
        }
        long top = r.finKnown ? Math.max(r.highest, r.finalSize) : r.highest;
        if (end > top) {
            if (!conn.received(end - top)) {
                throw WireException.withDetails(TransportError.FLOW_CONTROL_ERROR, "session data beyond the limit",
                        "stream", st.tab.id(), "limit", conn.advertised());
            }
        }
        boolean effect = end > r.highest || f.fin && !r.finKnown;
        r.highest = Math.max(r.highest, end);
        if (f.fin && !r.finKnown) {
            r.finKnown = true;
            r.finalSize = end;
            if (r.state == Stream.RECV_RECV) r.state = Stream.RECV_SIZE_KNOWN;
        }
        tuner.data(st, end, port.now());
        int dataPos = f.dataPos;
        int dataLen = f.dataLen;
        if (!st.local && feat.preamble() && !r.preDone && off == 0) {
            Preamble pre = Preamble.parse(f.src, dataPos, dataLen, cfg.local().maxPreambleSize);
            effect = true;
            r.preDone = true;
            r.preSize = pre.size();
            st.rcv.peerPre = pre.openInfo() == null && !pre.hasPriority() && !pre.pipeCarrier()
                    && pre.applicationTlvs().isEmpty() ? null : pre;
            if (pre.hasPriority() && st.hasSend && st.bidi) {
                table.preamblePriority(st, pre.urgency(), pre.incremental());
            }
            checkPreambleReset(st, feat);
            long d = r.buf.discard(r.preSize);
            if (d > 0) table.bufSub(d);
            long newPre = pre.size() - r.arrived.covered(0, pre.size());
            r.arrived.add(0, pre.size());
            conn.releaseUnbuffered(newPre);
            r.readOff = Math.max(r.readOff, r.preSize);
            r.pushOff = Math.max(r.pushOff, r.preSize);
            dataPos += pre.size();
            dataLen -= pre.size();
            off = pre.size();
            if (!r.stopped) table.offer(st);
        }
        if (dataLen > 0) {
            long lo = Math.max(off, r.pushOff);
            long hi = end;
            if (r.reset) hi = Math.min(hi, r.resetRel);
            long dup = Math.max(0, Math.min(end, r.pushOff) - off);
            if (lo < hi) {
                long nb = r.arrived.add(lo, hi);
                dup += hi - lo - nb;
                if (nb > 0) effect = true;
                if (r.stopped || st.tab.stackReject()) {
                    conn.releaseUnbuffered(nb);
                } else if (nb > 0) {
                    ByteBuffer part = f.src.slice(dataPos + (int) (lo - off), (int) (hi - lo));
                    long stored = r.buf.insert(lo, part, buf);
                    table.bufAdd(stored);
                    if (stored < nb) conn.releaseUnbuffered(nb - stored);
                }
            }
            if (end > hi && r.reset) r.arrived.add(Math.max(hi, off), end);
            if (dup > 0 && feat.multipath()) port.duplicateReceived(dup);
        }
        if (f.fin && !r.preDone && feat.preamble() && !st.local && end == 0 && !r.stopped) {
            throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "opener's sending part has final size 0", "stream", st.tab.id());
        }
        progress(st);
        if (!effect && (f.dataLen > 0 || !f.fin) && !feat.multipath()) return false;
        return effect || st.tab.opened();
    }

    /** EXTENSIONS.md Section 4.4 rule 2 once both the preamble size and a reset are known. */
    private static void checkPreambleReset(Stream st, Features feat) throws WireException {
        StreamRecv.RecvPart r = st.rcv;
        if (st.local || !feat.preamble() || r.stopSent || r.stopped || !r.reset) return;
        if (r.preDone && r.resetRel < r.preSize) {
            throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "reset below the preamble", "stream", st.tab.id());
        }
    }

    /** Updates the receiving state after input and wakes the reader. */
    private void progress(Stream st) {
        StreamRecv.RecvPart r = st.rcv;
        if (r.finKnown && r.state == Stream.RECV_SIZE_KNOWN && r.arrived.contains(r.readOff, r.finalSize)
                && r.arrived.prefix() >= r.finalSize) {
            r.state = Stream.RECV_DATA_RECVD;
        }
        if ((r.stopped || st.tab.releasedRecv()) && (r.state == Stream.RECV_DATA_RECVD || r.state == Stream.RECV_RESET_RECVD)) {
            r.state = r.state == Stream.RECV_DATA_RECVD ? Stream.RECV_DATA_READ : Stream.RECV_RESET_READ;
            table.maybeDiscard(st);
        }
        outbox.wake(st.rcv.rsig);
        creditChanged(st);
    }

    /** Processes RESET_STREAM (at false) or RESET_STREAM_AT; returns whether it had an effect. */
    boolean onReset(Frame f, boolean at) throws WireException {
        Stream st = table.lookup(f.streamId, true);
        if (st == null) return true;
        if (!st.hasRecv) throw WireException.withDetails(TransportError.STREAM_STATE_ERROR, "reset of a send-only stream", "stream", st.tab.id());
        Features feat = port.features();
        StreamRecv.RecvPart r = st.rcv;
        long rel = at ? f.reliableSize : 0;
        if (r.finKnown && f.finalSize != r.finalSize || f.finalSize < r.highest) {
            throw WireException.withDetails(TransportError.FINAL_SIZE_ERROR, "final size changed", "stream", st.tab.id());
        }
        if (f.finalSize > r.adv) {
            throw WireException.withDetails(TransportError.FLOW_CONTROL_ERROR, "final size beyond the limit", "stream", st.tab.id());
        }
        if (r.reset && f.code != r.resetCode) {
            throw WireException.withDetails(TransportError.STREAM_STATE_ERROR, "reset changes the error code", "stream", st.tab.id());
        }
        long top = r.finKnown ? r.finalSize : r.highest; // a known final size is at least highest (checked above)
        if (f.finalSize > top) {
            if (!conn.received(f.finalSize - top)) {
                throw WireException.withDetails(TransportError.FLOW_CONTROL_ERROR, "session data beyond the limit", "stream", st.tab.id());
            }
        }
        if (!st.local && feat.preamble() && !r.stopSent && !r.stopped && (!at || f.finalSize == 0)) {
            throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "opener reset without the preamble", "stream", st.tab.id());
        }
        boolean effect = !r.reset || rel < r.resetRel;
        if (!effect) return false;
        if (!r.reset) {
            r.reset = true;
            r.resetCode = f.code;
        }
        r.resetRel = rel; // an effective repeat only lowers it
        if (!r.finKnown) {
            r.finKnown = true;
            r.finalSize = f.finalSize;
        }
        checkPreambleReset(st, feat);
        long lo = r.resetRel, hi = Math.min(r.relLo, r.finalSize);
        if (lo < hi) {
            long gap = (hi - lo) - r.arrived.covered(lo, hi);
            r.relLo = lo;
            conn.releaseUnbuffered(gap);
        }
        long d = r.buf.discardFrom(Math.max(r.resetRel, r.pushOff));
        if (d > 0) table.bufSub(d);
        if (r.state != Stream.RECV_DATA_READ) {
            r.state = Stream.RECV_RESET_RECVD;
            boolean rejected = r.resetCode == TransportError.REJECTED && st.local;
            r.errState = new StreamError(rejected ? StreamError.Kind.REJECTED : StreamError.Kind.RESET, st.tab.id(),
                    r.resetCode, r.resetRel, true);
        }
        progress(st);
        return true;
    }

    /** Applies STREAM_DATA_BLOCKED; returns false when it repeats a reported limit of a receiving part (no effect). */
    boolean onDataBlocked(Frame f) throws WireException {
        Stream st = table.lookup(f.streamId, true);
        if (st == null) return true;
        if (!st.hasRecv) {
            throw WireException.withDetails(TransportError.STREAM_STATE_ERROR, "STREAM_DATA_BLOCKED for a send-only stream", "stream", st.tab.id());
        }
        StreamRecv.RecvPart r = st.rcv;
        if (f.value > r.adv) {
            throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "STREAM_DATA_BLOCKED above the advertised limit",
                    "stream", st.tab.id(), "value", f.value);
        }
        boolean repeat = f.value <= r.blockSeen && r.blockSeen > 0 && r.state == Stream.RECV_RECV;
        r.blockSeen = Math.max(r.blockSeen, f.value);
        if (r.tune != null) r.tune.blockedAt(f.value);
        creditChanged(st);
        return !repeat;
    }

    // ---- reads ----

    /**
     * Copies contiguous data into dst. Returns WAIT when the caller must wait; throws the
     * stream's or the session's outcome when the part ended otherwise.
     */
    public ReadResult read(Stream st, ByteBuffer dst) throws CoreException {
        if (!st.hasRecv) throw CoreException.of(CoreException.Reason.INVALID);
        StreamRecv.RecvPart r = st.rcv;
        if (r.stopped) throw CoreException.stream(new StreamError(StreamError.Kind.CLOSED_LOCALLY, st.tab.id(), r.stopCode, 0, false));
        if (r.state == Stream.RECV_DATA_READ) return new ReadResult(0, true);
        int n = 0;
        if (dst.hasRemaining()) {
            if (r.reset) {
                int lim = (int) Math.min(dst.remaining(), r.resetRel - Math.min(r.resetRel, r.pushOff));
                ByteBuffer d = dst.slice(dst.position(), lim);
                n = r.buf.read(r.pushOff, d);
                dst.position(dst.position() + n);
            } else {
                n = r.buf.read(r.pushOff, dst);
            }
        }
        if (n > 0) { r.pushOff += n; consumed(st, n); }
        if (r.reset && r.readOff >= r.resetRel && r.buf.contiguous(r.pushOff) == r.pushOff) {
            if (n > 0) return new ReadResult(n, false);
            r.state = Stream.RECV_RESET_READ;
            table.maybeDiscard(st);
            throw CoreException.stream(r.errState);
        }
        if (r.finKnown && r.readOff == r.finalSize && !r.reset) {
            r.state = Stream.RECV_DATA_READ;
            table.maybeDiscard(st);
            return new ReadResult(n, true);
        }
        SessionError err = port.error();
        if (n == 0 && err != null) throw CoreException.session(err);
        return n == 0 ? ReadResult.WAIT : new ReadResult(n, false);
    }

    private void consumed(Stream st, long n) {
        st.rcv.readOff += n;
        stats.appRead(n);
        st.counters.read(n);
        table.bufSub(n);
        creditChanged(st);
        tuner.consumed();
    }

    /** Consumes up to max contiguous bytes without copying; null when the caller must wait. */
    public Chunk readChunk(Stream st, int max) throws CoreException {
        if (!st.hasRecv) throw CoreException.of(CoreException.Reason.INVALID);
        StreamRecv.RecvPart r = st.rcv;
        if (r.stopped) throw CoreException.stream(new StreamError(StreamError.Kind.CLOSED_LOCALLY, st.tab.id(), r.stopCode, 0, false));
        if (r.state == Stream.RECV_DATA_READ) return new Chunk(ByteBuffer.allocate(0), null, true);
        if (r.reset && r.pushOff + max > r.resetRel) max = (int) (r.resetRel - Math.min(r.resetRel, r.pushOff));
        RecvBuffer.Chunk c = r.buf.take(r.pushOff, max);
        if (c != null) {
            r.pushOff += c.data().remaining();
            consumed(st, c.data().remaining());
            return new Chunk(c.data(), c.buf(), false);
        }
        if (r.reset && r.readOff >= r.resetRel) {
            r.state = Stream.RECV_RESET_READ;
            table.maybeDiscard(st);
            throw CoreException.stream(r.errState);
        }
        if (r.finKnown && r.readOff == r.finalSize && !r.reset) {
            r.state = Stream.RECV_DATA_READ;
            table.maybeDiscard(st);
            return new Chunk(ByteBuffer.allocate(0), null, true);
        }
        SessionError err = port.error();
        if (err != null) throw CoreException.session(err);
        return null;
    }

    // ---- the receive lane: in-order data handed to the application ahead of reads ----

    /**
     * Moves the next contiguous piece past the pushed offset out of the buffer, without
     * copying: the caller owns the returned reference. Null when nothing is deliverable.
     * The bytes stay counted (window, session buffer) until {@link #laneConsumed}.
     */
    public Chunk push(Stream st, int max) {
        if (!st.hasRecv) return null;
        StreamRecv.RecvPart r = st.rcv;
        if (r.stopped || r.state == Stream.RECV_DATA_READ || r.state == Stream.RECV_RESET_READ) return null;
        if (r.reset) max = (int) Math.min(max, r.resetRel - Math.min(r.resetRel, r.pushOff));
        RecvBuffer.Chunk c = r.buf.take(r.pushOff, max);
        if (c == null) return null;
        r.pushOff += c.data().remaining();
        return new Chunk(c.data(), c.buf(), false);
    }

    /**
     * What follows the pushed data, once everything deliverable was pushed: null while the
     * part may receive more, {@link #FIN}, or the part's or the session's error.
     */
    public Object pushEnd(Stream st) {
        if (!st.hasRecv) return CoreException.of(CoreException.Reason.INVALID);
        StreamRecv.RecvPart r = st.rcv;
        if (r.stopped) return CoreException.stream(new StreamError(StreamError.Kind.CLOSED_LOCALLY, st.tab.id(), r.stopCode, 0, false));
        if (r.state == Stream.RECV_DATA_READ) return FIN;
        if (r.reset && r.pushOff >= r.resetRel) return CoreException.stream(r.errState);
        if (r.finKnown && r.pushOff == r.finalSize && !r.reset) return FIN;
        SessionError err = port.error();
        return err != null ? CoreException.session(err) : null;
    }

    /** The offset up to which data was pushed. */
    public long pushed(Stream st) {
        return st.rcv.pushOff;
    }

    /** The end marker of a finished part. */
    public static final Object FIN = new Object() { @Override public String toString() { return "FIN"; } };

    /** Where the application must stop delivering pushed bytes: the Reliable Size of a reset, else none. */
    public long pushCut(Stream st) {
        return st.hasRecv && st.rcv.reset ? st.rcv.resetRel : Long.MAX_VALUE;
    }

    /**
     * The application consumed read bytes and discarded dropped pushed bytes (beyond a
     * reset's Reliable Size); the window and the buffer count follow.
     */
    public void laneConsumed(Stream st, long read, long dropped) {
        if (!st.hasRecv || read + dropped == 0) return;
        StreamRecv.RecvPart r = st.rcv;
        if (r.stopped) return;
        long n = Math.min(read + dropped, r.pushOff - r.readOff);
        long counted = Math.min(read, n);
        if (counted > 0) {
            stats.appRead(counted);
            st.counters.read(counted);
            tuner.consumed();
        }
        r.readOff += n;
        table.bufSub(n);
        creditChanged(st);
    }

    /** The application saw the part's end (FIN or reset): the part is read. */
    public void laneEnded(Stream st) {
        if (!st.hasRecv) return;
        StreamRecv.RecvPart r = st.rcv;
        if (r.reset && r.readOff >= r.resetRel && r.state == Stream.RECV_RESET_RECVD) r.state = Stream.RECV_RESET_READ;
        else if (r.finKnown && r.readOff == r.finalSize && !r.reset && r.state != Stream.RECV_DATA_READ) r.state = Stream.RECV_DATA_READ;
        else return;
        table.maybeDiscard(st);
    }

    /** Where window updates become due: the application reports consumption at least this often. */
    public long laneReportBytes(Stream st) {
        return Math.max(1, ConnFlow.updateThreshold(st.rcv.window));
    }

    /** Bytes a read returns now; with none, whether the next read returns the part's end at once. */
    public long readable(Stream st) {
        if (!st.hasRecv) return 0;
        StreamRecv.RecvPart r = st.rcv;
        long n = r.buf.contiguous(r.pushOff) - r.pushOff;
        if (r.reset) n = Math.min(n, r.resetRel - Math.min(r.resetRel, r.pushOff));
        return n;
    }

    /** Whether a read with nothing readable would return the part's end (FIN, reset, stop, session error). */
    public boolean readEnded(Stream st) {
        if (!st.hasRecv) return true;
        StreamRecv.RecvPart r = st.rcv;
        return r.stopped || r.state == Stream.RECV_DATA_READ || r.finKnown && r.readOff == r.finalSize && !r.reset
                || r.reset && r.readOff >= r.resetRel || port.error() != null;
    }

    public void waitRead(Stream st, Waiter sig) { st.rcv.rsig = sig; }

    /** Removes only this operation's receiving wake-up slot. */
    public void cancelReadWait(Stream st, Waiter sig) { if (st.rcv.rsig == sig) st.rcv.rsig = null; }

    /** Abandons the receiving part with code (STOP_SENDING). */
    public void stop(Stream st, long code) {
        if (!st.hasRecv) return;
        stopRecv(st, code);
        table.maybeDiscard(st);
    }

    void stopRecv(Stream st, long code) {
        StreamRecv.RecvPart r = st.rcv;
        if (r.stopped || r.state == Stream.RECV_DATA_READ || r.state == Stream.RECV_RESET_READ) return;
        r.stopped = true;
        r.stopCode = code;
        // Pushed bytes the application did not consume leave the buffer count with the rest;
        // the lane was closed first, so it never reports them.
        long d = r.buf.clear() + (r.pushOff - r.readOff);
        if (d > 0) table.bufSub(d);
        r.readOff = r.finKnown ? r.finalSize : r.highest;
        r.pushOff = r.readOff;
        if (!st.snd.cancelled() && (r.state == Stream.RECV_RECV || r.state == Stream.RECV_SIZE_KNOWN)) ctl.stream(st, ControlQueue.STOP);
        progress(st);
    }

    // ---- stream credit ----

    /**
     * Sets a receiving part's window: initial raises the advertised limit at once, max
     * caps auto-tuning. Values below 16 KiB are raised to 16 KiB; 0 leaves a value.
     */
    public void setReceiveWindow(Stream st, long initial, long max) {
        StreamRecv.RecvPart r = st.rcv;
        if (max > 0) r.maxWin = Math.max(max, 16 << 10);
        if (initial > 0) {
            long w = Math.max(initial, 16 << 10);
            if (r.maxWin > 0) w = Math.min(w, r.maxWin);
            if (w > r.window) {
                r.window = w;
                ctl.stream(st, ControlQueue.MAX_STREAM_DATA);
            }
        }
    }

    private void creditChanged(Stream st) {
        StreamRecv.RecvPart r = st.rcv;
        if (r.finKnown || r.stopped || r.reset) return;
        if (ConnFlow.creditDue(r.readOff + r.window, r.adv, r.window, r.highest, r.blockSeen == r.adv && r.blockSeen > 0)) {
            ctl.stream(st, ControlQueue.MAX_STREAM_DATA);
        }
    }

    // ---- writing ----

    /** Writes MAX_STREAM_DATA for st when its window moved (or a copy is forced); false when the batch is full. */
    boolean writeMaxStreamData(RecordWriter w, Stream st, PathLayer path, long now) {
        StreamRecv.RecvPart r = st.rcv;
        tuner.streamUpdate(st);
        long v = r.readOff + r.window;
        if (r.finKnown || r.stopped || !(v > r.adv || st.rcv.forceMaxStreamData)) return true;
        long id = st.tab.id();
        if (!w.frame(1 + io.zmux.carrier.VarInt.length(id) + io.zmux.carrier.VarInt.length(v), true,
                bb -> io.zmux.internal.wire.FrameEncoder.maxStreamData(bb, id, v))) return false;
        w.logFrame(FrameDesc.id(FrameDesc.Kind.MAX_STREAM_DATA, id));
        path.maxStreamDataSent(st, r.highest >= r.adv || r.blockSeen == r.adv && r.blockSeen > 0);
        if (r.tune != null && v > r.adv) r.tune.sent(r.adv, now, r.highest == r.adv);
        r.adv = Math.max(r.adv, v);
        st.rcv.forceMaxStreamData = false;
        return true;
    }

    /** Writes STOP_SENDING for st; false when the batch is full. */
    boolean writeStop(RecordWriter w, Stream st) {
        long id = st.tab.id(), c = st.rcv.stopCode;
        if (!w.frame(1 + io.zmux.carrier.VarInt.length(id) + io.zmux.carrier.VarInt.length(c), true,
                bb -> io.zmux.internal.wire.FrameEncoder.stopSending(bb, id, c))) return false;
        w.logFrame(FrameDesc.id(FrameDesc.Kind.STOP, id));
        st.rcv.stopSent = true;
        w.wroteTraffic();
        return true;
    }

    /** Sets the error the receiving part reports; with keep an earlier error stays. */
    static void error(Stream st, StreamError e, boolean keep) {
        if (!keep || st.rcv.errState == null) st.rcv.errState = e;
    }

    /** The initial receive window, advertised as such. */
    static void initWindow(Stream st, long w) {
        st.rcv.window = w;
        st.rcv.adv = w;
    }

    /** Window auto-tuning starts for st. */
    static void startTuning(Stream st) {
        st.rcv.tune = new Tuner();
    }

    /** Auto-tuning grew st's receive window. */
    static void growWindow(Stream st, long w) {
        st.rcv.window = w;
    }

    /** The receiving half of a local stream that failed before its first frame: reads report e. */
    static void failUnopened(Stream st, StreamError e) {
        RecvPart r = st.rcv;
        r.state = Stream.RECV_RESET_RECVD;
        r.reset = true;
        r.resetRel = 0;
        r.errState = e;
    }

    static void neverOpened(Stream st) {
        st.rcv.state = Stream.RECV_DATA_READ;
    }

    /** A lost MAX_STREAM_DATA must be resent even without a window change. */
    static void forceMaxStreamData(Stream st) {
        st.rcv.forceMaxStreamData = true;
    }

    /** CLOSED: the buffer is dropped and pushed bytes no longer count (the session's count is cleared). */
    static void releaseBuffer(Stream st) {
        st.rcv.buf.clear();
        st.rcv.readOff = st.rcv.pushOff;
    }

    /** The application's read waiter of st. */
    static void waiter(Stream st, Waiter w) {
        st.rcv.rsig = w;
    }

    /** The receiving part of a stream (RFC 9000 Section 3.2); written only here. */
    static final class RecvPart {
        private io.zmux.internal.wire.Preamble peerPre;
        private Waiter rsig;
        private boolean forceMaxStreamData;
        private final RecvBuffer buf = new RecvBuffer();
        private final RangeSet arrived = new RangeSet();
        private long highest;
        private long finalSize;
        private long readOff;
        /** The offset up to which data left the buffer for the receive lane; readOff <= pushOff. */
        private long pushOff;
        private long window;
        private long adv;
        private long stopCode;
        private long resetCode;
        private long resetRel;
        private long relLo;
        private StreamError errState;
        private long preSize;
        private long blockSeen;
        private Tuner tune;
        private long maxWin;
        private int state = Stream.RECV_RECV;
        private boolean finKnown;
        private boolean stopped;
        private boolean stopSent;
        private boolean reset;
        private boolean preDone;
    
        RecvPart(boolean hasRecv) {
            if (hasRecv) relLo = (1L << 62) - 1;
        }

        RecvBuffer buf() {
            return buf;
        }
        RangeSet arrived() {
            return arrived;
        }
        long highest() {
            return highest;
        }
        long finalSize() {
            return finalSize;
        }
        long readOff() {
            return readOff;
        }
        long pushOff() {
            return pushOff;
        }
        long window() {
            return window;
        }
        long adv() {
            return adv;
        }
        long stopCode() {
            return stopCode;
        }
        long resetCode() {
            return resetCode;
        }
        long resetRel() {
            return resetRel;
        }
        long relLo() {
            return relLo;
        }
        StreamError errState() {
            return errState;
        }
        long preSize() {
            return preSize;
        }
        long blockSeen() {
            return blockSeen;
        }
        Tuner tune() {
            return tune;
        }
        long maxWin() {
            return maxWin;
        }
        int state() {
            return state;
        }
        boolean finKnown() {
            return finKnown;
        }
        boolean stopped() {
            return stopped;
        }
        boolean stopSent() {
            return stopSent;
        }
        boolean reset() {
            return reset;
        }
        boolean preDone() {
            return preDone;
        }

        io.zmux.internal.wire.Preamble peerPre() {
            return peerPre;
        }

        Waiter rsig() {
            return rsig;
        }

        boolean forceMaxStreamData() {
            return forceMaxStreamData;
        }
    }
}
