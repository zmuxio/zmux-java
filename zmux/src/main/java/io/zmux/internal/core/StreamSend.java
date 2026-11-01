package io.zmux.internal.core;

import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import java.util.ArrayList;

/**
 * The sending parts of a session's streams (SPEC.md Sections 4.3, 7.1 and 8.4,
 * IMPLEMENTATION.md Section 6.4): application writes into the send buffer, the
 * water-filling share of the session's send buffer, FIN, resets and the peer's
 * STOP_SENDING and MAX_STREAM_DATA, send-capacity waiters, and the terminal states.
 * Effects on the session go through {@link Port}.
 */
public final class StreamSend {
    /** What the sending parts ask of the session. */
    interface Port {
        long now();

        /** The session's terminal error, or null. */
        SessionError error();

        /** Whether the peer's parameters arrived. */
        boolean established();

        /** The peer's parameters (the assumed ones before establishment). */
        TransportParams peer();

        Features features();

        /** The session's effective send buffer (the path layer's share). */
        long sendBuffer();

        /** Sent bytes the path layer retained are released. */
        void unretain(long n);

        /** Stream data or a control frame wants to be written. */
        void wantWrite();
    }

    private final SessionStats stats;
    private final Outbox outbox;
    private final ConnFlow conn;
    private final StreamScheduler sched;
    private final ControlQueue ctl;
    private final StreamTable table;
    private final Port port;
    /** Streams with send-capacity waiters. */
    private final ArrayList<Stream> capWaiters = new ArrayList<>(0);

    StreamSend(SessionStats stats, Outbox outbox, ConnFlow conn, StreamScheduler sched, ControlQueue ctl, StreamTable table,
               Port port) {
        this.stats = stats;
        this.outbox = outbox;
        this.conn = conn;
        this.sched = sched;
        this.ctl = ctl;
        this.table = table;
        this.port = port;
    }

    /** The bytes the part could transmit now: the smaller remaining credit minus what it holds unsent. */
    public long sendCapacity(Stream st) {
        StreamSend.SendPart sp = st.snd;
        if (!st.hasSend || st.snd.cancelled || sp.errState != null || sp.finWritten || sp.reset || sp.stopped || port.error() != null) return 0;
        long unsent = st.unsent();
        long credit = sp.maxData - Math.min(sp.maxData, sp.sent);
        if (!st.tab.idSet() && !port.established()) {
            credit = 0;
        } else if (!st.tab.idSet()) {
            TransportParams peer = port.peer();
            long w = st.bidi ? peer.maxStreamDataBidiRemote : peer.maxStreamDataUni;
            long pre = port.features().preamble() ? st.preSize() : 0;
            credit = w - Math.min(w, pre);
        }
        long connCredit = conn.room();
        long c = Math.min(credit, connCredit);
        return c <= unsent ? 0 : c - unsent;
    }

    public boolean holdsUnsent(Stream st) {
        return st.unsent() > 0;
    }

    /** The error a write gets now, or null when it may proceed. */
    public CoreException writeError(Stream st) {
        StreamSend.SendPart sp = st.snd;
        if (!st.hasSend) return CoreException.of(CoreException.Reason.INVALID);
        if (sp.errState != null) return CoreException.stream(sp.errState);
        SessionError err = port.error();
        if (err != null) return CoreException.session(err);
        if (sp.finWritten) return CoreException.stream(StreamError.of(StreamError.Kind.CLOSED_LOCALLY, st.tab.id()));
        return null;
    }

    /**
     * Takes as much of p as the send buffer admits and returns the count; 0 means wait.
     * With fin, FIN follows p once all of it was taken.
     */
    public int write(Stream st, java.nio.ByteBuffer p, boolean fin) throws CoreException {
        CoreException e = writeError(st);
        if (e != null) throw e;
        StreamSend.SendPart sp = st.snd;
        int want = p.remaining();
        int n = (int) Math.min(want, sendRoom(st));
        long now = port.now();
        long hc = st.heldClass();
        if (n > 0) {
            if (st.unsent() == 0) sp.waitAt = now;
            java.nio.ByteBuffer part = p.slice(p.position(), n);
            sp.q.write(part);
            p.position(p.position() + n);
            stats.appWritten(n);
            sp.appWritten += n;
        }
        if (fin && n == want) sp.finWritten = true;
        if (n == want) {
            sp.wwait = false;
            sp.writeDone = now;
        }
        conn.held(hc, st.heldAfter());
        if (n > 0 || sp.finWritten) becomeSend(st);
        return n;
    }

    /** Queues data that buf keeps alive without copying; the queue takes its own reference. */
    public int writeChunk(Stream st, java.nio.ByteBuffer data, ReadBuf buf, boolean fin) throws CoreException {
        if (buf == null) return write(st, data, fin);
        CoreException e = writeError(st);
        if (e != null) throw e;
        StreamSend.SendPart sp = st.snd;
        int want = data.remaining();
        int n = (int) Math.min(want, sendRoom(st));
        long now = port.now();
        long hc = st.heldClass();
        if (n > 0) {
            if (st.unsent() == 0) sp.waitAt = now;
            sp.q.writeRef(data.slice(data.position(), n), buf);
            data.position(data.position() + n);
            stats.appWritten(n);
            sp.appWritten += n;
        }
        if (fin && n == want) sp.finWritten = true;
        if (n == want) {
            sp.wwait = false;
            sp.writeDone = now;
        }
        conn.held(hc, st.heldAfter());
        if (n > 0 || sp.finWritten) becomeSend(st);
        return n;
    }

    // ---- the send lane: bytes the application appends ahead of the core ----

    /**
     * Grants up to max of the part's remaining send-buffer share as a lease the application may fill
     * without asking (IMPLEMENTATION.md Section 6.4: leased bytes count as held, so the
     * water-filling sums stay the single authority). Returns the grant; 0 when the part
     * cannot take data or has no room.
     */
    public long lease(Stream st, long max) {
        if (writeError(st) != null || st.snd.cancelled || st.tab.discarded()) return 0;
        long room = Math.min(sendRoom(st), max);
        if (room <= 0) return 0;
        long h = st.heldAfter();
        st.snd.leased += room;
        conn.leased(room);
        conn.held(h, st.heldAfter());
        return room;
    }

    /** Takes back n leased bytes the application did not use. */
    public void unlease(Stream st, long n) {
        n = Math.min(n, st.snd.leased);
        if (n <= 0) return;
        long h = st.heldAfter();
        st.snd.leased -= n;
        conn.leased(-n);
        conn.held(h, st.heldAfter());
    }

    /** The bytes leased and not yet appended. */
    public long leased(Stream st) {
        return st.snd.leased;
    }

    /**
     * Appends leased bytes without copying; rb keeps them alive (the queue takes its own
     * reference). When the part can no longer take data the bytes are dropped (they leave
     * the lease all the same) and the part's error is thrown.
     */
    public void writeLeased(Stream st, java.nio.ByteBuffer data, ReadBuf rb) throws CoreException {
        StreamSend.SendPart sp = st.snd;
        int n = data.remaining();
        long h = st.heldAfter();
        long used = Math.min(n, sp.leased);
        sp.leased -= used;
        conn.leased(-used);
        CoreException e = writeError(st);
        if (e != null || sp.cancelled) {
            conn.held(h, st.heldAfter());
            if (e != null) throw e;
            return;
        }
        long now = port.now();
        if (n > 0) {
            if (st.unsent() == 0) sp.waitAt = now;
            sp.q.writeRef(data, rb);
            stats.appWritten(n);
            sp.appWritten += n;
        }
        sp.writeDone = now;
        conn.held(h, st.heldAfter());
        if (n > 0) becomeSend(st);
    }

    private void becomeSend(Stream st) {
        if (st.snd.state == Stream.SEND_READY && st.tab.idSet()) st.snd.state = Stream.SEND_SEND;
        sched.ensure(st);
        port.wantWrite();
    }

    /**
     * The send buffer by water-filling: small directions come off first, every other may
     * hold max(64 KiB, (send_buffer - small bytes) / n); each may always hold 64 KiB.
     */
    public long sendRoom(Stream st) {
        long cl = st.heldClass();
        long held = cl >>> 2;
        boolean big = (cl & 1) != 0, small = (cl & 2) != 0;
        int n = conn.bigCount();
        long total = port.sendBuffer();
        long pool = total - Math.min(total, conn.smallHeld());
        if (!big) {
            n++;
            if (small) pool += Math.min(held, total - pool);
        }
        long share = Math.max(Stream.SMALL_HELD, pool / Math.max(n, 1));
        if (st.snd.bufLimit > 0) share = Math.min(share, st.snd.bufLimit);
        long room = held >= share ? 0 : share - held;
        long totalRoom = total - Math.min(total, conn.sendHeld());
        long floor = st.snd.bufLimit > 0 ? Math.min(Stream.SMALL_HELD, st.snd.bufLimit) : Stream.SMALL_HELD;
        long floorRoom = floor - Math.min(floor, held);
        // The floor lets a stream keep its small share even when the session total is spent, but
        // never past its own share.
        long allowed = Math.max(totalRoom, floorRoom);
        return Math.min(room, allowed);
    }

    /** Bounds the part's written-but-unreleased bytes. */
    public void setSendBuffer(Stream st, long n) {
        st.snd.bufLimit = Math.max(n, 16 << 10);
        outbox.wake(st.snd.wsig);
    }

    /** Watches sending state without declaring an application write blocked on buffer space. */
    public void waitWriteState(Stream st, Waiter sig) { st.snd.wsig = sig; }

    public boolean resetAtInUse() { return port.established() && port.peer().has(TransportParams.RESET_AT); }

    /** Registers sig for write progress of st. */
    public void waitWrite(Stream st, Waiter sig) {
        st.snd.wsig = sig;
        StreamSend.SendPart sp = st.snd;
        if (!sp.wwait) {
            long now = port.now();
            if (!sp.wwaitSet || Time.sub(now, sp.writeDone) > Time.MILLISECOND) sp.wwaitAt = now;
            long h = st.heldClass();
            sp.wwait = true;
            sp.wwaitSet = true;
            conn.held(h, st.heldAfter());
        }
    }

    /** Removes only this operation's sending wake-up slot and waiting hint. */
    public void cancelWriteWait(Stream st, Waiter sig) {
        if (st.snd.wsig != sig) return;
        st.snd.wsig = null;
        long before = st.heldClass();
        st.snd.wwait = false;
        conn.held(before, st.heldAfter());
    }

    /** Queues FIN after the buffered data. */
    public void finish(Stream st) throws CoreException {
        if (st.hasSend && st.snd.finWritten && st.snd.errState == null) return;
        CoreException e = writeError(st);
        if (e != null) throw e;
        st.snd.finWritten = true;
        checkCapacity(st);
        becomeSend(st);
    }

    /** Marks the data written so far as delivered even if the stream is reset later. */
    public void setReliableBoundary(Stream st) {
        st.snd.boundary = st.snd.appWritten;
        st.snd.hasBound = true;
    }

    /** Resets the sending part with code. */
    public void reset(Stream st, long code) {
        StreamSend.SendPart sp = st.snd;
        if (!st.hasSend || sp.reset || sp.errState != null && sp.errState.kind() != StreamError.Kind.STOPPED
                || sp.state == Stream.SEND_DATA_RECVD || sp.state == Stream.SEND_RESET_RECVD) {
            table.maybeDiscard(st);
            return;
        }
        if (sp.errState == null) sp.errState = new StreamError(StreamError.Kind.CLOSED_LOCALLY, st.tab.id(), code, 0, false);
        if (!st.tab.idSet()) {
            if (!sp.hasBound || sp.boundary == 0 || !port.established()) {
                cancelUnopened(st);
                return;
            }
            if (!table.preambleAllowed(st, this)) return;
            table.allocId(st);
        }
        startReset(st, code, false);
    }

    /** Resets the part with ZMUX_REJECTED (EXTENSIONS.md Sections 3.4 and 7.2). */
    void reject(Stream st) {
        st.snd.errState = StreamError.of(StreamError.Kind.REJECTED, st.tab.id());
        startReset(st, TransportError.REJECTED, false);
    }

    /** Queues RESET_STREAM or RESET_STREAM_AT; stopReply: the reset answers STOP_SENDING. */
    private void startReset(Stream st, long code, boolean stopReply) {
        StreamSend.SendPart sp = st.snd;
        if (sp.reset) {
            if (stopReply && sp.resetRel > sp.sent) {
                sp.resetRel = sp.sent;
                sp.resetSent = false;
                ctl.stream(st, ControlQueue.RESET);
            }
            return;
        }
        sp.reset = true;
        sp.resetCode = code;
        if (stopReply) {
            sp.resetRel = 0;
        } else if (sp.hasBound) {
            sp.resetRel = sp.preLen + sp.boundary;
            sp.resetAt = true;
        } else if (port.features().preamble() && st.local) {
            sp.resetRel = sp.preLen;
            sp.resetAt = true;
        }
        if (sp.resetAt && !port.peer().has(TransportParams.RESET_AT)) {
            sp.resetAt = false;
            sp.resetRel = 0;
        }
        sp.resetRel = Math.min(sp.resetRel, sp.q.end());
        if (sp.resetRel == 0) sp.resetAt = false;
        sp.finWritten = false;
        ctl.stream(st, ControlQueue.RESET);
        sched.ensure(st);
        checkCapacity(st);
        port.wantWrite();
    }

    /** Forgets a stream that never emitted a frame (SPEC.md Section 7.2). */
    void cancelUnopened(Stream st) {
        if (st.snd.cancelled) return;
        st.snd.cancelled = true;
        // No peer can ever open the reverse half of an identifier that was not emitted.
        if (st.hasRecv) StreamRecv.neverOpened(st);
        table.wakeObservers(st);
        checkCapacity(st);
        ctl.remove(st);
        table.unopenedGone(st);
        long h = st.heldClass();
        st.snd.wwait = false;
        st.snd.q.drop();
        conn.held(h, st.heldAfter());
        sched.remove(st);
        table.unreserve(st);
    }

    /** A received STOP_SENDING (SPEC.md Section 4.3). */
    void onStopSending(Stream st, long code) {
        StreamSend.SendPart sp = st.snd;
        if (sp.state == Stream.SEND_DATA_RECVD || sp.state == Stream.SEND_RESET_RECVD) return;
        if (!sp.stopped) {
            sp.stopped = true;
            sp.stopCode = code;
            if (sp.errState == null || sp.errState.kind() == StreamError.Kind.CLOSED_LOCALLY && !sp.reset) {
                sp.errState = new StreamError(StreamError.Kind.STOPPED, st.tab.id(), code, 0, true);
            }
        }
        startReset(st, sp.reset ? sp.resetCode : code, true);
        outbox.wake(st.snd.wsig);
    }

    /** Applies MAX_STREAM_DATA; returns whether it raised the limit. */
    boolean onSendCredit(Stream st, long v) {
        if (v <= st.snd.maxData) return false;
        if (st.unsent() > 0 && Math.max(st.snd.sent, st.snd.q.base()) >= st.snd.maxData) st.snd.creditAt = port.now();
        st.snd.maxData = v;
        checkCapacity(st);
        sched.ensure(st);
        outbox.wake(st.snd.wsig);
        port.wantWrite();
        return true;
    }

    /** Releases the queue below upTo once nothing must be kept there, and wakes the writer. */
    void release(Stream st, long upTo) {
        StreamSend.SendPart sp = st.snd;
        long h = st.heldClass();
        sp.q.release(upTo);
        conn.held(h, st.heldAfter());
        outbox.wake(st.snd.wsig);
    }

    /** Records a terminal sending state and releases buffers. */
    void sendTerminal(Stream st, int sendState) {
        StreamSend.SendPart sp = st.snd;
        sp.state = sendState;
        if (sp.sent > sp.q.base()) port.unretain(sp.sent - sp.q.base());
        long h = st.heldClass();
        sp.wwait = false;
        sp.q.drop();
        conn.held(h, st.heldAfter());
        sched.remove(st);
        outbox.wake(st.snd.wsig);
        table.maybeDiscard(st);
    }

    // ---- send-capacity waiters ----

    /**
     * Registers sig until the part's capacity is at least min, or above 0 while it holds
     * nothing unsent, or the part can no longer send; true when that holds now.
     */
    public boolean waitSendCapacity(Stream st, long min, Waiter sig) {
        if (capReady(st, min)) return true;
        var waits = ext(st).capWaits;
        waits.removeIf(w -> w.sig() == sig);
        if (waits.isEmpty()) capWaiters.add(st);
        waits.add(new Stream.CapacityWait(sig, Math.max(min, 1)));
        return false;
    }

    /** Withdraws sig's capacity wait; other waiters of the stream stay registered. */
    public void cancelCapacityWait(Stream st, Waiter sig) {
        if (st.snd.ext == null) return;
        var waits = st.snd.ext.capWaits;
        if (waits.removeIf(w -> w.sig() == sig) && waits.isEmpty()) capWaiters.remove(st);
    }

    private boolean capReady(Stream st, long min) {
        StreamSend.SendPart sp = st.snd;
        if (!st.hasSend || st.snd.cancelled || sp.errState != null || sp.finWritten || sp.reset || sp.stopped || port.error() != null
                || st.tab.discarded()) {
            return true;
        }
        long c = sendCapacity(st);
        return c >= Math.max(min, 1) || c > 0 && st.unsent() == 0;
    }

    /** Wakes and removes the stream's satisfied waiters; true when none remain. */
    private boolean wakeSatisfied(Stream st) {
        var waits = st.snd.ext.capWaits;
        waits.removeIf(w -> {
            if (!capReady(st, w.min())) return false;
            outbox.wake(w.sig());
            return true;
        });
        return waits.isEmpty();
    }

    void checkCapacity(Stream st) {
        if (st.snd.ext != null && !st.snd.ext.capWaits.isEmpty() && wakeSatisfied(st)) capWaiters.remove(st);
    }

    /** Re-evaluates every capacity waiter (session credit or state changed). */
    void wakeCapacity() {
        capWaiters.removeIf(this::wakeSatisfied);
    }

    // ---- writing (the Transmitter frames, this component accounts) ----

    /** A STREAM frame of st is being written: it is opened and in SEND. */
    void writing(Stream st) {
        table.markOpened(st);
        if (st.snd.state == Stream.SEND_READY) st.snd.state = Stream.SEND_SEND;
    }

    /** Data of st up to end was written (fin: with FIN); returns the newly sent bytes beyond the old high mark. */
    long dataWritten(Stream st, long end, boolean fin) {
        StreamSend.SendPart sp = st.snd;
        long grown = 0;
        if (end > sp.sent) {
            grown = end - sp.sent;
            sp.sent = end;
        }
        if (fin) {
            sp.finSent = true;
            sp.state = Stream.SEND_DATA_SENT;
        }
        return grown;
    }

    /** Marks st blocked at its current credit; false when already marked there. */
    boolean markBlocked(Stream st) {
        StreamSend.SendPart sp = st.snd;
        if (sp.blocked == sp.maxData + 1) return false;
        sp.blocked = sp.maxData + 1;
        return true;
    }

    /** Writes RESET_STREAM(_AT) once the data below the Reliable Size went out (SPEC.md 4.3); false when the batch is full. */
    boolean writeReset(RecordWriter w, Stream st, boolean retainsSent) {
        StreamSend.SendPart sp = st.snd;
        if (sp.sent < sp.resetRel) return true;
        long id = st.tab.id();
        long fin = sp.sent; // at least resetRel here
        long code = sp.resetCode, rel = sp.resetRel;
        boolean at = sp.resetAt;
        int n = 1 + io.zmux.carrier.VarInt.length(id) + io.zmux.carrier.VarInt.length(code)
                + io.zmux.carrier.VarInt.length(fin) + (at ? io.zmux.carrier.VarInt.length(rel) : 0);
        if (!w.frame(n, true, bb -> {
            if (at) io.zmux.internal.wire.FrameEncoder.resetStreamAt(bb, id, code, fin, rel);
            else io.zmux.internal.wire.FrameEncoder.resetStream(bb, id, code, fin);
        })) return false;
        sp.resetSent = true;
        ControlQueue.done(st, ControlQueue.RESET);
        w.logFrame(FrameDesc.id(FrameDesc.Kind.RESET, id));
        conn.consumed(fin - sp.sent);
        sp.sent = fin;
        w.wroteActivity();
        w.wroteTraffic();
        if (!retainsSent) sendTerminal(st, Stream.SEND_RESET_RECVD);
        return true;
    }

    /** Writes PRIORITY_UPDATE for st (EXTENSIONS.md 5); false when the batch is full. */
    boolean writePriority(RecordWriter w, Stream st) {
        long id = st.tab.id();
        Ext x = ext(st);
        long seq = x.reqSeq;
        int u = x.reqUrgency;
        boolean inc = x.reqIncremental;
        int n = 4 + io.zmux.carrier.VarInt.length(id) + io.zmux.carrier.VarInt.length(seq) + 1;
        if (!w.frame(n, true, bb -> io.zmux.internal.wire.FrameEncoder.priorityUpdate(bb, id, seq, u, inc))) return false;
        w.logFrame(new FrameDesc(FrameDesc.Kind.PRIORITY, false, false, id, 0, 0, seq));
        x.reqSent = seq;
        return true;
    }

    /** Writes STREAM_DATA_BLOCKED while st is still blocked at the marked credit; false when the batch is full. */
    boolean writeBlocked(RecordWriter w, Stream st) {
        StreamSend.SendPart sp = st.snd;
        if (sp.blocked != sp.maxData + 1) return true;
        long id = st.tab.id(), v = sp.maxData;
        if (!w.frame(1 + io.zmux.carrier.VarInt.length(id) + io.zmux.carrier.VarInt.length(v), true,
                bb -> io.zmux.internal.wire.FrameEncoder.streamDataBlocked(bb, id, v))) return false;
        w.logFrame(FrameDesc.id(FrameDesc.Kind.STREAM_DATA_BLOCKED, id));
        return true;
    }

    /** The bootstrap restarted (early data rejected): st's data goes again from the start under the new parameters; true when a RESET_STREAM is owed again. */
    static boolean restart(Stream st) {
        SendPart sp = st.snd;
        sp.sent = 0;
        sp.finSent = false;
        sp.maxData = 0;
        if (sp.state == Stream.SEND_DATA_SENT) sp.state = Stream.SEND_SEND;
        if (!sp.reset) return false;
        sp.resetSent = false;
        return true;
    }

    /** Raises st's send credit to v (initial parameters); true when it grew. */
    static boolean raiseCredit(Stream st, long v) {
        if (v <= st.snd.maxData) return false;
        st.snd.maxData = v;
        return true;
    }

    /** Sets the error the sending part reports; with keep an earlier error stays. */
    static void error(Stream st, StreamError e, boolean keep) {
        if (!keep || st.snd.errState == null) st.snd.errState = e;
    }

    /** Puts the encoded preamble ahead of st's data (EXTENSIONS.md 3). */
    static void prependPreamble(Stream st, byte[] b) {
        SendPart sp = st.snd;
        sp.preLen = b.length;
        sp.q.prepend(b);
        if (sp.hasBound) sp.boundary += sp.preLen;
    }

    /** The sending part of a stream (RFC 9000 Section 3.1); written only here. */
    static final class SendPart {
        private boolean cancelled;
        private Waiter wsig;
        private Ext ext;
        private final SendQueue q = new SendQueue();
        /** Bytes granted to the application's send lane and not yet appended; counted as held. */
        private long leased;
        private final Lanes.Bind bind = new Lanes.Bind();
        private long sent;
        private long maxData;
        private long blocked;
        private long preLen;
        private long boundary;
        private long resetCode;
        private long resetRel;
        private long stopCode;
        private StreamError errState;
        private long bufLimit;
        private long appWritten;
        private long waitAt;
        private long wwaitAt;
        private long writeDone;
        private long creditAt;
        private int state = Stream.SEND_READY;
        private boolean finWritten;
        private boolean finSent;
        private boolean hasBound;
        private boolean reset;
        private boolean resetAt;
        private boolean resetSent;
        private boolean stopped;
        private boolean wwait;
        private boolean wwaitSet;
        /** The path-side state; null outside multipath. */
        private PathStreamState mps;

        PathStreamState mp() {
            if (mps == null) mps = new PathStreamState();
            return mps;
        }

        /** The end of the data that may still be sent: the written end, cut at the Reliable Size once reset. */
        long sendEnd() {
            return reset ? Math.min(q.end(), resetRel) : q.end();
        }

        SendQueue q() {
            return q;
        }
        long leased() {
            return leased;
        }
        Lanes.Bind bind() {
            return bind;
        }
        long sent() {
            return sent;
        }
        long maxData() {
            return maxData;
        }
        long blocked() {
            return blocked;
        }
        long preLen() {
            return preLen;
        }
        long boundary() {
            return boundary;
        }
        long resetCode() {
            return resetCode;
        }
        long resetRel() {
            return resetRel;
        }
        long stopCode() {
            return stopCode;
        }
        StreamError errState() {
            return errState;
        }
        long bufLimit() {
            return bufLimit;
        }
        long appWritten() {
            return appWritten;
        }
        long waitAt() {
            return waitAt;
        }
        long wwaitAt() {
            return wwaitAt;
        }
        long writeDone() {
            return writeDone;
        }
        long creditAt() {
            return creditAt;
        }
        int state() {
            return state;
        }
        boolean finWritten() {
            return finWritten;
        }
        boolean finSent() {
            return finSent;
        }
        boolean hasBound() {
            return hasBound;
        }
        boolean reset() {
            return reset;
        }
        boolean resetAt() {
            return resetAt;
        }
        boolean resetSent() {
            return resetSent;
        }
        boolean stopped() {
            return stopped;
        }
        boolean wwait() {
            return wwait;
        }
        boolean wwaitSet() {
            return wwaitSet;
        }
        PathStreamState mps() {
            return mps;
        }

        boolean cancelled() {
            return cancelled;
        }

        Waiter wsig() {
            return wsig;
        }

        Ext ext() {
            return ext;
        }
    }
    /** st's priority requests and capacity waiters, created on first use. */
    private static Ext ext(Stream st) {
        if (st.snd.ext == null) st.snd.ext = new Ext();
        return st.snd.ext;
    }

    /** Requests a priority for the peer's sending part with PRIORITY_UPDATE (EXTENSIONS.md 5). */
    static void requestPriority(Stream st, int urgency, boolean incremental) {
        Ext x = ext(st);
        x.reqPrio = true;
        x.reqUrgency = urgency;
        x.reqIncremental = incremental;
        x.reqSeq++;
    }

    /** The PRIORITY_UPDATE with seq was acknowledged. */
    static void priorityAcked(Stream st, long seq) {
        Ext x = st.snd.ext;
        if (x != null && x.reqSent == seq) x.reqAcked = seq;
    }

    /** Whether the latest requested priority is not acknowledged yet. */
    static boolean priorityOwed(Stream st) {
        Ext x = st.snd.ext;
        return x != null && x.reqAcked < x.reqSeq;
    }

    /** The application's write waiter of st. */
    static void waiter(Stream st, Waiter w) {
        st.snd.wsig = w;
    }


    /** A stream's PRIORITY_UPDATE requests and capacity waiters (EXTENSIONS.md 5); allocated on first use. */
    static final class Ext {
        private long reqSeq;
        private long reqSent;
        private long reqAcked;
        private boolean reqPrio;
        private int reqUrgency;
        private boolean reqIncremental;
        private final java.util.ArrayList<Stream.CapacityWait> capWaits = new java.util.ArrayList<>(0);

        Ext() {
        }

        long reqSeq() {
            return reqSeq;
        }

        long reqSent() {
            return reqSent;
        }

        long reqAcked() {
            return reqAcked;
        }

        boolean reqPrio() {
            return reqPrio;
        }

        int reqUrgency() {
            return reqUrgency;
        }

        boolean reqIncremental() {
            return reqIncremental;
        }

        java.util.ArrayList<Stream.CapacityWait> capWaits() {
            return capWaits;
        }
    }
}
