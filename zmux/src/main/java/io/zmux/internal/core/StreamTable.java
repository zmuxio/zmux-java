package io.zmux.internal.core;

import io.zmux.internal.wire.Preamble;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.WireException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * The streams of one session (SPEC.md Sections 7 and 8.5): the table of live streams,
 * local and peer stream counts and credit, the accept queues, open and accept waiters,
 * provisional streams (opened before establishment), identifier allocation, opening by
 * peer frames, and discarding. Effects on the sending and receiving parts and on the
 * session go through {@link Port}.
 */
public final class StreamTable {
    /** What the table asks of the session. */
    interface Port {
        /** A control frame or stream data wants to be written. */
        void wantWrite();

        /** The session's terminal error, or null. */
        SessionError error();

        /** Whether the peer's parameters arrived. */
        boolean established();

        /** Early data is being sent under assumed parameters. */
        boolean earlyActive();

        /** The peer's parameters (the assumed ones before establishment). */
        TransportParams peer();

        Features features();

        /** The session's role is the responder. */
        boolean responder();

        /** Resets the sending part with code (an application reset). */
        void reset(Stream st, long code);

        /** Resets the sending part with ZMUX_REJECTED (EXTENSIONS.md 3.4, 7.2). */
        void rejectSend(Stream st);

        /** Abandons the receiving part with code (STOP_SENDING). */
        void stopRecv(Stream st, long code);

        /** The stream was discarded: drop what other components keep for it. */
        void discarded(Stream st);

        /** The stream got its identifier. */
        void opened(Stream st);
    }

    private static final class LocalCount {
        private long next;
        private long reserved;
        private long limit;
        private long blocked;
    }

    private static final class RemoteCount {
        private long opened;
        private long limit;
        private long initial;
        private long discarded;
        private boolean staged;
        private long blockedSeen;
    }

    private final CoreConfig cfg;
    private final SessionStats stats;
    private final Outbox outbox;
    private final Lifecycle life;
    private final EarlyData early;
    private final Datagrams dg;
    private final ConnFlow conn;
    private final MemoryCharge mem;
    private final StreamScheduler sched;
    private final ControlQueue ctl;
    private final Port port;

    private final LongMap<Stream> streams = new LongMap<>();
    private final LocalCount[] loc = {new LocalCount(), new LocalCount()};
    private final RemoteCount[] rem = {new RemoteCount(), new RemoteCount()};
    @SuppressWarnings("unchecked") // Java has no generic array creation; both elements are ArrayDeque<Stream>
    private final ArrayDeque<Stream>[] acceptQ = (ArrayDeque<Stream>[]) new ArrayDeque<?>[] {new ArrayDeque<Stream>(0), new ArrayDeque<Stream>(0)};
    private final WaitList[] acceptW = {new WaitList(), new WaitList()};
    private final WaitList[] openW = {new WaitList(), new WaitList()};
    private final ArrayList<Stream> provisional = new ArrayList<>(0);
    private long openLocalBidiRecv;
    private boolean greaseFirst;

    StreamTable(CoreConfig cfg, SessionStats stats, Outbox outbox, Lifecycle life, EarlyData early, Datagrams dg,
                ConnFlow conn, MemoryCharge mem, StreamScheduler sched, ControlQueue ctl, Port port) {
        this.cfg = cfg;
        this.stats = stats;
        this.outbox = outbox;
        this.life = life;
        this.early = early;
        this.dg = dg;
        this.conn = conn;
        this.mem = mem;
        this.sched = sched;
        this.ctl = ctl;
        this.port = port;
        TransportParams l = cfg.local();
        rem[0].limit = l.maxStreamsBidi;
        rem[0].initial = l.maxStreamsBidi;
        rem[1].limit = l.maxStreamsUni;
        rem[1].initial = l.maxStreamsUni;
        rem[1].staged = cfg.stagedUni();
        greaseFirst = !cfg.test().enabled() || cfg.test().greaseTlv() != null;
    }

    // ---- queries ----

    Stream get(long id) { return streams.get(id); }

    Iterable<Stream> live() { return streams.values(); }

    /** The streams the session holds, those without an identifier yet included. */
    int count() { return streams.size() + provisional.size(); }

    /** A copy of the live streams, for loops that may discard. */
    ArrayList<Stream> snapshot() { return streams.valuesSnapshot(); }

    /** Whether no stream, live or provisional, remains. */
    boolean isEmpty() { return streams.isEmpty() && provisional.isEmpty(); }

    /** The local streams, provisional ones last. */
    List<Stream> locals() {
        List<Stream> r = new ArrayList<>();
        for (Stream st : streams.values()) if (st.local) r.add(st);
        r.addAll(provisional);
        return r;
    }

    /** The peer's stream limit of type t. */
    long localLimit(int t) { return loc[t].limit; }

    /** The limit of type t advertised to the peer. */
    long remoteLimit(int t) { return rem[t].limit; }

    /** Whether STREAMS_BLOCKED of type t still reports the current limit. */
    boolean streamsBlockedAtLimit(int t) { return loc[t].blocked == loc[t].limit + 1; }

    /** The local streams of type t holding stream-count credit. */
    long reserved(int t) { return loc[t].reserved; }

    /** Whether a local stream of type t may take an identifier now. */
    boolean idAvailable(int t) { return loc[t].next < loc[t].limit; }

    static boolean isLocalId(long id, boolean responder) {
        return ((id & 1) == 1) == responder;
    }

    // ---- buffer accounting (SPEC.md Section 8.3) ----

    static final long SESSION_WINDOW_CAP_FLOOR = 96L << 20;

    /** max(session window cap, S) (SPEC.md Section 8.3 rule 4). */
    long bufferCap() {
        long c = cfg.connWindowCap() > 0 ? cfg.connWindowCap() : SESSION_WINDOW_CAP_FLOOR;
        TransportParams l = cfg.local();
        long s = rem[0].initial * l.maxStreamDataBidiRemote + rem[1].initial * l.maxStreamDataUni
                + openLocalBidiRecv * l.maxStreamDataBidiLocal;
        return Math.max(c, s);
    }

    /** Accounts n bytes stored in a stream buffer; the only function that raises the buffered count (C1). */
    void bufAdd(long n) {
        if (n != 0) conn.buffer(n, bufferCap(), mem.pressure());
    }

    /** Accounts n buffered bytes consumed; the only function that lowers the buffered count (C1). */
    void bufSub(long n) {
        if (n != 0) conn.unbuffer(n, bufferCap(), mem.pressure());
    }

    // ---- establishment ----

    /** Applies the peer's stream limits (SPEC.md Section 6.1). */
    void peerLimits(long bidi, long uni) {
        loc[0].limit = bidi;
        loc[1].limit = uni;
    }

    /** Applies the assumed limits of early data; floor keeps the unidirectional one. */
    void assumedLimits(long bidi, long uni, boolean floor) {
        loc[0].limit = bidi;
        if (!floor) loc[1].limit = uni;
    }

    /** Sets every live stream's initial credit and window once parameters are known. */
    void applyPeerParamsAll() {
        for (Stream st : streams.values()) applyPeerParams(st);
    }

    /** Sets the initial credit from the peer's parameters and the receive window from the local ones. */
    void applyPeerParams(Stream st) {
        if (!port.established() && !port.earlyActive() || !st.tab.idSet) return;
        TransportParams peer = port.peer();
        if (st.hasSend && st.snd.maxData() == 0) {
            StreamSend.raiseCredit(st, !st.bidi ? peer.maxStreamDataUni : st.local ? peer.maxStreamDataBidiRemote : peer.maxStreamDataBidiLocal);
        }
        if (st.hasRecv && st.rcv.window() == 0) {
            TransportParams l = cfg.local();
            StreamRecv.initWindow(st, !st.bidi ? l.maxStreamDataUni : st.local ? l.maxStreamDataBidiLocal : l.maxStreamDataBidiRemote);
        }
    }

    // ---- opening local streams ----

    /**
     * Opens a local stream. Once established it reserves stream-count credit and fails
     * with STREAM_LIMIT without it; before establishment the stream is provisional.
     */
    public Stream openStream(boolean bidi) throws CoreException {
        openCheck();
        int t = Stream.typeIndex(bidi);
        if (port.established() && loc[t].reserved >= loc[t].limit) {
            markStreamsBlocked(t);
            throw CoreException.of(CoreException.Reason.STREAM_LIMIT);
        }
        loc[t].reserved++;
        Stream st = newStream(bidi, true);
        provisional.add(st);
        stats.streamOpened(true);
        return st;
    }

    private void openCheck() throws CoreException {
        SessionError err = port.error();
        if (err != null) throw CoreException.session(err);
        if (life.goAwayReceived() || life.closeWhenDone()) throw CoreException.stream(StreamError.of(StreamError.Kind.GOING_AWAY, 0));
    }

    /** Registers sig until credit for an open of this type may exist. */
    public void waitOpen(boolean bidi, Waiter sig) {
        openW[Stream.typeIndex(bidi)].add(sig);
    }

    public boolean openersWaiting(boolean bidi) {
        return !openW[Stream.typeIndex(bidi)].isEmpty();
    }

    /** Withdraws sig; when it was due to open, the next opener is woken instead. */
    public void cancelOpenWait(boolean bidi, Waiter sig) {
        int t = Stream.typeIndex(bidi);
        if (openW[t].remove(sig) && loc[t].reserved < loc[t].limit && !openW[t].isEmpty()) openW[t].wakeFirst(outbox);
    }

    void wakeOpeners() {
        for (int t = 0; t < 2; t++) {
            if (loc[t].reserved < loc[t].limit && !openW[t].isEmpty()) openW[t].wakeFirst(outbox);
        }
    }

    /** Wakes every open waiter (GOAWAY, shutdown). */
    void wakeOpenWaiters() {
        for (int t = 0; t < 2; t++) openW[t].wakeAll(outbox);
    }

    /** Wakes every open and accept waiter (close). */
    void wakeWaiters() {
        for (int i = 0; i < 2; i++) {
            acceptW[i].wakeAll(outbox);
            openW[i].wakeAll(outbox);
        }
    }

    /** Applies MAX_STREAMS; false when it did not raise the limit. */
    boolean onMaxStreams(boolean bidi, long v) {
        int t = Stream.typeIndex(bidi);
        if (v <= loc[t].limit) return false;
        loc[t].limit = v;
        wakeOpeners();
        port.wantWrite();
        return true;
    }

    /** Queues STREAMS_BLOCKED once per limit. */
    void markStreamsBlocked(int t) {
        if (!port.established()) return;
        LocalCount lc = loc[t];
        if (lc.blocked != lc.limit + 1) {
            lc.blocked = lc.limit + 1;
            ctl.wantStreamsBlocked(t);
            port.wantWrite();
        }
    }

    private Stream newStream(boolean bidi, boolean local) {
        Stream st = new Stream(bidi, local);
        st.replayable(!local && early.replayableIn());
        return st;
    }

    /** Gives a local stream its identifier (SPEC.md Section 7.2 rule 1). */
    void allocId(Stream st) {
        int t = Stream.typeIndex(st.bidi);
        long idx = loc[t].next++;
        st.tab.id = Stream.streamId(idx, st.bidi, port.responder());
        st.tab.idSet = true;
        provisional.remove(st);
        if (st.bidi) openLocalBidiRecv++;
        streams.put(st.tab.id, st);
        port.opened(st);
        applyPeerParams(st);
        if (port.features().preamble()) {
            Preamble.Tlv grease = null;
            if (greaseFirst) {
                greaseFirst = false;
                grease = grease();
            }
            Preamble pre = st.tab.pre != null ? st.tab.pre.toPreamble(grease) : new Preamble(0, null, -1, false, false, List.of(), grease);
            java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(pre.encodedSize());
            pre.encode(b);
            long h = st.heldClass();
            StreamSend.prependPreamble(st, b.array());
            conn.held(h, st.heldAfter());
        }
        st.tab.pre = null;
    }

    /** The local bound of a preamble, whatever the peer allows. */
    static final int MAX_PREAMBLE = 4096;
    /** The preamble bound before the peer's parameters (SPEC.md Section 6.1.2 floor). */
    static final int FLOOR_PREAMBLE = 1024;
    /** The session's GREASE TLV, chosen once before its first use; it goes in the first preamble sent. */
    private Preamble.Tlv grease;

    private Preamble.Tlv grease() {
        if (grease == null) {
            if (cfg.test().enabled()) {
                grease = cfg.test().greaseTlv();
                if (grease == null) grease = NO_GREASE;
            } else {
                grease = new Preamble.Tlv(0x21 + 0x1fL * cfg.random().nextLong(1000L), new byte[0]);
            }
        }
        return grease == NO_GREASE ? null : grease;
    }

    private static final Preamble.Tlv NO_GREASE = new Preamble.Tlv(0x21, new byte[0]);

    /**
     * The largest preamble a stream may send now: the peer's Maximum Preamble Size once its
     * parameters arrived, the assumed one while sending early (remembered, or the floor), the
     * floor before either, and never above {@link #MAX_PREAMBLE}.
     */
    int preambleLimit() {
        long peerMax = port.established() || port.earlyActive() ? port.peer().maxPreambleSize : FLOOR_PREAMBLE;
        if (peerMax <= 0) peerMax = FLOOR_PREAMBLE;
        return (int) Math.min(peerMax, MAX_PREAMBLE);
    }

    /** Whether st's preamble, with the GREASE TLV it may carry, fits the bound now. */
    private boolean preambleFits(Stream st) {
        PreambleSpec ps = st.tab.pre;
        if (ps == null) return true;
        return ps.toPreamble(greaseFirst ? grease() : null).encodedSize() <= preambleLimit();
    }

    /** Throws PREAMBLE_TOO_LARGE after undo when st's preamble no longer fits. */
    private void checkPreamble(Stream st, Runnable undo) throws CoreException {
        if (preambleFits(st)) return;
        undo.run();
        throw CoreException.of(CoreException.Reason.PREAMBLE_TOO_LARGE);
    }

    /**
     * Before a local stream takes its identifier: false when its preamble outgrew the bound
     * that holds now (the peer's parameters arrived below what was assumed). The stream then
     * fails locally, sending nothing; the session is unaffected.
     */
    boolean preambleAllowed(Stream st, StreamSend send) {
        if (!port.features().preamble() || preambleFits(st)) return true;
        StreamError e = StreamError.of(StreamError.Kind.PREAMBLE_TOO_LARGE, -1);
        StreamSend.error(st, e, false);
        send.cancelUnopened(st);
        if (st.hasRecv) StreamRecv.failUnopened(st, e);
        return false;
    }

    /** Returns the identifier, pinning it before the first frame (SPEC.md Section 7.2 rule 1). */
    public long pinId(Stream st) throws CoreException {
        if (st.tab.idSet) return st.tab.id;
        if (st.snd.cancelled()) throw CoreException.stream(st.snd.errState());
        if (!port.established()) throw CoreException.of(CoreException.Reason.NOT_ESTABLISHED);
        if (port.features().preamble() && !preambleFits(st)) throw CoreException.of(CoreException.Reason.PREAMBLE_TOO_LARGE);
        allocId(st);
        st.tab.pinned = true;
        sched.ensure(st);
        port.wantWrite();
        return st.tab.id;
    }

    /** A provisional stream that never emitted a frame is gone (SPEC.md Section 7.2). */
    void unopenedGone(Stream st) {
        provisional.remove(st);
        life.streamGone();
    }

    /** Returns the stream-count reservation of a stream that never took an identifier. */
    void unreserve(Stream st) {
        int t = Stream.typeIndex(st.bidi);
        if (loc[t].reserved > 0) loc[t].reserved--;
        wakeOpeners();
    }

    private void beforeFirstFrame(Stream st) throws CoreException {
        if (!st.local || st.tab.idSet) throw CoreException.of(CoreException.Reason.TOO_LATE);
        if (port.established() && !port.features().preamble()) throw CoreException.of(CoreException.Reason.NOT_NEGOTIATED);
    }

    /** Sets OPEN_INFO before the first frame. */
    public void setOpenInfo(Stream st, byte[] b) throws CoreException {
        beforeFirstFrame(st);
        PreambleSpec ps = preSpec(st);
        byte[] was = ps.openInfo;
        ps.openInfo = b.clone();
        checkPreamble(st, () -> ps.openInfo = was);
    }

    /** Adds an application TLV before the first frame. */
    public void addPreambleTlv(Stream st, long type, byte[] v) throws CoreException {
        if (!Preamble.isApplication(type)) throw CoreException.of(CoreException.Reason.INVALID);
        beforeFirstFrame(st);
        List<Preamble.Tlv> app = preSpec(st).app;
        app.add(new Preamble.Tlv(type, v.clone()));
        checkPreamble(st, () -> app.removeLast());
    }

    /** Marks the stream as a carrier before the first frame. */
    public void setPipeCarrier(Stream st) throws CoreException {
        beforeFirstFrame(st);
        PreambleSpec ps = preSpec(st);
        boolean was = ps.pipeCarrier;
        ps.pipeCarrier = true;
        checkPreamble(st, () -> ps.pipeCarrier = was);
    }

    /** Sets local scheduling priority. */
    public void setPriority(Stream st, int urgency, boolean incremental) {
        st.tab.urgency = Math.clamp(urgency, 0, 7);
        st.tab.incremental = incremental;
        sched.reprioritize(st);
    }

    /**
     * Requests a priority for the peer's sending part: the PRIORITY TLV before a local
     * stream's first frame, PRIORITY_UPDATE otherwise.
     */
    public void setPeerPriority(Stream st, int urgency, boolean incremental) throws CoreException {
        if (urgency < 0 || urgency > 7) throw CoreException.of(CoreException.Reason.INVALID);
        if (st.local && !st.tab.idSet) {
            StreamTable.PreambleSpec ps = preSpec(st);
            int u = ps.urgency;
            boolean inc = ps.incremental;
            ps.urgency = urgency;
            ps.incremental = incremental;
            checkPreamble(st, () -> { ps.urgency = u; ps.incremental = inc; });
            return;
        }
        if (!port.features().priority()) throw CoreException.of(CoreException.Reason.NOT_NEGOTIATED);
        if (!st.bidi && st.local) throw CoreException.of(CoreException.Reason.INVALID);
        StreamSend.requestPriority(st, urgency, incremental);
        ctl.stream(st, ControlQueue.PRIORITY);
    }

    // ---- peer streams ----

    /**
     * Resolves the stream a frame references, opening peer streams implicitly (SPEC.md
     * Section 7.2 rules 2-4); null for a discarded stream (crossing frame, Section 7.6).
     */
    Stream lookup(long id, boolean opens) throws WireException {
        Stream found = streams.get(id);
        if (found != null) return found;
        long idx = id >>> 2;
        int t = Stream.typeIndex(Stream.isBidiId(id));
        boolean responder = port.responder();
        if (isLocalId(id, responder)) {
            if (idx >= loc[t].next) {
                throw WireException.withDetails(TransportError.STREAM_STATE_ERROR, "frame for an unopened local stream", "stream", id);
            }
            return null;
        }
        RemoteCount rc = rem[t];
        if (idx < rc.opened) return null;
        if (!opens) throw WireException.withDetails(TransportError.STREAM_STATE_ERROR, "frame in the wrong direction", "stream", id);
        if (idx >= rc.limit) {
            throw WireException.withDetails(TransportError.STREAM_LIMIT_ERROR, "stream opened beyond the limit",
                    "stream", id, "limit", rc.limit);
        }
        Stream last = null;
        for (long i = rc.opened; i <= idx; i++) {
            long nid = Stream.streamId(i, t == 0, !responder);
            Stream st = newStream(t == 0, false);
            st.tab.id = nid;
            st.tab.idSet = true;
            st.tab.opened = true;
            streams.put(nid, st);
            port.opened(st);
            applyPeerParams(st);
            stats.streamOpened(false);
            if (life.goAwaySent() && i >= life.sentWater(t) || mem.pressure()) {
                stackReject(st);
            } else if (!port.features().preamble()) {
                offer(st);
            }
            last = st;
        }
        rc.opened = idx + 1;
        return last;
    }

    /** The stream a received datagram flow names (EXTENSIONS.md Section 6.3), null to drop it. */
    Stream flow(long id) throws WireException {
        if (!Stream.isBidiId(id)) throw WireException.protocol("datagram flow names a unidirectional stream");
        if (isLocalId(id, port.responder())) {
            if ((id >>> 2) >= loc[0].next) throw WireException.protocol("datagram flow names an unopened local stream");
        } else if ((id >>> 2) >= rem[0].limit) {
            throw WireException.protocol("datagram flow beyond the stream limit");
        }
        return streams.get(id);
    }

    /** Whether id names a bidirectional peer stream within the limit that has not been opened yet. */
    boolean unopenedPeer(long id) {
        if (!Stream.isBidiId(id) || isLocalId(id, port.responder())) return false;
        long idx = id >>> 2;
        return idx >= rem[0].opened && idx < rem[0].limit;
    }

    /** Applies STREAMS_BLOCKED; false when it repeats a reported value. */
    boolean onStreamsBlocked(boolean bidi, long v) throws WireException {
        int t = Stream.typeIndex(bidi);
        RemoteCount rc = rem[t];
        if (v > rc.limit) {
            throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "STREAMS_BLOCKED above the advertised limit", "value", v);
        }
        boolean repeat = v <= rc.blockedSeen && rc.blockedSeen > 0;
        rc.blockedSeen = Math.max(rc.blockedSeen, v);
        return !repeat;
    }

    /** Queues a peer stream for accept. */
    void offer(Stream st) {
        if (st.tab.offered || st.tab.stackReject) return;
        st.tab.offered = true;
        int t = Stream.typeIndex(st.bidi);
        long idx = (st.tab.id >>> 2) + 1;
        life.offered(t, idx);
        acceptQ[t].add(st);
        acceptW[t].wakeFirst(outbox);
    }

    /** Rejects a peer stream with ZMUX_REJECTED on both parts without offering it (EXTENSIONS.md 3.4, 7.2). */
    private void stackReject(Stream st) {
        st.tab.stackReject = true;
        st.tab.releasedRecv = true;
        st.tab.releasedSend = true;
        if (st.hasSend) port.rejectSend(st);
        port.stopRecv(st, TransportError.REJECTED);
    }

    /** Rejects the peer streams above the final GOAWAY watermarks that were never offered (EXTENSIONS.md 3.3). */
    void rejectAboveWater() {
        for (Stream st : streams.valuesSnapshot()) {
            if (!st.local && !st.tab.offered && !st.tab.stackReject && (st.tab.id >>> 2) >= life.sentWater(Stream.typeIndex(st.bidi))) {
                stackReject(st);
            }
        }
    }

    /** Pops an offered peer stream, or null; the first call for unidirectional streams raises staged credit. */
    public Stream acceptStream(boolean bidi) {
        int t = Stream.typeIndex(bidi);
        if (!bidi) unstageUni();
        Stream st = acceptQ[t].poll();
        if (st == null) return null;
        st.tab.accepted = true;
        if (!acceptW[t].isEmpty() && !acceptQ[t].isEmpty()) acceptW[t].wakeFirst(outbox);
        return st;
    }

    /** Returns an accepted stream that no application took (its accept was cancelled) to the front of the queue. */
    public void unaccept(Stream st) {
        int t = Stream.typeIndex(st.bidi);
        st.tab.accepted = false;
        acceptQ[t].addFirst(st);
        acceptW[t].wakeFirst(outbox);
    }

    public void waitAccept(boolean bidi, Waiter sig) {
        int t = Stream.typeIndex(bidi);
        if (!bidi) unstageUni();
        acceptW[t].add(sig);
    }

    public void cancelAcceptWait(boolean bidi, Waiter sig) {
        int t = Stream.typeIndex(bidi);
        acceptW[t].remove(sig);
        if (!acceptW[t].isEmpty() && !acceptQ[t].isEmpty()) acceptW[t].wakeFirst(outbox);
    }

    /** Raises staged unidirectional credit on the first accept (SPEC.md Section 8.5 rule 4). */
    private void unstageUni() {
        RemoteCount rc = rem[1];
        if (!rc.staged) return;
        rc.staged = false;
        rc.initial = cfg.uniTarget();
        if (life.goAwaySent()) return;
        long nl = rc.initial + rc.discarded;
        if (nl > rc.limit) {
            rc.limit = nl;
            ctl.wantMaxStreams(1);
            port.wantWrite();
        }
    }

    // ---- release and discard ----

    /** Drops every handle: an unfinished sending part is reset and an unread receiving part stopped, both with code 0. */
    public void release(Stream st) {
        if (st.hasSend && !st.snd.finWritten() && !st.snd.reset()) port.reset(st, 0);
        st.tab.releasedSend = true;
        if (st.hasRecv && st.rcv.state() != Stream.RECV_DATA_READ && st.rcv.state() != Stream.RECV_RESET_READ) {
            port.stopRecv(st, 0);
            maybeDiscard(st);
        }
        st.tab.releasedRecv = true;
        maybeDiscard(st);
    }

    /** Declines an accepted stream before reading (EXTENSIONS.md Section 7.2). */
    public void reject(Stream st) {
        if (st.hasSend && !st.snd.reset()) port.rejectSend(st);
        st.tab.releasedSend = true;
        if (st.hasRecv) {
            st.tab.releasedRecv = true;
            port.stopRecv(st, TransportError.REJECTED);
        }
        maybeDiscard(st);
    }

    /** Discards the stream's state once both parts are terminal and it was released (SPEC.md 7.6, 8.5). */
    void maybeDiscard(Stream st) {
        if (st.tab.discarded || !st.tab.idSet) return;
        boolean sendDone = !st.hasSend || st.snd.state() == Stream.SEND_DATA_RECVD || st.snd.state() == Stream.SEND_RESET_RECVD;
        boolean recvDone = !st.hasRecv || st.rcv.state() == Stream.RECV_DATA_READ || st.rcv.state() == Stream.RECV_RESET_READ;
        boolean released = st.tab.releasedSend && st.tab.releasedRecv || st.tab.stackReject;
        if (!st.local && !st.tab.offered && !st.tab.stackReject) released = false;
        if (!sendDone || !recvDone || !released) return;
        st.released(st.stats());
        st.tab.discarded = true;
        streams.remove(st.tab.id);
        sched.remove(st);
        ctl.remove(st);
        port.discarded(st);
        if (st.hasRecv) {
            long d = st.rcv.buf().clear();
            if (d > 0) bufSub(d);
        }
        if (st.local && st.bidi && st.hasRecv) openLocalBidiRecv--;
        int t = Stream.typeIndex(st.bidi);
        if (!st.local) {
            RemoteCount rc = rem[t];
            rc.discarded++;
            if (!life.goAwaySent() && !rc.staged) {
                long nl = rc.initial + rc.discarded;
                if (nl > rc.limit) {
                    rc.limit = nl;
                    ctl.wantMaxStreams(t);
                    port.wantWrite();
                }
            }
        } else if (loc[t].reserved > 0) {
            loc[t].reserved--;
        }
        dg.dropStream(st);
        life.streamGone();
    }

    // ---- session end ----

    /** Wakes every stream handle's waiters, provisional ones included (they can wait before their first frame). */
    void wakeObservers() {
        for (Stream st : streams.values()) wakeObservers(st);
        for (Stream st : provisional) wakeObservers(st);
    }

    void wakeObservers(Stream st) {
        outbox.wake(st.rcv.rsig());
        outbox.wake(st.snd.wsig());
        dg.wakeStream(st);
    }

    /** Drops every stream buffer (CLOSED). */
    void releaseBuffers() {
        for (Stream st : streams.values()) releaseBuffers(st);
        for (Stream st : provisional) releaseBuffers(st);
    }

    private static void releaseBuffers(Stream st) {
        if (st.released() == null) st.released(st.stats());
        if (st.hasRecv) StreamRecv.releaseBuffer(st);
        if (st.hasSend) st.snd.q().drop();
    }

    /** The local preamble of a stream opened with one (EXTENSIONS.md 3); written only here. */
    static final class PreambleSpec {
        private byte[] openInfo;
        private int urgency = -1;
        private boolean incremental;
        private boolean pipeCarrier;
        private final List<Preamble.Tlv> app = new ArrayList<>(0);

        Preamble toPreamble(Preamble.Tlv grease) {
            return new Preamble(0, openInfo, urgency, incremental, pipeCarrier, List.copyOf(app), grease);
        }
    }

    /** st's local preamble, created on first use. */
    private static PreambleSpec preSpec(Stream st) {
        if (st.tab.pre == null) st.tab.pre = new PreambleSpec();
        return st.tab.pre;
    }

    /** The stream became known to the peer: peer-opened, or its first frame went out. */
    void markOpened(Stream st) {
        st.tab.opened = true;
    }

    /** The bootstrap restarted: a local stream's opening frame goes again. */
    void restartOpen(Stream st) {
        st.tab.opened = false;
    }

    /** The priority the peer put in st's preamble (EXTENSIONS.md 3). */
    void preamblePriority(Stream st, int urgency, boolean incremental) {
        st.tab.urgency = urgency;
        st.tab.incremental = incremental;
    }

    /** Applies the peer's PRIORITY_UPDATE unless an equal or newer one was applied; false for a stale one. */
    boolean applyPeerPriority(Stream st, long seq, int urgency, boolean incremental) {
        Ident t = st.tab;
        if (seq <= t.prioSeqApplied && (seq > 0 || t.prioApplied)) return false;
        t.prioSeqApplied = seq;
        t.prioApplied = true;
        setPriority(st, urgency, incremental);
        return true;
    }

    /** The identity, lifecycle marks and priority of a stream; written only here. */
    static final class Ident {
        private long id;
        private boolean idSet;
        private boolean offered;
        private boolean accepted;
        private boolean releasedSend;
        private boolean releasedRecv;
        private boolean stackReject;
        private boolean discarded;
        private boolean pinned;
        private boolean opened;
        private int urgency = 3;
        private boolean incremental = true;
        private PreambleSpec pre;
        private long prioSeqApplied;
        private boolean prioApplied;

        Ident() {
        }

        long id() {
            return id;
        }

        boolean idSet() {
            return idSet;
        }

        boolean offered() {
            return offered;
        }

        boolean accepted() {
            return accepted;
        }

        boolean releasedSend() {
            return releasedSend;
        }

        boolean releasedRecv() {
            return releasedRecv;
        }

        boolean stackReject() {
            return stackReject;
        }

        boolean discarded() {
            return discarded;
        }

        boolean pinned() {
            return pinned;
        }

        boolean opened() {
            return opened;
        }

        int urgency() {
            return urgency;
        }

        boolean incremental() {
            return incremental;
        }

        PreambleSpec pre() {
            return pre;
        }

        long prioSeqApplied() {
            return prioSeqApplied;
        }

        boolean prioApplied() {
            return prioApplied;
        }
    }
}
