package io.zmux.internal.core;

import io.zmux.internal.wire.Preamble;

/**
 * The core state of one stream, owned by the session's event loop. Handles of the
 * public layer refer to it; nothing here is thread-safe.
 */
public final class Stream {
    // Sending part states (RFC 9000 Section 3.1).
    static final int SEND_READY = 0, SEND_SEND = 1, SEND_DATA_SENT = 2, SEND_DATA_RECVD = 3, SEND_RESET_SENT = 4,
            SEND_RESET_RECVD = 5;
    // Receiving part states (RFC 9000 Section 3.2).
    static final int RECV_RECV = 0, RECV_SIZE_KNOWN = 1, RECV_DATA_RECVD = 2, RECV_DATA_READ = 3,
            RECV_RESET_RECVD = 4, RECV_RESET_READ = 5;

    final StreamSend.SendPart snd = new StreamSend.SendPart();
    /** The stream's own counters of API.md Section 7.2 that its parts do not keep. */
    final Counters counters = new Counters();
    /** The final statistics, taken when the session released the stream; null before. */
    private volatile io.zmux.StreamStats released;
    final StreamRecv.RecvPart rcv;

    /** The stream's place in the scheduler and in the control queue. */
    final StreamScheduler.Node sch = new StreamScheduler.Node(this);
    final ControlQueue.Entry cq = new ControlQueue.Entry(this);

    final boolean local;
    final boolean bidi;
    final boolean hasSend;
    final boolean hasRecv;

    /** Whether this endpoint can send on the stream. */
    public boolean hasSend() { return hasSend; }

    /** Whether this endpoint can receive on the stream. */
    public boolean hasRecv() { return hasRecv; }

    /** Whether the peer may have sent the frames as replayable early data; read from any thread. */
    private volatile boolean replayable;
    final StreamTable.Ident tab = new StreamTable.Ident();

    Stream(boolean bidi, boolean local) {
        this.bidi = bidi;
        this.local = local;
        this.hasSend = local || bidi;
        this.hasRecv = !local || bidi;
        rcv = new StreamRecv.RecvPart(hasRecv);
    }

    /** One registered send-capacity wait. */
    record CapacityWait(Waiter sig, long min) {}

    /** Counts of one stream, written on its session's loop. */
    static final class Counters {
        private long bytesSent;
        private long bytesRead;
        private long datagramsSent;
        private long datagramsReceived;
        private long datagramsDroppedRecv;
        private long datagramBytesSent;
        private long datagramBytesReceived;

        void read(long n) { bytesRead += n; }

        /** A datagram of payload bytes went to a carrier; a negative count takes a refused one back. */
        void datagramSent(int count, long payload) { datagramsSent += count; datagramBytesSent += payload; }

        void datagramQueued() { datagramsReceived++; }

        void datagramDropped() { datagramsDroppedRecv++; }

        void datagramReturned(long payload) { datagramBytesReceived += payload; }
    }

    /** STREAM frame data [off, off + n) was put into a record: its payload bytes, the preamble excluded. */
    void payloadSent(long off, long n) {
        long from = Math.max(off, snd.preLen());
        if (off + n > from) counters.bytesSent += off + n - from;
    }

    /** The stream's statistics now (API.md Section 7.2); on its session's loop. */
    public io.zmux.StreamStats stats() {
        long sent = 0, acked = 0, sendBuffered = 0, written = 0;
        java.util.OptionalLong pipe = java.util.OptionalLong.empty();
        if (hasSend) {
            long pre = snd.preLen();
            written = snd.appWritten();
            PathStreamState m = snd.mps();
            acked = m != null ? m.ret.acked().covered(pre, Long.MAX_VALUE) : Math.max(0, snd.sent() - pre);
            sendBuffered = Math.max(0, snd.q().end() - Math.max(snd.q().base(), pre));
            if (m != null && m.sched.bound() >= 0) pipe = java.util.OptionalLong.of(m.sched.bound());
            sent = counters.bytesSent;
        }
        long received = 0, recvBuffered = 0, window = 0;
        if (hasRecv) {
            StreamRecv.RecvPart r = rcv;
            long pre = r.preSize();
            received = r.stopped() ? Math.max(0, r.highest() - pre) : r.arrived().covered(pre, Long.MAX_VALUE);
            recvBuffered = r.buf().bytes() + Math.max(0, r.pushOff() - r.readOff());
            window = r.window();
        }
        return new io.zmux.StreamStats(written, sent, acked, received, counters.bytesRead, counters.datagramsSent,
                counters.datagramsReceived, counters.datagramsDroppedRecv, counters.datagramBytesSent,
                counters.datagramBytesReceived, sendBuffered, recvBuffered, window, pipe);
    }

    /** The final statistics once the session released the stream, or null; any thread. */
    public io.zmux.StreamStats released() { return released; }

    void released(io.zmux.StreamStats s) { released = s; }

    /** Encoded size of the local preamble (1: an empty one). */
    int preSize() {
        return tab.pre() == null ? 1 : tab.pre().toPreamble(null).encodedSize();
    }

    /** The most a "small" stream direction holds (IMPLEMENTATION.md Section 6.4). */
    static final long SMALL_HELD = 64 << 10;

    /**
     * The direction's place in the send-buffer accounting: held bytes << 2 | small << 1 | big.
     * Every change of the held bytes or the waiting state is bracketed by heldClass before and
     * {@link ConnFlow#held}(before, {@link #heldAfter}) after: the single authority of the sums.
     */
    long heldClass() {
        long h = snd.q().end() - snd.q().base() + snd.leased();
        boolean small = h <= SMALL_HELD && !snd.wwait();
        return h << 2 | (small && h > 0 ? 2 : 0) | (small ? 0 : 1);
    }

    /** The class after a change; a discarded or cancelled direction holds nothing. */
    long heldAfter() {
        return tab.discarded() || snd.cancelled() ? 0 : heldClass();
    }

    /** Bytes the part holds and has not sent yet. */
    long unsent() {
        long end = snd.sendEnd();
        long from = Math.max(snd.sent(), snd.q().base());
        return end > from ? end - from : 0;
    }

    /** Whether the stream has anything for the data scheduler. */
    boolean sendable() {
        if (!hasSend || snd.cancelled() || tab.discarded()) return false;
        int st = snd.state();
        if (st == SEND_DATA_RECVD || st == SEND_RESET_RECVD || st == SEND_RESET_SENT) return false;
        if (unsent() > 0) return true;
        if (snd.mps() != null && (snd.mps().ret.resend().size() > 0 || snd.mps().ret.finResend())) return true;
        if (snd.finWritten() && !snd.finSent() && !snd.reset()) return true;
        return tab.pinned() && !tab.opened() || (!tab.idSet() && (snd.finWritten() || snd.q().end() > snd.q().base()));
    }

    public long id() { return tab.id(); }
    public boolean idSet() { return tab.idSet(); }
    public boolean local() { return local; }
    public boolean bidi() { return bidi; }
    public boolean replayable() { return replayable; }

    /** Set when the stream opens while the peer's data could be replayed; cleared once the handshake completes. */
    void replayable(boolean r) {
        replayable = r;
    }
    /** The peer's preamble, or null. */
    public boolean bidirectional() { return bidi; }

    public Preamble peerPreamble() { return rcv.peerPre(); }

    public void setWaiters(Waiter r, Waiter w) {
        StreamRecv.waiter(this, r);
        StreamSend.waiter(this, w);
    }

    static long streamId(long idx, boolean bidi, boolean responder) {
        long id = idx << 2;
        if (!bidi) id |= 2;
        if (responder) id |= 1;
        return id;
    }

    static boolean isBidiId(long id) {
        return (id & 2) == 0;
    }

    static int typeIndex(boolean bidi) {
        return bidi ? 0 : 1;
    }
}
