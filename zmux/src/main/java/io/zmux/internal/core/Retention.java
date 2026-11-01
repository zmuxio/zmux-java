package io.zmux.internal.core;

import io.zmux.internal.wire.FrameEncoder;
import io.zmux.carrier.VarInt;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

/**
 * What a multipath session retains of sent frames until acknowledged: lane logs, acknowledgement, loss and
 * reinjection, the retransmit budget, redundant and idle copies, opportunistic reinjection and penalization, and the
 * credit copies of the redundant snapshot (MULTIPATH.md 6.5, 7.4, 7.5.1; SCHEDULER.md 4.1, 13.1).
 */
final class Retention {
    /** What Retention asks of the rest of the path layer and the session. */
    interface Port {
        /** See {@link PathHealth}. */
        void abandonAcked(Pipe p);

        /** See {@link PathHealth}. */
        void abandonLost(Pipe p);

        /** See {@link Tiers}. */
        boolean admits(Tiers.Gate g, Pipe p);

        /** The usable capacity may have changed (quality floor). */
        void capacityChanged();

        /** See {@link Tiers}. */
        void checkTakeover();

        /** See {@link Probation}. */
        void endRevalidation(Pipe p);

        /** See {@link PathScheduler}. */
        Pipe fastestIn(Tiers.Gate g, int ti, long l, Pipe except);

        /** See {@link PathScheduler}. */
        boolean idleCopied(Stream st);

        /** See {@link PathHealth}. */
        void maybeRetire(Pipe p);

        /** The session clock. */
        long now();

        /** See {@link PathScheduler}. */
        boolean redundant(Stream st);

        /** The session's send buffer maximum. */
        long sendBufferLimit();

        /** See {@link Tiers}. */
        void statusLost(Pipe p, long seq);

        /** See {@link PathHealth}. */
        long tSuspect(Pipe p);
    }

    static final long IDLE_COPY_MIN = Time.SECOND;

    private final Port port;
    private final CoreConfig cfg;
    private final SessionStats stats;
    private final AckEngine ack;
    private final ConnFlow conn;
    private final ControlQueue ctl;
    private final StreamScheduler sched;
    private final Lanes lanes;
    private final Lifecycle life;
    private final PipeTable pipes;
    private final StreamsView streams;
    private final PathMetrics metrics;
    private long retained;
    /** The latest MAX_DATA is copied with the redundant snapshot (MULTIPATH.md 6.5). */
    private boolean copyMaxData;
    /** Stream directions whose latest MAX_STREAM_DATA is copied with the redundant snapshot. */
    private final HashSet<Stream> copyMsd = new HashSet<>();
    private final ArrayList<CopyRec> copies = new ArrayList<>(0);
    /** Credit-blocked stream directions of opportunistic reinjection. */
    private final ArrayList<Stream> blocked = new ArrayList<>(0);
    private final HashSet<Stream> blockedSet = new HashSet<>();

    Retention(CoreConfig cfg, SessionStats stats, AckEngine ack, ConnFlow conn, ControlQueue ctl, StreamScheduler sched, Lanes lanes,
              Lifecycle life, PipeTable pipes, StreamsView streams, PathMetrics metrics, Port port) {
        this.port = port;
        this.cfg = cfg;
        this.stats = stats;
        this.ack = ack;
        this.conn = conn;
        this.ctl = ctl;
        this.sched = sched;
        this.lanes = lanes;
        this.life = life;
        this.pipes = pipes;
        this.streams = streams;
        this.metrics = metrics;
    }

    /** Copies the limits sent while the window was the bottleneck into the redundant snapshot's record (6.5). */
    void writeCreditCopies(RecordWriter w) {
        if (copyMaxData) {
            long v = conn.advertised();
            if (w.frame(1 + VarInt.length(v), true, bb -> FrameEncoder.maxData(bb, v))) copyMaxData = false;
        }
        if (copyMsd.isEmpty()) return;
        for (Stream st : streams.live()) {
            if (!copyMsd.contains(st)) continue;
            long id = st.tab.id(), v = st.rcv.adv();
            if (w.frame(1 + VarInt.length(id) + VarInt.length(v), true, bb -> FrameEncoder.maxStreamData(bb, id, v))) copyMsd.remove(st);
        }
        copyMsd.removeIf(st -> st.tab.discarded());
    }

    /** A MAX_DATA went out; peerWaiting: the window was the bottleneck. */
    void maxDataSent(boolean peerWaiting) {
        if (peerWaiting) copyMaxData = true;
    }

    /** A MAX_STREAM_DATA of st went out; peerWaiting: the window was the bottleneck. */
    void maxStreamDataSent(Stream st, boolean peerWaiting) {
        if (peerWaiting) copyMsd.add(st);
    }

    /** Moves the logged frames of a batch into the lane's log. */
    void retainBatch(RecordWriter w, Pipe.OutLane ol, long start) {
        if (w.descRecs.isEmpty()) return;
        Batch b = w.b();
        long[] ends = new long[b.messageCount()];
        long pos = start;
        for (int i = 0; i < b.messageCount(); i++) {
            pos += Batch.positionBytes(b.recordSize(i));
            ends[i] = pos;
        }
        long due = 0;
        Pipe p = w.pipe();
        if (p != null && p.mp() != null) {
            due = Limiter.timeAt(pos - start, metrics.rateOf(p)) + p.mp().minRtt() + ack.peerDelay();
        }
        for (int i = 0; i < w.descRecs.size(); i++) {
            int rec = w.descRecs.get(i);
            long e = rec < ends.length ? ends[rec] : pos;
            ol.log.add(new Retained(e, w.descs.get(i), port.now(), due));
        }
        w.descRecs.clear();
        w.descs.clear();
    }

    /** Pops the entries a lane acknowledgement covers. */
    void ackLog(Pipe.OutLane ol, long pos) {
        while (!ol.log.isEmpty() && ol.log.peekFirst().end <= pos) acked(ol.log.pollFirst().d);
    }

    /** Marks the unacknowledged information of a pipe for sending again, once per entry (7.4). */
    void reinjectPipe(Pipe p) {
        if (p.out() == null) return;
        long bytes = 0;
        boolean any = false;
        for (Pipe.OutLane ol : p.out().lanes) {
            for (Retained e : ol.log) {
                if (!e.reinject()) continue;
                any = true;
                if (e.d.kind() == FrameDesc.Kind.STREAM) bytes += e.d.n();
                lost(e.d);
            }
        }
        if (any) {
            p.mp().ret.reinjectedBytes += bytes;
            stats.reinjected(bytes);
        }
        pipes.wakeWriters();
    }

    /** The acknowledgement of one frame. */
    void acked(FrameDesc d) {
        switch (d.kind()) {
            case STREAM -> {
                Stream st = streams.get(d.id());
                if (st == null || !st.hasSend) return;
                PathStreamState m = st.snd.mp();
                if (d.n() > 0) m.ret.acked.add(d.off(), d.off() + d.n());
                if (d.fin()) m.ret.finAcked = true;
                streamAckProgress(st);
            }
            case RESET -> {
                Stream st = streams.get(d.id());
                if (st != null && st.hasSend && st.snd.resetSent()) streams.sendTerminal(st, Stream.SEND_RESET_RECVD);
            }
            case PRIORITY -> {
                Stream st = streams.get(d.id());
                if (st != null) StreamSend.priorityAcked(st, d.val());
            }
            case ABANDON -> {
                Pipe p = pipes.get(d.id());
                if (p != null && p.mp() != null) port.abandonAcked(p);
            }
            default -> { }
        }
    }

    /** Releases acknowledged bytes and finishes the part. */
    void streamAckProgress(Stream st) {
        StreamSend.SendPart sp = st.snd;
        PathStreamState m = sp.mps();
        long pre = m.ret.acked.prefix();
        if (pre > sp.q().base()) {
            long rel = sp.q().base();
            streams.releaseAcked(st, Math.min(pre, sp.q().end()));
            retained -= Math.min(retained, Math.min(sp.q().base(), sp.sent()) - Math.min(rel, sp.sent()));
        }
        long end = sp.reset() ? sp.resetRel() : sp.q().end();
        if (!sp.reset() && sp.finSent() && m.ret.finAcked && pre >= end) streams.sendTerminal(st, Stream.SEND_DATA_RECVD);
    }

    /** Turns one frame's information into pending sending. */
    void lost(FrameDesc d) {
        switch (d.kind()) {
            case STREAM -> {
                Stream st = streams.get(d.id());
                if (st == null || !st.hasSend) return;
                StreamSend.SendPart sp = st.snd;
                if (sp.state() == Stream.SEND_DATA_RECVD || sp.state() == Stream.SEND_RESET_RECVD) return;
                PathStreamState m = sp.mp();
                long lo = d.off(), hi = d.off() + d.n();
                if (sp.reset()) hi = Math.min(hi, sp.resetRel());
                gaps(m.ret.acked, lo, hi, m.ret.resend);
                if (d.fin() && !m.ret.finAcked && !sp.reset()) m.ret.finResend = true;
                sched.ensure(st);
            }
            case RESET -> {
                Stream st = streams.get(d.id());
                if (st != null && st.hasSend && st.snd.reset() && st.snd.state() != Stream.SEND_RESET_RECVD) ctl.stream(st, ControlQueue.RESET);
            }
            case STOP -> {
                Stream st = streams.get(d.id());
                if (st != null && st.hasRecv && st.rcv.stopped() && st.rcv.state() != Stream.RECV_DATA_READ
                        && st.rcv.state() != Stream.RECV_RESET_READ) {
                    ctl.stream(st, ControlQueue.STOP);
                }
            }
            case MAX_DATA -> ctl.forceMaxData();
            case MAX_STREAM_DATA -> {
                Stream st = streams.get(d.id());
                if (st != null && st.hasRecv && st.rcv.state() == Stream.RECV_RECV) {
                    StreamRecv.forceMaxStreamData(st);
                    ctl.stream(st, ControlQueue.MAX_STREAM_DATA);
                }
            }
            case MAX_STREAMS -> ctl.wantMaxStreams(Stream.typeIndex(d.bidi()));
            case DATA_BLOCKED -> {
                if (conn.blockedAtLimit()) ctl.wantDataBlocked();
            }
            case STREAM_DATA_BLOCKED -> {
                Stream st = streams.get(d.id());
                if (st != null && st.snd.blocked() == st.snd.maxData() + 1) ctl.stream(st, ControlQueue.BLOCKED);
            }
            case STREAMS_BLOCKED -> {
                int t = Stream.typeIndex(d.bidi());
                if (streams.streamsBlockedAtLimit(t)) ctl.wantStreamsBlocked(t);
            }
            case GOAWAY -> {
                if (life.goAwaySent()) ctl.wantGoAway();
            }
            case PRIORITY -> {
                Stream st = streams.get(d.id());
                if (st != null && StreamSend.priorityOwed(st)) ctl.stream(st, ControlQueue.PRIORITY);
            }
            case STATUS -> {
                Pipe p = pipes.get(d.id());
                if (p != null && p.mp() != null && !p.mp().health.retired()) port.statusLost(p, d.val());
            }
            case ABANDON -> {
                Pipe p = pipes.get(d.id());
                if (p != null && p.mp() != null) port.abandonLost(p);
            }
        }
        pipes.wakeWriters();
    }

    /** Adds the subranges of [lo, hi) not in set to out. */
    static void gaps(RangeSet set, long lo, long hi, RangeSet out) {
        for (int i = 0; i < set.size() && lo < hi; i++) {
            long a = set.lo(i), b = set.hi(i);
            if (b <= lo) continue;
            if (a >= hi) break;
            if (a > lo) out.add(lo, a);
            lo = b; // b > lo: the range overlaps [lo, hi)
        }
        if (lo < hi) out.add(lo, hi);
    }

    /**
     * Bounds the retained unacknowledged stream data: four times the summed delivery rate
     * times (smoothed RTT + the peer's ack delay) of the live outbound pipes, at least 1 MiB
     * and at most the send buffer maximum (SCHEDULER.md Section 13.1).
     */
    long retransmitBudget() {
        double sum = 0;
        long pd = ack != null ? ack.peerDelay() : 25 * Time.MILLISECOND;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a == null || p.out() == null || !a.liveOut()) continue;
            sum += p.out().snd.re().value() * (a.rtt.smoothed() + pd) / 1e9;
        }
        long b = (long) (4 * sum);
        return Math.clamp(b, 1 << 20, Math.max(port.sendBufferLimit(), 1 << 20));
    }

    boolean pipeIdle(Pipe p) {
        Attach a = p.mp();
        long quiet = Time.sub(port.now(), a.link.lastAdvance());
        return !a.link.unacked() && quiet >= IDLE_COPY_MIN && quiet >= port.tSuspect(p);
    }

    long copyBound() {
        return Math.max(cfg.spreadChunk(), retransmitBudget() / 64);
    }

    long copyLeft(CopyRec c) {
        Stream st = c.st;
        if (c.dead || st.tab.discarded() || !st.hasSend) return 0;
        StreamSend.SendPart sp = st.snd;
        long end = sp.reset() ? Math.min(c.end, sp.resetRel()) : c.end;
        long lo = c.off;
        if (sp.mps() != null) lo = Math.max(lo, sp.mps().ret.acked.prefix());
        if (lo >= end) return c.fin && sp.mps() != null && !sp.mps().ret.finAcked && !sp.reset() ? 1 : 0;
        return end - lo;
    }

    long copiesOutstanding() {
        long sum = 0;
        for (Iterator<CopyRec> it = copies.iterator(); it.hasNext(); ) {
            CopyRec c = it.next();
            long n = copyLeft(c);
            if (n == 0) {
                c.dead = true;
                it.remove();
                continue;
            }
            sum += n;
        }
        return sum;
    }

    /** Queues a copy after new data [off, off+n) of st went on the decision's pipe. */
    void copyChunk(PathScheduler.Decision d, RecordWriter w, Stream st, long off, long n, boolean fin) {
        Pipe p = d.p();
        Attach a = p.mp();
        if (w.firstNewData()) {
            if (pipeIdle(p)) a.ret.idleArm = cfg.spreadChunk();
        }
        long idle = Math.min(a.ret.idleArm, n);
        a.ret.idleArm -= idle;
        long end = off + n;
        if (!port.redundant(st)) { // a redundant stream copies the whole chunk
            if (idle <= 0 || !port.idleCopied(st)) return;
            end = off + idle;
            fin = fin && idle == n;
        }
        Tiers.Gate g = d.g();
        if (d.tier() < 0 || d.tier() > g.last()) return;
        Pipe target = port.fastestIn(g, d.tier(), end - off, p);
        boolean self = false;
        if (target == null) {
            if (!port.redundant(st) || copyLaneOf(p) == null) return;
            target = p;
            self = true;
        }
        if (copiesOutstanding() + (end - off) > copyBound()) return;
        CopyRec c = new CopyRec(st, off, end, fin);
        copies.add(c);
        if (self) a.ret.selfCopyQ.add(c);
        else target.mp().ret.copyQ.add(c);
        pipes.wake(target);
    }

    /** Writes the copies of queue q ahead of p's other stream data; all are dropped when p is not eligible. */
    void writeCopies(PathScheduler.Decision d, RecordWriter w, ArrayList<CopyRec> q) {
        Pipe p = d.p();
        if (q.isEmpty()) return;
        if (d.g() == null || !port.admits(d.g(), p)) {
            dropCopies(p);
            return;
        }
        while (!q.isEmpty() && w.dataRoom() != 0) {
            CopyRec c = q.getFirst();
            if (copyLeft(c) == 0) {
                c.dead = true;
                q.removeFirst();
                continue;
            }
            Stream st = c.st;
            StreamSend.SendPart sp = st.snd;
            long start = Math.max(c.off, sp.q().base());
            if (sp.mps() != null) start = Math.max(start, sp.mps().ret.acked.prefix());
            if (start < sp.preLen()) start = 0;
            long end = Math.min(c.end, sp.q().end());
            if (sp.reset()) end = Math.min(end, sp.resetRel());
            boolean fin = c.fin && !sp.reset() && (sp.mps() == null || !sp.mps().ret.finAcked) && end == sp.q().end();
            if (start > end) start = end;
            long n = end - start;
            int room = w.dataFit(w.room() - FrameEncoder.streamHeaderLength(st.tab.id(), start, (int) n, true));
            if (room <= 0) return;
            if (n > room) {
                n = room;
                fin = false;
            }
            ByteBuffer data = null;
            Pin pin = Pin.NONE;
            if (n > 0) {
                // [start, end) lies within the queue (end is capped at its end above).
                SendQueue.Peek pk = java.util.Objects.requireNonNull(sp.q().peek(start, (int) n), "copy range outside the send queue");
                data = pk.data();
                pin = pk.pin();
                if (data.remaining() < n) fin = false;
                n = data.remaining();
            }
            if (n == 0 && !fin) {
                c.dead = true;
                q.removeFirst();
                continue;
            }
            if (!w.stream(st.tab.id(), start, data, pin, fin)) return;
            w.logFrame(FrameDesc.stream(st.tab.id(), start, n, fin));
            st.payloadSent(start, n);
            w.spendData(n);
            stats.redundant(n);
            if (w.pipe() != null && w.pipe().mp() != null) w.pipe().mp().ret.redundantBytes += n;
            if (start + n >= end) {
                q.removeFirst();
            } else {
                c.off = start + n;
            }
        }
    }

    void dropCopies(Pipe p) {
        Attach a = p.mp();
        for (CopyRec c : a.ret.copyQ) c.dead = true;
        a.ret.copyQ.clear();
        for (CopyRec c : a.ret.selfCopyQ) c.dead = true;
        a.ret.selfCopyQ.clear();
    }

    /** p became suspect, failed or started draining: its queued copies, idle copy and revalidation end. */
    void unsettle(Pipe p) {
        if (p.mp() == null) return;
        dropCopies(p);
        p.mp().ret.idleArm = 0;
        port.endRevalidation(p);
    }

    /** p's copy lane: the lowest-numbered confirmed lane above 0 with no bound stream direction. */
    Pipe.OutLane copyLaneOf(Pipe p) {
        Slot r = p.mp().ret;
        if (!p.out().lo.offered() || lanes.limit() <= 1) return null;
        Pipe.OutLane c = r.copyLane;
        if (c != null && !c.st.refused() && !c.st.shut() && p.out().lanes.contains(c)) return c;
        r.copyLane = null;
        for (int i = 1; i < p.out().lanes.size(); i++) {
            Pipe.OutLane x = p.out().lanes.get(i);
            if (x.st.confirmed() && !x.st.refused() && !x.st.shut() && x.boundSt.isEmpty()) {
                r.copyLane = x;
                return x;
            }
        }
        lanes.want(p);
        return null;
    }

    void noteBlocked(Stream st) {
        if (blockedSet.add(st)) blocked.add(st);
    }

    static boolean streamBlocked(Stream st) {
        StreamSend.SendPart sp = st.snd;
        return !st.tab.discarded() && st.hasSend && !sp.reset() && st.tab.idSet() && sp.sendEnd() > Math.max(sp.sent(), sp.q().base())
                && sp.sent() >= sp.maxData();
    }

    void opportunistic(PathScheduler.Decision d, RecordWriter w) {
        if (d.g() == null || !d.takes() || d.fastest() != d.p() || w.dataRoom() == 0) return;
        Pipe f = d.p();
        boolean connBlocked = conn.sent() >= conn.sendMax() && !sched.isEmpty();
        for (Iterator<Stream> it = blocked.iterator(); it.hasNext(); ) {
            Stream st = it.next();
            if (!streamBlocked(st)) {
                blockedSet.remove(st);
                it.remove();
            }
        }
        if (blocked.isEmpty() && !connBlocked) return;
        for (Stream st : new ArrayList<>(blocked)) if (!oppSend(w, f, st)) return;
        if (connBlocked) oppSend(w, f, null);
    }

    /** Re-sends one chunk of blocking data on f; false when the batch is full. */
    boolean oppSend(RecordWriter w, Pipe f, Stream want) {
        Object[] bf = blockingFrame(f, want);
        if (bf == null) return true;
        Pipe p = (Pipe) bf[0];
        Retained e = (Retained) bf[1];
        Attach a = p.mp(), fa = f.mp();
        // Re-send once the original is older than both the fast path's ack round trip and the
        // original path's loss horizon (its PTO, capped at twice the frame's own deadline).
        long fastAck = Time.mul(fa.rtt.smoothed(), 2) + ack.peerDelay();
        long horizon = Math.min(metrics.pto(a), Time.mul(e.due, 2));
        long thr = Math.max(fastAck, horizon);
        if (Time.sub(port.now(), e.sent) <= thr) return true;
        Stream st = streams.get(e.d.id());
        if (st == null) return true;
        StreamSend.SendPart sp = st.snd;
        PathStreamState ms = sp.mp();
        long lo = Math.max(e.d.off(), ms.ret.acked.prefix());
        long hi = e.d.off() + e.d.n();
        for (int i = 0; i < ms.ret.oppSent.size(); i++) {
            if (ms.ret.oppSent.lo(i) <= lo && lo < ms.ret.oppSent.hi(i)) lo = ms.ret.oppSent.hi(i);
        }
        if (lo < sp.preLen()) lo = 0;
        hi = Math.min(hi, Math.min(sp.q().end(), lo + cfg.spreadChunk()));
        if (lo >= hi) return true;
        int room = w.dataFit(w.room() - FrameEncoder.streamHeaderLength(st.tab.id(), lo, (int) (hi - lo), true));
        if (room <= 0) return false;
        long n = Math.min(hi - lo, room);
        SendQueue.Peek pk = sp.q().peek(lo, (int) n);
        if (pk == null || !pk.data().hasRemaining()) return true;
        n = pk.data().remaining();
        if (!w.stream(st.tab.id(), lo, pk.data(), pk.pin(), false)) return false;
        w.logFrame(FrameDesc.stream(st.tab.id(), lo, n, false));
        st.payloadSent(lo, n);
        w.spendData(n);
        ms.ret.oppSent.add(lo, lo + n);
        stats.opportunistic(n);
        f.mp().ret.oppBytes += n;
        penalize(p);
        return true;
    }

    /** The retained frame carrying the blocking data, on a pipe other than f: [pipe, entry], or null. */
    Object[] blockingFrame(Pipe f, Stream st) {
        long lo = 0, id = 0;
        if (st != null) {
            id = st.tab.id();
            if (st.snd.mps() != null) lo = st.snd.mps().ret.acked.prefix();
        }
        Pipe bp = null;
        Retained best = null;
        for (Pipe q : pipes.all()) {
            if (q.out() == null || q.mp() == null) continue;
            for (Pipe.OutLane l : q.out().lanes) {
                for (Retained e : l.log) {
                    if (e.d.kind() != FrameDesc.Kind.STREAM || e.d.n() == 0 || e.reinjected()) continue;
                    if (st != null && (e.d.id() != id || lo < e.d.off() || lo >= e.d.off() + e.d.n())) continue;
                    if (best == null || e.sent < best.sent) {
                        bp = q;
                        best = e;
                    }
                    break;
                }
            }
        }
        if (best == null || bp == f) return null;
        return new Object[] {bp, best};
    }

    /** Halves R_p, discards its samples and keeps new data off p until everything sent on it is acknowledged. */
    void penalize(Pipe p) {
        Attach a = p.mp();
        if (a.ret.penalized) return;
        a.ret.penalized = true;
        stats.penalty();
        a.ret.penaltyEnd = p.out().snd.so();
        p.out().snd.re().penalize();
        port.capacityChanged();
        port.checkTakeover();
    }

    void penaltyAcked(Pipe p) {
        Attach a = p.mp();
        if (a.ret.penalized && p.out().snd.cum() >= a.ret.penaltyEnd) {
            a.ret.penalized = false;
            port.capacityChanged();
            port.checkTakeover();
            pipes.wakeWriters();
        }
    }

    /** The retransmit budget left for new data. */
    long retainRoom() {
        long b = retransmitBudget();
        return retained >= b ? 0 : b - retained;
    }

    /** n new bytes were sent and are retained until acknowledged. */
    void retain(long n) {
        retained += n;
    }

    /** n retained bytes were released. */
    void unretain(long n) {
        retained -= Math.min(retained, n);
    }

    /** The retained unacknowledged stream bytes. */
    long retained() {
        return retained;
    }

    /** The copy queues, penalty and reinjection counters of one attachment. */
    static final class Slot {
        private long idleArm;
        private boolean penalized;
        private long penaltyEnd;
        private long reinjectedBytes;
        private long oppBytes;
        private long redundantBytes;
        /** Duplicate stream bytes received on the inbound pipe. */
        private long duplicateBytes;

        void duplicate(long n) {
            duplicateBytes += n;
        }

        long duplicateBytes() {
            return duplicateBytes;
        }
        private final ArrayList<CopyRec> copyQ = new ArrayList<>(0);
        private final ArrayList<CopyRec> selfCopyQ = new ArrayList<>(0);
        /** The lane that carries the pipe's own redundant copies; null until one is chosen. */
        private Pipe.OutLane copyLane;

        Slot() {
        }

        long idleArm() {
            return idleArm;
        }

        boolean penalized() {
            return penalized;
        }

        long penaltyEnd() {
            return penaltyEnd;
        }

        long reinjectedBytes() {
            return reinjectedBytes;
        }

        long redundantBytes() {
            return redundantBytes;
        }

        long oppBytes() {
            return oppBytes;
        }

        ArrayList<CopyRec> copyQ() {
            return copyQ;
        }

        Pipe.OutLane copyLane() {
            return copyLane;
        }

        ArrayList<CopyRec> selfCopyQ() {
            return selfCopyQ;
        }
    }

    /** A stream's retention state in the path layer: acknowledged and lost ranges, FIN, opportunistic copies. */
    static final class StreamSlot {
        private final RangeSet acked = new RangeSet();
        private final RangeSet resend = new RangeSet();
        private boolean finAcked;
        private boolean finResend;
        private final RangeSet oppSent = new RangeSet();

        StreamSlot() {
        }

        RangeSet acked() {
            return acked;
        }

        RangeSet resend() {
            return resend;
        }

        boolean finAcked() {
            return finAcked;
        }

        boolean finResend() {
            return finResend;
        }

        RangeSet oppSent() {
            return oppSent;
        }

        /** The first lost range has nothing left to send (released or reset). */
        void dropFirstResend() {
            resend.remove(resend.lo(0), resend.hi(0));
        }

        /** A lost range went out again (the transmitter's resend accounting). */
        void resent(long off, long n, boolean fin) {
            resend.remove(off, off + n);
            if (fin) finResend = false;
        }

        /** No lost FIN is left to send. */
        void finResendDone() {
            finResend = false;
        }
    }

    /** One redundant copy, queued or sent, counted against the copy bound until acknowledged or no longer owed. */
    static final class CopyRec {
        private final Stream st;
        private long off;
        private final long end;
        private final boolean fin;
        private boolean dead;

        CopyRec(Stream st, long off, long end, boolean fin) {
            this.st = st;
            this.off = off;
            this.end = end;
            this.fin = fin;
        }
    }
}
