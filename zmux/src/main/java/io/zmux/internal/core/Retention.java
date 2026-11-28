package io.zmux.internal.core;

import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.VarInt;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;

/**
 * What a multipath session retains of sent frames until acknowledged: lane logs, acknowledgement, loss and
 * reinjection, the retransmit budget, redundant and idle copies, opportunistic reinjection and penalization, and the
 * credit copies of the redundant snapshot.
 */
final class Retention {
    /** What Retention asks of the rest of the path layer and the session. */
    interface Port {
        /** See {@link PathHealth}. */
        void abandonAcked(Pipe p);

        /** See {@link PathHealth}. */
        void abandonLost(Pipe p);

        boolean peerAttached(Pipe p);

        /** See {@link Tiers}. */
        boolean admits(Tiers.Gate g, Pipe p);
        boolean takes(Tiers.Gate g, Pipe p);

        /** See {@link Resume}. */
        long reinjectAllowance(Pipe p);
        void prefixResent(Pipe p, long bytes);

        /** The scheduler's current queue-aware ETA, in nanoseconds. */
        long eta(Pipe p, long bytes);
        PathScheduler.Arrival arrival(Pipe p);

        /** The usable capacity may have changed (quality floor). */
        void capacityChanged();

        /** See {@link Tiers}. */
        void checkTakeover();

        /** See {@link PathSession}. */
        void healthChanged(Pipe p);

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
    private final CopyBudget copyBudget;
    private final PathHealth health;
    private long retained;
    /** The latest MAX_DATA is copied with the redundant snapshot. */
    private boolean copyMaxData;
    /** Stream directions whose latest MAX_STREAM_DATA is copied with the redundant snapshot. */
    private final TreeMap<Long, Stream> copyMsd = new TreeMap<>();
    private final OwnerQueue<CopyRec> copies = new OwnerQueue<>();
    /** Streams with an outstanding original range. Their first log entry owns the next prefix. */
    private final OwnerQueue<Stream> prefixWatch = new OwnerQueue<>();
    private final HashSet<Pipe> prefixReady = new HashSet<>();
    private final OwnerQueue<Retained> repairFlight = new OwnerQueue<>();
    // A copy and three intrusive links, including object headers and alignment.
    static final long COPY_CHARGE = 256;

    Retention(CoreConfig cfg, SessionStats stats, AckEngine ack, ConnFlow conn, ControlQueue ctl, StreamScheduler sched, Lanes lanes,
              Lifecycle life, PipeTable pipes, StreamsView streams, PathMetrics metrics, CopyBudget copyBudget, PathHealth health, Port port) {
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
        this.copyBudget = copyBudget;
        this.health = health;
    }

    /** Copies the limits sent while the window was the bottleneck into the redundant snapshot's record (6.5). */
    void writeCreditCopies(RecordWriter w) {
        if (copyMaxData) {
            long v = conn.advertised();
            if (w.copiedFrame(1 + VarInt.length(v), true, bb -> FrameEncoder.maxData(bb, v))) copyMaxData = false;
        }
        for (var it = copyMsd.values().iterator(); it.hasNext(); ) {
            Stream st = it.next();
            long id = st.tab.id(), v = st.rcv.adv();
            if (w.copiedFrame(1 + VarInt.length(id) + VarInt.length(v), true, bb -> FrameEncoder.maxStreamData(bb, id, v))) it.remove();
        }
    }

    /** A MAX_DATA went out; peerWaiting: the window was the bottleneck. */
    void maxDataSent(boolean peerWaiting) {
        if (peerWaiting) copyMaxData = true;
    }

    /** A MAX_STREAM_DATA of st went out; peerWaiting: the window was the bottleneck. */
    void maxStreamDataSent(Stream st, boolean peerWaiting) {
        if (peerWaiting) copyMsd.putIfAbsent(st.tab.id(), st);
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
        long now = port.now();
        Pipe p = w.pipe();
        Confirmation confirmation = null;
        for (int i = 0; i < w.descRecs.size(); i++) {
            int rec = w.descRecs.get(i);
            long end = rec < ends.length ? ends[rec] : pos;
            FrameDesc d = w.descs.get(i);
            boolean originalRange = copyBudget.enabled() && d.originalStream() && d.n() != 0;
            if (originalRange && confirmation == null) {
                long halfRtt = metrics.srtt(p) / 2;
                long feedback = Time.add(halfRtt, Time.add(Math.min(metrics.deviation(p), halfRtt), ack.peerDelay()));
                confirmation = new Confirmation(port.arrival(p), feedback);
            }
            Retained e = new Retained(end, d, now, originalRange ? confirmation : null);
            ol.log.add(e);
            if (!originalRange && !d.prefixRepair()) continue;
            Stream st = streams.get(d.id());
            if (st == null) continue;
            StreamSlot slot = st.snd.mp().ret;
            if (d.prefixRepair()) {
                e.index(null, slot.repairs, repairFlight);
            } else {
                e.index(p, slot.originals, null);
                prefixWatch.add(slot.watched);
            }
        }
        for (FrameDesc d : w.descs) {
            if (d.prefixRepair() && d.val() > 0) {
                Pipe source = pipes.get(d.val() - 1);
                if (source != null) penalize(source);
            }
        }
        w.descRecs.clear();
        w.descs.clear();
    }

    /** The handoff estimate shared by all original ranges of a batch. */
    record Confirmation(PathScheduler.Arrival arrival, long feedback) {
        long due(long sent, long bytes) {
            return Time.add(sent, Time.add(arrival.nanos(bytes), feedback));
        }
    }

    /** Pops the entries a lane acknowledgement covers. */
    void ackLog(Pipe.OutLane ol, long pos) {
        boolean full = ol.log.room() == 0;
        for (Retained first; (first = ol.log.first()) != null && first.end <= pos; ) {
            ol.log.removeFirst();
            acked(first.d);
        }
        if (full && ol.log.room() != 0) ol.requestWrite();
    }

    /** Marks the unacknowledged information of a pipe for sending again, once per entry (7.4). */
    void reinjectPipe(Pipe p) {
        if (p.out() == null) return;
        for (Pipe.OutLane ol : p.out().lanes) {
            for (Retained e : ol.log) e.forgetPrefix();
        }
        long bytes = 0;
        boolean any = false;
        for (Pipe.OutLane ol : p.out().lanes) {
            for (Retained e : ol.log) {
                if (!e.reinject()) continue;
                any = true;
                bytes += lost(e.d);
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
                if (d.n() > 0) m.ret.history.acknowledge(d.off(), Math.min(st.snd.sent(), d.off() + d.n()), st.snd.preLen());
                if (d.fin()) m.ret.finAcked = true;
                if (d.off() == 0 && m.openingPipe() != null) {
                    m.openingAcked();
                    pipes.wakeWriters(); // its datagrams go by the usual rule now
                }
                streamAckProgress(st);
            }
            case RESET -> {
                Stream st = streams.get(d.id());
                if (st != null && st.hasSend && st.snd.resetSent()) {
                    destroyCopies(st.snd.mp().ret.copies);
                    streams.sendTerminal(st, Stream.SEND_RESET_RECVD);
                }
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
        long pre = m.ret.history.prefix();
        if (pre > sp.q().base()) {
            long rel = sp.q().base();
            streams.releaseAcked(st, Math.min(pre, sp.q().end()));
            retained -= Math.min(retained, Math.min(sp.q().base(), sp.sent()) - Math.min(rel, sp.sent()));
        }
        reclaimCopies(m.ret.copies);
        m.ret.advanceOriginals(pre);
        long end = sp.reset() ? sp.resetRel() : sp.q().end();
        if (!sp.reset() && sp.finSent() && m.ret.finAcked && pre >= end) streams.sendTerminal(st, Stream.SEND_DATA_RECVD);
    }

    /** Turns one frame's information into pending sending. */
    long lost(FrameDesc d) {
        long bytes = 0;
        switch (d.kind()) {
            case STREAM -> {
                Stream st = streams.get(d.id());
                if (st == null || !st.hasSend) return 0;
                StreamSend.SendPart sp = st.snd;
                if (sp.state() == Stream.SEND_DATA_RECVD || sp.state() == Stream.SEND_RESET_RECVD) return 0;
                PathStreamState m = sp.mp();
                long lo = d.off(), hi = d.off() + d.n();
                if (sp.reset()) hi = Math.min(hi, sp.resetRel());
                long before = m.ret.history.resendBytes();
                m.ret.lose(lo, Math.min(hi, sp.sent()));
                bytes = m.ret.history.resendBytes() - before;
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
        return bytes;
    }

    /**
     * Bounds the retained unacknowledged stream data: four times the summed delivery rate
     * times (smoothed RTT + the peer's ack delay) of the live outbound pipes, at least 1 MiB
     * and at most the send buffer maximum.
     */
    long retransmitBudget() {
        double sum = 0;
        long pd = ack != null ? ack.peerDelay() : 25 * Time.MILLISECOND;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a == null || p.out() == null || a.handshaking() || !a.liveOut()) continue;
            sum += p.out().snd.re().value() * (a.rtt().smoothed() + pd) / 1e9;
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
        if (st.tab.discarded() || !st.hasSend
                || st.snd.state() == Stream.SEND_DATA_RECVD || st.snd.state() == Stream.SEND_RESET_RECVD) return 0;
        StreamSend.SendPart sp = st.snd;
        long end = sp.reset() ? Math.min(c.end, sp.resetRel()) : c.end;
        long lo = c.off;
        if (sp.mps() != null) lo = Math.max(lo, sp.mps().ret.history.prefix());
        if (lo >= end) return c.fin && sp.mps() != null && !sp.mps().ret.finAcked && !sp.reset() ? 1 : 0;
        return end - lo;
    }

    long copiesOutstanding() {
        long total = reclaimCopies(copies);
        for (var link = repairFlight.first(); link != null; ) {
            Retained entry = link.value();
            link = link.next();
            Stream st = streams.get(entry.d.id());
            long n = st == null || st.tab.discarded() || st.snd.reset() ? 0
                    : st.snd.mp().ret.history.unacknowledged(entry.d.off(), entry.d.off() + entry.d.n());
            if (n == 0) entry.forgetPrefix();
            else total += n;
        }
        return total;
    }

    private long copyRoom() { return Math.max(0, copyBound() - copiesOutstanding()); }

    private long reclaimCopies(OwnerQueue<CopyRec> queue) {
        long sum = 0;
        for (var link = queue.first(); link != null; ) {
            CopyRec c = link.value();
            link = link.next();
            long n = copyLeft(c);
            if (n == 0) retireCopy(c);
            else sum += n;
        }
        return sum;
    }

    private void retireCopy(CopyRec c) {
        c.pending.remove();
        c.stream.remove();
        c.account.remove();
        conn.releaseOptional(COPY_CHARGE);
    }

    private void destroyCopies(OwnerQueue<CopyRec> queue) {
        while (queue.first() != null) retireCopy(queue.first().value());
    }

    void discarded(Stream st) {
        copyMsd.remove(st.tab.id());
        if (st.hasSend && st.snd.mps() != null) {
            StreamSlot slot = st.snd.mps().ret;
            slot.clearOriginals();
            slot.clearRepairs();
            destroyCopies(slot.copies);
        }
    }

    void close() {
        destroyCopies(copies);
        copyMsd.clear();
        while (prefixWatch.first() != null) prefixWatch.first().value().snd.mp().ret.clearOriginals();
        while (repairFlight.first() != null) repairFlight.first().value().forgetPrefix();
        prefixReady.clear();
    }

    /** Queues a copy after new data [off, off+n) of st went on the decision's pipe. */
    void copyChunk(PathScheduler.Decision d, RecordWriter w, Stream st, long off, long n, boolean fin) {
        Pipe p = d.p();
        Attach a = p.mp();
        copyBudget.original(port.now(), n);
        if (cfg.copyPolicy().maxCopies() == 1) return;
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
        long bytes = Math.max(1, end - off);
        if (bytes > copyBound() - copiesOutstanding() || copyBudget.remaining(port.now()) < end - off) return;
        OwnerQueue<CopyRec> queue = self ? a.ret.selfCopyQ : target.mp().ret.copyQ;
        if (queueCopy(st, off, end, fin, queue)) pipes.wake(target);
    }

    boolean queueCopy(Stream st, long off, long end, boolean fin, OwnerQueue<CopyRec> queue) {
        if (!conn.reserveOptional(COPY_CHARGE)) return false;
        CopyRec c;
        try {
            c = new CopyRec(st, off, end, fin);
            st.snd.mp().ret.copies.add(c.stream);
            copies.add(c.account);
            queue.add(c.pending);
        } catch (RuntimeException | Error failure) {
            conn.releaseOptional(COPY_CHARGE);
            throw failure;
        }
        return true;
    }

    /** Writes the copies of queue q ahead of p's other stream data; all are dropped when p is not eligible. */
    void writeCopies(PathScheduler.Decision d, RecordWriter w, OwnerQueue<CopyRec> q) {
        Pipe p = d.p();
        if (q.isEmpty()) return;
        if (d.g() == null || !port.admits(d.g(), p)) {
            dropCopies(p);
            return;
        }
        while (!q.isEmpty() && w.dataRoom() != 0) {
            CopyRec c = q.first().value();
            if (copyLeft(c) == 0) {
                retireCopy(c);
                continue;
            }
            Stream st = c.st;
            StreamSend.SendPart sp = st.snd;
            long start = Math.max(c.off, sp.q().base());
            if (sp.mps() != null) start = Math.max(start, sp.mps().ret.history.prefix());
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
            SendQueue.Peek fragment = null;
            if (n > 0) {
                // [start, end) lies within the queue (end is capped at its end above).
                fragment = java.util.Objects.requireNonNull(sp.q().peek(start, (int) n), "copy range outside the send queue");
                if (fragment.data().remaining() < n) fin = false;
                n = fragment.data().remaining();
            }
            if (n == 0 && !fin) {
                retireCopy(c);
                continue;
            }
            if (copyBudget.remaining(port.now()) < n) {
                retireCopy(c);
                continue;
            }
            if (!w.stream(st, start, fragment, fin, false)) return;
            copyBudget.copy(port.now(), n);
            w.logFrame(FrameDesc.stream(st.tab.id(), start, n, fin));
            st.payloadSent(start, n);
            w.spendData(n);

            stats.redundant(n);
            if (w.pipe() != null && w.pipe().mp() != null) w.pipe().mp().ret.redundantBytes += n;
            if (start + n >= end) {
                c.pending.remove();
            } else {
                c.off = start + n;
            }
        }
    }

    void dropCopies(Pipe p) {
        Attach a = p.mp();
        destroyCopies(a.ret.copyQ);
        destroyCopies(a.ret.selfCopyQ);
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

    /**
     * Whether new data of a listed direction is held back: by its stream credit, or by a binding
     * that moves and so takes no new data until its data on the old pipe is acknowledged. A small
     * credit that arrives during the move (a blocked direction gets credit at once) leaves it as
     * held as before: the data that blocks it still sits on the old pipe.
     */
    static boolean streamBlocked(Stream st) {
        StreamSend.SendPart sp = st.snd;
        if (st.tab.discarded() || !st.hasSend || sp.reset() || !st.tab.idSet() || sp.sendEnd() <= Math.max(sp.sent(), sp.q().base())) return false;
        return sp.sent() >= sp.maxData() || (sp.mps() != null && sp.mps().sched.moving());
    }

    private boolean connBlocked() {
        return conn.sent() >= conn.sendMax() && !sched.isEmpty();
    }

    boolean hasPrefixWork() {
        return !prefixWatch.isEmpty();
    }

    boolean prefixReady(Pipe p) { return hasPrefixWork() && prefixReady.contains(p); }

    /** Computes the owner deadline, or at expiry wakes only the writer selected for the exact range. */
    long prefixTimers(Tiers.Gate gate, boolean apply) {
        prefixReady.clear();
        if (!hasPrefixWork() || gate == null) return Time.NEVER;
        long now = port.now();
        long next = Time.NEVER;
        long room = copyRoom();
        for (Prefix head : prefixes()) {
            Prefix range = head.limitedBy(room);
            Retained e = range.entry;
            Pipe dest = prefixDestination(gate, range, now);
            if (dest == null || range.bytes() > room || overlapsCopy(range) || copyBudget.remaining(now) < range.bytes()) {
                e.targetRepair(null);
                if (e.repairDue() > now) next = Math.min(next, e.repairDue());
                if (dest != null && now >= e.repairDue()) next = Math.min(next, copyBudget.nextRefill(now));
                continue;
            }
            if (now < e.repairDue()) {
                next = Math.min(next, e.repairDue());
            } else if (apply) {
                e.targetRepair(dest);
                prefixReady.add(dest);
                pipes.wake(dest);
            } else if (e.repairTarget() == dest) {
                prefixReady.add(dest);
            } else {
                next = Math.min(next, e.repairDue());
            }
        }
        return next;
    }

    void opportunistic(Tiers.Gate gate, RecordWriter w, Pipe dest) {
        if (gate == null || !prefixReady(dest) || w.dataRoom() == 0) return;
        long now = port.now();
        long copyRoom = copyRoom();
        for (Prefix head : prefixes()) {
            if (!w.historyRoom() || w.dataRoom() == 0) return;
            Prefix range = head.limitedBy(copyRoom);
            Retained e = range.entry;
            if (e.repairTarget() != dest || e.repairDue() > now || prefixDestination(gate, range, now) != dest) continue;
            long n = range.bytes();
            if (n > copyRoom || overlapsCopy(range) || copyBudget.remaining(now) < n) continue;
            Stream st = range.stream;
            var sp = st.snd;
            int room = w.dataFit(w.room() - FrameEncoder.streamHeaderLength(st.tab.id(), range.start, (int) n, true));
            if (room <= 0 || (range.start < sp.preLen() && range.start + Math.min(room, n) < sp.preLen())) return;
            long limit = Math.min(n, room);
            n = 0;
            while (n < limit) {
                long offset = range.start + n;
                SendQueue.Peek pk = sp.q().peek(offset, (int) (limit - n));
                if (pk == null || !w.stream(st, offset, pk, false, false)) break;
                n += pk.data().remaining();
            }
            if (n == 0) return;
            Pipe penalized = streamBlocked(st) || (connBlocked() && sp.sendEnd() > sp.sent()) ? e.source() : null;
            w.logFrame(FrameDesc.prefixRepair(st.tab.id(), range.start, n, penalized));
            st.payloadSent(range.start, n);
            w.spendData(n);
            e.repairedThrough(range.end);
            copyRoom -= n;
            sp.mp().ret.history.copied(range.start, range.start + n);
            copyBudget.copy(now, n);
            stats.opportunistic(n);
            dest.mp().ret.oppBytes += n;
            port.prefixResent(dest, n);
        }
    }

    private record Prefix(Retained entry, Stream stream, long start, long end) {
        long bytes() { return end - start; }

        Prefix limitedBy(long budget) {
            long limit = Math.min(end, start + budget);
            if (budget == 0 || limit < stream.snd.preLen() || limit == end) return this;
            return new Prefix(entry, stream, start, limit);
        }
    }

    private boolean overlapsCopy(Prefix range) {
        for (var link = range.stream.snd.mp().ret.copies.first(); link != null; link = link.next()) {
            var copy = link.value();
            if (copy.off < range.end && copy.end > range.start && copyLeft(copy) != 0) return true;
        }
        return false;
    }

    /** Each live stream contributes only its oldest outstanding original. */
    private ArrayList<Prefix> prefixes() {
        var ranges = new ArrayList<Prefix>();
        for (var link = prefixWatch.first(); link != null; ) {
            Stream st = link.value();
            var slot = st.snd.mp().ret;
            link = link.next();
            if (st.tab.discarded() || st.snd.cancelled() || st.snd.reset()) {
                slot.clearOriginals();
                continue;
            }
            if (slot.originals.isEmpty()) {
                slot.watched.remove();
                continue;
            }
            Retained e = slot.originals.first().value();
            Pipe p = e.source();
            if (p.mp().out() != Attach.OUT_ACTIVE || p.out().failed()
                    || (p.mp().topologyExcluded() & Action.DIR_OUT) != 0 || !port.peerAttached(p)
                    || slot.history.resendBytes() != 0) continue;
            long prefix = slot.history.prefix();
            if (e.d.off() > prefix) continue;
            long end = e.d.off() + e.d.n();
            long from = slot.history.nextUncopied(prefix, end);
            if (prefix < e.repairEnd()) end = Math.min(end, e.repairEnd());
            else if (from > prefix) continue;
            end = slot.history.repairableEnd(from, Math.min(end, from + cfg.spreadChunk()));
            if (from < end && (from >= st.snd.preLen() || end >= st.snd.preLen())) {
                ranges.add(new Prefix(e, st, from, end));
            }
        }
        return ranges;
    }

    private Pipe prefixDestination(Tiers.Gate gate, Prefix range, long now) {
        Retained e = range.entry;
        Pipe source = e.source();
        long bytes = range.bytes();
        long sourceEta = Time.add(port.eta(source, bytes), metrics.srtt(source) / 2);
        boolean liveIn = health.liveReturn();
        Pipe best = null;
        long bestEta = Time.NEVER, bestMargin = 0;
        for (Pipe q : pipes.all()) {
            if (q == source || q.mp() == null || q.out() == null || !port.takes(gate, q) || !metrics.open(q)
                    || bytes > port.reinjectAllowance(q) || !q.out().lanes.getFirst().st.confirmed()
                    || q.out().lanes.getFirst().st.shut() || ack.historyHeld(q) || q.out().lanes.getFirst().log.room() == 0
                    || Shaping.delaying(q, now) || !health.deliveringAfter(q, e.sent, now, liveIn)) continue;
            long eta = port.eta(q, bytes);
            long margin = Math.max(50 * Time.MILLISECOND, Time.add(metrics.srtt(q) / 2, ack.peerDelay()));
            if (Time.add(eta, margin) >= sourceEta) continue;
            if (best == null || eta < bestEta || (eta == bestEta && q.id < best.id)) {
                best = q;
                bestEta = eta;
                bestMargin = margin;
            }
        }
        if (best != null) {
            long sourceDue = e.confirmation.due(e.sent, bytes);
            long alternateDue = Time.add(e.sent, Time.add(bestEta, bestMargin));
            e.boundRepair(Math.min(sourceDue, alternateDue));
        }
        return best;
    }

    /** Halves R_p, discards its samples and keeps new data off p until everything sent on it is acknowledged. */
    void penalize(Pipe p) {
        Attach a = p.mp();
        if (a.ret.penalized) return;
        a.ret.penalized = true;
        stats.penalty();
        a.ret.penaltyEnd = p.out().snd.so();
        p.out().snd.re().penalize();
        port.healthChanged(p);
        port.capacityChanged();
        port.checkTakeover();
    }

    void penaltyAcked(Pipe p) {
        Attach a = p.mp();
        if (a.ret.penalized && p.out().snd.cum() >= a.ret.penaltyEnd) {
            a.ret.penalized = false;
            port.healthChanged(p);
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
        private final OwnerQueue<CopyRec> copyQ = new OwnerQueue<>();
        private final OwnerQueue<CopyRec> selfCopyQ = new OwnerQueue<>();
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

        OwnerQueue<CopyRec> copyQ() {
            return copyQ;
        }

        Pipe.OutLane copyLane() {
            return copyLane;
        }

        OwnerQueue<CopyRec> selfCopyQ() {
            return selfCopyQ;
        }
    }

    /** A stream's retention state in the path layer: acknowledged and lost ranges, FIN, opportunistic copies. */
    static final class StreamSlot {
        final SendHistory history;
        private final OwnerQueue<CopyRec> copies = new OwnerQueue<>();
        private final OwnerQueue.Link<Stream> watched;
        private final OwnerQueue<Retained> originals = new OwnerQueue<>();
        private final OwnerQueue<Retained> repairs = new OwnerQueue<>();
        private boolean finAcked;
        private boolean finResend;

        StreamSlot(SendHistory history, Stream stream) {
            this.history = history;
            this.watched = new OwnerQueue.Link<>(stream);
        }

        void advanceOriginals(long prefix) {
            while (originals.first() != null) {
                Retained first = originals.first().value();
                if (first.d.off() + first.d.n() > prefix) return;
                first.forgetPrefix();
            }
            watched.remove();
        }

        void clearOriginals() {
            watched.remove();
            while (originals.first() != null) originals.first().value().forgetPrefix();
        }

        void clearRepairs() {
            while (repairs.first() != null) repairs.first().value().forgetPrefix();
        }

        /** Only uncovered bytes need another send while a prefix copy is already outstanding. */
        void lose(long from, long to) {
            long pos = from;
            while (pos < to) {
                long next = to, covered = pos;
                for (var link = repairs.first(); link != null; link = link.next()) {
                    FrameDesc copy = link.value().d;
                    if (copy.off() <= pos) covered = Math.max(covered, copy.off() + copy.n());
                    else next = Math.min(next, copy.off());
                }
                if (covered > pos) {
                    pos = covered;
                } else {
                    history.lost(pos, next);
                    pos = next;
                }
            }
        }

        boolean finAcked() { return finAcked; }

        boolean finResend() { return finResend; }

        /** A lost range went out again. */
        void resent(long off, long n, boolean fin) {
            history.resent(off, off + n);
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
        private final OwnerQueue.Link<CopyRec> account = new OwnerQueue.Link<>(this);
        private final OwnerQueue.Link<CopyRec> pending = new OwnerQueue.Link<>(this);
        private final OwnerQueue.Link<CopyRec> stream = new OwnerQueue.Link<>(this);

        CopyRec(Stream st, long off, long end, boolean fin) {
            this.st = st;
            this.off = off;
            this.end = end;
            this.fin = fin;
        }
    }
}
