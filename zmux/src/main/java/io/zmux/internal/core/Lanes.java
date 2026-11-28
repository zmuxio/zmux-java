package io.zmux.internal.core;

import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.FrameType;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.VarInt;
import io.zmux.internal.wire.WireException;

/**
 * Lanes: opening, confirmation, refusal and the
 * handshake timeout of outbound lanes, the binding of stream directions to lanes, the
 * data turns of paced lanes, and the admission of inbound lanes. Owned by the session's
 * loop; carrier commands and wake-ups go through the {@link Outbox}.
 */
public final class Lanes {
    /** What the lanes ask of the session. */
    interface Port {
        long now();

        boolean established();

        boolean closing();

        Features features();

        TransportParams peer();

        /** Whether the direction stays on lane 0 (the path layer's urgency rule). */
        boolean urgentLane(Stream st);

        void failLocal(WireException e);

        /** An acknowledgement advanced: the close wait may be over. */
        void recheckCloseWait();
    }

    private final CoreConfig cfg;
    private final PipeTable pipes;
    private final Outbox outbox;
    private final Port port;
    private final TimerHeap deadlines = new TimerHeap();
    private Runnable deadlineChanged;

    void onDeadlineChanged(Runnable changed) { deadlineChanged = changed; }
    /** What the last {@link #laneFor} found: the binding holds; a new lane is wanted. */
    private boolean keep, needNew;

    Lanes(CoreConfig cfg, PipeTable pipes, Outbox outbox, Port port) {
        this.cfg = cfg;
        this.pipes = pipes;
        this.outbox = outbox;
        this.port = port;
    }

    /** The number of lanes the peer accepts on our pipes. */
    long limit() {
        return port.features().lanesOut() ? port.peer().maxLanes : 1;
    }

    // ---- binding ----

    /** The lane a stream direction's next chunk would go on, without changing anything. */
    Pipe.OutLane laneFor(Pipe p, Stream st) {
        keep = false;
        needNew = false;
        StreamSend.SendPart sp = st.snd;
        Pipe.OutDir d = p.out();
        if (sp.bind().laneBound && sp.bind().lanePipe == p.id) {
            Pipe.OutLane l = d.lane(sp.bind().lane);
            if (l != null && !l.st.refused && l.ack.acked() < sp.bind().laneEnd) {
                keep = true;
                return l;
            }
        }
        Pipe.OutLane lane0 = d.lanes.getFirst();
        if (!d.lo.offered || port.urgentLane(st) || limit() <= 1) return lane0;
        Pipe.OutLane best = null;
        int bestBound = Integer.MAX_VALUE;
        boolean free = false;
        for (int i = 1; i < d.lanes.size(); i++) {
            Pipe.OutLane x = d.lanes.get(i);
            if (!x.st.confirmed || x.st.refused || x == copyLaneOf(p)) continue;
            int bound = x.boundSt.size();
            if (sp.bind().laneBound && sp.bind().lanePipe == p.id && sp.bind().lane == x.lane) bound--;
            if (bound == 0) free = true;
            if (best == null || bound < bestBound
                    || (bound == bestBound && x.tx.pos() - x.ack.acked() < best.tx.pos() - best.ack.acked())) {
                best = x;
                bestBound = bound;
            }
        }
        if (best == null) best = lane0;
        needNew = !free;
        return best;
    }

    /** Binds the direction and wakes a different lane when it acquires the work. */
    Pipe.OutLane bind(Pipe p, Stream st, Pipe.OutLane consumer) {
        Pipe.OutLane l = laneFor(p, st);
        if (keep) return l;
        if (needNew) want(p);
        Bind bind = st.snd.bind();
        if (bind.laneBound && bind.lanePipe == p.id && bind.lane == l.lane) return l;
        unbind(st);
        bind.lanePipe = p.id;
        bind.lane = l.lane;
        bind.laneBound = true;
        l.boundSt.add(st);
        if (l != consumer) l.requestWrite();
        return l;
    }

    /** Drops a stream direction's lane binding. */
    void unbind(Stream st) {
        StreamSend.SendPart sp = st.snd;
        if (!sp.bind().laneBound) return;
        sp.bind().laneBound = false;
        Pipe p = pipes.get(sp.bind().lanePipe);
        if (p == null || p.out() == null) return;
        Pipe.OutLane l = p.out().lane(sp.bind().lane);
        if (l != null) l.boundSt.remove(st);
    }

    // ---- outbound lanes ----

    /** The pipe's copy lane (multipath), or null. */
    private static Pipe.OutLane copyLaneOf(Pipe p) {
        return p.mp() == null ? null : p.mp().ret.copyLane();
    }

    /** Asks the driver for a new lane when the rules allow one. */
    void want(Pipe p) {
        Lanes.Out lo = p.out().lo;
        if (lo.refused || lo.requested || lo.pending != null || !lo.offered) return;
        if (p.out().lanes.getFirst().ack.acked() == 0 || p.out().lanes.size() >= limit() || port.closing()) return;
        lo.requested = true;
        outbox.action(new Action(Action.Kind.OPEN_LANE, p.id, 0, Action.DIR_OUT));
    }

    /** Attaches the stream the driver opened for a requested lane; returns its lane number, or -1. */
    public long opened(long pipeId, CarrierInfo info) {
        Pipe p = pipes.get(pipeId);
        if (p == null || p.out() == null || p.out().failed() || port.closing()) return -1;
        Lanes.Out lo = p.out().lo;
        lo.requested = false;
        if (lo.refused || lo.pending != null) return -1;
        if (lo.next == 0) lo.next = 1;
        // Lanes are streams of the pipe's carrier connection: the pipe's record limit holds on each.
        int cap = CarrierInfo.minRecord(info.maxRecord(), p.out().lanes.getFirst().st.info().maxRecord());
        Pipe.OutLane l = new Pipe.OutLane(lo.next++, cap == info.maxRecord() ? info : info.withMaxRecord(cap), false);
        l.st.laneFirst = true;
        lo.pending = new LaneDeadline(p, l);
        deadlines.schedule(lo.pending, Time.add(port.now(), cfg.joinDeadline()));
        deadlineChanged.run();
        p.out().add(l);
        pipes.changed();
        pipes.wake(p);
        return l.lane;
    }

    /** A requested lane could not be opened now. */
    public void openFailed(long pipeId) {
        Pipe p = pipes.get(pipeId);
        if (p != null && p.out() != null) p.out().lo.requested = false;
    }

    /** Writes the PIPE_LANE record of a new lane. */
    void writeFirst(RecordWriter w, Pipe p, Pipe.OutLane ol) {
        byte[] key = port.peer().laneKey;
        int n = VarInt.length(FrameType.PIPE_LANE) + 16 + VarInt.length(p.id) + VarInt.length(ol.lane);
        if (!w.setupFrame(n, true, bb -> FrameEncoder.pipeLane(bb, key, p.id, ol.lane))) return;
        ol.st.laneFirst = false;
        w.end();
        w.closeLaneOnly();
    }

    private void confirmed(Pipe p, Pipe.OutLane ol) {
        if (ol.st.confirmed || ol.lane == 0) return;
        ol.st.confirmed = true;
        p.out().lo.cancel(ol);
        pipes.wakeWriters();
    }

    /** A lane that ended before it was confirmed. */
    void refused(Pipe p, Pipe.OutLane ol) {
        ol.st.refused = true;
        ol.retire();
        p.out().lo.refused = true;
        p.out().laneRefused();
        p.out().lo.cancel(ol);
        outbox.action(new Action(Action.Kind.RESET_LANE, p.id, ol.lane, Action.DIR_OUT));
    }

    /** Runs after an acknowledgement advance on a lane. */
    void acked(Pipe p, Pipe.OutLane ol) {
        if (ol.ack.acked() > 0) confirmed(p, ol);
        for (int i = 0; i < ol.boundSt.size(); ) {
            Stream st = ol.boundSt.get(i);
            if (st.snd.bind().laneEnd <= ol.ack.acked()) {
                unbind(st);
                continue;
            }
            i++;
        }
        port.recheckCloseWait();
        pipes.wakeWriters();
    }

    /**
     * Whether the records CONNECTION_CLOSE must not overtake are acknowledged: those on lanes
     * other than 0 or, in a multipath session (whose peer closes on the first copy it reads), those
     * on every lane of every outbound pipe that has not failed. Records that elicit no
     * acknowledgement (a PIPE_ACK alone) are not waited for: no peer acknowledges them.
     */
    boolean covered() {
        int first = port.features().multipath() ? 0 : 1;
        for (Pipe p : pipes.all()) {
            if (p.out() == null || p.out().failed()) continue;
            for (int i = first; i < p.out().lanes.size(); i++) {
                Pipe.OutLane l = p.out().lanes.get(i);
                if (!l.st.refused && l.ack.acked() < l.tx.elicitPos()) return false;
            }
        }
        return true;
    }

    /** The earliest handshake deadline of a lane not confirmed yet. */
    long deadline() {
        return deadlines.earliest();
    }

    /** Refuses the lanes not confirmed within the handshake timeout. */
    void onTimeout(long t) {
        deadlines.fireDue(t);
    }

    void closeTimers() { deadlines.clear(); }

    private final class LaneDeadline extends TimerHeap.Timer {
        private final Pipe pipe;
        private final Pipe.OutLane lane;

        LaneDeadline(Pipe pipe, Pipe.OutLane lane) {
            this.pipe = pipe;
            this.lane = lane;
        }

        @Override public void fire(long now) { refused(pipe, lane); }

        void cancel() {
            deadlines.remove(this);
            deadlineChanged.run();
        }
    }

    // ---- data turns ----

    /** Whether data lane ol may take new stream data from the paced room now. */
    boolean dataTurn(Pipe p, Pipe.OutLane ol) {
        if (ol.lane == 0 || ol == copyLaneOf(p) || !ol.st.served) return true;
        for (int i = 1; i < p.out().lanes.size(); i++) {
            Pipe.OutLane x = p.out().lanes.get(i);
            if (x != ol && x.st.waiting && !x.st.served) {
                ol.st.waiting = true;
                return false;
            }
        }
        for (int i = 1; i < p.out().lanes.size(); i++) p.out().lanes.get(i).st.served = false;
        return true;
    }

    /** Records a data batch of ol and wakes the lanes waiting for their turn. */
    void tookTurn(Pipe p, Pipe.OutLane ol) {
        if (ol.lane == 0 || ol == copyLaneOf(p)) return;
        ol.st.served = true;
        ol.st.waiting = false;
        for (int i = 1; i < p.out().lanes.size(); i++) {
            Pipe.OutLane x = p.out().lanes.get(i);
            if (x.st.waiting) x.requestWrite();
        }
    }

    // ---- inbound lanes ----

    /** The session's own Lane Key, or null when lanes toward it are not offered. */
    public byte[] key() {
        return cfg.local().has(TransportParams.LANES) ? cfg.local().laneKey.clone() : null;
    }

    /** Compares a key with the own Lane Key in constant time. */
    public boolean keyMatches(byte[] key) {
        return key != null && key.length == 16 && java.security.MessageDigest.isEqual(cfg.local().laneKey, key);
    }

    /**
     * Attaches an inbound lane whose first record starts with a PIPE_LANE naming (pipe,
     * lane) with this session's key; false when the stream must be aborted without any
     * effect. Protocol violations close the session.
     */
    public boolean accept(Object c, long pipeId, long lane, CarrierInfo info) {
        if (port.closing() || !port.established()) return false;
        Pipe p = pipes.get(pipeId);
        if (p == null || p.in() == null || p.in().ended() || p.conn() == null || p.conn() != c) return false;
        if (!port.features().lanesIn()) {
            port.failLocal(new WireException(TransportError.FRAME_ENCODING_ERROR, FrameType.PIPE_LANE,
                    "PIPE_LANE without lanes in use", ""));
            return false;
        }
        if (lane == 0 || lane >= cfg.local().maxLanes || p.in().lane(lane) != null) {
            port.failLocal(WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "invalid lane", "pipe", pipeId, "value", lane));
            return false;
        }
        Pipe.InLane il = new Pipe.InLane(lane, false, true);
        p.in().add(il);
        return true;
    }

    /** Marks the lane shut; true when it was not shut yet (the caller then shuts the carrier). */
    static boolean shut(Pipe.OutLane l) {
        if (l.st.shut) return false;
        l.st.shut = true;
        return true;
    }

    /** The lane lifecycle of one outbound lane. */
    static final class LaneState {
        private final CarrierInfo info;
        private boolean shut;
        private boolean laneFirst;
        private boolean confirmed;
        private boolean refused;
        private boolean served;
        private boolean waiting;

        LaneState(CarrierInfo info, boolean confirmed) {
            this.info = info;
            this.confirmed = confirmed;
        }

        CarrierInfo info() {
            return info;
        }

        boolean shut() {
            return shut;
        }

        boolean laneFirst() {
            return laneFirst;
        }

        boolean confirmed() {
            return confirmed;
        }

        boolean refused() {
            return refused;
        }

        boolean served() {
            return served;
        }

        boolean waiting() {
            return waiting;
        }

    }

    /** The lane state of an outbound pipe. */
    static final class Out {
        private long next;
        private LaneDeadline pending;
        private final boolean offered;
        private boolean requested;
        private boolean refused;

        Out(boolean offered) {
            this.offered = offered;
        }

        long next() {
            return next;
        }

        void cancel(Pipe.OutLane lane) {
            if (pending != null && pending.lane == lane) cancel();
        }

        void cancel() {
            if (pending == null) return;
            pending.cancel();
            pending = null;
        }

        boolean offered() {
            return offered;
        }

        boolean requested() {
            return requested;
        }

        boolean refused() {
            return refused;
        }
    }

    /** The data of st written so far ends at end on its bound lane. */
    static void boundEnd(Stream st, long end) {
        st.snd.bind().laneEnd = end;
    }

    /** st's lane binding ended with its pipe. */
    static void pipeGone(Stream st) {
        st.snd.bind().laneBound = false;
    }

    /** The lane a stream's sending part is bound to. */
    static final class Bind {
        private long lane;
        private long lanePipe;
        private long laneEnd;
        private boolean laneBound;

        Bind() {
        }

        long lane() {
            return lane;
        }

        long lanePipe() {
            return lanePipe;
        }

        long laneEnd() {
            return laneEnd;
        }

        boolean laneBound() {
            return laneBound;
        }
    }
}
