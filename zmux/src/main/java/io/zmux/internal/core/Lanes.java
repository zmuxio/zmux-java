package io.zmux.internal.core;

import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.FrameType;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.carrier.VarInt;
import io.zmux.internal.wire.WireException;

/**
 * Lanes (MULTIPATH.md Sections 2.5 and 5.8): opening, confirmation, refusal and the
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
    }

    private final CoreConfig cfg;
    private final PipeTable pipes;
    private final Outbox outbox;
    private final Lifecycle life;
    private final Port port;
    /** What the last {@link #laneFor} found: the binding holds; a new lane is wanted. */
    private boolean keep, needNew;

    Lanes(CoreConfig cfg, PipeTable pipes, Outbox outbox, Lifecycle life, Port port) {
        this.cfg = cfg;
        this.pipes = pipes;
        this.outbox = outbox;
        this.life = life;
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
        boolean free = false;
        for (int i = 1; i < d.lanes.size(); i++) {
            Pipe.OutLane x = d.lanes.get(i);
            if (!x.st.confirmed || x.st.refused || x == copyLaneOf(p)) continue;
            int bound = x.boundSt.size();
            if (sp.bind().laneBound && sp.bind().lanePipe == p.id && sp.bind().lane == x.lane) bound--;
            if (bound == 0) free = true;
            if (best == null || bound < best.boundSt.size()
                    || bound == best.boundSt.size() && x.tx.pos() - x.ack.acked() < best.tx.pos() - best.ack.acked()) {
                best = x;
            }
        }
        if (best == null) best = lane0;
        needNew = !free;
        return best;
    }

    /** Binds the direction to the lane laneFor picks. */
    Pipe.OutLane bind(Pipe p, Stream st) {
        Pipe.OutLane l = laneFor(p, st);
        if (keep) return l;
        boolean wantNew = needNew;
        unbind(st);
        if (wantNew) want(p);
        st.snd.bind().lanePipe = p.id;
        st.snd.bind().lane = l.lane;
        st.snd.bind().laneBound = true;
        l.boundSt.add(st);
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
    public long opened(long pipeId, CarrierInfo info, Waiter sig) {
        Pipe p = pipes.get(pipeId);
        if (p == null || p.out() == null || port.closing()) return -1;
        Lanes.Out lo = p.out().lo;
        lo.requested = false;
        if (lo.refused) return -1;
        if (lo.next == 0) lo.next = 1;
        Pipe.OutLane l = new Pipe.OutLane(lo.next++, info, false);
        l.st.laneFirst = true;
        l.st.openedAt = port.now();
        l.st.sig = sig;
        lo.pending = l;
        p.out().add(l);
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
        if (!w.frame(n, true, bb -> FrameEncoder.pipeLane(bb, key, p.id, ol.lane))) return;
        ol.st.laneFirst = false;
        w.end();
        w.closeLaneOnly();
    }

    private void confirmed(Pipe p, Pipe.OutLane ol) {
        if (ol.st.confirmed || ol.lane == 0) return;
        ol.st.confirmed = true;
        if (p.out().lo.pending == ol) p.out().lo.pending = null;
        pipes.wakeWriters();
    }

    /** A lane that ended before it was confirmed. */
    void refused(Pipe p, Pipe.OutLane ol) {
        ol.st.refused = true;
        p.out().lo.refused = true;
        p.out().laneRefused();
        if (p.out().lo.pending == ol) p.out().lo.pending = null;
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
        if (life.waitingLanes() && covered()) life.lanesDone();
        pipes.wakeWriters();
    }

    /** Whether every record written on lanes other than 0 is acknowledged (SPEC.md Section 6.5 step 1). */
    boolean covered() {
        for (Pipe p : pipes.all()) {
            if (p.out() == null) continue;
            for (int i = 1; i < p.out().lanes.size(); i++) {
                Pipe.OutLane l = p.out().lanes.get(i);
                if (!l.st.refused && l.ack.acked() < l.tx.pos()) return false;
            }
        }
        return true;
    }

    /** The earliest handshake deadline of a lane not confirmed yet. */
    long deadline() {
        return timeouts(0, false);
    }

    /** Refuses the lanes not confirmed within the handshake timeout. */
    void onTimeout(long t) {
        timeouts(t, true);
    }

    private long timeouts(long t, boolean apply) {
        long r = Time.NEVER;
        for (Pipe p : pipes.all()) {
            if (p.out() == null || p.out().lo.pending == null) continue;
            Pipe.OutLane l = p.out().lo.pending;
            long d = Time.add(l.st.openedAt, cfg.handshakeTimeout());
            if (apply && t >= d) {
                refused(p, l);
                continue;
            }
            r = Math.min(r, d);
        }
        return r;
    }

    // ---- data turns (N13) ----

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
            if (x.st.waiting) outbox.wake(x.st.sig);
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
     * effect (MULTIPATH.md Section 5.8 rule 1). Protocol violations close the session.
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
        Pipe.InLane il = new Pipe.InLane(lane, false, true, false);
        p.in().add(il);
        return true;
    }

    /** Marks the lane shut; true when it was not shut yet (the caller then shuts the carrier). */
    static boolean shut(Pipe.OutLane l) {
        if (l.st.shut) return false;
        l.st.shut = true;
        return true;
    }

    /** A reserved pipe's actual outbound carrier, installed before it performs I/O. */
    static void rebind(Pipe.OutDir d, CarrierInfo info) {
        d.lanes.getFirst().st.info = info;
        d.lo.offered = info.lanes();
    }

    /** The lane lifecycle of one outbound lane. */
    static final class LaneState {
        private CarrierInfo info;
        private boolean shut;
        private boolean laneFirst;
        private boolean confirmed;
        private boolean refused;
        private boolean served;
        private boolean waiting;
        private Waiter sig;
        private long openedAt;

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

        Waiter sig() {
            return sig;
        }

        long openedAt() {
            return openedAt;
        }
    }

    /** The lane state of an outbound pipe. */
    static final class Out {
        private long next;
        private Pipe.OutLane pending;
        private boolean offered;
        private boolean requested;
        private boolean refused;

        Out(boolean offered) {
            this.offered = offered;
        }

        long next() {
            return next;
        }

        Pipe.OutLane pending() {
            return pending;
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

    /** The lane a stream's sending part is bound to (MULTIPATH.md 6.5). */
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
