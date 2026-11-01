package io.zmux.internal.core;

import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.FrameType;
import io.zmux.internal.wire.JoinCrypto;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.carrier.VarInt;
import io.zmux.internal.wire.WireException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 * PIPE_JOIN generation and verification, the pipe ID space and its replay window, the attachment caps and the
 * refusal backoff, and PIPE_ABANDON owed by ID (MULTIPATH.md 4, 5).
 */
public final class Joins {
    /** What Joins asks of the rest of the path layer and the session. */
    interface Port {
        /** See {@link PathHealth}. */
        void requestPing(Pipe p, long now);

        /** See {@link PathHealth}. */
        void failWithoutAbandon(Pipe p, long code);

        /** See {@link Resume}. */
        void checkResumed();

        /** See {@link Tiers}. */
        void checkTakeover();

        /** Queues an event (with the session's quality floor check). */
        void emit(Event e);

        /** See {@link PathSession}. */
        void emitPipeState(Pipe p);

        /** Fails the session on a local protocol error. */
        void failLocal(WireException e);

        /** See {@link PathHealth}. */
        void failPipe(Pipe p, long code, Throwable cause);

        /** The session clock. */
        long now();

        /** See {@link PathHealth}. */
        void retire(Pipe p);

        /** See {@link Probation}. */
        void startProbation(Pipe p);

        /** See {@link Resume}. */
        void startReinjectHold(Pipe p);

        /** The session state. */
        SessionState state();
    }

    private final Port port;
    private final CoreConfig cfg;
    private final PipeTable pipes;
    private final Outbox outbox;
    private final PathMetrics metrics;
    private final boolean initiator;
    private final byte[] keyOwn;
    private final byte[] keyPeer;
    private final byte[] peerLocator;
    private long nextId;
    private final int cap;
    final int ownCap;
    private final long replayW;
    private long h;
    private boolean hSet;
    private final HashSet<Long> bits = new HashSet<>();
    /** PIPE_ABANDON owed for IDs without attachment state: id -> code. */
    private final HashMap<Long, Long> pendAbandon = new HashMap<>();
    private long backoffUntil;
    private int backoffExp;
    private final SessionStats stats;

    Joins(CoreConfig cfg, SessionStats stats, TransportParams peer, boolean initiator, PipeTable pipes, Outbox outbox, PathMetrics metrics, Port port) {
        this.port = port;
        this.cfg = cfg;
        this.stats = stats;
        this.pipes = pipes;
        this.outbox = outbox;
        this.metrics = metrics;
        this.initiator = initiator;
        TransportParams l = cfg.local();
        peerLocator = peer.locator;
        int sel = l.secretFlags & peer.secretFlags;
        byte[] exporter = (sel & TransportParams.SECRET_EXPORTER) != 0 && cfg.exporter() != null ? cfg.exporter() : new byte[0];
        byte[] psk = (sel & TransportParams.SECRET_PSK) != 0 && cfg.psk() != null ? cfg.psk() : new byte[0];
        byte[] initN = l.sessionNonce, respN = peer.sessionNonce;
        if (!initiator) {
            initN = peer.sessionNonce;
            respN = l.sessionNonce;
        }
        byte[][] keys = JoinCrypto.joinKeys(JoinCrypto.sessionSecret(initN, respN, exporter, psk));
        if (initiator) {
            keyOwn = keys[0];
            keyPeer = keys[1];
            nextId = 2;
        } else {
            keyOwn = keys[1];
            keyPeer = keys[0];
            nextId = 1;
        }
        int maxPipes = Math.max(1, cfg.maxPipes());
        int c = maxPipes + cfg.headroom();
        ownCap = c;
        if (cfg.test().pipeCap() > 0) c = cfg.test().pipeCap();
        cap = c;
        long rw = Math.max(64, 2L * (maxPipes + cfg.headroom()));
        if (cfg.replayWindow() > 0) rw = Math.max(cfg.replayWindow(), maxPipes + cfg.headroom());
        replayW = rw;
        if (!initiator) {
            h = 0;
            hSet = true;
        }
    }

    /** The RFC 9002 initial estimate, or the test hook's. */
    void initialRtt(Attach a, long pipe) {
        long d = 333 * Time.MILLISECOND;
        if (cfg.test().rtt() > 0) d = cfg.test().rtt();
        Long v = cfg.test().pipeRtt().get(pipe);
        if (v != null) d = v;
        a.rtt.initial(d);
    }

    boolean ownParity(long id) {
        return ((id & 1) == 1) == !initiator;
    }

    boolean idRetired(long id) {
        return bits.contains(id) || stale(id);
    }

    boolean stale(long id) {
        return hSet && id + 2 * replayW < h;
    }

    boolean inWindow(long id) {
        if (!hSet) return id < 2 * replayW;
        return !stale(id) && id <= h + 2 * replayW;
    }

    /** Moves h after a verified join and drops what slides out (MULTIPATH.md 4.3). */
    void raise(long id) {
        if (hSet && id <= h) return;
        h = id;
        hSet = true;
        bits.removeIf(this::stale);
        pendAbandon.keySet().removeIf(this::stale);
    }

    /** Options of a local join. */
    public record JoinOptions(int dir, long fateGroup, boolean oob, boolean carrierBound, byte[] binding, Long sendPriority, Boolean sendStandby, long recvPriority, boolean recvStandby, long weight, Long probation, long suspectFloor, long deadTimeout, long probeInterval, double probeJitter, CarrierInfo in, CarrierInfo out) {
        /** The defaults. */
        public static final JoinOptions DEFAULT = builder().build();

        public static Builder builder() { return new Builder(); }

        /** A builder holding these values. */
        public Builder toBuilder() {
            Builder b = new Builder();
            b.dir = dir;
            b.fateGroup = fateGroup;
            b.oob = oob;
            b.carrierBound = carrierBound;
            b.binding = binding;
            b.sendPriority = sendPriority;
            b.sendStandby = sendStandby;
            b.recvPriority = recvPriority;
            b.recvStandby = recvStandby;
            b.weight = weight;
            b.probation = probation;
            b.suspectFloor = suspectFloor;
            b.deadTimeout = deadTimeout;
            b.probeInterval = probeInterval;
            b.probeJitter = probeJitter;
            b.in = in;
            b.out = out;
            return b;
        }

        /** Collects the values of a {@link JoinOptions}; every setter returns the builder. */
        public static final class Builder {
            private int dir = 2;
            private long fateGroup;
            private boolean oob;
            private boolean carrierBound;
            private byte[] binding;
            private Long sendPriority;
            Long sendPriority() { return sendPriority; }
            private Boolean sendStandby;
            Boolean sendStandby() { return sendStandby; }
            private long recvPriority;
            long recvPriority() { return recvPriority; }
            private boolean recvStandby;
            boolean recvStandby() { return recvStandby; }
            private long weight = 1;
            long weight() { return weight; }
            private Long probation;
            Long probation() { return probation; }
            private long suspectFloor;
            private long deadTimeout;
            private long probeInterval = -1;
            private double probeJitter = Double.NaN;
            private CarrierInfo in;
            private CarrierInfo out;

            private Builder() { }

            public JoinOptions build() { return new JoinOptions(dir, fateGroup, oob, carrierBound, binding, sendPriority, sendStandby, recvPriority, recvStandby, weight, probation, suspectFloor, deadTimeout, probeInterval, probeJitter, in, out); }

            /** MULTIPATH 5.1: 0 up, 1 down, 2 bidirectional (the joiner's view). */
            public Builder dir(int v) { dir = v; return this; }

            public Builder fateGroup(long v) { fateGroup = v; return this; }

            public Builder oob(boolean v) { oob = v; return this; }

            public Builder carrierBound(boolean v) { carrierBound = v; return this; }

            public Builder binding(byte[] v) { binding = v; return this; }

            public Builder sendPriority(Long v) { sendPriority = v; return this; }

            public Builder sendStandby(Boolean v) { sendStandby = v; return this; }

            public Builder recvPriority(long v) { recvPriority = v; return this; }

            public Builder recvStandby(boolean v) { recvStandby = v; return this; }

            public Builder weight(long v) { weight = v; return this; }

            /** null: the session default; 0 disables. */
            public Builder probation(Long v) { probation = v; return this; }

            public Builder suspectFloor(long v) { suspectFloor = v; return this; }

            public Builder deadTimeout(long v) { deadTimeout = v; return this; }

            /** The pipe's probe interval k; 0: the session's. */
            public Builder probeInterval(long v) { probeInterval = v; return this; }

            /** The pipe's probe jitter; NaN: the session's. */
            public Builder probeJitter(double v) { probeJitter = v; return this; }

            public Builder in(CarrierInfo v) { in = v; return this; }

            public Builder out(CarrierInfo v) { out = v; return this; }
        }
    }

    /** Whether a held local join may go now. */
    boolean joinAllowedFor(Pipe p) {
        Attach a = p.mp();
        if (!a.joining.held) return true;
        if (port.now() < backoffUntil) return false;
        int n = 0;
        for (Pipe q : pipes.all()) if (q != p && q.mp() != null && q.mp().joinerLocal && !q.mp().health.retired() && !q.mp().joining.held) n++;
        if (n >= ownCap) return false;
        a.joining.held = false;
        a.joining.attachedAt = port.now();
        return true;
    }

    boolean joinAllowed() {
        return port.now() >= backoffUntil && ownAttachments() < ownCap;
    }

    /** Maintenance targets exclude failed attachments; headroom is for their retained state. */
    boolean joinReady() {
        if (port.now() < backoffUntil) return false;
        if (ownAttachments() >= ownCap && (port.state() != SessionState.SUSPENDED || roomCandidate(true, true, true) == null)) return false;
        int target = 0;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a == null || !a.joinerLocal || a.health.retired() || a.health.out() == Attach.OUT_FAILED
                    || a.health.out() == Attach.OUT_NONE && a.health.in() == Attach.IN_ENDED) continue;
            target++;
        }
        return target < Math.max(1, cfg.maxPipes());
    }

    int ownAttachments() {
        int n = 0;
        for (Pipe p : pipes.all()) if (p.mp() != null && p.mp().joinerLocal && !p.mp().health.retired()) n++;
        return n;
    }

    int peerAttachments() {
        int n = 0;
        for (Pipe p : pipes.all()) if (p.mp() != null && !p.mp().joinerLocal && !p.mp().health.retired()) n++;
        return n;
    }

    /** The encoded PIPE_JOIN from Type through Priority. */
    static byte[] joinMac(byte[] locator, long id, int dir, boolean carrierBound, boolean standby, long fate, long prio) {
        ByteBuffer b = ByteBuffer.allocate(64);
        FrameEncoder.joinMacInput(b, locator, id, dir, carrierBound, standby, fate, prio);
        return Arrays.copyOf(b.array(), b.position());
    }

    /** Starts a local join (MULTIPATH.md 5.7); returns the pipe ID, negated - 1 when it is held (pending). */
    public record Reservation(long id, byte[] frame) {}

    long addPipe(JoinOptions o) { return addPipeResult(o).id(); }

    Reservation reservePipe(JoinOptions o) {
        return addPipeResult(o.toBuilder().oob(true).build());
    }

    private Reservation addPipeResult(JoinOptions o) {
        if (port.state() == SessionState.SUSPENDED && ownAttachments() >= ownCap) makeRoom(true, o.dir != 0, o.dir != 1);
        long id = nextId;
        nextId += 2;
        Attach a = new Attach(true, o.dir, o.fateGroup, o.oob, o.suspectFloor, o.deadTimeout, o.probeInterval, o.probeJitter, o.dir != 1 ? Attach.OUT_JOINING : Attach.OUT_NONE,
                o.dir != 0 ? Attach.IN_JOINING : Attach.IN_NONE, o.dir != 1, port.now());
        Tiers.initLocal(a, o);
        initialRtt(a, id);
        CarrierInfo info = o.dir != 1 ? o.out : o.in;
        Pipe p = new Pipe(id, o.dir != 0 ? new Pipe.InDir(new Pipe.InLane(0, false, false, true)) : null,
                o.dir != 1 ? new Pipe.OutDir(o.out, null) : null, a, info.kind());
        p.label(info.label());
        boolean standby = o.dir != 0 && o.recvStandby;
        long prio = o.dir != 0 ? o.recvPriority : 0;
        byte[] mac = joinMac(peerLocator, id, o.dir, o.carrierBound, standby, o.fateGroup, prio);
        byte[] binding = o.carrierBound && o.binding != null ? o.binding : new byte[0];
        byte[] token = JoinCrypto.token(keyOwn, mac, binding);
        byte[] frame = Arrays.copyOf(mac, mac.length + 16);
        System.arraycopy(token, 0, frame, mac.length, 16);
        boolean pending = !joinAllowed();
        if (pending) a.joining.held = true;
        pipes.add(p);
        stats.pipeAdded();
        if (p.out() != null) port.startReinjectHold(p);
        if (o.oob) port.emit(Event.oobJoin(id, frame));
        else a.joining.join = frame;
        port.emitPipeState(p);
        if (p.out() != null) pipes.wake(p);
        return new Reservation(pending ? -id - 1 : id, frame);
    }

    /**
     * The outcome of a peer join at the acceptor: accepted, held until the peer's parameters arrive, closed without a
     * distinguishable reason, or refused at the session's limit (ZMUX_PIPE_RESOURCE_LIMIT).
     */
    public enum JoinResult { ACCEPTED, HOLD, CLOSE, REFUSED }

    /** A carrier offering a peer join: the directions it gives the acceptor. */
    public record JoinCarrier(boolean hasIn, boolean hasOut, CarrierInfo in, CarrierInfo out, Waiter outSig, boolean refuse) {}

    /** Steps 4 to 9 of MULTIPATH.md 5.4 for a PIPE_JOIN that named this session's locator. */
    JoinResult acceptJoin(io.zmux.internal.wire.Frame f, byte[] macInput, byte[] binding, JoinCarrier c) {
        io.zmux.internal.wire.Frame.Join j = f.join;
        byte[] token = new byte[16];
        f.src.get(j.tokenPos, token);
        byte[] b = j.carrierBound && binding != null ? binding : new byte[0];
        if (!JoinCrypto.verify(keyPeer, macInput, b, token)) {
            stats.joinAuthFailed();
            port.emit(Event.joinAuthFailed(j.carrierBound ? "mac_carrier_bound" : "mac"));
            return JoinResult.CLOSE;
        }
        long id = j.pipeId;
        if (ownParity(id)) {
            port.failLocal(WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "join with the wrong parity", "pipe", id));
            return JoinResult.CLOSE;
        }
        if (pipes.get(id) != null || idRetired(id)) {
            stats.joinReplay();
            return JoinResult.CLOSE;
        }
        boolean needIn = j.direction != 1, needOut = j.direction != 0;
        if (needIn && !c.hasIn() || needOut && !c.hasOut()) {
            refuseJoin(id, TransportError.PIPE_FAILED);
            return JoinResult.CLOSE;
        }
        if (c.refuse()) {
            refuseJoin(id, TransportError.PIPE_RESOURCE_LIMIT);
            return JoinResult.REFUSED;
        }
        if (peerAttachments() >= cap && (port.state() != SessionState.SUSPENDED || !makeRoom(false, needIn, needOut))) {
            refuseJoin(id, TransportError.PIPE_RESOURCE_LIMIT);
            return JoinResult.REFUSED;
        }
        bits.add(id);
        raise(id);
        Attach a = new Attach(false, j.direction, j.fateGroup, false, 0, 0, -1, Double.NaN, needOut ? Attach.OUT_ACTIVE : Attach.OUT_NONE,
                needIn ? Attach.IN_OPEN : Attach.IN_NONE, needOut, port.now());
        Tiers.initPeer(a, j.priority, j.standby);
        initialRtt(a, id);
        CarrierInfo info = needOut ? c.out() : c.in();
        Pipe p = new Pipe(id, needIn ? new Pipe.InDir(new Pipe.InLane(0, false, false, false)) : null,
                needOut ? new Pipe.OutDir(c.out(), c.outSig()) : null, a, info.kind());
        p.label(info.label());
        pipes.add(p);
        stats.pipeAdded();
        if (p.out() != null) {
            port.requestPing(p, port.now());
            port.startProbation(p);
            port.startReinjectHold(p);
        }
        port.emitPipeState(p);
        if (p.out() != null) outbox.wake(c.outSig());
        port.checkTakeover();
        port.checkResumed();
        return JoinResult.ACCEPTED;
    }

    /** Records a refused ID and owes PIPE_ABANDON (5.4 step 7). */
    void refuseJoin(long id, long code) {
        bits.add(id);
        raise(id);
        pendAbandon.put(id, code);
        pipes.wakeWriters();
    }

    /**
     * Frees a place among the unretired attachments of one joiner while suspended (RESUME.md
     * 4.3): the oldest failed attachment is retired locally; when none failed and the new
     * attachment provides a missing direction, the live attachment with the oldest last
     * progress is displaced.
     */
    Pipe roomCandidate(boolean local, boolean provideIn, boolean provideOut) {
        Pipe oldest = null;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a == null || a.health.retired() || a.joinerLocal != local) continue;
            if (a.liveOut() || a.health.in() == Attach.IN_OPEN || a.health.in() == Attach.IN_JOINING) continue;
            if (oldest == null || a.joining.attachedAt < oldest.mp().joining.attachedAt) oldest = p;
        }
        if (oldest == null) {
            int[] lc = metrics.liveCounts();
            if ((!provideOut || lc[0] != 0) && (!provideIn || lc[1] != 0)) return null;
            for (Pipe p : pipes.all()) {
                Attach a = p.mp();
                if (a == null || a.health.retired() || a.joinerLocal != local) continue;
                if (oldest == null || a.progress() < oldest.mp().progress()) oldest = p;
            }
            if (oldest == null) return null;
        }
        return oldest;
    }

    boolean makeRoom(boolean local, boolean provideIn, boolean provideOut) {
        Pipe oldest = roomCandidate(local, provideIn, provideOut);
        if (oldest == null) return false;
        Attach a = oldest.mp();
        if (a.liveOut() || a.health.in() == Attach.IN_OPEN || a.health.in() == Attach.IN_JOINING) {
            port.failWithoutAbandon(oldest, TransportError.PIPE_FAILED);
        }
        retireLocally(oldest);
        return true;
    }

    /** Retires a failed attachment without the PIPE_ABANDON exchange; an owed PIPE_ABANDON stays owed by ID. */
    void retireLocally(Pipe p) {
        Attach a = p.mp();
        if (a.health.abandonQueued() && !a.health.abandonSent()) pendAbandon.put(p.id, a.health.abandonCode());
        port.retire(p);
    }

    /** Whether id is of the local parity and not yet taken. */
    boolean untaken(long id) {
        return ownParity(id) && id >= nextId;
    }

    /** The ID the next local join takes. */
    long nextId() {
        return nextId;
    }

    /** A PIPE_ABANDON for a pipe without attachment state. */
    void abandonUnknown(long id) throws WireException {
        if (ownParity(id)) {
            if (id >= nextId) {
                throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "PIPE_ABANDON for an untaken pipe ID", "pipe", id);
            }
            return;
        }
        if (inWindow(id) && !bits.contains(id)) {
            bits.add(id);
            pendAbandon.put(id, TransportError.PIPE_FAILED);
            pipes.wakeWriters();
        }
    }

    /** Whether PIPE_ABANDON is owed for IDs without attachment state. */
    boolean owesAbandons() {
        return !pendAbandon.isEmpty();
    }

    /** Writes the PIPE_ABANDON owed by ID; false when the batch is full. */
    boolean writeOwedAbandons(RecordWriter w) {
        for (Iterator<java.util.Map.Entry<Long, Long>> it = pendAbandon.entrySet().iterator(); it.hasNext(); ) {
            var e = it.next();
            long id = e.getKey(), code = e.getValue();
            int n = VarInt.length(FrameType.PIPE_ABANDON) + VarInt.length(id) + VarInt.length(code);
            if (!w.frame(n, true, bb -> FrameEncoder.pipeAbandon(bb, id, code))) return false;
            it.remove();
        }
        return true;
    }

    /** A local join became ACTIVE: the refusal backoff restarts. */
    void confirmed() {
        backoffExp = 0;
    }

    /** The peer refused a local join for resources: the next join waits (MULTIPATH.md 5.7). */
    void refusedForResources() {
        stats.joinRefused();
        backoffExp++;
        long d = Time.SECOND << Math.min(backoffExp - 1, 6);
        backoffUntil = Time.add(port.now(), Math.min(d, 60 * Time.SECOND));
    }

    /** A peer attachment retired: its ID stays used. */
    void peerIdRetired(long id) {
        bits.add(id);
    }

    /** A local attachment retired without a resource refusal: the backoff ends. */
    void backoffCleared() {
        backoffUntil = 0;
        pipes.wakeWriters();
    }

    /** The backoff deadline; with apply an expired backoff ends. Time.NEVER when none is pending. */
    long backoffTimer(long now, boolean apply) {
        if (backoffUntil > now) return backoffUntil;
        if (backoffUntil == 0) return Time.NEVER;
        if (!apply) return backoffUntil; // Overdue: pending until applied.
        backoffUntil = 0;
        pipes.wakeWriters();
        return Time.NEVER;
    }

    /** The in-band PIPE_JOIN of the pipe was written. */
    void joinWritten(Pipe p) {
        p.mp().joining.join = null;
    }

    /** The join state of one attachment (MULTIPATH.md 5.7). */
    static final class Slot {
        private long attachedAt;
        private byte[] join;
        private boolean held;

        Slot(long now) {
            attachedAt = now;
        }

        long attachedAt() {
            return attachedAt;
        }

        byte[] join() {
            return join;
        }

        boolean held() {
            return held;
        }
    }
}
