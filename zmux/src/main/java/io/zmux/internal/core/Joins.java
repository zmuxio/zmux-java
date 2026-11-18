package io.zmux.internal.core;

import io.zmux.internal.join.Sealer;
import io.zmux.carrier.HandshakeShaper.UnitPlan;
import io.zmux.internal.join.Acceptor;
import io.zmux.internal.join.Framing;
import io.zmux.internal.join.Hello;
import io.zmux.internal.join.HelloPayload;
import io.zmux.seal.SecureCarrierException;
import io.zmux.internal.join.JoinPipe;
import io.zmux.internal.join.Joiner;
import io.zmux.internal.join.Kdf;
import io.zmux.internal.join.Mode;
import io.zmux.internal.join.ReplyPayload;
import io.zmux.internal.join.Result;
import io.zmux.internal.join.Suite;
import io.zmux.internal.join.Tlv;
import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.FrameType;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.VarInt;
import io.zmux.internal.wire.WireException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 * The Join handshake seen from the session, the join keys, the pipe ID space and its replay window, the attachment caps and the
 * refusal backoff, and PIPE_ABANDON owed by ID.
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

        /** First batch frames of unacknowledged stream data, or null. */
        byte[] firstBatch(int limit);

        /** Applies frames of a verified Hello or Reply. */
        void applyBatch(byte[] frames);

        /** A verified Reply accepted: the attachment is ACTIVE. */
        void proven(Pipe p);

        /** A verified Reply refused: retired at once. */
        void retireDeclined(Pipe p);

        /** A Join's message grew by n bytes. */
        void joinPadding(long n);

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

        /** Whether the session is closing. */
        boolean isClosing();

        /** New local joins use a later epoch. */
        void epochInUse();
    }

    private final Port port;
    private final CoreConfig cfg;
    private final PipeTable pipes;
    private final Outbox outbox;
    private final boolean initiator;
    private final long[] usable;
    /** The exporter kept while the join keys wait. */
    private byte[] heldExporter;
    private final byte[] initNonce, respNonce;
    /** {@code session_crypto}: both ends' sets, by session role. */
    private final byte[] sessionCrypto;
    /** The usable Groups, ascending. */
    private final long[] usableGroups;
    private final boolean usePsk;
    private long joinFailures;
    private boolean keysPending;
    /** The session epochs; empty while the join keys wait. */
    private final EpochSet ep;
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

    Joins(CoreConfig cfg, SessionStats stats, TransportParams peer, boolean initiator, PipeTable pipes, Outbox outbox, Port port,
          byte[] exporter, byte[] psk) {
        this.port = port;
        this.cfg = cfg;
        this.stats = stats;
        this.pipes = pipes;
        this.outbox = outbox;
        this.initiator = initiator;
        TransportParams l = cfg.local();
        ep = new EpochSet(peer.locator);
        initNonce = initiator ? l.sessionNonce : peer.sessionNonce;
        respNonce = initiator ? peer.sessionNonce : l.sessionNonce;
        var own = l.sessionCrypto;
        var other = peer.sessionCrypto;
        // Without the parameter on both ends no join key exists: inputs() refuses to derive one.
        // Both sessions' crypto parameters are set by the decoder and the local configuration, in other classes; the analyzer sees neither.
        //noinspection ConstantValue
        if (own != null && other != null) {
            sessionCrypto = io.zmux.internal.join.SessionCrypto.session(initiator ? own : other, initiator ? other : own);
            usable = io.zmux.internal.join.SessionCrypto.intersect(own.suites(), other.suites());
            usableGroups = io.zmux.internal.join.SessionCrypto.intersect(own.groups(), other.groups());
        } else {
            sessionCrypto = new byte[0];
            usable = new long[0];
            usableGroups = new long[0];
        }
        usePsk = (l.secretFlags & TransportParams.SECRET_PSK) != 0;
        nextId = initiator ? 2 : 1;
        heldExporter = (l.secretFlags & TransportParams.SECRET_EXPORTER) != 0 && exporter != null ? exporter : new byte[0];
        keysPending = true;
        int maxPipes = Math.max(1, cfg.maxPipes());
        int c = maxPipes + cfg.headroom();
        ownCap = c;
        if (cfg.test().pipeCap() > 0) c = cfg.test().pipeCap();
        cap = c;
        long rw = Math.max(64, 2L * (maxPipes + cfg.headroom()));
        if (cfg.replayWindow() > 0) rw = cfg.replayWindow(); // at least maxPipes + headroom: checked by the public configuration
        replayW = rw;
        if (!initiator) {
            h = 0;
            hSet = true;
        }
    }

    /** Derives the join keys once every input is there. */
    void inputs(boolean declared, boolean deferredWaiting, byte[] psk) throws WireException {
        if (!keysPending || !declared || deferredWaiting) return;
        Epoch e0 = new Epoch(0, Kdf.sessionSecret(initNonce, respNonce, heldExporter, usePsk ? psk : new byte[0]), cfg.local().locator, ep.peerLocator0);
        try {
            deriveKeys(e0);
        } catch (SecureCarrierException invalid) {
            e0.wipe();
            throw WireException.param("zmux_session_crypto");
        }
        ep.list.add(e0);
        ep.join = e0;
        ep.stableSince = port.now();
        java.util.Arrays.fill(heldExporter, (byte) 0);
        heldExporter = null;
        keysPending = false;
    }

    /** The session closed: an exporter still waiting for the deferred PSK and every epoch secret are cleared. */
    void clearHeldSecret() {
        for (Epoch e : ep.list) e.wipe();
        ep.list.clear();
        ep.join = null;
        ep.offer = null;
        ep.frame = null;
        if (heldExporter == null) return;
        java.util.Arrays.fill(heldExporter, (byte) 0);
        heldExporter = null;
    }

    /** Whether the join keys wait for the deferred PSK. */
    boolean keysPending() { return keysPending; }

    /** The RFC 9002 initial estimate, or the test hook's. */
    void initialRtt(Attach a, long pipe) {
        long d = 333 * Time.MILLISECOND;
        if (cfg.test().rtt() > 0) d = cfg.test().rtt();
        Long v = cfg.test().pipeRtt().get(pipe);
        if (v != null) d = v;
        a.rtt().initial(d);
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

    /** Moves h after a verified join and drops what slides out. */
    void raise(long id) {
        if (hSet && id <= h) return;
        h = id;
        hSet = true;
        bits.removeIf(this::stale);
        pendAbandon.keySet().removeIf(this::stale);
    }

    /** Options of a local join. */
    public record JoinOptions(int dir, long fateGroup, Mode mode, byte[] binding, Long sendPriority, Boolean sendStandby, long recvPriority, boolean recvStandby, long weight, Long probation, long suspectFloor, long deadTimeout, long probeInterval, double probeJitter, CarrierInfo in, CarrierInfo out, long group) {
        /** The defaults. */
        public static final JoinOptions DEFAULT = builder().build();

        public static Builder builder() { return new Builder(); }

        /** A builder holding these values. */
        public Builder toBuilder() {
            Builder b = new Builder();
            b.dir = dir;
            b.fateGroup = fateGroup;
            b.mode = mode;
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
            b.group = group;
            return b;
        }

        /** Collects the values of a {@link JoinOptions}; every setter returns the builder. */
        public static final class Builder {
            private int dir = 2;
            private long fateGroup;
            private Mode mode = Mode.SEALED;
            private byte[] binding;
            /** The Group to join with; 0: Group 1 when usable, otherwise Group 2. */
            private long group;

            public Builder group(long g) { group = g; return this; }
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

            public JoinOptions build() { return new JoinOptions(dir, fateGroup, mode, binding, sendPriority, sendStandby, recvPriority, recvStandby, weight, probation, suspectFloor, deadTimeout, probeInterval, probeJitter, in, out, group); }

            /** The direction of the pipe: 0 up, 1 down, 2 bidirectional (the joiner's view). */
            public Builder dir(int v) { dir = v; return this; }

            public Builder fateGroup(long v) { fateGroup = v; return this; }

            // Internal packages are exported only to this project's modules (qualified export), so these signatures name types that stay unexported.
            //noinspection ClassEscapesItsScope
            public Builder mode(Mode v) { mode = v; return this; }

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

    /**
     * Whether a held local join may go now. Held joins are judged one by one (MULTIPATH.md Section 5.4 step 9): at the cap, a
     * suspended joiner lets one that provides a direction it lacks displace an attachment, whatever joins were held before it.
     */
    boolean joinAllowedFor(Pipe p) {
        return joinAllowedFor(p, true);
    }

    /** {@link #joinAllowedFor(Pipe)}; without mayDisplace a join at the cap stays held rather than retire an attachment. */
    private boolean joinAllowedFor(Pipe p, boolean mayDisplace) {
        Attach a = p.mp();
        if (!a.joining.held) return true;
        if (port.now() < backoffUntil) return false;
        int n = 0;
        for (Pipe q : pipes.all()) if (q != p && q.mp() != null && q.mp().joinerLocal && !q.mp().health.retired() && !q.mp().joining.held) n++;
        if (n >= ownCap) {
            boolean in = p.in() != null, out = p.out() != null;
            if (!mayDisplace || port.state() != SessionState.SUSPENDED || !lacks(in, out) || !makeRoom(true, in, out)) return false;
        }
        a.joining.held = false;
        return true;
    }

    boolean joinAllowed() {
        return port.now() >= backoffUntil && ownAttachments() < ownCap;
    }

    /** Maintenance targets exclude failed attachments; headroom is for their retained state. */
    boolean joinReady() {
        if (keysPending || port.now() < backoffUntil) return false;
        if (ownAttachments() >= ownCap && (port.state() != SessionState.SUSPENDED || !lacks(true, true) || roomCandidate(true, true, true) == null)) return false;
        int target = 0;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a == null || !a.joinerLocal || a.health.retired() || a.health.out() == Attach.OUT_FAILED
                    || (a.health.out() == Attach.OUT_NONE && a.health.in() == Attach.IN_ENDED)) continue;
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

    /** Takes a pipe ID and enters JOINING; returns the ID, -id - 1 when the cap or the backoff holds it. */
    long addPipe(JoinOptions o) {
        if (port.state() == SessionState.SUSPENDED && ownAttachments() >= ownCap && lacks(o.dir != 0, o.dir != 1)) makeRoom(true, o.dir != 0, o.dir != 1);
        long id = nextId;
        nextId += 2;
        Attach a = new Attach(true, o.dir, o.fateGroup, o.suspectFloor, o.deadTimeout, o.probeInterval, o.probeJitter, o.dir != 1 ? Attach.OUT_JOINING : Attach.OUT_NONE,
                o.dir != 0 ? Attach.IN_JOINING : Attach.IN_NONE, o.dir != 1, port.now());
        a.startHandshake(o.mode, o.binding);
        a.group(o.group);
        Tiers.initLocal(a, o);
        initialRtt(a, id);
        CarrierInfo info = o.dir != 1 ? o.out : o.in;
        Pipe p = new Pipe(id, o.dir != 0 ? new Pipe.InDir(new Pipe.InLane(0, false, false)) : null,
                o.dir != 1 ? new Pipe.OutDir(o.out, null) : null, a, info.kind());
        p.label(info.label());
        p.variant(info.variant());
        boolean pending = !joinAllowed();
        if (pending) a.joining.held = true;
        pipes.add(p);
        stats.pipeAdded();
        port.emitPipeState(p);
        return pending ? -id - 1 : id;
    }

    /** Takes the local join's place and checks the keys: the name of its Hello's plan; null while the cap or the backoff holds it. */
    String joinPrepare(Pipe p, int earlyLimit) throws CoreException {
        Attach a = p == null ? null : p.mp();
        if (a == null || !a.joining()) throw CoreException.of(CoreException.Reason.INVALID);
        // A join that cannot go before the keys does not displace an attachment for nothing.
        if (!joinAllowedFor(p, !keysPending)) return null;
        if (keysPending) throw CoreException.joinPending(0);
        boolean early = earlyLimit > 0 && noUsablePipe(p);
        a.joining.early = early;
        return early ? "hello_early" : "hello";
    }

    /** What may go in a Hello's frames item: at most limit, and no more than reaches padTo from base, the Hello without it. */
    static int firstBatchLimit(int limit, int padTo, int base) {
        if (padTo <= 0) return limit;
        // limit is never negative: the configuration rejects a negative joinEarlyHello.
        return Math.clamp(HandshakePlans.fit(padTo - base), 0, limit);
    }

    /** What a Reply may hold: the frames, and the room left for padding, -1 when not limited. */
    record ReplyRoom(int framesLimit, int paddingRoom) {}

    /**
     * The room of a Reply of base bytes without frames or padding on a carrier that received bytes so far: before a
     * Confirmation, Sealed and Bound hold it to three times those bytes, frames first.
     */
    static ReplyRoom replyRoom(Mode mode, long received, int base, int wantFrames) {
        if (mode == Mode.TRUSTED) return new ReplyRoom(wantFrames, -1);
        long room = Math.min(3 * received - base, Sealer.MESSAGE_FLOOR - base);
        int fit = HandshakePlans.fit((int) Math.max(room, -1));
        int frames = fit == HandshakePlans.NO_ITEM ? 0 : Math.min(fit, wantFrames);
        int used = frames > 0 ? HandshakePlans.itemSize(frames) : 0;
        return new ReplyRoom(frames, (int) Math.max(room - used, 0));
    }

    private static final java.security.SecureRandom RANDOM = new java.security.SecureRandom();

    /** A key of the process that no joiner has: a Hello of an epoch not accepted opens with it and fails as auth. */
    private static final byte[] NO_EPOCH_KEY = noEpochKey();

    private static byte[] noEpochKey() {
        byte[] k = new byte[32];
        RANDOM.nextBytes(k);
        return k;
    }

    /** The Group of a join: the one asked for when usable, otherwise Group 1 when usable, otherwise Group 2. */
    private io.zmux.internal.join.Group joinGroup(long asked) {
        boolean one = false;
        for (long g : usableGroups) {
            if (g == asked && asked != 0) return io.zmux.internal.join.Group.of(asked);
            one |= g == io.zmux.internal.join.Group.X25519.id();
        }
        return one ? io.zmux.internal.join.Group.X25519 : io.zmux.internal.join.Group.X25519_MLKEM768;
    }

    /** The Hello of a prepared join, with a first batch while no other pipe is usable. overhead: what the carrier adds to a write. */
    byte[] joinHello(Pipe p, Framing framing, int earlyLimit, UnitPlan plan, int overhead) throws SecureCarrierException, CoreException {
        Attach a = p == null ? null : p.mp();
        // No epoch once the session closed and cleared its secrets.
        Epoch e = ep.join;
        if (a == null || !a.joining() || a.joining.held() || keysPending || e == null) throw CoreException.of(CoreException.Reason.INVALID);
        Suite suite = Suite.preferred(usable);
        // No common Suite leaves nothing to join with.
        if (suite == null) throw CoreException.of(CoreException.Reason.INVALID);
        byte[][] k = e.keysOf(suite);
        // The epoch new joins use, with the Locator the peer announced for it.
        io.zmux.internal.join.Group used = joinGroup(a.group());
        a.usedGroup(used.id());
        Joiner j = Joiner.create(suite, a.mode(), framing, e.peer, k[initiator ? 0 : 1], a.binding(), used, (int) (e.n & 1));
        boolean up = a.dir == 0;
        JoinPipe pipe = new JoinPipe(p.id, a.dir, a.fate, up ? 0 : a.tier.recvPrio(), !up && a.tier.recvStandby());
        HelloPayload empty = new HelloPayload(pipe, null, 0);
        int limit = a.joining.early ? firstBatchLimit(earlyLimit, plan.padTo(), j.helloLength(empty) + overhead) : 0;
        // The frames item stays within what the Hello's ciphertext holds.
        // and within the handshake message floor, which every acceptor takes.
        if (limit > 0) limit = Math.clamp(HandshakePlans.fit(Sealer.MESSAGE_FLOOR - j.helloLength(empty)), 0, limit);
        byte[] batch = limit > 0 ? port.firstBatch(limit) : null;
        HelloPayload bare = new HelloPayload(pipe, batch, 0);
        int item = HandshakePlans.item(plan, j.helloLength(bare) + overhead, Sealer.MESSAGE_FLOOR - j.helloLength(bare));
        byte[] hello = j.writeHello(new HelloPayload(pipe, batch, Tlv.paddingField(item)));
        if (item != HandshakePlans.NO_ITEM) port.joinPadding(HandshakePlans.itemSize(item));
        a.joiner(j, framing);
        return hello;
    }

    private boolean noUsablePipe(Pipe p) {
        for (Pipe q : pipes.all()) {
            Attach a = q.mp();
            if (q != p && a != null && q.out() != null && !a.handshaking() && a.health.out() == Attach.OUT_ACTIVE && !a.health.retired()) return false;
        }
        return true;
    }

    /** What {@link #verifyReply} concluded. */
    public enum Step { MORE, PROVEN, DECLINED, FAILED }

    /** One result of {@link #verifyReply}: bytes consumed and the total length that can progress. */
    public record JoinStep(Step status, int consumed, int need) {}

    /** What {@link #joinFinish} made: the confirmation to write and the record keys. */
    public record Finished(byte[] confirm, Result result) {}

    /** The bound of what a joiner reads before a verified Reply: never max_record_size. */
    private static final int JOIN_READ_BOUND = 2 * 65535 + 64;

    /** Feeds bytes read from a JOINING carrier; nothing of them reaches the session before they verify. PROVEN changes no state of the pipe. */
    JoinStep verifyReply(Pipe p, byte[] b, int off, int len) {
        Attach a = p.mp();
        if (a == null || !a.joining() || a.joiner() == null || a.verified() != null) return new JoinStep(Step.FAILED, 0, 0);
        if (len > JOIN_READ_BOUND) return failedStep(p, new SecureCarrierException(SecureCarrierException.Kind.TOO_LARGE));
        int skip = 0;
        if (len > 0 && b[off] != 0x03) {
            if (a.tpSkipped()) return failedStep(p, new SecureCarrierException(SecureCarrierException.Kind.MALFORMED));
            if (a.joinFraming() == Framing.MESSAGE) {
                if (!startsWithParameters(b, off, len)) return failedStep(p, new SecureCarrierException(SecureCarrierException.Kind.MALFORMED));
                a.tpSkipped(true);
                return new JoinStep(Step.MORE, len, 0);
            }
            var cl = CarrierAdmission.classify(java.nio.ByteBuffer.wrap(b), off, off + len, 65535);
            if (cl.kind() == CarrierAdmission.Kind.MORE || (cl.kind() == CarrierAdmission.Kind.SESSION && cl.firstRecordLength() == 0)) {
                return new JoinStep(Step.MORE, 0, len + 1);
            }
            if (cl.kind() != CarrierAdmission.Kind.SESSION) return failedStep(p, new SecureCarrierException(SecureCarrierException.Kind.MALFORMED));
            a.tpSkipped(true);
            skip = cl.firstRecordLength();
            off += skip;
            len -= skip;
        }
        Joiner j = a.joiner();
        ReplyPayload reply;
        int used;
        try {
            reply = j.readReply(b, off, len);
            used = j.replyConsumed();
        } catch (SecureCarrierException e) {
            if (e.kind() == SecureCarrierException.Kind.INCOMPLETE) return new JoinStep(Step.MORE, skip, e.need());
            return failedStep(p, e);
        }
        joinFailures = 0;
        stats.joinFailures(0);
        if (reply.decision() == 1) {
            refusedForResources();
            port.retireDeclined(p);
            return new JoinStep(Step.DECLINED, skip + used, 0);
        }
        a.verified(reply);
        return new JoinStep(Step.PROVEN, skip + used, 0);
    }

    /**
     * The verified join goes active: its confirmation, its record keys and the Reply's frames. Null when the pipe failed meanwhile.
     * overhead: what the carrier adds to a write.
     */
    Finished joinFinish(Pipe p, UnitPlan plan, int overhead) {
        Attach a = p == null ? null : p.mp();
        if (a == null || !a.joining() || a.verified() == null) return null;
        ReplyPayload reply = a.verified();
        Joiner j = a.joiner();
        byte[] confirm;
        Result result;
        try {
            int fill = HandshakePlans.fill(plan, j.confirmLength() + overhead, j.confirmRoom());
            confirm = j.writeConfirm(fill);
            if (fill > 0) port.joinPadding(fill);
            result = j.finish();
        } catch (SecureCarrierException e) {
            failedStep(p, e);
            return null;
        }
        a.endHandshake();
        port.proven(p);
        port.applyBatch(reply.frames());
        return new Finished(confirm, result);
    }

    private JoinStep failedStep(Pipe p, SecureCarrierException cause) {
        port.failPipe(p, TransportError.PIPE_FAILED, cause);
        return new JoinStep(Step.FAILED, 0, 0);
    }

    private static boolean startsWithParameters(byte[] b, int off, int len) {
        byte[] prefix = io.zmux.internal.wire.Records.TRANSPORT_PARAMETERS_PREFIX;
        if (len < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) if (b[off + i] != prefix[i]) return false;
        return true;
    }

    /** The outcome of a peer join at the acceptor. */
    public enum JoinResult { ACCEPTED, HOLD, KEY_WAIT, CLOSE, REFUSED }

    /** The carrier of a peer join: the directions it gives the acceptor, its properties, and whether it would loop. */
    public record JoinCarrier(boolean hasIn, boolean hasOut, CarrierInfo in, CarrierInfo out, Waiter outSig, boolean refuse) {}

    /** An authenticated Hello: holds no session state until Confirmed. */
    public static final class PeerJoin {
        private final JoinPipe pipe;
        private final Mode mode;
        private final Acceptor acceptor;
        private final HelloPayload payload;

        private PeerJoin(JoinPipe pipe, Mode mode, Acceptor acceptor, HelloPayload payload) {
            this.pipe = pipe;
            this.mode = mode;
            this.acceptor = acceptor;
            this.payload = payload;
        }

        //noinspection ClassEscapesItsScope
        public JoinPipe pipe() { return pipe; }
        //noinspection ClassEscapesItsScope
        public Mode mode() { return mode; }

        /** Reads the confirmation; the bytes taken. In Bound there is none. INCOMPLETE carries the need. */
        public int readConfirm(byte[] b, int off, int len) throws SecureCarrierException {
            return mode == Mode.BOUND ? 0 : acceptor.readConfirm(b, off, len);
        }

        //noinspection ClassEscapesItsScope
        public Result result() throws SecureCarrierException { return acceptor.finish(); }
    }

    /**
     * The outcome of {@link #openHello}; reason is the local reject reason of a refused Hello,
     * {@code auth} unless one is finer.
     */
    public record Opened(JoinResult result, PeerJoin join, String reason) {
        public Opened(JoinResult result, PeerJoin join) { this(result, join, "auth"); }
    }

    /** A Hello in Bound refused because its carrier has no Binding. */
    void helloWithoutBinding() {
        stats.joinAuthFailed();
        port.emit(Event.joinAuthFailed("auth"));
    }

    /** The acceptor's checks of a Hello that named this session. */
    Opened openHello(Hello h, Framing f, byte[] binding) {
        if (keysPending) {
            stats.joinPending();
            return new Opened(JoinResult.KEY_WAIT, null);
        }
        // A Suite outside the session's set, refused before any AEAD.
        if (!usableSuite(h.suite().id())) return new Opened(JoinResult.CLOSE, null, "unknown_suite");
        Epoch e = ep.byPhase(h.phase());
        Acceptor acceptor;
        try {
            // The key of the joiner's role, which is the peer's, never inferred from the ID.
            // The epoch by the key phase; none matching opens with a key no joiner has, failing as auth.
            byte[] psk = e != null ? e.keysOf(h.suite())[initiator ? 1 : 0] : NO_EPOCH_KEY;
            acceptor = Acceptor.accept(h, f, psk, binding, usableGroups);
        } catch (SecureCarrierException failed) {
            stats.joinAuthFailed();
            port.emit(Event.joinAuthFailed(failed.kind().name().toLowerCase(java.util.Locale.ROOT)));
            return new Opened(JoinResult.CLOSE, null);
        }
        if (e != null) epochPeerJoined(e.n); // a verified join of d+1: A uses d+1 too
        HelloPayload payload = acceptor.payload();
        long id = payload.pipe().id();
        if (ownParity(id)) {
            port.failLocal(WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "join with the wrong parity", "pipe", id));
            return new Opened(JoinResult.CLOSE, null);
        }
        if (pipes.get(id) != null || idRetired(id)) {
            stats.joinReplay();
            return new Opened(JoinResult.CLOSE, null);
        }
        return new Opened(JoinResult.ACCEPTED, new PeerJoin(payload.pipe(), h.mode(), acceptor, payload));
    }

    /**
     * The reply of a claimed Hello, and whether it refuses; evicted, when not null, is the session's oldest
     * Unconfirmed join, which the caller closes as a failed join (MULTIPATH.md 5.4 step 7) before it writes the Reply.
     */
    public record Claimed(JoinResult result, byte[] reply, PeerJoin evicted) {
        public Claimed(JoinResult result, byte[] reply) { this(result, reply, null); }
    }

    /** The session's Unconfirmed peer joins, oldest first (MULTIPATH.md 5.6): a Bound join never holds one. */
    private final java.util.ArrayDeque<PeerJoin> unconfirmed = new java.util.ArrayDeque<>();

    /**
     * Admits an accepted join as Unconfirmed within cap: at the bound, the oldest is taken out and returned
     * for the caller to close; below it nothing is evicted.
     */
    private PeerJoin holdUnconfirmed(PeerJoin pj, int cap) {
        if (pj.mode == Mode.BOUND) return null;
        PeerJoin evicted = unconfirmed.size() >= cap ? unconfirmed.pollFirst() : null;
        unconfirmed.addLast(pj);
        return evicted;
    }

    /** An Unconfirmed join ended without its confirmation (closed, timed out, failed): it holds no slot any more. */
    void unconfirmedEnded(PeerJoin pj) { unconfirmed.remove(pj); }

    /** The rest of step 7: sets the Pipe ID's bit, applies the first batch, writes the Reply. Moves no pipe, cap or suspension. */
    Claimed claimHello(PeerJoin pj, JoinCarrier c, UnitPlan plan, int overhead, long received, int unconfirmedCap) {
        long id = pj.pipe.id();
        if (pipes.get(id) != null || idRetired(id)) {
            stats.joinReplay();
            return new Claimed(JoinResult.CLOSE, null);
        }
        bits.add(id);
        raise(id);
        try {
            pj.acceptor.exchange(); // after the bit: a refused share is kex, and the bit stays set
        } catch (SecureCarrierException kex) {
            stats.joinAuthFailed();
            port.emit(Event.joinAuthFailed("kex"));
            return new Claimed(JoinResult.CLOSE, null);
        }
        port.applyBatch(pj.payload.firstBatch());
        boolean needIn = pj.pipe.direction() != 1, needOut = pj.pipe.direction() != 0;
        boolean refuse = (needIn && !c.hasIn()) || (needOut && !c.hasOut()) || c.refuse() || !capRoom(needIn, needOut);
        try {
            ReplyPayload bare = new ReplyPayload(refuse ? 1 : 0, null, 0);
            int wire = pj.acceptor.replyLength(bare) + overhead;
            int room = Sealer.MESSAGE_FLOOR - pj.acceptor.replyLength(bare);
            int limited = replyRoom(pj.mode, received, wire, 0).paddingRoom();
            int item = HandshakePlans.item(plan, wire, limited < 0 ? room : Math.min(room, limited));
            byte[] reply = pj.acceptor.writeReply(new ReplyPayload(bare.decision(), null, Tlv.paddingField(item)));
            if (item != HandshakePlans.NO_ITEM) port.joinPadding(HandshakePlans.itemSize(item));
            return refuse ? new Claimed(JoinResult.REFUSED, reply) : new Claimed(JoinResult.ACCEPTED, reply, holdUnconfirmed(pj, unconfirmedCap));
        } catch (SecureCarrierException failed) {
            return new Claimed(JoinResult.CLOSE, null);
        }
    }

    private boolean capRoom(boolean needIn, boolean needOut) {
        if (peerAttachments() < cap) return true;
        return port.state() == SessionState.SUSPENDED && roomCandidate(false, needIn, needOut) != null;
    }

    /** Admission of a confirmed peer join: the only place where a peer join moves a pipe, the cap or a suspension. */
    JoinResult confirmJoin(PeerJoin pj, JoinCarrier c) {
        unconfirmed.remove(pj);
        JoinPipe j = pj.pipe;
        long id = j.id();
        boolean needIn = j.direction() != 1, needOut = j.direction() != 0;
        if (c.refuse() || (peerAttachments() >= cap && (port.state() != SessionState.SUSPENDED || !makeRoom(false, needIn, needOut)))) {
            pendAbandon.put(id, TransportError.PIPE_RESOURCE_LIMIT);
            pipes.wakeWriters();
            return JoinResult.REFUSED;
        }
        Attach a = new Attach(false, j.direction(), j.fateGroup(), 0, 0, -1, Double.NaN, needOut ? Attach.OUT_ACTIVE : Attach.OUT_NONE,
                needIn ? Attach.IN_OPEN : Attach.IN_NONE, needOut, port.now());
        a.mode(pj.mode);
        a.usedGroup(pj.acceptor.joinerShare().group().id());
        Tiers.initPeer(a, j.priority(), j.standby());
        initialRtt(a, id);
        CarrierInfo info = needOut ? c.out() : c.in();
        Pipe p = new Pipe(id, needIn ? new Pipe.InDir(new Pipe.InLane(0, false, false)) : null,
                needOut ? new Pipe.OutDir(c.out(), c.outSig()) : null, a, info.kind());
        p.label(info.label());
        p.variant(info.variant());
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
        ep.joins++;
        epochPipeAdded(pj.mode);
        return JoinResult.ACCEPTED;
    }

    /** The weakest record mode; firstMode is pipe 0's, which counts for life. */
    Mode weakest(Mode firstMode) {
        Mode m = firstMode;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a != null && !a.health.retired() && !a.handshaking() && a.mode() != null && a.mode().wire() > m.wire()) m = a.mode();
        }
        return m;
    }

    /** A carrier's join failed: counted. */
    void joinFailed() {
        joinFailures++;
        stats.joinFailures(joinFailures);
    }

    /** Whether a Suite is in both sets and has its keys. */
    boolean usableSuite(long id) {
        return !ep.list.isEmpty() && ep.list.getFirst().keys.containsKey(id);
    }

    /**
     * The attachment the cap displaces among the unretired ones of one joiner (local: this endpoint's own) while suspended, the
     * one choice of MULTIPATH.md Section 5.4 step 9 for the acceptor, the joiner and Section 8.4: the oldest failed attachment whose
     * own PIPE_ABANDON was acknowledged, else the oldest failed one whose PIPE_ABANDON was not; only when none has failed and the
     * new attachment provides a missing direction (provideIn: the local endpoint reads on it, provideOut: it writes), the live
     * attachment with the oldest last progress. Held local joins are never chosen; null when nothing may be displaced.
     */
    Pipe roomCandidate(boolean local, boolean provideIn, boolean provideOut) {
        Pipe acked = null, pending = null;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a == null || a.health.retired() || a.joinerLocal != local || a.joining.held) continue;
            if (a.liveOut() || a.health.in() == Attach.IN_OPEN || a.health.in() == Attach.IN_JOINING) continue;
            if (a.health.abandonAcked()) {
                if (acked == null || a.joining.attachedAt < acked.mp().joining.attachedAt) acked = p;
            } else if (pending == null || a.joining.attachedAt < pending.mp().joining.attachedAt) {
                pending = p;
            }
        }
        if (acked != null) return acked;
        if (pending != null) return pending;
        if (!lacks(provideIn, provideOut)) return null;
        Pipe oldest = null;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a == null || a.health.retired() || a.joinerLocal != local || a.joining.held) continue;
            if (oldest == null || a.progress() < oldest.mp().progress()) oldest = p;
        }
        return oldest;
    }

    /**
     * Whether an attachment providing these directions fills one the endpoint lacks: no unretired attachment has a live outbound
     * (provideOut) or inbound (provideIn) direction. Held local joins are not live: they have not started.
     */
    boolean lacks(boolean provideIn, boolean provideOut) {
        boolean out = false, in = false;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a == null || a.health.retired() || a.joining.held) continue;
            out |= p.out() != null && a.liveOut();
            in |= p.in() != null && (a.health.in() == Attach.IN_JOINING || a.health.in() == Attach.IN_OPEN);
        }
        return (provideOut && !out) || (provideIn && !in);
    }

    boolean makeRoom(boolean local, boolean provideIn, boolean provideOut) {
        Pipe oldest = roomCandidate(local, provideIn, provideOut);
        if (oldest == null) return false;
        Attach a = oldest.mp();
        if (a.liveOut() || a.health.in() == Attach.IN_OPEN || a.health.in() == Attach.IN_JOINING) {
            port.failWithoutAbandon(oldest, TransportError.PIPE_FAILED);
        }
        a.health.dropQueuedAbandon(); // MULTIPATH.md Section 5.4: a displaced attachment's pending PIPE_ABANDON is dropped
        port.retire(oldest);
        return true;
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
        ep.joins++;
    }

    /** The peer refused a local join for resources: the next join waits. */
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

    // ---- session epochs ----

    /** One epoch an endpoint accepts: its secret, both join_psk values per usable Suite, and both ends' Locators. */
    private static final class Epoch {
        final long n;
        private byte[] secret;
        /** Per usable Suite: {initiator's, responder's}. */
        final HashMap<Long, byte[][]> keys = new HashMap<>();
        /** The local Locator joins of this epoch name, and the peer's. */
        final byte[] local, peer;

        Epoch(long n, byte[] secret, byte[] local, byte[] peer) {
            this.n = n;
            this.secret = secret;
            this.local = local;
            this.peer = peer;
        }

        /** The keys of a usable Suite: an epoch derives them for every Suite of the session's set (deriveKeys). */
        byte[][] keysOf(Suite s) {
            return java.util.Objects.requireNonNull(keys.get((long) s.id()), "an epoch has the keys of every usable Suite");
        }

        void wipe() {
            if (secret != null) java.util.Arrays.fill(secret, (byte) 0);
            secret = null;
            for (byte[][] k : keys.values()) {
                java.util.Arrays.fill(k[0], (byte) 0);
                java.util.Arrays.fill(k[1], (byte) 0);
            }
            keys.clear();
        }
    }

    /** Where the exchange of epoch d+1 stands. */
    private enum EpochState {
        STABLE,
        /** O: offer(d+1) sent. */
        OFFERING,
        /** A: d+1 committed and answered; new joins still use d. */
        ANSWERED,
        /** Both: new joins use d+1; d is deleted at dropAt. */
        RETAINING
    }

    /** A record carrying the answer of epoch n ends at pos on lane. */
    private record AnswerSent(Pipe.OutLane lane, long pos, long n) {}

    private static final int OFFER = 0, ANSWER = 1;

    /**
     * The epoch state of the session: at most one exchange, one set of private values, two
     * epoch secrets and two local Locators at a time.
     */
    private static final class EpochSet {
        /** The peer's Locator of epoch 0, from its parameters. */
        final byte[] peerLocator0;
        /** The epoch last completed. */
        private long d;
        private EpochState st = EpochState.STABLE;
        /** The accepted epochs, ascending; the one new local joins use. */
        final java.util.ArrayList<Epoch> list = new java.util.ArrayList<>(2);
        private Epoch join;
        /** The private values and the body of an offer; the body of the frame repeated until the next phase. */
        private io.zmux.internal.join.KeyShare.Initiator offer;
        private byte[] offerBody, frame;
        /** The last phase of d+1 processed or sent, -1 for none. */
        private int last = -1;
        /** The local Locator announced for d+1, empty to keep. */
        private byte[] nextLocal;
        private long dropAt;
        /** An exchange asked for, the start of Stable and the joins since. */
        private boolean advance;
        /** Whether a Sealed or Bound pipe has existed yet. */
        private boolean securePipeSeen;
        private long stableSince, joins;
        final java.util.ArrayList<AnswerSent> answers = new java.util.ArrayList<>(0);

        EpochSet(byte[] peerLocator0) {
            this.peerLocator0 = peerLocator0;
        }

        /** The accepted epoch of a key phase; at most one matches. */
        Epoch byPhase(int phase) {
            for (Epoch e : list) if ((e.n & 1) == phase) return e;
            return null;
        }
    }

    private void deriveKeys(Epoch e) throws SecureCarrierException {
        for (long id : usable) {
            Suite suite = Suite.lookup(id);
            if (suite == null) continue;
            e.keys.put(id, new byte[][] {Kdf.joinPsk(suite, e.secret, true, sessionCrypto), Kdf.joinPsk(suite, e.secret, false, sessionCrypto)});
        }
    }

    /** The epoch new local joins use; 0 before the join keys. */
    long epoch() {
        return ep.join == null ? 0 : ep.join.n;
    }

    /** Whether a Hello naming loc is for this session: a Locator of an accepted epoch. */
    boolean acceptsLocator(byte[] loc) {
        if (ep.list.isEmpty()) return java.util.Arrays.equals(loc, cfg.local().locator);
        for (Epoch e : ep.list) if (java.util.Arrays.equals(e.local, loc)) return true;
        return false;
    }

    /** Starts an exchange now when Stable, or right after the one in progress. */
    void advanceEpoch() {
        ep.advance = true;
        epochProgress();
    }

    /**
     * A pipe of mode m now exists. The first Sealed or Bound one starts an exchange in a session whose
     * secret comes from a TLS exporter or a pre-shared key, not from an Establish on its first pipe.
     */
    void epochPipeAdded(Mode m) {
        if (ep.securePipeSeen || (m != Mode.SEALED && m != Mode.BOUND)) return;
        ep.securePipeSeen = true;
        if (cfg.epochs().afterFirstPipe() && cfg.firstPipeMode() == null) advanceEpoch();
    }

    /** The session left a suspension: once, when Stable. */
    void epochResumed() {
        if (cfg.epochs().afterResume() && ep.st == EpochState.STABLE) advanceEpoch();
    }

    /** Whether SESSION_EPOCH may go on p: a Sealed or Bound pipe. */
    private static boolean epochPipe(Pipe p) {
        Attach a = p.mp();
        return a != null && (a.mode() == Mode.SEALED || a.mode() == Mode.BOUND);
    }

    /**
     * The local Locator of epoch n: the instance prefix and at least 8 random bytes, as long as
     * the current one, or the test hook's prefix and n in decimal. Empty for an end that is not
     * joined: it keeps having none.
     */
    private byte[] newLocator(long n) {
        byte[] cur = cfg.local().locator;
        if (cur == null || cur.length == 0) return new byte[0];
        byte[] t = cfg.test().epochLocatorPrefix();
        if (t != null) return Kdf.concat(t, Long.toString(n).getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        byte[] pre = cfg.epochs().locatorPrefix();
        byte[] b = new byte[Math.max(cur.length, pre.length + 8)];
        System.arraycopy(pre, 0, b, 0, pre.length);
        byte[] r = new byte[b.length - pre.length];
        RANDOM.nextBytes(r);
        System.arraycopy(r, 0, b, pre.length, r.length);
        return b;
    }


    private void setFrame(byte[] body) {
        ep.frame = body;
        for (Pipe p : pipes.all()) if (p.mp() != null) p.mp().joining.epochSent = false;
        pipes.wakeWriters();
    }

    /** Writes offer(d+1) with the share of the usable Group of highest priority. */
    private void startOffer() throws SecureCarrierException {
        var g = io.zmux.internal.join.Group.highest(usableGroups);
        var o = io.zmux.internal.join.KeyShare.initiate(g, null, null);
        ep.nextLocal = newLocator(ep.d + 1);
        byte[] body = FrameEncoder.sessionEpochBody(OFFER, ep.d + 1, o.share().share(), ep.nextLocal);
        ep.offer = o;
        ep.offerBody = body;
        ep.st = EpochState.OFFERING;
        ep.last = -1;
        ep.advance = false;
        setFrame(body);
    }

    /** Derives and accepts epoch d+1 from the two bodies and the shared secrets. */
    private void commit(byte[] offerBody, byte[] answerBody, byte[][] ss, byte[] localLoc, byte[] peerLoc) throws WireException {
        Epoch cur = ep.list.getLast();
        long n = ep.d + 1;
        byte[] secret = Kdf.nextEpochSecret(cur.secret, n, sessionCrypto, offerBody, answerBody, ss);
        for (byte[] x : ss) java.util.Arrays.fill(x, (byte) 0);
        Epoch e = new Epoch(n, secret, localLoc.length > 0 ? localLoc : cur.local, peerLoc.length > 0 ? peerLoc : cur.peer);
        try {
            deriveKeys(e);
        } catch (SecureCarrierException invalid) {
            e.wipe();
            throw new WireException(TransportError.INTERNAL_ERROR, "epoch keys: " + invalid.getMessage());
        }
        ep.list.add(e);
        port.emit(Event.epoch(Event.Kind.EPOCH_COMMITTED, n));
        if (!java.util.Arrays.equals(e.local, cur.local)) port.emit(Event.locator(Event.Kind.LOCATOR_ADDED, e.local.clone()));
    }

    /** Deletes epoch d, its keys and its Locators, and enters Stable(d+1). */
    private void deleteOld() {
        if (ep.list.size() >= 2) {
            Epoch old = ep.list.removeFirst();
            ep.join = ep.list.getFirst();
            if (!java.util.Arrays.equals(old.local, ep.join.local)) port.emit(Event.locator(Event.Kind.LOCATOR_REMOVED, old.local.clone()));
            old.wipe();
            port.emit(Event.epoch(Event.Kind.EPOCH_DELETED, old.n));
        }
        ep.d++;
        ep.st = EpochState.STABLE;
        ep.last = -1;
        ep.frame = ep.offerBody = ep.nextLocal = null;
        ep.offer = null;
        ep.answers.clear();
        ep.stableSince = port.now();
        ep.joins = 0;
    }

    /** New joins use d+1, and the retention of d starts. */
    private void useNext() {
        ep.join = ep.list.getLast();
        ep.st = EpochState.RETAINING;
        ep.dropAt = Time.add(port.now(), CoreConfig.EPOCH_RETENTION);
        port.epochInUse();
    }

    /** A verified join of epoch n from the peer: A's switch. */
    private void epochPeerJoined(long n) {
        if (ep.st == EpochState.ANSWERED && n == ep.d + 1) useNext();
    }

    /** The steps that wait for time: the deletion of d after the retention, and an asked or due exchange once Stable. */
    void epochProgress() {
        if (keysPending || ep.list.isEmpty() || port.isClosing()) return;
        long now = port.now();
        if (ep.st == EpochState.RETAINING && now >= ep.dropAt) deleteOld();
        if (ep.st == EpochState.STABLE && (ep.advance || now >= Time.add(ep.stableSince, cfg.epochs().period()) || ep.joins >= cfg.epochs().joins())) {
            try {
                startOffer();
            } catch (SecureCarrierException e) {
                port.failLocal(new WireException(TransportError.INTERNAL_ERROR, "epoch offer: " + e.getMessage()));
            }
        }
    }

    /** A SESSION_EPOCH received on p. */
    void onSessionEpoch(Pipe p, io.zmux.internal.wire.Frame f) throws WireException {
        if (keysPending || ep.list.isEmpty()) return; // no session secret yet
        if (!epochPipe(p)) {
            throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "SESSION_EPOCH on a pipe that is neither Sealed nor Bound", "pipe", p.id);
        }
        long e = f.value;
        int phase = f.phase;
        if (e <= ep.d) return;
        if (e == ep.d + 2 && phase == OFFER && endsExchangeFor()) {
            onSessionEpoch(p, f);
            return;
        }
        if (e >= ep.d + 2) throw violation("epoch too far ahead", e);
        if (phase <= ep.last) return; // a copy
        var g = io.zmux.internal.join.Group.highest(usableGroups);
        if (phase == OFFER && ep.st == EpochState.OFFERING) {
            if (initiator) return; // collision: the initiator's offer stands
            // The responder yields: its offer and private values are dropped.
            ep.offer = null;
            ep.offerBody = null;
            ep.st = EpochState.STABLE;
            answerOffer(f, g);
        } else if (phase == OFFER && ep.st == EpochState.STABLE) {
            answerOffer(f, g);
        } else if (phase == ANSWER && ep.st == EpochState.OFFERING) {
            if (f.dataLen != g.shareLength(false)) throw violation("share length", e);
            byte[][] ss;
            try {
                ss = ep.offer.finish(f.dataBytes());
            } catch (SecureCarrierException refused) {
                throw violation("share refused", e);
            } finally {
                ep.offer = null;
            }
            commit(ep.offerBody, f.bodyBytes(), ss, ep.nextLocal, f.locatorBytes());
            // New joins use d+1; d stays accepted for the retention.
            ep.last = phase;
            ep.frame = null;
            useNext();
        } else {
            throw violation("in this state", e);
        }
    }

    /**
     * Rule 5: an offer(d+2) while d is still accepted shows that the peer committed d+1, so this end uses d+1, deletes d
     * and processes the offer as Stable(d+1). Whether it did.
     */
    private boolean endsExchangeFor() {
        if (ep.st == EpochState.STABLE || ep.st == EpochState.OFFERING) return false;
        if (ep.st == EpochState.ANSWERED) useNext();
        deleteOld();
        return true;
    }

    private static WireException violation(String why, long epoch) {
        return WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "SESSION_EPOCH " + why, "epoch", epoch);
    }

    /** Commits d+1 from the peer's offer and writes the answer. */
    private void answerOffer(io.zmux.internal.wire.Frame f, io.zmux.internal.join.Group g) throws WireException {
        if (f.dataLen != g.shareLength(true)) throw violation("share length", f.value);
        io.zmux.internal.join.KeyShare.Response r;
        try {
            r = io.zmux.internal.join.KeyShare.respond(new io.zmux.internal.join.KeyShare(g, f.dataBytes()), null, null);
        } catch (SecureCarrierException refused) {
            throw violation("share refused", f.value);
        }
        byte[] loc = newLocator(ep.d + 1);
        byte[] answer = FrameEncoder.sessionEpochBody(ANSWER, ep.d + 1, r.share().share(), loc);
        commit(f.bodyBytes(), answer, r.secrets(), loc, f.locatorBytes());
        ep.st = EpochState.ANSWERED;
        ep.last = ANSWER;
        setFrame(answer);
    }

    /** Whether the current SESSION_EPOCH is owed on p. */
    boolean epochWanted(Pipe p) {
        Attach a = p.mp();
        return ep.frame != null && !a.joining.epochSent && a.health.out() == Attach.OUT_ACTIVE && epochPipe(p);
    }

    /** Puts the current SESSION_EPOCH on p once per repeat interval; a record carrying an answer is tracked for its acknowledgement. */
    void writeEpoch(RecordWriter w, Pipe p, long repeat) {
        if (!epochWanted(p)) return;
        byte[] body = ep.frame;
        if (!w.frame(VarInt.length(FrameType.SESSION_EPOCH) + body.length, true, b -> FrameEncoder.sessionEpoch(b, body))) return;
        Attach a = p.mp();
        a.joining.epochSent = true;
        a.joining.epochAt = Time.add(port.now(), repeat);
        if (ep.last == ANSWER) w.epochAnswer(ep.d + 1);
    }

    /** A batch carrying the answer of epoch n ended at pos on ol. */
    void answerSent(Pipe.OutLane ol, long pos, long n) {
        ep.answers.add(new AnswerSent(ol, pos, n));
    }

    /** ol is acknowledged up to pos: an answer it carried is no longer repeated, and A switches. */
    void laneAcked(Pipe.OutLane ol, long pos) {
        if (ep.answers.isEmpty()) return;
        for (Iterator<AnswerSent> it = ep.answers.iterator(); it.hasNext(); ) {
            AnswerSent a = it.next();
            if (a.lane() != ol || a.pos() > pos) continue;
            it.remove();
            if (a.n() != ep.d + 1 || ep.last != ANSWER) continue;
            if (ep.st == EpochState.ANSWERED || ep.st == EpochState.RETAINING) ep.frame = null;
            if (ep.st == EpochState.ANSWERED) useNext();
        }
    }

    /** The epoch deadlines: the repeat of each pipe, the retention and the policy; with apply the due ones act. */
    long epochTimers(long now, boolean apply) {
        if (keysPending || ep.list.isEmpty()) return Time.NEVER;
        long n = Time.NEVER;
        if (ep.frame != null) {
            for (Pipe p : pipes.all()) {
                Attach a = p.mp();
                if (a == null || !a.joining.epochSent || !epochPipe(p)) continue;
                if (a.joining.epochAt > now || !apply) {
                    n = Math.min(n, a.joining.epochAt);
                } else {
                    a.joining.epochSent = false;
                    pipes.wakeWriters();
                }
            }
        }
        long due = switch (ep.st) {
            case RETAINING -> ep.dropAt;
            case STABLE -> Time.add(ep.stableSince, cfg.epochs().period());
            default -> Time.NEVER;
        };
        if (due > now || !apply) return Math.min(n, due);
        epochProgress();
        return Math.min(n, epochTimers(now, false));
    }

    /** The join state of one attachment. */
    static final class Slot {
        private final long attachedAt;
        private boolean held;
        /** No other pipe was usable when the Hello was prepared. */
        private boolean early;
        /** The current SESSION_EPOCH went on this pipe; it goes again at epochAt. */
        private boolean epochSent;
        private long epochAt;

        Slot(long now) {
            attachedAt = now;
        }

        long attachedAt() {
            return attachedAt;
        }

        boolean held() {
            return held;
        }
    }
}
