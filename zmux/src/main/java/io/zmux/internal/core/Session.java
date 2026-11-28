package io.zmux.internal.core;

import io.zmux.internal.core.CoreException.DatagramDrop;
import io.zmux.DatagramSendOptions;
import io.zmux.internal.buffer.CarrierCharge;

import io.zmux.internal.memory.SharedMemory;


import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.buffer.ReceiveSpans;

import io.zmux.internal.core.CoreConfig.Role;
import io.zmux.internal.core.Lifecycle.CloseFrame;
import io.zmux.internal.wire.Gates;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.VarInt;
import io.zmux.internal.wire.WireException;
import io.zmux.internal.wire.ChannelDatagram;
import io.zmux.internal.wire.FrameDecoder;
import java.util.List;

/**
 * The sans-IO state of one zmux session. It is owned
 * by one thread, the session's event loop: nothing here is thread-safe, blocks,
 * performs I/O or calls application code. Inputs are records, carrier events,
 * API commands and timeouts; outputs are batches, events, carrier actions, wake-ups
 * and the next deadline.
 */
public final class Session {

    final CoreConfig cfg;
    private CoreSessionPolicy sessionPolicy;
    private int localCarriers, authorizationExecutions;
    private final SessionCapacity capacity = new SessionCapacity(this);
    private io.zmux.shaping.ShapingLimits shapingLimits;
    private io.zmux.shaping.ShapingBudget shapingBudget;
    private long now;
    final long start;
    private SessionState state = SessionState.ESTABLISHING;

    private Role role;
    /** The scheduling policy; starts from the configuration and changes with {@link #setPolicy}. */
    private CoreConfig.Policy policy;
    private boolean roleKnown;
    private TransportParams peer = new TransportParams();
    TransportParams peer() { return peer; }
    private boolean peerSeen;
    private Long establishedAt;
    private Features feat = Features.NONE;
    private Gates gates;

    final StreamTable streams;
    final StreamSend send;
    final StreamRecv recv;
    /** What the path layer sees of the streams. */
    final StreamsView streamsView;
    final StreamScheduler sched = new StreamScheduler();
    final Outbox outbox = new Outbox();
    /** The pipes by ID and in attachment order. */
    final PipeTable pipes = new PipeTable(outbox, this::carrierReplaced);
    final ControlQueue ctl = new ControlQueue(pipes::wakeWriters);
    final ConnFlow conn;
    final KeepAlive ka;
    /** Pipe acknowledgement and pacing; null without acknowledgement. */
    private AckEngine ack;
    private Pacer pacer;
    /** The resume grace deadline while SUSPENDED. */
    private long grace = Time.NEVER;
    private final java.util.Set<java.util.function.LongConsumer> suspensionObservers = new java.util.LinkedHashSet<>();
    private volatile SessionError err;
    final Lifecycle life;
    final SessionStats stats = new SessionStats();
    final Datagrams dg;
    final WaitList closedW = new WaitList();
    private final WaitList endingW = new WaitList();
    /** Close calls waiting for the carriers to take CONNECTION_CLOSE ({@link #closeHandedOff}). */
    private final WaitList handOffW = new WaitList();
    final WaitList estW = new WaitList();
    final WindowTuner tuner;
    /** The path layer: single pipe until multipath is negotiated. */
    private PathLayer path = singlePath();
    /** Quality events; null until a band or floor is set. */
    private QualityMonitor quality;
    QualityMonitor quality() { return quality; }
    private SessionState preSuspend = SessionState.ACTIVE;
    final Lanes lanes;
    final RecordReceiver rx;
    final Transmitter tx;
    private final ProtocolTimeline deadlines = new ProtocolTimeline(this::protocolDeadline);

    /** Creates a session; the bootstrap pipe is attached with {@link #attachBootstrap}. */
    public Session(CoreConfig cfg, long now) {
        this(cfg, now, false);
    }

    /** A policy refusal can exchange parameters and close without promising receive capacity. */
    public static Session refusing(CoreConfig cfg, long now) {
        return new Session(cfg, now, true);
    }

    private Session(CoreConfig cfg, long now, boolean refusing) {
        cfg = cfg.withMemoryLedger();
        var requestedParams = cfg.local();
        long requested = refusing ? 0 : requestedParams.initialMaxData;
        long desiredBidi = refusing ? 0 : requestedParams.maxStreamsBidi;
        long desiredUni = refusing ? 0 : requestedParams.maxStreamsUni;
        boolean incrementalCredit = !refusing && cfg.budget() != null && cfg.budget().limit() != CoreConfig.UNLIMITED;
        boolean multipath = requestedParams.has(TransportParams.MULTIPATH);
        long firstBidi = incrementalCredit ? Math.min(desiredBidi, StreamTable.initialBatch(multipath)) : desiredBidi;
        long firstUni = incrementalCredit ? Math.min(desiredUni, StreamTable.initialBatch(false)) : desiredUni;
        var reservation = refusing ? null : ConnFlow.reserveInitial(cfg.budget(), requested, desiredBidi > 0, multipath);
        try {
            long credit = reservation == null ? requested : reservation.window();
            long bidi = firstBidi;
            long uni = firstUni;
            if (reservation != null) {
                long mandatory = desiredBidi > 0 ? 1 : 0;
                bidi = mandatory + reservation.reserveStreams(firstBidi - mandatory, ReceiveLayout.STREAM_CHARGE, ConnFlow.sendFloor(multipath));
                uni = reservation.reserveStreams(firstUni, ReceiveLayout.STREAM_CHARGE, 0);
            }
            var granted = requestedParams.withReceiveLimits(credit, bidi, uni);
            if (granted != requestedParams) cfg = cfg.toBuilder().local(granted).build();
            this.cfg = cfg;
            sessionPolicy = cfg.sessionPolicy();
            this.policy = cfg.policy();
            this.joinPsk = cfg.psk();
            this.declared = cfg.declaredEndToEnd() || cfg.secureFirstPipe();
            this.shapingLimits = cfg.shapingLimits();
            this.shapingBudget = cfg.shapingBudget();
            this.now = now;
            this.start = now;
            dg = new Datagrams(cfg, stats, outbox, this::now);
            TransportParams l = cfg.local();
            if (l.maxRecordSize == 0) l.maxRecordSize = TransportParams.DEFAULT_MAX_RECORD_SIZE;
            var receiveStorage = new RecvBuffer.Storage(reservation);
            conn = new ConnFlow(credit, this::maxDataDue, reservation, multipath, pipes::wakeWriters);
            tuner = new WindowTuner(cfg, conn, new TunerPort());
            if (cfg.role() != Role.AUTO) {
                role = cfg.role();
                roleKnown = true;
            } else if (!l.has(TransportParams.ROLE)) {
                role = cfg.transportRole();
                roleKnown = true;
            }
            gates = new Gates(l.has(TransportParams.DATAGRAM) ? l.maxDatagramFrameSize : 0,
                    l.has(TransportParams.RESET_AT), false, false, false, false, false, false);
            ka = new KeepAlive(cfg, new KeepAlivePort(), outbox, now);
            life = new Lifecycle(Time.add(now, cfg.handshakeTimeout()),
                    () -> deadlines.changed(ProtocolTimeline.Reason.LIFE.bit()));
            StreamsPort sp = new StreamsPort();
            streamsView = sp;
            streams = new StreamTable(cfg, stats, outbox, life, dg, conn, sched, ctl, sp, desiredBidi, desiredUni, incrementalCredit);
            send = new StreamSend(stats, outbox, conn, sched, ctl, streams, sp);
            recv = new StreamRecv(cfg, stats, outbox, conn, tuner, ctl, streams, sp, receiveStorage, reservation);
            IoPort ioPort = new IoPort();
            lanes = new Lanes(cfg, pipes, outbox, ioPort);
            lanes.onDeadlineChanged(() -> deadlines.changed(ProtocolTimeline.Reason.LANE.bit()));
            rx = new RecordReceiver(cfg, stats, life, ioPort);
            tx = new Transmitter(cfg, stats, pipes, lanes, streams, send, recv, sched, ctl, conn, tuner, ka, life, dg, ioPort);
            dg.recordCap(tx::largestRecordCap);
            dg.channels(new ChannelsPort());
            var bounds = shapingLimitsInForce();
            if (bounds.wire() != null || !shapingBudget.equals(io.zmux.shaping.ShapingBudget.NONE)) shaping();
            if (cfg.shaper() != null) setShaper(cfg.shaper());
            refreshDeadlines();
        } catch (RuntimeException | Error failure) {
            if (reservation != null) reservation.close();
            throw failure;
        }
    }

    /** Advances the session clock for API calls; the driver calls it before every operation. */
    public void setNow(long t) {
        if (t > now) now = t;
    }

    public long now() { return now; }

    private java.util.function.LongFunction<PathEvidence.Loss> carrierLoss = ignored -> null;

    public void carrierLoss(java.util.function.LongFunction<PathEvidence.Loss> source) { carrierLoss = source; }
    PathEvidence.Loss publishedLoss(long pipe) { return carrierLoss.apply(pipe); }

    /** Publishes a host wake only after a denied receive commitment can be retried. */
    public void onReceiveRoom(Runnable ready) { conn.onRoom(ready); }

    private java.util.function.Consumer<Runnable> onOwner = Runnable::run;

    public void onMemoryCommand(java.util.function.Consumer<Runnable> onOwner) { this.onOwner = onOwner; }

    public void receiveRoom() {
        if (isClosing()) return;
        streams.receiveRoom();
        conn.backingRoom();
        recv.loanRoom();
        conn.checkCredit();
    }
    public SessionState state() { return state; }
    /** The terminal error, null while open. */
    public SessionError error() { return err; }
    public Features features() { return feat; }
    public boolean initiator() { return role == Role.INITIATOR; }
    public boolean roleKnown() { return roleKnown; }
    /** The peer's parameters once established, else null. */
    public TransportParams peerParams() { return peerSeen ? peer : null; }
    public TransportParams localParams() { return cfg.local(); }
    /** Bootstrap transport direction, independent of negotiated protocol role. */
    public Role transportRole() { return cfg.transportRole(); }
    public CoreConfig config() { return cfg; }

    public CoreSessionPolicy sessionPolicy() { return sessionPolicy; }

    /** Returns a consistent snapshot of the current membership and resource owners. */
    public SessionCapacity.Snapshot capacity() {
        return capacity.snapshot(localCarriers, authorizationExecutions);
    }

    public boolean validQualityInput(io.zmux.MemberId member, io.zmux.PipeId pipe, io.zmux.Direction direction,
                                     long generation, PathEvidence.Sample sample) {
        return capacity.validQualityInput(member, pipe, direction, generation, sample);
    }

    public long capacityRevision() { return capacity.observed(); }

    /** Attributes the original, confirmed bootstrap member to a matching carrier registration. */
    public void bindBootstrapCandidate(String candidate, SessionCapacity.CandidateIdentity identity) throws io.zmux.PipeGoneException {
        Pipe first = pipes.get(0);
        if (first == null || (first.mp() != null && first.mp().retired())) {
            throw new io.zmux.PipeGoneException(io.zmux.PipeId.BOOTSTRAP);
        }
        var member = validateBootstrapCandidate(candidate, identity, first);
        member.checkIdentity(candidate, identity);
        boolean changed = member.bind(candidate);
        member.attribute(identity);
        if (!changed) return;
        if (path instanceof PathSession mp) applyTopology(mp, first);
        capacity.changed();
    }

    private SessionCapacity.Member validateBootstrapCandidate(String candidate, SessionCapacity.CandidateIdentity identity, Pipe first) {
        if (!identity.layout().equals(List.of(io.zmux.Directions.BOTH)) || first.out() == null || first.in() == null) {
            throw new IllegalArgumentException("bootstrap candidate must match the duplex attachment");
        }
        var kind = identity.kind();
        var variant = identity.variant();
        if (kind != first.carrier() || !variant.equals(first.variant())) {
            throw new IllegalArgumentException("bootstrap candidate " + candidate + " has carrier " + kind + " variant '"
                    + variant + "', pipe 0 has carrier " + first.carrier() + " variant '" + first.variant() + "'");
        }
        var member = first.member();
        var view = capacity().members().stream().filter(snapshot -> snapshot.id().equals(member.id())).findFirst();
        if (view.isEmpty() || !view.get().complete() || view.get().draining()) {
            throw new IllegalArgumentException("bootstrap member is not confirmed and available");
        }
        return member;
    }

    public void checkCandidateIdentity(String candidate, SessionCapacity.CandidateIdentity identity) {
        for (var pipe : pipes.all()) {
            if (pipe.mp() == null || !pipe.mp().retired()) pipe.member().checkIdentity(candidate, identity);
        }
    }

    /** Registers an internal owner-loop observer of the recovery grace. */
    public Runnable watchSuspension(java.util.function.LongConsumer observer) {
        suspensionObservers.add(observer);
        return () -> suspensionObservers.remove(observer);
    }

    /** Bounds an attempt strictly before an active recovery grace deadline. */
    public long boundAttemptDeadline(long deadline) {
        return state == SessionState.SUSPENDED ? Math.min(deadline, Time.sub(grace, 1)) : deadline;
    }

    /** Tests the observed version and subscribes to a later capacity commit in the same owner turn. */
    public boolean waitCapacity(long observed, Waiter waiter) { return capacity.waitForChange(observed, waiter); }

    /** Withdraws a capacity observation. */
    public void cancelCapacityWait(Waiter waiter) { capacity.withdraw(waiter); }

    public boolean drainClaimAvailable(java.util.Set<io.zmux.MemberId> members) { return capacity.drainClaimAvailable(members); }

    /** Claims the selected members for a single prospective drain. */
    public SessionCapacity.DrainClaim claimDrain(java.util.Set<io.zmux.MemberId> members) throws CoreException {
        requireMultipath();
        return capacity.claimDrain(members);
    }

    public void releaseDrain(SessionCapacity.DrainClaim claim) { capacity.releaseDrain(claim); }

    public void setMemberPolicy(io.zmux.MemberId member, Tiers.PipeSettings preferences,
                                long sendLimit, long receiveLimit) throws CoreException {
        capacity.setMemberPolicy(requireMultipath(), member, preferences, sendLimit, receiveLimit);
    }

    /** Fresh outgoing protocol evidence committed by the session owner. */
    @FunctionalInterface
    public interface PerformanceFeedback {
        void record(io.zmux.MemberId member, long at, long validUntil, java.time.Duration rtt, long rate);
    }

    private PerformanceFeedback performanceFeedback;
    public void performanceFeedback(PerformanceFeedback observer) { performanceFeedback = observer; }

    /** A credited byte resident at the first ACK, identified without retaining the stream. */
    record DeliveryBacklog(long stream, long end, long connectionLimit, long at, long topology, int pipesVersion) {}
    private void clearDeliveryBacklog() {
        for (var pipe : pipes.all()) if (pipe.out() != null) pipe.out().snd.deliveryBacklog(null);
    }


    public void targetChanged(long generation, long revision, boolean initial, Long previous) {
        emit(Event.targetChanged(generation, revision, initial, previous));
    }

    public void memberFeedback(java.util.function.Consumer<SessionCapacity.MemberHealth> observer) {
        capacity.feedback(observer);
    }

    private void memberBytes(Pipe pipe, boolean sending, long bytes) {
        if (pipe.member().record(sending, bytes)) capacity.changed();
    }

    /** Validates a retirement batch, then passes the attachments starting a drain to the optional reservation callback. */
    public boolean drainMembers(java.util.Set<io.zmux.MemberId> members, long minimumEpoch, long policyRevision,
                                java.util.function.Predicate<List<io.zmux.PipeId>> beforeDrain) throws CoreException {
        PathSession path = requireMultipath();
        return policyRevision == sessionPolicy.revision() && capacity.drain(path, members, minimumEpoch, beforeDrain);
    }

    /** Cleans up a provisional member when doing so preserves currently confirmed service. */
    public boolean rollbackMember(io.zmux.MemberId member) {
        return path instanceof PathSession mp && !isClosing() && capacity.rollback(mp, member);
    }

    /** Reserves new physical connections. Already charged connections consume no new slots. */
    public boolean acquireCarriers(int count) {
        if (isClosing() || (count > 0 && count > sessionPolicy.limits().localCarriers() - localCarriers)) return false;
        localCarriers += count;
        if (count > 0) capacity.changed();
        return true;
    }

    /** Releases connections whose last attachment and I/O owner has exited. */
    public void releaseCarriers(int count) {
        if (count > localCarriers) throw new IllegalStateException("unbalanced carrier capacity");
        localCarriers -= count;
        if (count > 0) capacity.changed();
    }

    /** Admits an authorization callback until its actual execution ends. */
    public boolean acquireJoinAuthorization() {
        if (isClosing() || authorizationExecutions >= sessionPolicy.limits().pendingPeerJoins()) return false;
        authorizationExecutions++;
        capacity.changed();
        return true;
    }

    public void releaseJoinAuthorization() {
        if (authorizationExecutions == 0) throw new IllegalStateException("unbalanced Join authorization capacity");
        authorizationExecutions--;
        capacity.changed();
    }

    /** Publishes authorization and hard limits in one owner turn. */
    public void setSessionPolicy(CoreSessionPolicy next) throws CoreException {
        deadlines.changed(ProtocolTimeline.ALL);
        if (isClosing()) throw CoreException.session(err);
        java.util.Objects.requireNonNull(next, "sessionPolicy");
        if (next.equals(sessionPolicy)) return;
        if (Long.compareUnsigned(next.revision(), sessionPolicy.revision()) <= 0
                || Long.compareUnsigned(next.authGeneration(), sessionPolicy.authGeneration()) < 0) {
            throw new IllegalArgumentException("stale or conflicting session policy, current revision "
                    + Long.toUnsignedString(sessionPolicy.revision()));
        }
        if (next.limits().attachmentLimit() > cfg.sessionPolicy().limits().attachmentLimit()) {
            throw new IllegalArgumentException("session policy exceeds the initial attachment capacity");
        }
        boolean revoke = next.authGeneration() != sessionPolicy.authGeneration();
        sessionPolicy = next;
        if (path instanceof PathSession mp) {
            mp.joins.invalidateReservations();
            for (Pipe pipe : java.util.List.copyOf(pipes.all())) {
                var attachment = pipe.mp();
                if (attachment.retired()) continue;
                applyTopology(mp, pipe);
                if (revoke && pipe.id != 0 && !attachment.joinerLocal
                        && Long.compareUnsigned(attachment.peerAuthorization(), next.authGeneration()) < 0) {
                    mp.health.removePipe(pipe.id, true);
                }
            }
            mp.tiers.refreshTiers(mp.tiers.buildGate());
            mp.tiers.checkTakeover();
        }
        capacity.invalidateDrains();
        capacity.changed();
        pipes.wakeWriters();
    }
    private void applyTopology(PathSession path, Pipe pipe) {
        String candidate = pipe.member().candidate();
        int excluded = 0;
        if (!sessionPolicy.allows(candidate, Action.DIR_IN)) excluded |= Action.DIR_IN;
        if (!sessionPolicy.allows(candidate, Action.DIR_OUT)) excluded |= Action.DIR_OUT;
        var attachment = pipe.mp();
        if (excluded == attachment.topologyExcluded()) return;
        boolean lostSend = (excluded & ~attachment.topologyExcluded() & Action.DIR_OUT) != 0;
        attachment.topologyExcluded(excluded);
        if (lostSend) {
            dg.cancelPathCopies(pipe);
            path.retention.reinjectPipe(pipe);
        }
        path.syncHealth(pipe);
        path.scheduler.pickChanged();
    }

    public SessionStats stats() { return stats; }

    /**
     * The session's statistics now, gauges included: RTTs of pipe 0
     * outside multipath, of the primary pipe in a multipath session.
     */
    public SessionStats.Snapshot statsSnapshot() {
        Pipe rp = path instanceof PathSession ps ? ps.primary() : pipes.get(0);
        long[] rtt = rp == null ? null : rttOf(rp, new io.zmux.PipeStats.RttSource[1]);
        long active = 0;
        for (Pipe p : pipes.all()) {
            PipeInfo.States s = pipeState(p.id);
            if (s != null && s.out() == PipeInfo.Out.ACTIVE) active++;
        }
        long retained = path.retained();
        var g = new SessionStats.Gauges(streams.count(), Math.max(0, conn.sendMemory() - retained), conn.buffered(), retained,
                active, rtt == null ? -1 : rtt[0], rtt == null ? -1 : rtt[1], rtt == null ? -1 : rtt[3], rtt == null ? -1 : rtt[2]);
        return stats.snapshot(ka.budgetTokens(), g);
    }
    public boolean established() { return peerSeen; }
    boolean isClosing() { return state.compareTo(SessionState.CLOSING) >= 0; }
    /** The session's streams: opening, accepting, identifiers, priorities, release. */
    public StreamTable streams() { return streams; }
    /** The sending parts of the session's streams. */
    public StreamSend sending() { return send; }
    /** The receiving parts of the session's streams. */
    public StreamRecv receiving() { return recv; }

    /** Gives the sending part the send buffer held last (see {@link StreamTable#holdSendBuffer}); on the loop. */
    public void applyHeldSendBuffer(Stream st) {
        long held = streams.heldSendBufferSet(st);
        if (held >= 0) send.setSendBuffer(st, held);
    }

    /** Gives the receiving part the receive window held last (see {@link StreamTable#holdReceiveWindow}); on the loop. */
    public void applyHeldReceiveWindow(Stream st) {
        var held = streams.heldWindowSet(st);
        if (held != null) recv.setReceiveWindow(st, held.initial(), held.max());
    }
    /** The session's pipes. */
    public PipeTable pipes() { return pipes; }
    public Outbox outbox() { return outbox; }
    public Transmitter transmitter() { return tx; }

    public void routeOutput() {
        tx.routeOutput(hasChannelOutput());
        deadlines.changed(ProtocolTimeline.Reason.PACING.bit() | ProtocolTimeline.Reason.SHAPING.bit()
                | ProtocolTimeline.Reason.CUSTOM_PICK.bit());
    }
    public WindowTuner tuner() { return tuner; }
    public long reservedBytes() { return conn.reservedBytes(); }
    public KeepAlive keepAlive() { return ka; }
    PathLayer path() { return path; }

    /** Whether the path layer has a custom pick request waiting for the application picker. */
    public boolean wantsPick() { return path.wantsPick(); }

    /** Fills req with the waiting pick request; false when none is waiting. */
    public boolean takePick(PathScheduler.PickRequest req) { return path.takePick(req); }

    /** Hands the application's answers (or its failure) for req back to the path layer. */
    public void answerPick(PathScheduler.PickRequest req, List<PathScheduler.PickAnswer> answers, boolean failed) {
        deadlines.changed(ProtocolTimeline.PATH);
        path.answerPick(req, answers, failed);
    }
    /** The lanes of the session's pipes. */
    public Lanes lanes() { return lanes; }

    /**
     * Attaches the bootstrap pipe (both directions); the driver then polls it for the first
     * record. With datagrams enabled, the session offers zmux_carrier_datagrams when the
     * carrier has a datagram channel, or a joined pipe may have one.
     */
    public void attachBootstrap(CarrierInfo in, CarrierInfo out) {
        deadlines.changed(ProtocolTimeline.ALL);
        TransportParams l = cfg.local();
        if (l.has(TransportParams.DATAGRAM) && (out.channel() != null || l.has(TransportParams.MULTIPATH))) {
            l.present |= TransportParams.CARRIER_DATAGRAMS | TransportParams.CARRIER_DATAGRAM_PADDING;
        }
        pipes.attachBootstrap(in, out);
        stats.pipeAdded();
    }

    // ---- establishment ----

    /** Runs when the first frame of the inbound bootstrap pipe decoded. */
    void processPeerParams(TransportParams p) throws WireException {
        peer = p;
        peerSeen = true;
        TransportParams l = cfg.local();
        boolean fRole = false;
        if (l.has(TransportParams.ROLE) && p.has(TransportParams.ROLE)) {
            fRole = true;
            int lr = l.role, pr = p.role;
            if (lr == TransportParams.ROLE_AUTO && pr == TransportParams.ROLE_AUTO) {
                int c = Long.compareUnsigned(l.roleNonce, p.roleNonce);
                if (c == 0) throw new WireException(TransportError.ROLE_CONFLICT, "equal role nonces");
                role = c > 0 ? Role.INITIATOR : Role.RESPONDER;
            } else if (lr == TransportParams.ROLE_AUTO) {
                role = pr == TransportParams.ROLE_INITIATOR ? Role.RESPONDER : Role.INITIATOR;
            } else if (pr == TransportParams.ROLE_AUTO || lr != pr) {
                role = lr == TransportParams.ROLE_INITIATOR ? Role.INITIATOR : Role.RESPONDER;
            } else {
                throw new WireException(TransportError.ROLE_CONFLICT, "both endpoints claim the same role");
            }
        } else {
            Role tr = cfg.transportRole();
            if (tr == Role.AUTO || (cfg.role() != Role.AUTO && cfg.role() != tr)) {
                throw new WireException(TransportError.ROLE_CONFLICT, "no transport role");
            }
            role = tr;
        }
        roleKnown = true;
        boolean goAway = both(l, p, TransportParams.GOAWAY);
        boolean preamble = both(l, p, TransportParams.PREAMBLE);
        boolean priority = both(l, p, TransportParams.PRIORITY);
        boolean flows = both(l, p, TransportParams.FAMILY);
        boolean carrierDatagrams = flows && both(l, p, TransportParams.CARRIER_DATAGRAMS);
        boolean multipath = false, jsu = false, pskSelected = false;
        if (both(l, p, TransportParams.MULTIPATH)) {
            int sources = TransportParams.SECRET_EXPORTER | TransportParams.SECRET_PSK;
            int lf = l.secretFlags & sources, pf = p.secretFlags & sources;
            if (lf == 0 || pf == 0) {
                if (cfg.secureFirstPipe()) throw WireException.param("secret flags of a secure first pipe");
                jsu = true;
            } else if (lf != pf) {
                // Never the intersection: a party on the path could clear a flag.
                throw WireException.param("differing secret flags");
            } else {
                multipath = true;
                pskSelected = (lf & TransportParams.SECRET_PSK) != 0;
            }
        }
        boolean pipeAck = both(l, p, TransportParams.PIPE_ACK) && !multipath;
        boolean resume = multipath && both(l, p, TransportParams.RESUME) && (l.locator.length > 0 || p.locator.length > 0);
        boolean acked = multipath || pipeAck;
        boolean lanesOut = p.has(TransportParams.LANES) && p.maxLanes > 1 && acked;
        boolean lanesIn = l.has(TransportParams.LANES) && l.maxLanes > 1 && acked;
        feat = new Features(fRole, goAway, preamble, priority, flows, multipath, pipeAck, resume, lanesOut, lanesIn, jsu, carrierDatagrams, carrierDatagrams && both(l, p, TransportParams.CARRIER_DATAGRAM_PADDING));
        gates = new Gates(gates.maxDatagram(), gates.resetStreamAt(), goAway, priority, multipath, pipeAck, lanesIn,
                flows && p.has(TransportParams.DATAGRAM) && p.maxDatagramFrameSize > 0);
        tx.peerRecordLimit(p.maxRecordSize);
        dg.peer(p.has(TransportParams.DATAGRAM) ? p.maxDatagramFrameSize : 0, p.maxRecordSize, flows);
        conn.sendLimit(p.initialMaxData);
        streams.peerLimits(p.maxStreamsBidi, p.maxStreamsUni);
        if (multipath && (l.secretFlags & TransportParams.SECRET_EXPORTER) != 0) readExporter();
        if (cfg.deferredPsk() && secretSet && !pskSelected) dropSecret();
        streams.applyPeerParamsAll();
        if (acked) {
            pacer = new Pacer(cfg, p.has(TransportParams.ACK_THRESHOLD) ? p.ackThreshold : 65536, AckEngine.peerDelay(p), new PacerPort());
            ack = new AckEngine(l, p, pipes.all(), pacer, new AckPort());
        }
        establishedAt = now;
        if (multipath) {
            path = multiPath();
            if (cfg.declaredEndToEnd() || cfg.secureFirstPipe()) declared = true;
            keysArrived();
        }
        if (state == SessionState.ESTABLISHING) state = SessionState.ACTIVE;
        life.established();
        ka.start(idleTimeout(), now);
        emit(Event.established(feat, role == Role.INITIATOR));
        noteDatagramPath();
        estW.wakeAll(outbox);
        pipes.wakeWriters();
        streams.wakeOpeners();
    }

    /** The exporter value of the session secret, kept until the join keys are derived. */
    private byte[] exporterValue;
    /** The PSK: configured, or supplied for a deferred one. */
    private byte[] joinPsk;
    /** A deferred PSK was supplied. */
    private boolean secretSet;

    /**
     * Reads the exporter once, when the peer's parameters arrive: they came through the carrier,
     * so its handshake has completed even when early data preceded it.
     */
    private void readExporter() throws WireException {
        byte[] v;
        try {
            v = cfg.exporter() == null ? null : cfg.exporter().get();
        } catch (RuntimeException e) {
            throw new WireException(TransportError.INTERNAL_ERROR, "exporter: " + e.getMessage());
        }
        if (v == null) throw new WireException(TransportError.INTERNAL_ERROR, "exporter: not available");
        exporterValue = v;
    }

    /** The session closed: the secrets the join keys come from are cleared. */
    private void clearJoinSecrets() {
        if (path instanceof PathSession mp) mp.joins.clearHeldSecret();
        if (exporterValue != null) java.util.Arrays.fill(exporterValue, (byte) 0);
        exporterValue = null;
        if (joinPsk != null) java.util.Arrays.fill(joinPsk, (byte) 0);
        joinPsk = null;
        secretSet = false;
    }

    /** A deferred PSK supplied early that the negotiation did not select is dropped and cleared. */
    private void dropSecret() {
        java.util.Arrays.fill(joinPsk, (byte) 0);
        joinPsk = null;
        secretSet = false;
    }

    /**
     * Supplies the deferred PSK: once the negotiation selected it the join keys are derived and
     * the driver lets the held joins go on; before establishment it is kept for the negotiation.
     */
    public void setJoinSecret(byte[] psk) throws CoreException {
        if (err != null) throw CoreException.session(err);
        boolean selected = path instanceof PathSession && (cfg.local().secretFlags & TransportParams.SECRET_PSK) != 0;
        if (peerSeen && !selected) throw CoreException.of(CoreException.Reason.NOT_NEGOTIATED);
        if (secretSet) throw CoreException.of(CoreException.Reason.JOIN_SECRET_SET);
        joinPsk = psk.clone();
        secretSet = true;
        keysArrived();
    }

    /** The host has proven pipe 0 end to end: local, sends nothing. A secure first pipe needs no call. */
    public void declareEndToEnd() {
        deadlines.changed(ProtocolTimeline.PATH);
        declared = true;
        keysArrived();
    }

    /** Derives the join keys when the last missing input came. */
    private void keysArrived() {
        deadlines.changed(ProtocolTimeline.PATH);
        if (!(path instanceof PathSession mp) || !mp.joins.keysPending()) return;
        try {
            mp.joins.inputs(declared, cfg.deferredPsk() && !secretSet, joinPsk);
        } catch (WireException e) {
            failLocal(e);
            return;
        }
        if (!mp.joins.keysPending()) pipes.wakeWriters();
    }

    /** Whether joining waits for join keys. */
    private boolean declared;

    /** The weakest record mode of the session, null when pipe 0 is Unmanaged. */
    // Internal packages are exported only to this project's modules (qualified export), so these signatures name types that stay unexported.
    //noinspection ClassEscapesItsScope
    public io.zmux.internal.join.Mode weakestMode() {
        if (!cfg.secureFirstPipe()) return null;
        return path instanceof PathSession mp ? mp.joins.weakest(cfg.firstPipeMode()) : cfg.firstPipeMode();
    }

    /** The record mode of a pipe, null for an Unmanaged pipe 0. */
    //noinspection ClassEscapesItsScope
    public io.zmux.internal.join.Mode pipeMode(long id) {
        Pipe p = pipes.get(id);
        if (p != null && p.mp() != null && p.mp().mode() != null) return p.mp().mode();
        return cfg.secureFirstPipe() ? cfg.firstPipeMode() : null;
    }

    /**
     * What a pipe was fixed as when it was added: its direction as the local endpoint has it (0 send only, 1 receive
     * only, 2 both), its record mode (null for an Unmanaged pipe 0), the Group of its key exchange (0 when none ran)
     * and its fate group.
     */
    public record PipeFixed(int direction, io.zmux.internal.join.Mode mode, long keyGroup, long fateGroup) {}

    /** The pipe's {@link PipeFixed}, or null when it is not there; on the loop. */
    public PipeFixed pipeFixed(long id) {
        Pipe p = pipes.get(id);
        if (p == null) return null;
        Attach a = p.mp();
        int dir = p.in() != null && p.out() != null ? 2 : p.out() != null ? 0 : 1;
        return new PipeFixed(dir, pipeMode(id), a == null ? 0 : a.usedGroup(), a == null ? 0 : a.fate);
    }

    public long joinFailures() { return stats.joinFailures(); }

    /** Whether an attachment whose join finished is still there, neither of its directions failed or ended. */
    public boolean pipeAttached(long id) {
        Pipe p = pipes.get(id);
        Attach a = p == null ? null : p.mp();
        return a != null && !a.health.retired() && a.health.out() != Attach.OUT_FAILED && a.health.out() != Attach.OUT_CLOSED
                && a.health.in() != Attach.IN_ENDED;
    }

    /** Takes a local join's place: the name of its Hello's plan; null while the cap or the backoff holds it. */
    public String joinPrepare(long id, long t) throws CoreException {
        deadlines.changed(ProtocolTimeline.PATH);
        setNow(t);
        if (isClosing()) throw CoreException.of(CoreException.Reason.INVALID);
        try {
            return requireMultipath().joins.joinPrepare(pipes.get(id), cfg.joinEarlyHello());
        } catch (CoreException e) {
            if (e.reason() == CoreException.Reason.JOIN_PENDING) throw CoreException.joinPending(joinMissing());
            throw e;
        }
    }

    /** The Hello of a prepared join. */
    //noinspection ClassEscapesItsScope
    public byte[] joinHello(long id, io.zmux.internal.join.Framing f, io.zmux.carrier.HandshakeShaper.UnitPlan plan, int overhead, long t)
            throws io.zmux.seal.SecureCarrierException, CoreException {
        return joinHello(id, f, plan, overhead, t, null);
    }

    public void joinHello(Batch batch, long id, io.zmux.internal.join.Framing framing,
                          io.zmux.carrier.HandshakeShaper.UnitPlan plan, int overhead, long t)
            throws io.zmux.seal.SecureCarrierException, CoreException {
        batch.appendUnit(joinHello(id, framing, plan, overhead, t, batch));
    }

    private byte[] joinHello(long id, io.zmux.internal.join.Framing f, io.zmux.carrier.HandshakeShaper.UnitPlan plan,
                             int overhead, long t, Batch receipt) throws io.zmux.seal.SecureCarrierException, CoreException {
        setNow(t);
        if (isClosing()) throw CoreException.of(CoreException.Reason.INVALID);
        return requireMultipath().joins.joinHello(pipes.get(id), f, cfg.joinEarlyHello(), plan, overhead, receipt);
    }

    /** Feeds bytes read from a JOINING carrier. */
    public Joins.JoinStep verifyReply(long id, byte[] b, int off, int len, long t) {
        deadlines.changed(ProtocolTimeline.PATH);
        setNow(t);
        Pipe p = pipes.get(id);
        if (p == null || !(path instanceof PathSession mp)) return new Joins.JoinStep(Joins.Step.FAILED, 0, 0);
        return mp.joins.verifyReply(p, b, off, len);
    }

    /** A verified join goes active; null when the session or the pipe ended since its Reply. */
    public Joins.Finished joinFinish(long id, io.zmux.carrier.HandshakeShaper.UnitPlan plan, int overhead, long t) throws io.zmux.seal.SecureCarrierException {
        deadlines.changed(ProtocolTimeline.PATH);
        setNow(t);
        if (isClosing() || !(path instanceof PathSession mp)) return null;
        return mp.joins.joinFinish(pipes.get(id), plan, overhead);
    }

    /** A Hello in Bound refused because its carrier has no Binding. */
    public void helloWithoutBinding() {
        deadlines.changed(ProtocolTimeline.PATH);
        if (path instanceof PathSession mp) mp.joins.helloWithoutBinding();
    }

    /** The acceptor's handling of a Hello naming this session, up to its verification. */
    //noinspection ClassEscapesItsScope
    public Joins.Opened openHello(io.zmux.internal.join.Hello h, io.zmux.internal.join.Framing f, byte[] binding) {
        deadlines.changed(ProtocolTimeline.PATH);
        if (isClosing()) return new Joins.Opened(Joins.JoinResult.CLOSE, null);
        if (!peerSeen) return new Joins.Opened(Joins.JoinResult.HOLD, null);
        return path instanceof PathSession mp ? mp.joins.openHello(h, f, binding) : new Joins.Opened(Joins.JoinResult.CLOSE, null);
    }

    /** Checks the captured generation when the policy callback returns. */
    public boolean currentJoinAuthorization(Joins.PeerJoin pj) {
        return path instanceof PathSession mp && mp.joins.currentAuthorization(pj);
    }

    /** Claims an authorized Hello, with the entry's original deadline. */
    public Joins.Claimed claimHello(Joins.PeerJoin pj, Joins.JoinCarrier c, io.zmux.carrier.HandshakeShaper.UnitPlan plan,
                                    int overhead, long received, long deadline) {
        deadlines.changed(ProtocolTimeline.PATH);
        if (isClosing() || !(path instanceof PathSession mp)) {
            pj.close();
            return new Joins.Claimed(Joins.JoinResult.CLOSE, null);
        }
        var result = mp.joins.claimHello(pj, c, plan, overhead, received, deadline);
        if (result.result() == Joins.JoinResult.ACCEPTED) capacity.changed();
        return result;
    }

    /** An accepted Hello of {@link #claimHello} ended without confirmation. */
    public void unconfirmedEnded(Joins.PeerJoin pj) {
        deadlines.changed(ProtocolTimeline.PATH);
        if (path instanceof PathSession mp && mp.joins.unconfirmedEnded(pj)) capacity.changed();
    }

    /** Commits confirmation and consumes its pending identity. */
    public Joins.Confirmed confirmJoin(Joins.PeerJoin pj) {
        deadlines.changed(ProtocolTimeline.PATH);
        if (!(path instanceof PathSession mp)) return new Joins.Confirmed(Joins.JoinResult.CLOSE, null, "session_closed");
        capacity.changed();
        return mp.joins.confirmJoin(pj);
    }

    /** Frames of a verified Hello or Reply: STREAM without FIN only; one in error is dropped and counted, since a batch can be a replay. */
    private void applyBatch(byte[] b) {
        if (b == null || b.length == 0) return;
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(b);
        ReadBuf buf = new ReadBuf(bb, null);
        io.zmux.internal.wire.FrameDecoder decoder = new io.zmux.internal.wire.FrameDecoder();
        io.zmux.internal.wire.Frame f = new io.zmux.internal.wire.Frame();
        try {
            applyBatch(buf, bb, decoder, f);
        } finally {
            buf.unref();
        }
    }

    private void applyBatch(ReadBuf buf, java.nio.ByteBuffer bb, io.zmux.internal.wire.FrameDecoder decoder, io.zmux.internal.wire.Frame f) {
        for (int pos = 0; pos < bb.limit(); ) {
            int n;
            try {
                n = decoder.decode(bb, pos, bb.limit(), f);
            } catch (WireException bad) {
                stats.joinBatchDrop();
                return;
            }
            pos += n;
            if (f.kind != io.zmux.internal.wire.FrameKind.STREAM || f.fin) {
                stats.joinBatchDrop();
                continue;
            }
            try {
                recv.onStream(f, buf);
            } catch (WireException bad) {
                stats.joinBatchDrop();
            }
        }
    }

    /** What keeps the join keys back: {@link CoreException#MISSING_END_TO_END} and {@link CoreException#MISSING_PSK}. */
    private int joinMissing() {
        return (declared ? 0 : CoreException.MISSING_END_TO_END) | (cfg.deferredPsk() && !secretSet ? CoreException.MISSING_PSK : 0);
    }

    public boolean joinPending() {
        return path instanceof PathSession mp && mp.joins.keysPending();
    }

    /**
     * Starts a session epoch exchange now when Stable, or right after
     * the one in progress. NOT_NEGOTIATED without multipath; join pending before the join keys.
     */
    public void advanceEpoch() throws CoreException {
        deadlines.changed(ProtocolTimeline.PATH);
        if (err != null) throw CoreException.session(err);
        if (!(path instanceof PathSession mp)) throw CoreException.of(CoreException.Reason.NOT_NEGOTIATED);
        if (mp.joins.keysPending()) throw CoreException.joinPending(joinMissing());
        mp.joins.advanceEpoch();
    }

    /** The epoch new local joins use. */
    public long epoch() {
        return path instanceof PathSession mp ? mp.joins.epoch() : 0;
    }

    /** Registers sig until new local joins use epoch n; true when they do now. */
    public boolean waitEpoch(long n, Waiter sig) throws CoreException {
        if (err != null) throw CoreException.session(err);
        if (!(path instanceof PathSession mp)) throw CoreException.of(CoreException.Reason.NOT_NEGOTIATED);
        if (mp.joins.epoch() >= n) return true;
        mp.waitEpoch(sig);
        return false;
    }

    public void cancelEpochWait(Waiter sig) { if (path instanceof PathSession mp) mp.cancelEpochWait(sig); }

    /** Whether a Hello naming loc is for this session: the Locator of an accepted epoch. */
    public boolean acceptsLocator(byte[] loc) {
        return path instanceof PathSession mp ? mp.joins.acceptsLocator(loc) : java.util.Arrays.equals(loc, cfg.local().locator);
    }

    /**
     * The session binding, the same at both ends: null before establishment
     * and without multipath, whose parameter carries the Session Nonces.
     */
    public byte[] sessionBinding() {
        if (!(path instanceof PathSession)) return null;
        byte[] l = cfg.local().sessionNonce, r = peer.sessionNonce;
        return role == Role.INITIATOR ? io.zmux.internal.join.Kdf.sessionBinding(l, r) : io.zmux.internal.join.Kdf.sessionBinding(r, l);
    }

    // ---- settings replaced while the session runs ----

    /** Replaces the session's liveness settings; nothing is sent, timers rearm with the next round. */
    public void setLiveness(CoreConfig.Live l) {
        deadlines.changed(ProtocolTimeline.ALL);
        for (Pipe p : pipes.all()) if (path instanceof PathSession mp) mp.health.keepSuspect(p);
        cfg.live().set(l);
        ka.settings(l.keepAlive(), l.probeJitter());
        pipes.wakeWriters();
    }

    /** Replaces a pipe's own liveness settings: 0 (for the interval -1, for the jitter NaN) follows the session's. */
    public void setPipeLiveness(long id, long suspectFloor, long deadTimeout, long probeInterval, double probeJitter) throws CoreException {
        deadlines.changed(ProtocolTimeline.ALL);
        Pipe p = pipes.get(id);
        if (p == null || p.mp() == null || p.mp().health.retired()) throw CoreException.of(CoreException.Reason.INVALID);
        mp().health.keepSuspect(p);
        p.mp().liveness(suspectFloor, deadTimeout, probeInterval, probeJitter);
        pipes.wakeWriters();
    }

    /** Raises the concurrency of the peer's streams of one type; credit only grows. */
    public void raiseIncomingStreams(boolean bidi, long n) {
        streams.raiseIncoming(bidi, n);
    }

    /** The session's shaping limits. */
    public io.zmux.shaping.ShapingLimits shapingLimits() { return shapingLimits; }

    /** The local limits under the budget. */
    public io.zmux.shaping.ShapingLimits.Effective shapingLimitsInForce() { return shapingLimits.under(shapingBudget); }

    /** The session's shaping readings. */
    public io.zmux.shaping.ShapingMeters shapingMeters() {
        Shaping sh = pipes.shaping();
        return sh == null ? io.zmux.shaping.ShapingMeters.ZERO : sh.snapshotMeters();
    }

    /** Bytes available to the Join encoder for optional padding. */
    long handshakeRoom() {
        Shaping sh = pipes.shaping();
        return shapingLimits.padding() && (sh == null || sh.exact()) ? io.zmux.internal.join.Sealer.MESSAGE_FLOOR : 0;
    }

    public void submitHandshake(Batch unit, int fixed, boolean atomic, long now) {
        setNow(now);
        unit.submitHandshake(shaping(), fixed, atomic);
    }

    public boolean handshakeFits(io.zmux.carrier.HandshakeCost cost) {
        return cost.baselineOnly() || shaping().handshakeFits(cost);
    }

    public void transferHandshake(io.zmux.carrier.HandshakeCost cost, long observedNanoTime) {
        if (!cost.units().isEmpty()) shaping().transferHandshake(cost, observedNanoTime);
    }

    public long shapingGeneration() { return pipes.shaping() == null ? 0 : pipes.shaping().generation(); }

    public void holdJoin(Object key, long wait, java.util.function.BooleanSupplier live, Runnable release) {
        deadlines.changed(ProtocolTimeline.Reason.SHAPING.bit());
        shaping().holdJoin(key, wait, live, release);
    }

    public void cancelJoinHold(Object key, long at) {
        if (pipes.shaping() == null) return;
        setNow(at);
        pipes.shaping().cancelJoinHold(key);
        deadlines.changed(ProtocolTimeline.Reason.SHAPING.bit());
    }

    /** Updates the local limits when existing commitments fit within the new bounds. */
    public void setShapingLimits(io.zmux.shaping.ShapingLimits l) {
        var effective = l.under(shapingBudget);
        Shaping sh = pipes.shaping();
        if (sh != null) sh.validateLimits(effective);
        deadlines.changed(ProtocolTimeline.Reason.SHAPING.bit());
        shapingLimits = l;
        if (sh != null) sh.limits(effective);
    }

    private static boolean both(TransportParams l, TransportParams p, int bit) {
        return l.has(bit) && p.has(bit);
    }

    /** Registers sig until establishment or close; true when the condition holds now. */
    public boolean waitEstablished(Waiter sig) {
        if (peerSeen || isClosing()) return true;
        estW.add(sig);
        return false;
    }

    /** Registers sig until CLOSED; true when the session is closed now. */
    public boolean waitClosed(Waiter sig) {
        if (state == SessionState.CLOSED) return true;
        closedW.add(sig);
        return false;
    }

    public void cancelClosedWait(Waiter sig) { closedW.remove(sig); }

    /** Registers sig until terminal arbitration; true after the error is committed. */
    public boolean waitEnding(Waiter sig) {
        if (isClosing()) return true;
        endingW.add(sig);
        return false;
    }

    public void cancelEndingWait(Waiter sig) { endingW.remove(sig); }

    /**
     * Whether written stream data, a written FIN or a reset's RESET_STREAM is still unsent: a graceful close flushes
     * until this is false or its bound ends, waiting for the peer's credit meanwhile (a peer that
     * reads opens its windows; one that does not holds the close to its bound). False once
     * closing, or without a live outbound pipe.
     */
    public boolean flushable() {
        if (isClosing() || !liveDirections()[0]) return false;
        for (Stream st : streams.live()) {
            if (!st.hasSend) continue;
            StreamSend.SendPart sp = st.snd;
            // A reset the application made before the close reaches the peer ahead of it, as a FIN does.
            if (sp.reset()) {
                if (!sp.resetSent()) return true;
                continue;
            }
            if (sp.state() >= Stream.SEND_DATA_SENT) continue;
            if (st.unsent() > 0 || (sp.finWritten() && !sp.finSent())) return true;
        }
        return false;
    }

    /**
     * The application's close at t: {@link #close} plus the bound limit, from t, on
     * the hand-off of CONNECTION_CLOSE to the carriers; a carrier that has not taken it by then is
     * closed with the others and the session ends locally. A repeated call adds nothing and
     * never extends the bound.
     */
    public void closeBounded(long code, byte[] reason, long limit, long t) {
        deadlines.changed(ProtocolTimeline.ALL);
        setNow(t);
        boolean first = !isClosing();
        close(code, reason);
        // The caller's bound is the hand-off's; a coverage wait comes first and the hand-off bound starts at its end.
        if (first && state == SessionState.CLOSING && !closeHandedOff() && !life.closeWait()) life.handOffBy(Time.add(now, limit));
    }

    /**
     * Whether a close no longer waits for the carriers: CONNECTION_CLOSE was written to every
     * live pipe that took it, and no pipe that can carry it still lacks it; also once CLOSED or
     * when the close sends no CONNECTION_CLOSE. False before the close.
     */
    public boolean closeHandedOff() {
        if (state.compareTo(SessionState.CLOSING) < 0) return false;
        if (state == SessionState.CLOSED || !life.closeQueued()) return true;
        if (life.closePending()) return false;
        return handOffOver || pipes.closeShut();
    }

    /** Registers sig until {@link #closeHandedOff}; true when it holds now. */
    public boolean waitCloseHandedOff(Waiter sig) {
        if (closeHandedOff()) return true;
        handOffW.add(sig);
        return false;
    }

    public void cancelCloseHandOffWait(Waiter sig) { handOffW.remove(sig); }

    private void wakeHandOff() {
        if (lingerOnHandOff && (state == SessionState.CLOSED || pipes.closeShut())) {
            lingerOnHandOff = false;
            life.linger(Time.add(now, lingerTimeout()));
        }
        if (!handOffW.isEmpty() && closeHandedOff()) handOffW.wakeAll(outbox);
    }

    /** The longest a close (flush and hand-off of CONNECTION_CLOSE) waits for the carriers before they are closed: the one authority, read by the session facade too; tests shorten it. */
    public static volatile long closeLimit = java.util.concurrent.TimeUnit.SECONDS.toNanos(1);

    /**
     * The longest a public close can take after its flush: the coverage wait (at most the linger
     * timeout, 2 s or the test value) and then the hand-off. The core enforces it; the facade waits
     * this long only to cover a loop that lags.
     */
    public static long closeCeiling() {
        return Math.max(MAX_LINGER, lingerForTests) + closeLimit;
    }

    /**
     * CONNECTION_CLOSE was queued on every carrier, and the linger waits for the hand-off: its writes
     * completed and the write sides shut. A linger running meanwhile could expire during a slow shutdown
     * and end the session (aborting the carriers) before the hand-off, so the hand-off's own bound
     * ({@link #closeLimit}) is the only one until then.
     */
    private boolean lingerOnHandOff;

    /** Ends a close whose carriers did not take CONNECTION_CLOSE in time: CLOSED, every carrier closed. */
    public void endClose() {
        deadlines.changed(ProtocolTimeline.ALL);
        if (state != SessionState.CLOSING || closeHandedOff()) return;
        if (!life.closeQueued() || life.closePending() || !pipes.closeWritten()) {
            finishClose();
            return;
        }
        // CONNECTION_CLOSE went out but a write side never reported its shutdown (its carrier went
        // away with the write, or it is slow): the hand-off is over, and the linger of step 3 bounds
        // what remains, the read drain included.
        handOffOver = true;
        lingerOnHandOff = false;
        life.linger(Time.add(now, lingerTimeout()));
        wakeHandOff();
        checkLingerDone();
    }

    /** The hand-off bound passed after CONNECTION_CLOSE was written: the close no longer waits for a write side's shutdown. */
    private boolean handOffOver;

    /** The negotiated idle timeout, 0 when none. */
    private long idleTimeout() {
        long a = cfg.localIdleTimeout(), b = Time.millis(peer.maxIdleTimeoutMillis);
        if (a == 0) return b;
        if (b == 0) return a;
        return Math.min(a, b);
    }

    /** The session SRTT: the smoothed RTT over its pipes. */
    long srtt() {
        if (cfg.test().rtt() > 0 && ack == null) return cfg.test().rtt();
        if (ack != null) {
            long r = srttMax();
            if (r > 0) return r;
        }
        if (cfg.test().rtt() > 0) return cfg.test().rtt();
        return 333 * Time.MILLISECOND;
    }

    public static final long MAX_LINGER = 2 * Time.SECOND;

    /** A fixed linger timeout in nanoseconds for tests (0: none); set through Diagnostics. */
    public static volatile long lingerForTests;

    /** min(2 s, max(250 ms, 3 x SRTT)). */
    public long lingerTimeout() {
        long fixed = lingerForTests;
        if (fixed > 0) return fixed;
        return Math.clamp(Time.mul(srtt(), 3), 250 * Time.MILLISECOND, MAX_LINGER);
    }

    // ---- closing ----

    /** A lane 0 write failure awaiting the peer's close on the surviving inbound lane. */
    private SessionError pendingWriteFailure;

    /**
     * Enters CLOSING and queues CONNECTION_CLOSE fr on every live outbound pipe; with fr
     * null, or without a live outbound pipe, closes the carriers at once. The first error
     * wins.
     */
    void closeWith(SessionError e, CloseFrame fr) {
        deadlines.changed(ProtocolTimeline.ALL);
        if (isClosing()) return;
        pendingWriteFailure = null;
        err = e;
        state = SessionState.CLOSING;
        for (Pipe p : pipes.all()) if (p.shape() != null) p.shape().cancelIntents();
        endingW.wakeAll(outbox);
        failAllStreams();
        wakeAll();
        if (closeReceived) streams.startDrain();
        if (fr == null || !pipes.hasLiveOutbound()) {
            if (closeReceived) {
                // No pipe takes this side's close, but the peer's was read: the linger runs from
                // here, for the inbound ends and the application's reads of what came before it.
                life.linger(Time.add(now, lingerTimeout()));
                emitClosedEvent();
                checkLingerDone();
                return;
            }
            finishClose();
            return;
        }
        boolean holdForLanes = !e.remote() && (fr.app() || fr.code() == TransportError.NO_ERROR) && !closeCovered();
        long laneWait = holdForLanes ? Time.add(now, lingerTimeout()) : Time.NEVER;
        life.closeQueued(fr, laneWait);
        // Every close is bounded: a carrier whose write side is stuck (a peer that does not read)
        // never takes CONNECTION_CLOSE, and without this bound nothing would end the session.
        life.handOffBy(Time.add(laneWait == Time.NEVER ? now : laneWait, closeLimit));
        pipes.wakeWriters();
    }

    /**
     * Ends a session whose peer has not been heard from without CONNECTION_CLOSE: its carriers
     * close at once, as when the handshake times out. False when the peer was heard from or the
     * session is closing already.
     */
    public boolean abandon() {
        if (peerSeen || isClosing()) return false;
        closeWith(SessionError.of(SessionError.Kind.HANDSHAKE_TIMEOUT, false), null);
        return true;
    }

    /**
     * Whether everything handed to the transport is covered for CONNECTION_CLOSE (SPEC.md 6.5 step
     * 1): the records it must not overtake are acknowledged and, in a multipath session, nothing
     * lost waits to be sent again while a pipe could carry it.
     */
    boolean closeCovered() {
        if (!lanes.covered()) return false;
        return !feat.multipath() || !pipes.hasLiveOutbound() || (!tx.reinjectPending() && !ctl.pending());
    }

    /** Ends the close wait once {@link #closeCovered} holds; the writers then take CONNECTION_CLOSE. */
    void recheckCloseWait() {
        if (!life.closeWait() || !closeCovered()) return;
        endCloseWait();
    }

    private void endCloseWait() {
        if (!life.waitingLanes()) return;
        life.lanesDone(now);
        pipes.wakeWriters();
    }

    /** Closes the session with an application error. */
    public void close(long code, byte[] reason) {
        CloseFrame f = CloseFrame.of(true, code, 0, reason);
        closeWith(SessionError.application(code, new String(f.reason(), java.nio.charset.StandardCharsets.UTF_8), false), f);
    }

    /** Closes after a failure inside the endpoint: INTERNAL_ERROR. */
    public void forceClose(Throwable cause) {
        closeTransport(SessionError.transport(TransportError.INTERNAL_ERROR, 0, "internal error", "", false));
    }

    /** Closes with a locally detected transport error. */
    void failLocal(WireException e) {
        closeTransport(SessionError.transport(e.code(), e.frameType(), e.reason(), e.details(), false));
    }

    void closeTransport(SessionError e) {
        String rs = e.details().isEmpty() ? e.reason() : e.reason() + "\n" + e.details();
        closeWith(e, CloseFrame.of(false, e.code(), e.frameType(), rs.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    /** Closes without CONNECTION_CLOSE; graceful shuts down the write sides and lingers. */
    void closeSilently(SessionError e, boolean graceful) {
        deadlines.changed(ProtocolTimeline.ALL);
        if (isClosing()) return;
        pendingWriteFailure = null;
        err = e;
        state = SessionState.CLOSING;
        for (Pipe p : pipes.all()) if (p.shape() != null) p.shape().cancelIntents();
        endingW.wakeAll(outbox);
        failAllStreams();
        wakeAll();
        if (graceful) {
            pipes.shutdownOut();
            life.linger(Time.add(now, lingerTimeout()));
            emitClosedEvent();
            return;
        }
        finishClose();
    }

    /** Releases everything and enters CLOSED; the carriers are aborted. */
    void finishClose() { finishClose(false); }

    /**
     * Releases everything and enters CLOSED. graceful: the linger ended with every inbound pipe
     * (after this side's CONNECTION_CLOSE and FIN), so the carriers close without an abort; an
     * abort (a TCP RST) comes only from a close that did not linger or whose linger expired.
     */
    void finishClose(boolean graceful) {
        if (state == SessionState.CLOSED) return;
        if (err != null && state != SessionState.CLOSING) failAllStreams();
        state = SessionState.CLOSED;
        for (Pipe p : pipes.all()) if (p.shape() != null) p.shape().cancelIntents();
        clearJoinSecrets();
        // The end first: what the pipes' release reports after it never reaches the event stream or the tracer.
        emitClosedEvent();
        if (graceful) pipes.closeAll(); else pipes.abortAll();
        path.closeTimers();
        if (path instanceof PathSession mp) mp.retention.close();
        pipes.releaseOutbound();
        lanes.closeTimers();
        if (pacer != null) pacer.closeTimers();
        if (ack != null) ack.clearTimers();
        life.clearTimers();
        grace = Time.NEVER;
        closedW.wakeAll(outbox);
        handOffW.wakeAll(outbox);
        releaseAll();
        conn.releaseReservation();
        wakeAll();
    }

    void emitClosedEvent() {
        if (!life.markClosedEmitted()) return;
        emit(Event.closed(err));
    }

    // ---- events, actions, wake-ups ----

    void emit(Event e) {
        switch (e.kind()) {
            case PIPE_STATE, PIPE_HEALTH, PIPE_CLOSED, ESTABLISHED, CLOSED, EPOCH_COMMITTED -> capacity.changed();
            default -> { }
        }
        if (quality != null && quality.floorSet() && (e.kind() == Event.Kind.PIPE_STATE || e.kind() == Event.Kind.PIPE_PRIORITY)) quality.checkFloor();
        if (e.kind() == Event.Kind.PIPE_STATE) {
            path.wakePeerAttachedWaiters();
            if (state == SessionState.CLOSING) wakeHandOff();
        }
        outbox.event(e);
        if (e.kind() == Event.Kind.PIPE_STATE) noteDatagramPath(); // the datagram pipe may have changed
    }

    /** The path datagram payload of a session datagram last reported (DATAGRAM_PATH). */
    private int datagramPath, datagramNative = -1;

    /** Reports a change of a session datagram's path datagram payload. */
    private void noteDatagramPath() {
        deadlines.changed(ProtocolTimeline.DATAGRAM | ProtocolTimeline.Reason.SHAPING.bit());
        if (!peerSeen) return;
        int v = dg.pathPayload(1), nativePayload = dg.nativePayload(1);
        if (v == datagramPath && nativePayload == datagramNative) return;
        datagramPath = v;
        datagramNative = nativePayload;
        emit(Event.datagramPath(v, nativePayload));
    }


    void act(Action a) {
        outbox.action(a);
    }


    /** Queues sig for delivery at the end of the driver's round. */
    void wake(Waiter w) {
        outbox.wake(w);
    }


    /** Queues every waiter of the session (close, GOAWAY). */
    void wakeAll() {
        streams.wakeObservers();
        streams.wakeWaiters();
        estW.wakeAll(outbox);
        if (path != null) path.wakePeerAttachedWaiters();
        if (path instanceof PathSession mp) mp.wakeEpochWaiters();
        ka.wakeAll();
        dg.wakeAll();
    }

    // ---- streams ----

    /** Remaining adapter lane allowance; application openers always take precedence. */
    public long nestedLaneAllowance() {
        if (!peerSeen || isClosing() || streams.openersWaiting(true)) return 0;
        return pipes.spreadable() ? peer.maxStreamsBidi / 2 : 0;
    }

    void failAllStreams() {
        streams.dropUnaccepted();
        streams.wakeObservers();
        send.wakeCapacity();
    }

    /** Drops every buffer of the session (CLOSED). */
    void releaseAll() {
        recv.closeLoanWaits();
        streams.releaseBuffers();
        dg.releaseAll();
        conn.clearBuffers();
        sched.clear();
        ctl.clear();
    }

    // ---- flow control ----

    /** Session credit is due: MAX_DATA goes out with the next control. */
    private void maxDataDue() {
        ctl.wantMaxData();
        pipes.wakeWriters();
    }

    /** Absolute send-buffer max from the memory account. */
    long sendBufferLimit() {
        if (cfg.sendBufferMax() > 0) return Math.max(cfg.sendBufferMax(), cfg.sendBuffer());
        long room = memoryHeadroom();
        long held = conn.sendHeld();
        long cap = room >= Long.MAX_VALUE - held ? Long.MAX_VALUE : held + room;
        return Math.max(Math.max(64L << 20, cap), cfg.sendBuffer());
    }

    /** The single effective buffer used for fair sharing and affinity load. */
    long sessionSendBuffer() {
        return path.sendBuffer(cfg.sendBuffer());
    }

    // ---- input ----

    /**
     * Processes the complete records in data[pos, limit), read from an inbound lane, and
     * returns how many bytes it consumed; the caller keeps the rest (a partial record).
     * buf owns data; pieces of stream data may keep a reference to it.
     */
    public int receive(long pipeId, long lane, ReadBuf buf, java.nio.ByteBuffer data, int pos, int limit, long t) {
        return receive(pipeId, lane, buf, data, pos, limit, t, false);
    }

    /** Processes records whose carrier has identified the early-data provenance. */
    public int receive(long pipeId, long lane, ReadBuf buf, java.nio.ByteBuffer data, int pos, int limit, long t,
                       boolean carrierEarly) {
        return receive(pipeId, lane, buf, data, null, pos, limit, t, carrierEarly);
    }

    /** Processes the byte carrier's owned ranges without joining them into a record buffer. */
    public int receive(long pipeId, long lane, ReceiveSpans spans, long t) {
        return receive(pipeId, lane, null, null, spans, 0, spans.size(), t, false);
    }

    private int receive(long pipeId, long lane, ReadBuf buf, java.nio.ByteBuffer data, ReceiveSpans spans,
                        int pos, int limit, long t, boolean carrierEarly) {
        setNow(t);
        if (handshaking(pipeId)) return limit - pos; // nothing read before Proven reaches the session
        path.pickInput();
        return rx.receive(pipes.get(pipeId), lane, buf, data, spans, pos, limit, now, carrierEarly);
    }

    /** Processes one message of a message-framed carrier. */
    public void receiveMessage(long pipeId, long lane, ReadBuf buf, java.nio.ByteBuffer data, int pos, int limit, long t) {
        setNow(t);
        if (handshaking(pipeId)) return; // nothing read before Proven reaches the session
        rx.receiveMessage(pipes.get(pipeId), lane, buf, data, pos, limit, now);
    }

    private boolean handshaking(long pipeId) {
        Pipe p = pipes.get(pipeId);
        return p != null && p.mp() != null && p.mp().handshaking();
    }

    /** A record was applied: acknowledgement, attachment liveness and the idle timer. */
    private void recorded(Pipe p, Pipe.InLane il, long posBytes, boolean elicit, boolean pipePing, boolean pingOnly, boolean traffic,
                          boolean activity) {
        deadlines.changed(ProtocolTimeline.RECORD);
        // A PIPE_PING alone in a quiet session is answered on its pipe pair only.
        boolean pairOnly = pingOnly && p.mp() != null && mp().pingAnsweredOnPair(p);
        if (ack != null && ack.onRecord(p, il, posBytes, elicit && !pairOnly, pipePing && !pairOnly, now)) pipes.wakeWriters();
        memberBytes(p, false, posBytes);
        if (traffic) p.traffic(now);
        if (p.mp() != null) mp().recorded(p, il, traffic);
        if (!feat.acked() || activity) ka.activity(now);
    }

    /** Applies one decoded frame. */
    void dispatch(Pipe p, io.zmux.internal.wire.Frame f, ReadBuf buf) throws WireException {
        boolean multi = feat.multipath();
        switch (f.kind) {
            case PADDING, TRANSPORT_PARAMETERS -> { }
            case STREAM -> {
                if (!recv.onStream(f, buf) && !multi) charge();
            }
            case RESET_STREAM, RESET_STREAM_AT -> {
                if (!recv.onReset(f, f.kind == io.zmux.internal.wire.FrameKind.RESET_STREAM_AT) && !multi) charge();
            }
            case STOP_SENDING -> {
                Stream st = streams.lookup(f.streamId, Stream.isBidiId(f.streamId));
                if (st == null) return;
                if (!st.hasSend) {
                    throw WireException.withDetails(TransportError.STREAM_STATE_ERROR, "STOP_SENDING for a receive-only stream", "stream", st.tab.id());
                }
                if (st.snd.stopped() && !multi && st.snd.state() != Stream.SEND_DATA_RECVD && st.snd.state() != Stream.SEND_RESET_RECVD) charge();
                send.onStopSending(st, f.code);
            }
            case MAX_DATA -> {
                if (conn.onMaxData(f.value, now)) {
                    pipes.wakeWriters();
                    send.wakeCapacity();
                } else if (!multi) {
                    charge();
                }
            }
            case MAX_STREAM_DATA -> {
                Stream st = streams.lookup(f.streamId, Stream.isBidiId(f.streamId));
                if (st == null) return;
                if (!st.hasSend) {
                    throw WireException.withDetails(TransportError.STREAM_STATE_ERROR, "MAX_STREAM_DATA for a receive-only stream", "stream", st.tab.id());
                }
                if (!send.onSendCredit(st, f.value) && !multi && st.snd.state() < Stream.SEND_DATA_RECVD) charge();
            }
            case MAX_STREAMS -> {
                if (!streams.onMaxStreams(f.bidi, f.value) && !multi) charge();
            }
            case DATA_BLOCKED -> {
                if (f.value > conn.advertised()) {
                    throw WireException.withDetails(TransportError.PROTOCOL_VIOLATION, "DATA_BLOCKED above the advertised limit", "value", f.value);
                }
                if (conn.onDataBlocked(f.value) && !multi) charge();
            }
            case STREAM_DATA_BLOCKED -> {
                if (!recv.onDataBlocked(f) && !multi) charge();
            }
            case STREAMS_BLOCKED -> {
                if (!streams.onStreamsBlocked(f.bidi, f.value) && !multi) charge();
            }
            case CONNECTION_CLOSE -> onClose(f);
            case DATAGRAM -> {
                dg.onFrame(f, flows, p.id, false);
                if (feat.flows()) pipes.wakeWriters();
            }
            case DATAGRAM_ACK -> dg.feedback.acknowledge(f, now);
            case PING -> ka.onPing(f, feat.multipath(), now);
            case GOAWAY -> onGoAway(f);
            case PRIORITY_UPDATE -> onPriorityUpdate(f);
            case PIPE_ACK -> onPipeAck(p, f);
            case PIPE_LANE -> throw WireException.protocol("PIPE_LANE out of place");
            case PIPE_PING -> {
                charge(); // Every PIPE_PING.
                path.onPathFrame(p, f);
            }
            case PIPE_ABANDON, PIPE_STATUS, SESSION_EPOCH -> path.onPathFrame(p, f);
        }
    }

    /** A received CONNECTION_CLOSE. */
    void onClose(io.zmux.internal.wire.Frame f) {
        String reason = new String(f.dataBytes(), java.nio.charset.StandardCharsets.UTF_8);
        SessionError e;
        if (f.application) {
            e = SessionError.application(f.code, reason, true);
        } else if (f.code == TransportError.IDLE_TIMEOUT) {
            e = new SessionError(SessionError.Kind.IDLE, f.code, f.errorFrameType, "", "", null, true);
        } else {
            int i = reason.lastIndexOf('\n');
            e = i < 0 ? SessionError.transport(f.code, f.errorFrameType, reason, "", true)
                    : SessionError.transport(f.code, f.errorFrameType, reason.substring(0, i), reason.substring(i + 1), true);
        }
        closeReceived = !isClosing();
        closeWith(e, CloseFrame.of(false, TransportError.NO_ERROR, 0, null));
    }

    /** The session ends by the peer's CONNECTION_CLOSE: what arrived before it is drained by the application's reads. */
    private boolean closeReceived;

    /**
     * Reports the end of an inbound lane: graceful (eof) or failed; partial when it ended
     * inside a record. The driver calls it after feeding every complete record it read.
     */
    public void pipeEnded(long pipeId, long lane, boolean graceful, boolean partial, Throwable cause, long t) {
        deadlines.changed(ProtocolTimeline.ALL);
        setNow(t);
        Pipe p = pipes.get(pipeId);
        if (p == null || p.in() == null) return;
        Pipe.InLane il = p.in().lane(lane);
        if (il == null || !rx.laneEnded(il)) return;
        if (ack != null && (!isClosing() || life.waitingLanes()) && ack.laneEnded(il)) pipes.wakeWriters();
        boolean earlyEnd = il.lane > 0 && il.rx.recs() <= 1;
        boolean allEnded = true;
        for (Pipe.InLane l : p.in().lanes) if (!l.rx.ended()) { allEnded = false; break; }
        if (allEnded) p.in().end();
        boolean failed = (!graceful || partial) && !earlyEnd;
        if (isClosing()) {
            // During the close wait a failed pipe is a failure as before: what it carried is reinjected.
            if (failed && life.closeWait() && feat.multipath()) path.inboundEnded(p, true, allEnded, cause);
            checkLingerDone();
            return;
        }
        if (lane == 0 && pendingWriteFailure != null) {
            closeWith(pendingWriteFailure, null);
            return;
        }
        path.inboundEnded(p, failed, allEnded, cause);
    }

    /** The single pipe's inbound side ended. */
    private void singleInboundEnded(boolean failed, boolean allEnded, Throwable cause) {
        if (failed) {
            closeWith(SessionError.pipesLost(cause), null);
            return;
        }
        if (!allEnded) return;
        if (peerSeen && !feat.acked()) {
            closeSilently(SessionError.of(SessionError.Kind.IDLE, true), true);
            return;
        }
        closeWith(SessionError.pipesLost(null), null);
    }

    /** Reports a failed write on an outbound lane. */
    void pipeWriteFailed(long pipeId, long lane, Throwable cause) {
        deadlines.changed(ProtocolTimeline.ALL);
        if (isClosing()) {
            if (state == SessionState.CLOSING) {
                // During the close wait a failed pipe is a failure as before: what it carried is reinjected.
                if (life.closeWait() && feat.multipath()) path.outboundFailed(pipeId, cause);
                Pipe p = pipes.get(pipeId);
                if (p != null && p.out() != null && !p.out().failed()) { p.out().fail(); pipes.changed(); }
                checkLingerDone();
                wakeHandOff();
            }
            return;
        }
        if (lane > 0) {
            Pipe p = pipes.get(pipeId);
            if (p != null && p.out() != null) {
                Pipe.OutLane ol = p.out().lane(lane);
                if (ol != null && !ol.st.confirmed()) {
                    lanes.refused(p, ol);
                    return;
                }
            }
        }
        if (lane == 0 && peerSeen && !feat.multipath()) {
            Pipe p = pipes.get(pipeId);
            if (p != null && p.out() != null && p.in() != null && !p.in().first().rx.ended()) {
                if (pendingWriteFailure == null) {
                    p.out().fail();
                    pipes.changed();
                    pendingWriteFailure = SessionError.pipesLost(cause);
                    life.linger(Time.add(now, lingerTimeout()));
                }
                return;
            }
        }
        path.outboundFailed(pipeId, cause);
    }

    // ---- output ----

    /** The write batch. */
    public static final int DEFAULT_BATCH = 256 << 10;
    public static final long FRAME_HISTORY_WORKSPACE = FrameHistory.WORKSPACE;


    /**
     * Fills b with records for the outbound lane, at most max bytes; false when nothing
     * is due. The driver writes the batch and reports it with {@link #writeDone}.
     */
    public boolean pollTransmit(long pipeId, long lane, Batch b, int max, long t) {
        return pollReady(pipeId, lane, b, max, 0, t, null);
    }

    /** Claims ready work and samples the carrier only after the lane can build a batch. */
    public boolean pollReady(long pipeId, long lane, Batch b, int max, int recordWorkspace, long t, java.util.function.LongSupplier unsent) {
        setNow(t);
        int width = dg.sequenceWidth();
        boolean written = tx.poll(pipeId, lane, b, max, recordWorkspace, t, unsent);
        if (written) {
            deadlines.changed(ProtocolTimeline.RECORD);
            if (width != dg.sequenceWidth()) noteDatagramPath();
        } else if (path instanceof PathSession || pipes.shaping() != null) {
            deadlines.changed(ProtocolTimeline.Reason.CUSTOM_PICK.bit() | ProtocolTimeline.Reason.SHAPING.bit());
        }
        return written;
    }

    /** The carrier of pipe's lane shut its write side (after CONNECTION_CLOSE: the close is handed off). */
    public void outputShut(long pipeId, long lane, long t) {
        deadlines.changed(ProtocolTimeline.ALL);
        setNow(t);
        tx.outputShut(pipeId, lane);
        if (state == SessionState.CLOSING) {
            wakeHandOff();
            checkLingerDone();
        }
    }

    public void writeProgress(Batch b, int prefix, long t) {
        deadlines.changed(ProtocolTimeline.Reason.SHAPING.bit());
        setNow(t);
        b.progress(tx, pipes.shaping(), prefix, false);
    }

    public void writeDone(Batch b, Throwable cause, long t) {
        writeDone(b, cause == null ? b.bytes() : 0, cause, t);
    }

    public void writeDone(Batch b, int confirmed, Throwable cause, long t) {
        deadlines.changed(ProtocolTimeline.PATH | ProtocolTimeline.Reason.LIFE.bit() | ProtocolTimeline.Reason.SHAPING.bit());
        setNow(t);
        Pipe p = pipes.get(b.pipe());
        Pipe.OutLane current = p == null || p.out() == null ? null : p.out().lane(b.lane());
        if (current != null && current == b.owner() && p.member().rotating()) {
            memberBytes(p, true, b.tally(confirmed).bytes());
        }
        tx.written(b, confirmed, cause == null, now);
        if (cause != null && current != b.owner()) return;
        if (cause != null) pipeWriteFailed(b.pipe(), b.lane(), cause);
        if (state == SessionState.CLOSING) wakeHandOff();
    }

    /**
     * The carrier's unsent count of an outbound lane as the driver queried it (outside the
     * core), -1 when the adapter reports none.
     */
    public void unsentReported(long pipeId, long lane, long unsent) {
        tx.setUnsent(pipeId, lane, unsent);
        deadlines.changed(ProtocolTimeline.Reason.PREFIX_REPAIR.bit());
    }

    /**
     * records &times; lost / sent rounded up for non-negative counts and sent &gt; 0, exact and
     * without overflowing the product; Long.MAX_VALUE when the quotient does not fit.
     */
    static long lostRecords(long lost, long records, long sent) {
        long n = lost * records;
        if (Math.multiplyHigh(lost, records) == 0 && n >= 0) return Math.ceilDiv(n, sent);
        java.math.BigInteger q = java.math.BigInteger.valueOf(lost).multiply(java.math.BigInteger.valueOf(records))
                .add(java.math.BigInteger.valueOf(sent - 1)).divide(java.math.BigInteger.valueOf(sent));
        return q.bitLength() < 64 ? q.longValue() : Long.MAX_VALUE;
    }

    /** Starts the linger timer once CONNECTION_CLOSE was handed to every live outbound pipe. */
    void closeSentOn() {
        if (!pipes.closeSentEverywhere()) return;
        pipes.shutdownOut();
        // The linger runs from the hand-off of CONNECTION_CLOSE (written, write side shut), not from its queueing.
        lingerOnHandOff = !pipes.closeShut();
        life.closeSent(lingerOnHandOff ? Time.NEVER : Time.add(now, lingerTimeout()));
        emitClosedEvent();
        checkLingerDone();
    }

    /** Advances the physical linger and enters CLOSED once the application drain also ends. */
    void checkLingerDone() {
        if (state != SessionState.CLOSING) return;
        // The physical carrier can close while the accepted streams retain their unread data.
        pipes.endLingeringPipes();
        if (life.closePending()) {
            // A pipe that failed is no longer waited for.
            recheckCloseWait();
            if (!life.waitingLanes()) closeSentOn();
            return;
        }
        // The carriers close once the peer ended its directions and this side's write sides are
        // shut (FIN or close_notify out, not only requested); the linger timer bounds the wait.
        if (!pipes.inboundEnded()) return;
        // After the peer's close, what arrived before it stays readable while the application
        // reads it, within the same linger bound: the peer's close follows its data.
        if (closeReceived && streams.unreadHeld()) return;
        // A write side whose shutdown is slow, or never reported (its carrier went away with the
        // write), is bounded by the hand-off bound, which then ends the session.
        if (pipes.closeShut()) finishClose(true);
        else if (handOffOver) finishClose(); // nothing more to wait for: the write side's shutdown never came
    }

    // ---- pipes ----

    /** Records whether a carrier half accepts bytes now. */
    public void setWritable(long pipeId, long lane, boolean writable, long t) {
        deadlines.changed(ProtocolTimeline.PATH);
        setNow(t);
        tx.setWritable(pipeId, lane, writable, t);
    }

    /** The protocol's pipe state names (out, in), or null when unknown. */
    public PipeInfo.States pipeState(long id) {
        Pipe p = pipes.get(id);
        if (p == null) return null;
        return path.pipeState(p);
    }

    /** Where a local join stands. */
    public enum JoinProgress { CONFIRMED, PENDING, GONE }

    /** Whether the join of pipe id is confirmed, still pending, or ended without confirmation. */
    public JoinProgress joinProgress(long id) {
        Pipe p = pipes.get(id);
        return p == null ? JoinProgress.GONE : path.joinProgress(p);
    }

    /** Captures both directions of a live or retiring attachment without querying the carrier. */
    public io.zmux.PipeHealth pipeHealth(long id) {
        Pipe pipe = pipes.get(id);
        return pipe == null ? null : pipeHealth(pipe);
    }

    io.zmux.PipeHealth pipeHealth(Pipe pipe) {
        if (path instanceof PathSession mp) {
            io.zmux.PipeHealth snapshot = pipe.mp().projection.snapshot();
            if (snapshot == null) snapshot = mp.syncHealth(pipe);
            var joined = snapshot.joinedAt();
            var age = joined.isEmpty() ? java.time.Duration.ZERO : HealthProjection.sinceJoin(now, joined.getAsLong());
            return new io.zmux.PipeHealth(snapshot.seq(), snapshot.send(), snapshot.receive(), snapshot.peerAttached(),
                    joined, snapshot.attachedAt(), age, snapshot.failure());
        }
        var ended = new io.zmux.DirectionHealth(io.zmux.DirectionHealth.State.ENDED,
                io.zmux.DirectionHealth.Reason.SESSION_CLOSING, now, false, false,
                java.util.OptionalLong.empty(), 0, io.zmux.DirectionHealth.ProofKind.NONE);
        var established = establishedAt == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(establishedAt);
        var active = new io.zmux.DirectionHealth(peerSeen ? io.zmux.DirectionHealth.State.USABLE : io.zmux.DirectionHealth.State.JOINING,
                peerSeen ? io.zmux.DirectionHealth.Reason.PROGRESS : io.zmux.DirectionHealth.Reason.AWAIT_ATTACHMENT,
                established.orElse(start), peerSeen, peerSeen, established, 0, io.zmux.DirectionHealth.ProofKind.NONE);
        var send = pipe.out() == null ? HealthProjection.ABSENT : isClosing() || pipe.out().failed() ? ended : active;
        var receive = pipe.in() == null ? HealthProjection.ABSENT : isClosing() ? ended : active;
        return new io.zmux.PipeHealth(0, send, receive, peerSeen, established, established,
                established.isEmpty() ? java.time.Duration.ZERO : HealthProjection.sinceJoin(now, established.getAsLong()), null);
    }

    /** Asks the probe owner for evidence to settle this attachment in its current observation interval. */
    public void observeStability(long id) {
        Pipe pipe = pipes.get(id);
        if (pipe != null && path instanceof PathSession mp) mp.observeStability(pipe);
    }

    public int pipeVersion() { return pipes.version(); }

    /** Aggregate of a pool member's pipe health and directions. */
    public record PipeGroup(boolean alive, boolean usable, boolean settled, boolean outbound, boolean inbound) {}

    /** A pool member's state on one owner turn. */
    public PipeGroup pipeGroup(long[] ids) {
        boolean alive = true, usable = true, settled = true, outbound = false, inbound = false;
        for (long id : ids) {
            var h = pipeHealth(id);
            if (h == null) return new PipeGroup(false, false, false, false, false);
            alive &= h.alive();
            usable &= h.usable();
            settled &= h.settled();
            outbound |= h.send().state() != io.zmux.DirectionHealth.State.ABSENT;
            inbound |= h.receive().state() != io.zmux.DirectionHealth.State.ABSENT;
        }
        return new PipeGroup(alive, usable, settled, outbound, inbound);
    }

    /** Whether a pipe is ready to carry new data on all its directions. */
    public boolean pipeUsable(long id) {
        Pipe p = pipes.get(id);
        return p != null && !isClosing() && path.pipeUsable(p);
    }

    /** Whether the peer is known to hold pipe id (see {@link PathLayer#peerAttached}); false once it is gone. */
    public boolean pipePeerAttached(long id) {
        Pipe p = pipes.get(id);
        return p != null && (p.mp() == null || !p.mp().health.retired()) && path.peerAttached(p);
    }

    /**
     * One attempt of a wait for the peer to attach pipe id: true when it has, null after
     * registering sig; throws once the pipe is gone (INVALID) or the session ended.
     */
    public Boolean waitPeerAttached(long id, Waiter sig) throws CoreException {
        Pipe p = pipes.get(id);
        if (p == null || (p.mp() != null && p.mp().health.retired())) throw CoreException.of(CoreException.Reason.INVALID);
        if (path.peerAttached(p)) return Boolean.TRUE;
        // A pipe that failed, closed or is draining is gone for the wait: its retirement may
        // wait for a peer that never learns of it.
        if (p.mp() != null && p.mp().ended(p.out() != null)) {
            throw p.mp().health.aborted() ? CoreException.pipeAborted(id) : CoreException.of(CoreException.Reason.INVALID);
        }
        if (err != null) throw CoreException.session(err);
        path.waitPeerAttached(sig);
        return null;
    }

    public void cancelPeerAttachedWait(Waiter sig) { path.cancelPeerAttachedWait(sig); }

    /** One attempt of a wait for pipe id to be removed: true once it is, false after registering sig. */
    public boolean waitPipeRemoved(long id, Waiter sig) {
        Pipe p = pipes.get(id);
        if (p == null || (p.mp() != null && p.mp().health.retired())) return true;
        pipes.waitRemoved(sig);
        return false;
    }

    public void cancelPipeRemovedWait(Waiter sig) { pipes.cancelRemovedWait(sig); }

    private static io.zmux.PipeStats.SettingSource settingSource(Attach a, boolean local, boolean peerRequest) {
        if (a == null) return io.zmux.PipeStats.SettingSource.DEFAULT;
        return local ? io.zmux.PipeStats.SettingSource.LOCAL
                : peerRequest ? io.zmux.PipeStats.SettingSource.PEER : io.zmux.PipeStats.SettingSource.DEFAULT;
    }

    private static java.time.Duration nanos(long n) {
        return java.time.Duration.ofNanos(Math.max(0, n));
    }

    /** The RTT estimate of pipe p's outbound direction: {smoothed, min, variation, latest}, null without a sample; src[0] its source. */
    private long[] rttOf(Pipe p, io.zmux.PipeStats.RttSource[] src) {
        if (p.out() == null) return null;
        RttEstimator r = p.out().snd.rtt();
        long[] v = r.has() ? new long[] {r.smoothed(), r.min(), r.rttvar(), r.latest()} : null;
        src[0] = v != null ? io.zmux.PipeStats.RttSource.ACK : null;
        if (p.mp() == null && tuner.carrierMinRtt() > 0) {
            if (v == null) v = new long[] {tuner.carrierMinRtt(), tuner.carrierMinRtt(), -1, tuner.carrierMinRtt()};
            v[1] = tuner.carrierMinRtt();
            src[0] = io.zmux.PipeStats.RttSource.KERNEL;
        }
        if (p.mp() == null && v == null && ka.lastRtt() > 0) {
            v = new long[] {ka.lastRtt(), ka.lastRtt(), -1, ka.lastRtt()};
            src[0] = io.zmux.PipeStats.RttSource.PING;
        }
        return v;
    }

    /**
     * The statistics of attachment id, at snapshotTime of the core's clock,
     * whose {@link System#nanoTime()} reading is nanoTime; null once it is missing or retired.
     */
    public io.zmux.PipeStats pipeStats(long id, long snapshotTime, long nanoTime) {
        Pipe p = pipes.get(id);
        if (p == null) return null;
        PipeInfo.States state = path.pipeState(p);
        Attach a = p.mp();
        boolean acks = a != null || feat.pipeAck();
        long sent = 0, received = 0, recordsIn = 0, acked = 0, recordsOut = 0, inflight = 0, lost = 0;
        double rate = 0;
        long unsent = -1;
        boolean writable = false;
        long lanesOut = 0, lanesIn = 0;
        long lanesRefused = 0;
        if (p.out() != null) {
            for (Pipe.OutLane l : p.out().lanes) {
                sent += l.tx.pos();
                if (l.tx.unsent() >= 0) unsent = Math.max(unsent, 0) + l.tx.unsent();
            }
            lanesOut = p.out().lanes.size();
            lanesRefused = p.out().lanesRefused();
            acked = p.out().snd.cum();
            lost = p.out().bytesLost();
            recordsOut = p.out().records();
            inflight = p.out().snd.so() - Math.min(p.out().snd.so(), acked);
            rate = p.out().snd.re().value();
            Pipe.OutLane l = p.out().lane(0);
            writable = l != null && !l.tx.notWritable() && !l.tx.writing() && !Pacer.held(p);
        }
        if (p.in() != null) {
            for (Pipe.InLane l : p.in().lanes) { received += l.rx.pos(); recordsIn += l.rx.recs(); }
            lanesIn = p.in().lanes.size();
        }
        var lanes = lanesOut > 1 || lanesIn > 1 ? laneStats(p, acks) : java.util.List.<io.zmux.PipeStats.LaneStats>of();
        var src = new io.zmux.PipeStats.RttSource[1];
        long[] rtt = rttOf(p, src);
        boolean eligible = false, saturated = false;
        if (a != null && p.out() != null) {
            Tiers.Gate g = mp().tiers.buildGate();
            if (g != null) {
                eligible = mp().tiers.admits(g, p);
                int tier = g.tierOf(p);
                saturated = tier >= 0 && tier <= g.last() && g.tiers.get(tier).sat();
            }
        }
        long until = a != null ? mp().probation.probationEnd(p) : Long.MIN_VALUE;
        io.zmux.PipeStats.PeerRequest peer = a != null && (a.tier.peerSeq() > 0 || (p.id != 0 && !a.joinerLocal))
                ? new io.zmux.PipeStats.PeerRequest(a.tier.peerPrio(), a.tier.peerStandby()) : null;
        boolean peerRequest = peer != null;
        boolean outLive = acks && p.out() != null;
        boolean live = state.out() == PipeInfo.Out.ACTIVE || state.out() == PipeInfo.Out.SUSPECT || state.in() == PipeInfo.In.OPEN;
        long attachedAt = a == null ? start : a.joining.attachedAt();
        long epoch = id == 0 ? 0 : a == null ? -1 : a.joining.joinedEpoch();
        // The policy allows two epoch periods for an attachment before replacement.
        long replaceBy = a == null || epoch < 0 ? Time.NEVER : Time.add(attachedAt, Time.mul(cfg.epochs().period(), 2));
        return new io.zmux.PipeStats(io.zmux.PipeId.of(id), io.zmux.internal.join.Mode.record(pipeMode(id)), a == null ? initiator() : a.joinerLocal,
                a == null ? 0 : a.fate, p.label(), p.carrier(), p.variant(),
                state.out().toPublic(), state.in().toPublic(),
                nanos(Time.sub(snapshotTime, attachedAt)),
                epoch < 0 ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(epoch),
                replaceBy == Time.NEVER ? java.util.OptionalLong.empty()
                        : java.util.OptionalLong.of(nanoTime + Time.sub(replaceBy, snapshotTime)),
                path.peerAttached(p),
                a == null ? 0 : a.health.suspects(), a == null ? 0 : a.health.failures(),
                a == null || a.health.failCode() < 0 ? null : new io.zmux.PipeStats.Failure(a.health.failCode(), a.health.failCause()),
                a == null ? 0 : a.effPrio(), settingSource(a, a != null && a.tier.sendPrio() != null, peerRequest),
                a != null && a.effStandby(), settingSource(a, a != null && a.tier.sendStandby() != null, peerRequest),
                a == null ? 0 : a.tier.recvPrio(), a != null && a.tier.recvStandby(), a == null ? 0 : a.tier.statusSeq(),
                peer,
                a == null ? 1 : a.tier.weight(),
                until == Long.MIN_VALUE || until >= Time.NEVER / 2 ? java.util.OptionalLong.empty()
                        : java.util.OptionalLong.of(nanoTime + Time.sub(until, snapshotTime)),
                a != null && a.prob.probOn(), a != null && a.prob.reval(), eligible, writable, saturated,
                src[0], rtt == null ? null : nanos(rtt[0]), rtt == null ? null : nanos(rtt[1]),
                rtt == null || rtt[2] < 0 ? null : nanos(rtt[2]),
                a == null || p.out() == null ? null : nanos(mp().metrics.owd(p)),
                outLive ? java.util.OptionalLong.of((long) rate) : java.util.OptionalLong.empty(),
                // queued_bytes: the carrier's part of Q_p only, absent where no lane's adapter reports a count.
                unsent < 0 ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(unsent),
                unsent < 0 || !outLive || !(rate > 0) ? null : nanos((long) Math.min(Time.NEVER / 2.0, unsent / rate * Time.SECOND)),
                outLive ? java.util.OptionalLong.of(inflight) : java.util.OptionalLong.empty(), sent, received,
                outLive ? java.util.OptionalLong.of(acked) : java.util.OptionalLong.empty(), recordsOut, recordsIn,
                a == null ? 0 : a.ret.reinjectedBytes(),
                outLive ? java.util.OptionalLong.of(lost) : java.util.OptionalLong.empty(),
                outLive ? java.util.OptionalLong.of(sent == 0 ? 0 : lostRecords(lost, recordsOut, sent)) : java.util.OptionalLong.empty(),
                java.util.Optional.empty(),
                a == null ? 0 : a.ret.oppBytes(), a == null ? 0 : a.ret.redundantBytes(),
                a == null ? 0 : a.ret.duplicateBytes(), a == null ? 0 : a.health.pingsSent(), a == null ? 0 : a.health.pingsAnswered(),
                a == null || p.out() == null ? null : nanos(Time.sub(snapshotTime, a.link.lastAdvance())),
                a == null || p.in() == null ? null : nanos(Time.sub(snapshotTime, a.link.lastRecord())),
                live ? nanos(Time.sub(snapshotTime, Math.max(p.lastTraffic(), a == null ? start : a.link.liveAt()))) : null,
                lanesOut, lanesIn, lanesRefused, lanes);
    }

    /** Per-lane statistics of a pipe with more than one lane in either direction, ordered by lane number. */
    private static java.util.List<io.zmux.PipeStats.LaneStats> laneStats(Pipe p, boolean acks) {
        var lanes = new java.util.ArrayList<io.zmux.PipeStats.LaneStats>();
        if (p.out() != null) {
            for (Pipe.OutLane ol : p.out().lanes) {
                Pipe.InLane il = p.in() == null ? null : p.in().lane(ol.lane);
                lanes.add(new io.zmux.PipeStats.LaneStats(ol.lane, java.util.OptionalLong.of(ol.tx.pos()),
                        acks ? java.util.OptionalLong.of(ol.ack.acked()) : java.util.OptionalLong.empty(),
                        il == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(il.rx.pos())));
            }
        }
        if (p.in() != null) {
            for (Pipe.InLane il : p.in().lanes) {
                if (p.out() == null || p.out().lane(il.lane) == null) {
                    lanes.add(new io.zmux.PipeStats.LaneStats(il.lane, java.util.OptionalLong.empty(),
                            java.util.OptionalLong.empty(), java.util.OptionalLong.of(il.rx.pos())));
                }
            }
            // Inbound lanes can arrive out of order or have nonconsecutive numbers.
            lanes.sort(java.util.Comparator.comparingLong(io.zmux.PipeStats.LaneStats::lane));
        }
        return lanes;
    }

    // ---- timers ----

    /** The earliest registered protocol deadline, or {@link Time#NEVER}. */
    public long nextTimeout() { return deadlines.earliest(); }

    /** Applies the owner's input changes to the deadline index before parking. */
    public void refreshDeadlines() {
        long enabled;
        if (state == SessionState.CLOSED) enabled = 0;
        else if (pendingWriteFailure != null) enabled = ProtocolTimeline.Reason.LIFE.bit();
        else if (isClosing()) enabled = ProtocolTimeline.Reason.LIFE.bit() | (life.closeWait() ? ProtocolTimeline.DRAIN : 0);
        else enabled = ProtocolTimeline.ALL;
        deadlines.refresh(enabled);
    }

    private long protocolDeadline(ProtocolTimeline.Reason reason) {
        return switch (reason) {
            case LIFE -> life.deadline();
            case LANE -> lanes.deadline();
            case RESUME -> state == SessionState.SUSPENDED ? grace : Time.NEVER;
            case KEEPALIVE -> ka.deadline(peerSeen && state != SessionState.SUSPENDED);
            case ACK -> ack == null ? Time.NEVER : ack.deadline();
            case PACING -> pacer == null ? Time.NEVER : pacer.deadline(!sched.isEmpty()
                    || (path instanceof PathSession mp && mp.retention.hasPrefixWork()));
            case QUALITY -> ack == null || quality == null ? Time.NEVER : quality.timers(now, false);
            case SHAPING -> pipes.shaping() == null ? Time.NEVER : pipes.shaping().deadline();
            case DATAGRAM_HOLD -> dg.heldDeadline();
            case DATAGRAM_SEND -> dg.sendDeadline();
            case DATAGRAM_FEEDBACK -> {
                long at = nextDatagramAck();
                yield at > now ? at : Time.NEVER;
            }
            default -> path instanceof PathSession mp ? mp.protocolDeadline(reason, now) : Time.NEVER;
        };
    }

    long deadlineVisits() { return deadlines.visits(); }

    /** Drains chained deadlines at equality in protocol order. */
    public void handleTimeout(long t) {
        refreshDeadlines();
        deadlines.beginDispatch();
        boolean first = true;
        do {
            long due = deadlines.dueReasons(t);
            long stalled = deadlines.recordDispatch(due);
            for (long rest = stalled; rest != 0; rest &= rest - 1) {
                emit(Event.stalledDeadline(ProtocolTimeline.reasonAt(Long.numberOfTrailingZeros(rest))));
            }
            if (!first && due == stalled) break;
            first = false;
            applyTimeout(t);
            deadlines.changed(ProtocolTimeline.ALL);
            refreshDeadlines();
        } while (nextTimeout() != Time.NEVER && nextTimeout() <= t);
    }

    private void applyTimeout(long t) {
        setNow(t);
        path.pickInput();
        if (state == SessionState.CLOSED) return;
        if (pendingWriteFailure != null) {
            if (life.lingerDue(t)) closeWith(pendingWriteFailure, null);
            return;
        }
        if (life.handshakeDue(t)) {
            if (!peerSeen) {
                closeWith(SessionError.of(SessionError.Kind.HANDSHAKE_TIMEOUT, false), null);
                return;
            }
        }
        if (life.handOffDue(t)) endClose();
        if (life.readsCheckDue(t)) checkLingerDone();
        if (state == SessionState.CLOSED) return;
        if (life.lingerDue(t)) {
            finishClose();
            return;
        }
        if (life.laneWaitDue(t)) {
            pipes.wakeWriters();
        }
        if (isClosing()) {
            if (life.closeWait()) closeWaitTimeout(t);
            return;
        }
        lanes.onTimeout(t);
        if (state == SessionState.SUSPENDED && grace != Time.NEVER && t >= grace) {
            // Grace expiry: CONNECTION_CLOSE on any live outbound pipe, otherwise a local close.
            grace = Time.NEVER;
            closeWith(new SessionError(SessionError.Kind.RESUME_TIMEOUT, TransportError.RESUME_TIMEOUT, 0, "", "",
                    mp().resume.cause(), false), CloseFrame.of(false, TransportError.RESUME_TIMEOUT, 0, null));
            return;
        }
        if (ka.onTimeout(t, peerSeen && state != SessionState.SUSPENDED)) {
            idleExpired();
            return;
        }
        if (ack != null) {
            if (ack.onTimeout(now)) pipes.wakeWriters();
            pacer.onTimeout(t);
        }
        path.onTimeout(t);
        dg.expireHeld(t);
        dg.expireSend(t);
        if (nextDatagramAck() <= now) pipes.wakeWriters();
        if (ack != null && quality != null) quality.timers(t, true);
        if (pipes.shaping() != null) pipes.shaping().timeout(t);
    }

    private void closeWaitTimeout(long t) {
        if (ack != null) {
            if (ack.onTimeout(now)) pipes.wakeWriters();
            pacer.onTimeout(t);
        }
        path.onTimeout(t);
        lanes.onTimeout(t);
        checkLingerDone();
    }

    /** Closes on the idle timeout. */
    void idleExpired() {
        SessionError e = SessionError.of(SessionError.Kind.IDLE, false);
        if (feat.acked()) closeWith(e, CloseFrame.of(false, TransportError.IDLE_TIMEOUT, 0, null));
        else closeSilently(e, true);
    }



    // ---- pipe acknowledgement and pacing ----

    /** The largest smoothed RTT over outbound pipes with a sample. */
    long srttMax() {
        return path.srttMax();
    }

    /** Processes a PIPE_ACK. */
    private final io.zmux.internal.wire.Frame.AckEntries ackIt = new io.zmux.internal.wire.Frame.AckEntries();

    void onPipeAck(Pipe received, io.zmux.internal.wire.Frame f) throws WireException {
        path.onPipeAck(received, f.entries(ackIt));
    }

    private final class AckPort implements AckEngine.Port {
        private final PathScheduler.Decision deliveryDecision = new PathScheduler.Decision();
        private final StreamScheduler.Iter deliveryStreams = new StreamScheduler.Iter();
        @Override public void activity() {
            deadlines.changed(ProtocolTimeline.Reason.KEEPALIVE.bit());
            ka.activity(now);
        }
        @Override public void rttSampled(Pipe pipe, long min) {
            tuner.loopRtt(min, now);
            if (performanceFeedback != null && pipe.mp() != null && pipe.out() != null) {
                var health = pipe.mp().projection.snapshot();
                if (health != null) pipe.out().snd.quality(health.send(), now).currentRtt()
                        .ifPresent(sample -> performanceFeedback.record(pipe.member().id(), sample.sampledAt(),
                                sample.validUntil(), sample.rtt(), 0));
            }
            if (pipe.mp() != null) {
                mp().health.refreshSilence(pipe);
                deadlines.changed(ProtocolTimeline.Reason.HEALTH.bit());
            }
        }
        @Override public void deliverySampled(Pipe pipe, long start, long end, boolean constrained) {
            var send = pipe.out().snd;
            if (!(path instanceof PathSession ps) || pipe.mp() == null || pipe.mp().health.out() != Attach.OUT_ACTIVE
                    || Pacer.paced(pipe) || send.re().bound() > 0 || (pipe.shape() != null && pipe.shape().active())) {
                send.deliveryBacklog(null);
                return;
            }
            ps.scheduler.newDecision(deliveryDecision, pipe);
            long topology = ps.health.stateGen();
            var previous = send.deliveryBacklog();
            var stream = previous == null ? null : streams.get(previous.stream());
            if (previous != null && previous.topology() == topology && previous.pipesVersion() == pipes.version()
                    && conn.sent() < previous.connectionLimit() && credited(ps, deliveryDecision, stream, previous.end())) {
                if (previous.at() <= start && !constrained) {
                    send.backlogged();
                    var health = pipe.mp().projection.snapshot();
                    if (performanceFeedback != null && health != null) {
                        send.quality(health.send(), end).delivery().ifPresent(rate ->
                                performanceFeedback.record(pipe.member().id(), rate.end(), rate.validUntil(),
                                        java.time.Duration.ZERO, (long) rate.bytesPerSecond()));
                    }
                }
                return;
            }
            send.deliveryBacklog(null);
            if (conn.sent() >= conn.sendMax()) return;
            var ready = deliveryStreams.reset(sched);
            while ((stream = ready.next()) != null) {
                long offset = Math.min(stream.snd.sendEnd(), stream.snd.maxData());
                if (!credited(ps, deliveryDecision, stream, offset)) continue;
                send.deliveryBacklog(new DeliveryBacklog(stream.id(), offset, conn.sendMax(), end, topology, pipes.version()));
                return;
            }
        }

        private static boolean credited(PathSession ps, PathScheduler.Decision decision, Stream stream, long end) {
            return stream != null && stream.idSet() && !stream.tab.discarded() && !stream.snd.cancelled()
                    && !stream.snd.reset() && ps.scheduler.policyOf(stream) != CoreConfig.Policy.CUSTOM
                    && Math.max(stream.snd.sent(), stream.snd.q().base()) < end && end <= stream.snd.sendEnd()
                    && ps.scheduler.onPipe(decision, stream);
        }

        @Override public boolean quiet() { return path instanceof PathSession ps && ps.quiet(); }

        @Override public void advanced(Pipe p, Pipe.OutLane ol, boolean cum) {
            if (cum) {
                if (p.mp() != null && p.out() != null) {
                    var health = p.mp().projection.snapshot();
                    if (health != null && p.out().snd.qualityEvidence.observe(
                            p.out().snd.quality(health.send(), now), publishedLoss(p.id), now)) capacity.changed();
                }
                if (quality != null) quality.sample(p);
                if (p.mp() != null) mp().advanced(p, ol);
            } else if (p.mp() != null) {
                mp().laneAdvanced(p);
            }
            lanes.acked(p, ol);
        }
    }

    private final class PacerPort implements Pacer.Port {
        @Override public void wakeAllWriters() { pipes.wakeWriters(); }
        @Override public void wakePipe(Pipe p) { pipes.wake(p); }
    }

    // ---- receive-window auto-tuning and the memory budget ----

    private long memoryHeadroom() {
        var budget = cfg.budget();
        if (budget == null) return Long.MAX_VALUE;
        long limit = budget.limit();
        if (limit == CoreConfig.UNLIMITED) return Long.MAX_VALUE;
        return limit - Math.min(limit, budget.used());
    }

    /** The largest session window the memory budget allows. */
    long connAllowance() {
        long room = memoryHeadroom();
        return room == Long.MAX_VALUE ? room : conn.window() + room / ConnFlow.RECEIVE_COST;
    }

    private final class TunerPort implements WindowTuner.Port {
        @Override public boolean multipath() { return path instanceof PathSession; }
        @Override public long inputPipeMinRtt() { return pipes.minRtt(rx.rxPipe()); }
        @Override public long bufferCap() { return streams.bufferCap(); }
        @Override public long allowance() { return connAllowance(); }
        @Override public boolean pressure() { return underPressure(); }
        @Override public boolean closing() { return isClosing(); }
    }


    /** The listener's shared budget, or null for a standalone session. */
    public SharedMemory sharedBudget() { return cfg.budget(); }



    /** Listener memory pressure: the session behaves as at its buffer cap and refuses new peer streams. */
    boolean underPressure() {
        return conn.underPressure();
    }

    /** The received GOAWAY watermarks [maxBidi, maxUni, code], or null while none was received. */
    public long[] peerGoAway() {
        return life.peerGoAway();
    }

    /** Closes a session the listener does not take by its policy (CONNECTION_REFUSED after its own parameters). */
    public void refuse(String reason) {
        closeTransport(SessionError.transport(TransportError.CONNECTION_REFUSED, 0, reason, "", false));
    }

    // ---- QX_PING and the no-effect budget ----

    /** Asks for a QX_PING request, or joins the outstanding one; returns a ticket. */
    public long ping(Waiter w) throws CoreException {
        deadlines.changed(ProtocolTimeline.Reason.KEEPALIVE.bit());
        if (err != null) throw CoreException.session(err);
        return ka.ping(w);
    }

    /** The RTT of a completed ticket, or -1 while it is pending. */
    public long pingResult(long ticket) throws CoreException {
        long r = ka.result(ticket);
        if (r < 0 && err != null) throw CoreException.session(err);
        return r;
    }


    /** Charges one no-effect frame. */
    void charge() {
        ka.charge(now);
    }


    private boolean peerZmux() {
        TransportParams peer = peerParams();
        return peer != null && peer.isZmux();
    }

    private final class KeepAlivePort implements KeepAlive.Port {
        @Override public long now() { return now; }
        @Override public boolean peerZmux() { return Session.this.peerZmux(); }
        @Override public void wantWrite() {
            deadlines.changed(ProtocolTimeline.Reason.PACING.bit() | ProtocolTimeline.Reason.SHAPING.bit()
                    | ProtocolTimeline.Reason.KEEPALIVE.bit());
            pipes.wakeWriters();
        }
        @Override public void budgetExhausted() { failLocal(new WireException(TransportError.EXCESSIVE_LOAD, "no-effect budget exhausted")); }
        @Override public void pingMeasured(long rtt) { tuner.loopRtt(rtt, now); if (pacer != null) pacer.onPing(pipes.get(0), rtt, now); }
        @Override public void fenceReached() { fenceDone(); }
    }

    // ---- GOAWAY and PRIORITY_UPDATE ----

    /** Starts a graceful close: the two-phase GOAWAY when it is in use, then CONNECTION_CLOSE once no stream remains. */
    public void shutdown(long code, byte[] reason) {
        if (isClosing()) return;
        life.shutdownRequested(code, reason);
        streams.wakeOpenWaiters();
        if (!feat.goAway() || !peerSeen) {
            checkShutdownDone();
            return;
        }
        if (life.beginGoAway(streams.remoteLimit(0), streams.remoteLimit(1))) {
            ctl.wantGoAway();
            ka.startFence();
            if (state == SessionState.ACTIVE) state = SessionState.DRAINING;
            pipes.wakeWriters();
        }
        checkShutdownDone();
    }

    /** The final GOAWAY. */
    void fenceDone() {
        ka.clearFence();
        boolean[] lowered = {false};
        if (!life.finalGoAway(lowered)) return;
        if (lowered[0]) {
            ctl.wantGoAway();
            pipes.wakeWriters();
        }
        streams.rejectAboveWater();
        checkShutdownDone();
    }

    /** Completes what an operation left pending; the driver calls it at the end of every round. */
    public void settle() {
        if (life.takeRecheck()) checkShutdownDone();
    }

    void checkShutdownDone() {
        if (!life.shutdownReady() || isClosing() || !streams.isEmpty()) return;
        close(life.shutdownCode(), life.shutdownReason());
    }

    void onGoAway(io.zmux.internal.wire.Frame f) {
        boolean lowered = life.onGoAway(f);
        if (!lowered) {
            if (!feat.multipath()) charge();
            return;
        }
        if (state == SessionState.ACTIVE) state = SessionState.DRAINING;
        for (Stream st : streams.locals()) {
            if (!st.tab.idSet()) {
                StreamError e = StreamError.of(StreamError.Kind.GOING_AWAY, 0);
                StreamSend.error(st, e, false);
                StreamRecv.error(st, e, false);
                send.cancelUnopened(st);
                StreamSend.wakeAll(outbox, st);
                wake(st.rcv.rsig());
                continue;
            }
            if ((st.tab.id() >>> 2) >= life.recvWater(Stream.typeIndex(st.bidi))) {
                StreamError e = StreamError.of(StreamError.Kind.REJECTED, st.tab.id());
                if (st.hasSend) StreamSend.error(st, e, true);
                if (st.hasRecv) StreamRecv.error(st, e, true);
                StreamSend.wakeAll(outbox, st);
                wake(st.rcv.rsig());
            }
        }
        streams.wakeOpenWaiters();
        emit(Event.goAway(life.recvWater(0), life.recvWater(1), f.code));
    }

    void onPriorityUpdate(io.zmux.internal.wire.Frame f) throws WireException {
        long id = f.streamId;
        if (!Stream.isBidiId(id) && !StreamTable.isLocalId(id, role == Role.RESPONDER)) {
            throw WireException.withDetails(TransportError.STREAM_STATE_ERROR, "PRIORITY_UPDATE for the sender's unidirectional stream", "stream", id);
        }
        Stream st = streams.lookup(id, Stream.isBidiId(id));
        if (st == null || !st.hasSend) return;
        if (st.snd.state() == Stream.SEND_DATA_RECVD || st.snd.state() == Stream.SEND_RESET_RECVD) return;
        if (!streams.applyPeerPriority(st, f.seq, f.urgency, f.incremental) && !feat.multipath()) charge();
    }


    // ---- datagrams ----

    /** The session's datagram queues. */
    public Datagrams datagrams() {
        return dg;
    }

    /** Representations the caller can commit for this packet. */
    public enum DatagramMode { DATAGRAM_ONLY, DATAGRAM_OR_CAPSULE, CAPSULE_ONLY }

    /** A local submission either queued a datagram, selected reliable output, or is waiting for room. */
    public enum DatagramAdmission { QUEUED, CAPSULE, WAITING }

    /** Resolved datagram options with a deadline in the session clock domain. */
    public record DatagramOptions(DatagramSendOptions options, long deadline) {
        private static final DatagramOptions INHERIT = new DatagramOptions(DatagramSendOptions.INHERIT, Time.NEVER);

        public DatagramOptions expiresAt(long at) {
            return at == deadline ? this : new DatagramOptions(options, at);
        }
    }

    public DatagramOptions resolveDatagram(Stream st, DatagramSendOptions options, long at) throws CoreException {
        if (st != null) options = options.inheriting(st.datagramDefaults());
        options = options.inheriting(cfg.datagramDefaults());
        if (options == DatagramSendOptions.INHERIT) return DatagramOptions.INHERIT;
        long deadline = switch (options.deadline()) {
            case null -> Time.NEVER;
            case DatagramSendOptions.Deadline.None ignored -> Time.NEVER;
            case DatagramSendOptions.Deadline.After(var ttl) -> {
                long end = Time.add(at, ttl.toNanos());
                if (end == Time.NEVER) throw CoreException.of(CoreException.Reason.INVALID_DATAGRAM_OPTIONS);
                yield end;
            }
            case DatagramSendOptions.Deadline.At(long end) -> end;
        };
        return new DatagramOptions(options, deadline);
    }

    /** Queues an original, evicting older entries only after this send passes admission. */
    public void sendDatagram(Stream st, long context, byte[] payload) throws CoreException {
        sendDatagram(st, context, payload, DatagramSendOptions.INHERIT, now);
    }

    public void sendDatagram(Stream st, long context, byte[] payload, DatagramSendOptions options, long at)
            throws CoreException {
        admitDatagram(st, context, payload, payload.length, resolveDatagram(st, options, at), at,
                DatagramMode.DATAGRAM_ONLY, false, false, null);
    }

    public DatagramAdmission admitDatagram(Stream st, long context, byte[] payload, int length, DatagramOptions options, long at,
                                          DatagramMode mode, boolean limited, boolean wait, CarrierCharge charge)
            throws CoreException {
        DatagramAdmission admission = planDatagram(st, context, length, 1, options, mode);
        if (admission == DatagramAdmission.CAPSULE) return admission;
        if (wait && dg.sendQueueFull()) return DatagramAdmission.WAITING;
        requireDatagramRoom(st, length, 1, limited, false);
        if (st != null) streams.pinId(st);
        dg.enqueueSingle(st, context, payload, at, options.deadline(), options.options(), charge);
        queuedDatagrams();
        return DatagramAdmission.QUEUED;
    }

    /** Validates and publishes a relay group in one session-owner turn. */
    public DatagramAdmission admitDatagramGroup(Stream st, long context, StreamRecv.Chunk[] payloads, int largest,
                                               long bytes, DatagramOptions options, long at, DatagramMode mode,
                                               boolean limited, CarrierCharge[] charges) throws CoreException {
        DatagramAdmission admission = planDatagram(st, context, largest, payloads.length, options, mode);
        if (admission == DatagramAdmission.CAPSULE) return admission;
        requireDatagramRoom(st, bytes, payloads.length, limited, true);
        if (st != null) streams.pinId(st);
        dg.enqueueGroup(st, context, payloads, at, options.deadline(), options.options(), charges);
        queuedDatagrams();
        return DatagramAdmission.QUEUED;
    }

    /** Quotes the representation for a relay's exact limiter request. Admission rechecks the quote. */
    public DatagramAdmission planDatagram(Stream st, long context, int largest, int count, DatagramOptions options,
                                          DatagramMode mode) throws CoreException {
        if (err != null) throw CoreException.session(err);
        if (st != null && st.released() != null)
            throw CoreException.stream(StreamError.of(StreamError.Kind.CLOSED_LOCALLY, st.id()));
        if (context < 0 || context > VarInt.MAX) throw CoreException.of(CoreException.Reason.INVALID_DATAGRAM_OPTIONS);
        boolean strict = options.options().carriage() == DatagramSendOptions.Carriage.REQUIRE_NATIVE;
        if (strict && mode == DatagramMode.CAPSULE_ONLY)
            throw CoreException.of(CoreException.Reason.INVALID_DATAGRAM_OPTIONS);
        if (options.deadline() <= now) throw dg.reject(DatagramDrop.DEADLINE);
        if (mode == DatagramMode.CAPSULE_ONLY) return DatagramAdmission.CAPSULE;
        if (dg.exhausted(count)) throw dg.reject(DatagramDrop.SEQUENCE_EXHAUSTED);

        if (!peerSeen && st == null && cfg.datagrams()) return DatagramAdmission.QUEUED;
        boolean negotiated = peerSeen && peer.has(TransportParams.DATAGRAM) && peer.maxDatagramFrameSize > 0
                && (st == null || (feat.flows() && st.bidi));
        if (mode == DatagramMode.DATAGRAM_OR_CAPSULE && !strict && peerSeen) {
            int structural = negotiated ? dg.structuralPayload(flowWidth(st, context, true)) : -1;
            if (largest > structural) return DatagramAdmission.CAPSULE;
        }
        if (!negotiated) throw CoreException.of(CoreException.Reason.NOT_NEGOTIATED);
        if (strict && !feat.carrierDatagrams()) throw dg.reject(DatagramDrop.NO_NATIVE_PATH);
        int max = dg.admissionPayload(flowWidth(st, context, false));
        if (largest > max) throw CoreException.tooLarge(Math.max(0, max));
        return DatagramAdmission.QUEUED;
    }

    private int flowWidth(Stream st, long context, boolean structural) throws CoreException {
        if (st == null) return 1;
        long id = structural && !st.tab.idSet() ? 0 : streams.candidateId(st);
        if (id >= VarInt.MAX) throw CoreException.of(CoreException.Reason.STREAM_LIMIT);
        return VarInt.length(id + 1) + VarInt.length(context);
    }

    private void requireDatagramRoom(Stream st, long bytes, int count,
                                     boolean limited, boolean group) throws CoreException {
        if (limited) throw dg.reject(DatagramDrop.LIMITER);
        if (!dg.sendFits(st, count, bytes, group)) throw dg.reject(DatagramDrop.QUEUE_OVERFLOW);
    }

    private void queuedDatagrams() {
        deadlines.changed(ProtocolTimeline.DATAGRAM);
        pipes.wakeWriters();
    }

    private final java.util.function.LongUnaryOperator feedbackRtt = pipe -> {
        Pipe p = pipes.get(pipe);
        if (p == null) return 0;
        var rtt = p.mp() == null ? (p.out() == null ? null : p.out().snd.rtt()) : p.mp().rtt();
        return rtt != null && rtt.has() ? rtt.smoothed() : 0;
    };

    private int feedbackInboundPipes() {
        int count = 0;
        for (Pipe p : pipes.all()) {
            if (p.in() != null && (p.mp() == null || p.mp().in() == Attach.IN_OPEN)) count++;
        }
        return count;
    }

    private long nextDatagramAck() {
        return dg.feedback.reportPending() ? dg.feedback.next(now, feedbackInboundPipes(), feedbackRtt) : Time.NEVER;
    }

    // ---- datagram channels ----

    private final Datagrams.FlowLookup flows = new Datagrams.FlowLookup() {
        @Override public Stream flow(long id) throws WireException { return streams.flow(id); }
        @Override public boolean unopened(long id) { return streams.unopenedPeer(id); }
        @Override public long holdUntil() { return Time.add(now, cfg.datagramHoldNanos() < 0 ? srtt() : cfg.datagramHoldNanos()); }
    };

    /**
     * The pipe a datagram of st follows in a multipath session: the pipe of st's opening frame, or of its latest
     * reinjected copy, while that frame is unacknowledged and the pipe lives (MULTIPATH.md S7); null: the usual rule.
     */
    static Pipe followPipe(Stream st) {
        PathStreamState m = st.snd.mps();
        Pipe p = m == null ? null : m.openingPipe();
        if (p == null || p.mp() == null || p.out() == null || p.out().failed() || !p.mp().liveOut() || p.mp().retired()) return null;
        return p;
    }

    /** The pipe DATAGRAM frames go to now: the primary pipe of a multipath session, else the outbound pipe. */
    Pipe datagramPipe() {
        if (path instanceof PathSession mp) return mp.primary();
        for (Pipe p : pipes.all()) if (p.out() != null && !p.out().failed()) return p;
        return null;
    }

    /**
     * How DATAGRAM frames for p may use its carrier's datagram channel: when both endpoints sent
     * zmux_carrier_datagrams, the carrier has a channel that takes datagrams now, and the pipe has an outbound direction.
     */
    Datagrams.ChannelUse channelOf(Pipe p) {
        if (!feat.carrierDatagrams() || p == null || p.out() == null || p.out().failed()) return Datagrams.ChannelUse.NONE;
        Pipe.OutLane l0 = p.out().lane(0);
        DatagramChannel ch = l0 == null ? null : l0.st.info().channel();
        if (ch == null) return Datagrams.ChannelUse.NONE;
        int room = ch.maxChannelDatagram();
        int limit = ch.channelDatagramLimit(room);
        if (limit == 0) return Datagrams.ChannelUse.NONE;
        return new Datagrams.ChannelUse(limit, limit < 0 ? room : Math.min(room, limit), ch.channelStreamId(), feat.carrierDatagramPadding());
    }

    private java.util.Set<io.zmux.DatagramCopyPolicy.IndependentPair> datagramIndependence = java.util.Set.of();

    public void datagramIndependence(java.util.Set<io.zmux.DatagramCopyPolicy.IndependentPair> paths) {
        for (var prior : datagramIndependence) {
            if (!paths.contains(prior)) dg.feedback.clearPairings(prior.first().value(), prior.second().value());
        }
        datagramIndependence = java.util.Set.copyOf(paths);
    }

    private void carrierReplaced(long pipeId) {
        dg.feedback.resetOutbound(new DatagramFeedback.Path(pipeId, false));
        dg.feedback.resetOutbound(new DatagramFeedback.Path(pipeId, true));
        var current = new java.util.HashSet<>(datagramIndependence);
        current.removeIf(pair -> pair.first().value() == pipeId || pair.second().value() == pipeId);
        datagramIndependence = java.util.Set.copyOf(current);
    }

    /** The outcome of submitting a carrier loss observation. */
    public enum DatagramLossReport { ACCEPTED, IGNORED, SOURCE_EXPIRED }

    public DatagramLossReport reportDatagramLoss(long pipeId, boolean nativePath, long observed, long generation,
                                                 long finalized, long lost) {
        Pipe pipe = pipes.get(pipeId);
        if (!established() || isClosing() || !feat.flows() || pipe == null || pipe.out() == null || pipe.out().failed()
                || (pipe.mp() != null && pipe.mp().retired())
                || (nativePath && observeChannel(pipe).limit() == 0)) return DatagramLossReport.IGNORED;
        return dg.feedback.reportLoss(new DatagramFeedback.Path(pipe.id, nativePath), observed, generation,
                finalized, lost, now);
    }

    private boolean hasIndependenceEvidence(Pipe first, Pipe second) {
        return datagramIndependence.contains(new io.zmux.DatagramCopyPolicy.IndependentPair(
                io.zmux.PipeId.of(first.id), io.zmux.PipeId.of(second.id)));
    }

    private final class ChannelsPort implements Datagrams.Channels {
        @Override public Pipe datagramPipe() { return Session.this.datagramPipe(); }
        @Override public Datagrams.ChannelUse of(Pipe p) { return channelOf(p); }
        @Override public boolean fixedRecordPath() { return !(path instanceof PathSession); }
        @Override public boolean nativeNegotiated() { return feat.carrierDatagrams(); }

        @Override public int recordRoom() {
            int best = 0;
            for (Pipe pipe : pipes.all()) {
                if (pathEligible(pipe)) best = Math.max(best, tx.recordRoom(pipe));
            }
            return best;
        }

        @Override public int nativeLimit() { return nativeCapacity(false); }

        @Override public int nativeRoom() { return nativeCapacity(true); }

        private int nativeCapacity(boolean available) {
            if (!feat.carrierDatagrams()) return -1;
            int best = -1;
            for (Pipe pipe : pipes.all()) {
                if (!pathEligible(pipe)) continue;
                var use = of(pipe);
                int limit = available ? use.room() : Math.max(use.room(), use.limit());
                best = Math.max(best, limit - ChannelDatagram.length(use.id(), 0));
            }
            return best;
        }

        private boolean pathEligible(Pipe pipe) {
            if (!carries(null, pipe) || isClosing()) return false;
            if (!(path instanceof PathSession mp)) return true;
            int eligible = mp.tiers.usableClass();
            return (eligible & 4) != 0 && mp.tiers.classUsable((eligible & 1) != 0, (eligible & 2) != 0, pipe);
        }

        @Override public Pipe pick(Stream stream, Pipe current, int bytes, long deadline,
                                   io.zmux.DatagramSendOptions.Carriage carriage) {
            if (!(path instanceof PathSession mp)) {
                return deadline == Time.NEVER || Time.add(now, Math.max(cfg.test().rtt(), srttMax()) / 2) <= deadline ? current : null;
            }
            var selected = select(mp, stream, null, java.util.Set.of(), bytes, deadline, carriage, false, false, null);
            return selected == null ? null : selected.pipe();
        }

        @Override public boolean redundant(Stream stream, Pipe original) {
            if (!(path instanceof PathSession) || original == null) return false;
            return stream == null || !stream.local || (stream.snd.mps() != null && stream.snd.mps().openingConfirmed());
        }

        @Override public Datagrams.CopyPath copyPath(Stream stream, Pipe original, java.util.Set<Pipe> selected,
                                                     int length, long deadline, io.zmux.DatagramSendOptions.Carriage carriage,
                                                     boolean distinct, boolean adaptive, DatagramFeedback.Path impaired) {
            return select((PathSession) path, stream, original, selected, length, deadline, carriage, distinct, adaptive, impaired);
        }

        @Override public boolean copyEligible(Stream stream, Pipe original, Pipe candidate, java.util.Set<Pipe> selected,
                                              int length, long deadline, io.zmux.DatagramSendOptions.Carriage carriage,
                                              boolean distinct) {
            if (!(path instanceof PathSession mp) || !redundant(stream, original)
                    || pipes.get(original.id) != original) return false;
            int eligible = mp.tiers.usableClass();
            if ((eligible & 4) == 0) return false;
            boolean standby = (eligible & 1) != 0, probation = (eligible & 2) != 0;
            if (!canCarry(mp, stream, candidate, standby, probation, true)) return false;
            long firstTier = original.mp().effPrio();
            if (!inCopyTier(candidate, firstTier, nextCopyTier(mp, stream, firstTier, standby, probation))) return false;
            if (distinct && !independent(candidate, selected)) return false;
            return arrival(mp, candidate, of(candidate).supports(length), length, deadline, carriage, false) < Double.POSITIVE_INFINITY;
        }

        @Override public void wake(Pipe pipe) { pipes.wake(pipe); }

        private record Alternative(Datagrams.CopyPath path, double eta) {}

        private Datagrams.CopyPath select(PathSession mp, Stream stream, Pipe original, java.util.Set<Pipe> selected,
                                          int bytes, long deadline, io.zmux.DatagramSendOptions.Carriage carriage,
                                          boolean distinct, boolean adaptive, DatagramFeedback.Path impaired) {
            int eligible = mp.tiers.usableClass();
            if ((eligible & 4) == 0) {
                if (impaired != null) dg.feedback.clearImpairment(impaired);
                return null;
            }
            boolean standby = (eligible & 1) != 0;
            boolean probation = (eligible & 2) != 0;
            long firstTier = original == null ? Long.MIN_VALUE : original.mp().effPrio();
            long nextTier = original == null ? Long.MAX_VALUE : nextCopyTier(mp, stream, firstTier, standby, probation);
            Pipe best = null;
            boolean bestNative = false;
            var alternatives = impaired == null ? null : new java.util.ArrayList<Alternative>();
            long tier = Long.MAX_VALUE;
            double eta = Double.POSITIVE_INFINITY;
            for (Pipe candidate : pipes.all()) {
                if (selected.contains(candidate) || !canCarry(mp, stream, candidate, standby, probation, original != null)) continue;
                long priority = candidate.mp().effPrio();
                if (original != null && !inCopyTier(candidate, firstTier, nextTier)) continue;
                if (distinct && !independent(candidate, selected)) continue;
                var use = of(candidate);
                boolean nativePath = use.supports(bytes);
                if (nativePath && !use.takes(bytes)) continue;
                double arrival = arrival(mp, candidate, nativePath, bytes, deadline, carriage, original != null);
                if (arrival == Double.POSITIVE_INFINITY) continue;
                if (alternatives != null) {
                    alternatives.add(new Alternative(new Datagrams.CopyPath(candidate, nativePath), arrival));
                } else if (best == null || (adaptive ? arrival < eta
                        : priority < tier || (priority == tier && ((nativePath && !bestNative)
                        || (nativePath == bestNative && arrival < eta))))) {
                    best = candidate;
                    bestNative = nativePath;
                    tier = priority;
                    eta = arrival;
                }
            }
            if (alternatives == null) return best == null ? null : new Datagrams.CopyPath(best, bestNative);
            alternatives.sort(java.util.Comparator.comparingDouble(Alternative::eta));
            for (Alternative alternative : alternatives) {
                var choice = alternative.path();
                if (dg.feedback.impairment(impaired, new DatagramFeedback.Path(choice.pipe().id, choice.nativePath()), now)) {
                    return choice;
                }
            }
            dg.feedback.clearImpairment(impaired);
            return null;
        }

        private long nextCopyTier(PathSession mp, Stream stream, long firstTier, boolean standby, boolean probation) {
            long next = Long.MAX_VALUE;
            for (Pipe candidate : pipes.all()) {
                if (candidate.mp() == null) continue;
                long priority = candidate.mp().effPrio();
                if (priority > firstTier && priority < next && canCarry(mp, stream, candidate, standby, probation, true)) next = priority;
            }
            return next;
        }

        private static boolean inCopyTier(Pipe candidate, long firstTier, long nextTier) {
            long tier = candidate.mp().effPrio();
            return tier == firstTier || tier == nextTier;
        }

        private double arrival(PathSession mp, Pipe candidate, boolean nativePath, int bytes, long deadline,
                               io.zmux.DatagramSendOptions.Carriage carriage, boolean planCopy) {
            if (!nativePath && (carriage == io.zmux.DatagramSendOptions.Carriage.REQUIRE_NATIVE
                    || !(mp.metrics.open(candidate) || (planCopy && mp.metrics.writingOnAcceptingLane(candidate)))
                    || !dg.fitsRecord(tx.recordRoom(candidate), bytes))) return Double.POSITIVE_INFINITY;
            var shape = candidate.shape();
            long wait = nativePath && shape != null ? Math.max(0, Time.sub(shape.nativeReady(), now)) : 0;
            double seconds = nativePath ? (mp.metrics.owd(candidate) + (double) wait) / 1e9 : mp.scheduler.eta(candidate, bytes);
            return deadline != Time.NEVER && (deadline <= now || seconds * 1e9 > deadline - now)
                    ? Double.POSITIVE_INFINITY : seconds;
        }

        private boolean canCarry(PathSession mp, Stream stream, Pipe pipe, boolean standby, boolean probation, boolean copy) {
            if (!carries(stream, pipe)) return false;
            if (copy && pipe.mp().effStandby() && !cfg.copyPolicy().standby()) return false;
            return mp.tiers.classUsable(standby, probation, pipe)
                    || (copy && cfg.copyPolicy().standby() && mp.tiers.classUsable(true, probation, pipe));
        }

        private boolean independent(Pipe candidate, java.util.Set<Pipe> selected) {
            for (Pipe used : selected) {
                var a = candidate.mp();
                var b = used.mp();
                if (candidate.conn() != null && candidate.conn() == used.conn()) return false;
                if (a.fate != 0 && a.fate == b.fate && a.joinerLocal == b.joinerLocal) return false;
                if (!hasIndependenceEvidence(candidate, used)) return false;
            }
            return true;
        }

        @Override public boolean carries(Stream st, Pipe p) {
            if (p == null || p.out() == null || p.out().failed()) return false;
            if (!(path instanceof PathSession)) return true;
            if (p.mp().out() != Attach.OUT_ACTIVE || p.mp().handshaking() || p.mp().retired()) return false;
            Pipe opening = st == null ? null : followPipe(st);
            return opening == null || opening == p;
        }
    }

    /**
     * Appends to dst the channel datagrams due on the datagram channel of pipe, in order; the
     * driver reports each outcome through {@link #channelDone}.
     */
    public void pollChannel(long pipeId, List<byte[]> dst, List<Datagrams.ChannelSent> sent, long t) {
        if (path instanceof PathSession mp) mp.beginRoute();
        deadlines.changed(ProtocolTimeline.DATAGRAM | ProtocolTimeline.Reason.SHAPING.bit());
        setNow(t);
        if (!peerSeen || isClosing()) return;
        noteDatagramPath(); // the channel's room as read now
        Pipe p = pipes.get(pipeId);
        if (p == null) return;
        if (pipes.shaping() != null) pipes.shaping().of(p);
        int first = sent.size();
        dg.pollChannel(p, observeChannel(p), dst, sent, pipes.shaping());

        if (sent.size() != first) noteDatagramPath();
    }

    /** Settles the carrier receipt for a submitted datagram. */
    public void channelDone(Datagrams.ChannelSent ticket, io.zmux.carrier.CarrierChannel.Outcome outcome, long t) {
        deadlines.changed(ProtocolTimeline.DATAGRAM | ProtocolTimeline.Reason.SHAPING.bit());
        setNow(t);
        boolean accepted = outcome == io.zmux.carrier.CarrierChannel.Outcome.ACCEPTED;
        if (!dg.channelDone(ticket, outcome)) return;
        Shaping sh = pipes.shaping();
        if (!accepted) {
            if (sh != null && outcome == io.zmux.carrier.CarrierChannel.Outcome.UNKNOWN) sh.unknownProgress();
            if (sh != null) sh.reason(outcome == io.zmux.carrier.CarrierChannel.Outcome.EXPIRED
                    ? io.zmux.shaping.RuntimeReason.APPLICATION_EXPIRED : io.zmux.shaping.RuntimeReason.CARRIER_REJECTED);
            return;
        }
        var pipe = pipes.get(ticket.pipe);
        if (pipe != null) memberBytes(pipe, true, ticket.length);
        if (ticket.copy) return;
        tx.earn(ticket.payload);
        if (sh != null) sh.outbound(ticket.shape, 0, ticket.length, io.zmux.shaping.Event.Unit.NATIVE_DATAGRAM, true, false);
    }

    /** Whether any datagram remains for a record or a native channel. */
    public boolean hasChannelOutput() {
        return peerSeen && feat.carrierDatagrams() && !isClosing() && dg.queued();
    }

    public void channelChanged(long pipeId, long at) {
        deadlines.changed(ProtocolTimeline.Reason.SHAPING.bit());
        setNow(at);
        Pipe p = pipes.get(pipeId);
        if (p != null) observeChannel(p);
        noteDatagramPath();
    }

    /** Observes a channel identity at the send boundary, without changing pure capacity probes. */
    private Datagrams.ChannelUse observeChannel(Pipe pipe) {
        var use = channelOf(pipe);
        Pipe.OutLane lane = pipe.out() == null ? null : pipe.out().lane(0);
        if (lane == null) return use;
        long id = use.limit() == 0 ? Long.MIN_VALUE + 1 : use.id();
        long previous = lane.replaceNativeChannel(id);
        var shape = pipe.shape();
        if (previous != Long.MIN_VALUE && previous != id) {
            dg.feedback.resetOutbound(new DatagramFeedback.Path(pipe.id, true));
            if (shape != null) shape.cancelNative(io.zmux.shaping.RuntimeReason.STALE_GENERATION);
        } else if (shape != null && shape.nativeItem() != null) {
            shape.nativeRoom(use);
        }
        return use;
    }

    /** A native completion needs a fresh clock sample for shaping meters and callbacks. */
    public boolean usesCompletionClock() { return pipes.shaping() != null || path instanceof PathSession; }

    private final FrameDecoder channelDecoder = new FrameDecoder();
    private final io.zmux.internal.wire.Frame channelFrame = new io.zmux.internal.wire.Frame();

    /**
     * Processes a channel datagram received for pipe, b[pos, end) without its Pipe Stream ID
     *. It is not a record: it advances no PIPE_ACK position, is not
     * charged to the no-effect budget and restarts no idle timer. The payload is copied.
     */
    public void receiveChannelDatagram(long pipeId, java.nio.ByteBuffer b, int pos, int end, long t) {
        deadlines.changed(ProtocolTimeline.DATAGRAM | ProtocolTimeline.Reason.SHAPING.bit());
        setNow(t);
        if (isClosing()) return;
        Pipe pipe = pipes.get(pipeId);
        if (pipe == null || !peerSeen || !feat.carrierDatagrams()) {
            dg.dropReceived();
            return;
        }
        io.zmux.internal.wire.Frame f = channelFrame;
        try {
            ChannelDatagram.decodeFrame(channelDecoder, b, pos, end, f, feat.carrierDatagramPadding());
            gates.check(f);
            dg.onFrame(f, flows, pipeId, true);
            memberBytes(pipe, false, end - pos);
            pipes.wakeWriters();
        } catch (WireException e) {
            failLocal(e);
        } finally {
            f.src = null;
        }
    }

    /** Applies a receive-limiter decision once; true when the datagram reached the application's queue. */
    public boolean deliverDatagram(Datagrams.Limited token, boolean drop) {
        return dg.deliver(token, drop, isClosing());
    }

    // ---- multipath API ----

    private PathSession requireMultipath() throws CoreException {
        if (!(path instanceof PathSession mp)) throw CoreException.of(CoreException.Reason.NOT_NEGOTIATED);
        if (err != null) throw CoreException.session(err);
        return mp;
    }

    /** The handshake timeout of the session's carriers. */
    public boolean preambleInUse() { return peerSeen ? feat.preamble() : cfg.local().has(TransportParams.PREAMBLE); }

    /** The join deadline in nanoseconds. */
    public long joinDeadline() { return cfg.joinDeadline(); }

    public long handshakeTimeout() {
        return cfg.handshakeTimeout();
    }

    /** Whether the peer's Join refusal still blocks local openings. */
    public boolean joinBackedOff() {
        if (!(path instanceof PathSession mp)) return false;
        long retry = mp.joins.backoffTimer(now, false);
        return retry != Time.NEVER && retry > now;
    }

    /** Reserves local protocol room after the optional controller grant succeeds in the same owner turn. */
    public Joins.JoinReservation reserveJoins(int count, boolean replacement, int directions,
                                               java.util.function.BooleanSupplier grant) throws CoreException {
        return requireMultipath().joins.reserve(count, replacement, directions, grant);
    }

    /** Releases all slots this batch still holds. */
    public void releaseJoins(Joins.JoinReservation reservation) {
        if (path instanceof PathSession mp) mp.joins.release(reservation);
    }

    /** Starts a local join; returns the pipe ID, or -id - 1 when the cap or the refusal backoff holds it. */
    public long addPipe(Joins.JoinOptions options) throws CoreException {
        deadlines.changed(ProtocolTimeline.PATH);
        return addPipe(options, null, null);
    }

    /** Starts a local Join using the given batch's reservation. */
    public long addPipe(Joins.JoinOptions options, Joins.JoinReservation reservation, SessionCapacity.Member member)
            throws CoreException {
        if (joinPending()) throw CoreException.joinPending(joinMissing());
        Joins joins = requireMultipath().joins;
        if (reservation != null && !joins.consume(reservation)) throw CoreException.of(CoreException.Reason.PIPE_REFUSED);
        return joins.addPipe(options, member);
    }

    /** Binds the intended group before publishing its first attachment. */
    public SessionCapacity.Member newMember(String candidate, int directions, int attachments) throws CoreException {
        requireMultipath();
        if (!sessionPolicy.allows(candidate, directions)) throw CoreException.of(CoreException.Reason.PIPE_REFUSED);
        return pipes.member(candidate, directions, attachments);
    }

    /** The session's own locator (joins name it). */
    public byte[] locator() {
        return cfg.local().locator;
    }

    /** The session is closing and a local join's carrier is gone: its pipe takes no part in the close. */
    public void closingWithoutPipe(long id) {
        if (state == SessionState.CLOSED) return; // An earlier one ended the close: everything is released.
        Pipe p = pipes.get(id);
        if (p != null && p.mp() != null) mp().health.failPipe(p, TransportError.PIPE_FAILED, null);
        checkLingerDone();
    }

    /**
     * Whether removing pipe id would leave a direction without a live attachment while the
     * session has open streams: an application's removal that suspends or ends the session.
     */
    public boolean removalStrands(long id) {
        if (!(path instanceof PathSession) || streams.count() == 0) return false;
        Pipe p = pipes.get(id);
        if (p == null || p.mp() == null || p.mp().retired()) return false;
        boolean otherOut = false, otherIn = false;
        for (Pipe q : pipes.all()) {
            if (q == p || q.mp() == null || q.mp().retired()) continue;
            if (q.out() != null && q.mp().liveOut()) otherOut = true;
            if (q.in() != null && liveIn(q.mp().in())) otherIn = true;
        }
        return (p.out() != null && p.mp().liveOut() && !otherOut) || (p.in() != null && liveIn(p.mp().in()) && !otherIn);
    }

    private static boolean liveIn(int in) { return in == Attach.IN_JOINING || in == Attach.IN_OPEN; }

    public void removePipe(long id, boolean graceful) throws CoreException {
        if (!requireMultipath().health.removePipe(id, graceful)) throw CoreException.of(CoreException.Reason.INVALID);
    }

    /** Starts a planned single-attachment drain if the remaining members cover both directions. */
    public boolean drainPipe(long id) throws CoreException {
        return capacity.drainPipe(requireMultipath(), id);
    }

    public void setPipe(long id, Tiers.PipeSettings o) throws CoreException {
        deadlines.changed(ProtocolTimeline.PATH);
        if (!requireMultipath().tiers.setPipe(id, o)) throw CoreException.of(CoreException.Reason.INVALID);
    }

    public void revalidatePipes(long id, boolean all) throws CoreException {
        if (!requireMultipath().probation.revalidate(id, all)) throw CoreException.of(CoreException.Reason.INVALID);
    }

    /** Changes the session's scheduling policy; the previous policy's state is discarded. */
    public void setPolicy(CoreConfig.Policy p) {
        clearDeliveryBacklog();
        deadlines.changed(ProtocolTimeline.PATH);
        policy = p;
        path.policyChanged();
        pipes.wakeWriters();
    }

    /** Sets a stream direction's own policy; null returns it to the session's. */
    public void setStreamPolicy(Stream st, CoreConfig.Policy p) {
        clearDeliveryBacklog();
        deadlines.changed(ProtocolTimeline.PATH);
        PathScheduler.streamPolicy(st, p);
        pipes.wakeWriters();
    }

    /** Overrides the session's redundancy for a stream direction; null returns it to the default. */
    public void setRedundant(Stream st, Boolean on) {
        deadlines.changed(ProtocolTimeline.PATH);
        PathScheduler.streamRedundant(st, on == null ? 0 : on ? 1 : -1);
        pipes.wakeWriters();
    }

    /** A pool that dials for the session started (+1) or stopped (-1); its absence makes a suspension unattended. */
    public void attend(int delta) {
        if (path != null) path.attend(delta);
    }

    public boolean joinReady() {
        return !isClosing() && path.joinReady();
    }

    /** Closes a suspended session with ZMUX_RESUME_TIMEOUT (a listener's limit). */
    public void closeResumeTimeout() {
        if (state != SessionState.SUSPENDED) return;
        closeWith(new SessionError(SessionError.Kind.RESUME_TIMEOUT, TransportError.RESUME_TIMEOUT, 0, "", "",
                mp().resume.cause(), false), CloseFrame.of(false, TransportError.RESUME_TIMEOUT, 0, null));
    }

    /** [live outbound, live inbound] directions. */
    public boolean[] liveDirections() {
        return path.liveDirections(isClosing());
    }

    // ---- record shaping ----

    private Shaping shaping() {
        Shaping active = pipes.shaping();
        if (active == null) {
            active = newShaping();
            pipes.shaping(active);
        }
        return active;
    }

    private Shaping newShaping() {
        return new Shaping(new Shaping.Port() {
            @Override public long now() { return now; }
            @Override public int preambleLimit() {
                return preambleInUse() ? (peerSeen ? streams.preambleLimit() : StreamTable.MAX_PREAMBLE) : 0;
            }
            @Override public int peerRecordLimit() { return tx.peerRecordLimit(); }
            @Override public boolean peerZmux() { return Session.this.peerZmux(); }
            @Override public boolean standaloneRoom(int earlier) { return ka.limitedRoom(now, earlier); }
            @Override public void markStandalone() { ka.markLimited(now); }
            @Override public long earnBytes() { return tx.earnBytes(); }
            @Override public boolean dataWaiting() { return !sched.isEmpty(); }
            @Override public boolean coverEligible(Pipe p) {
                if (!peerSeen || isClosing() || p.out() == null || p.out().failed()) return false;
                Attach a = p.mp();
                if (a != null && (a.out() != Attach.OUT_ACTIVE || a.handshaking() || a.retired()
                        || (a.topologyExcluded() & Action.DIR_OUT) != 0)) return false;
                Pipe.OutLane lane = p.out().lane(0);
                return lane != null && lane.st.confirmed() && !lane.st.shut() && !lane.st.laneFirst()
                        && (p.id != 0 || lane.tx.tpSent());
            }
            @Override public long ackRelease(Pipe p) {
                if (ack == null) return Time.NEVER;
                return path instanceof PathSession m ? m.ackRelease(p) : ack.release();
            }
        }, pipes, shapingLimitsInForce(), start, now);
    }

    /** Installs f on every outbound pipe, replacing earlier instances; null: no shaping. */
    public void setShaper(io.zmux.shaping.ShaperFactory f) {
        setShaper(f, shapingBudget);
    }

    public void setShaper(io.zmux.shaping.ShaperFactory f, io.zmux.shaping.ShapingBudget budget) {
        var shape = shapingCandidate();
        shape.setFactory(f, shapingLimits.under(budget), () -> {
            shapingBudget = budget;
            publishShaping(shape);
        });
    }

    /** Installs x on one outbound pipe until the next setShaper; false when the pipe has no outbound direction. */
    public boolean setPipeShaper(long pipe, io.zmux.shaping.Shaper x) {
        var shape = shapingCandidate();
        return shape.setPipe(pipe, x, () -> publishShaping(shape));
    }

    private Shaping shapingCandidate() {
        Shaping active = pipes.shaping();
        return active == null ? newShaping() : active;
    }

    private void publishShaping(Shaping shape) {
        pipes.shaping(shape);
        clearDeliveryBacklog();
        deadlines.changed(ProtocolTimeline.Reason.SHAPING.bit());
    }

    void observeShaping(java.util.function.Consumer<io.zmux.shaping.Event> observer) {
        shaping().observe(observer);
    }

    /** A value that grows whenever a shaping counter changes; -1 when no shaper was ever set. */
    public long shapingSignature() {
        return (pipes.shaping() == null ? -1 : pipes.shaping().signature()) + handshakeFaults + handshakeUnplanned;
    }

    /** The shaping counters; null when no shaper was ever set. */
    public io.zmux.shaping.ShapingStats shapingStats() {
        if (pipes.shaping() == null && handshakeFaults == 0 && handshakeUnplanned == 0) return null;
        return shaping().stats(handshakeFaults, handshakeUnplanned);
    }

    /** The handshake shaper threw on a Join. */
    public void handshakeFault() { handshakeFaults++; }

    /** The handshake shaper had no plan for a Join's message. */
    public void handshakeUnplanned() { handshakeUnplanned++; }

    /** The counters of a Join's handshake, which exist with or without a shaper. */
    private long handshakeFaults, handshakeUnplanned;

    /**
     * Sets the quality band of one pipe, or (pipe negative) the default of the session's pipes;
     * false when the pipe has no outbound direction.
     */
    public boolean setQualityBand(long pipe, long rtt, double rateRatio, long minInterval) throws CoreException {
        return band(pipe, new QualityMonitor.Band(rtt, rateRatio, minInterval)); // minInterval 0: no floor
    }

    /** Turns pipe's quality reports off whatever the session's band is; false when it has no outbound direction. */
    public boolean offQualityBand(long pipe) throws CoreException {
        if (pipe < 0) throw new IllegalArgumentException("off is a pipe's setting");
        return band(pipe, QualityMonitor.Band.OFF);
    }

    /** Makes pipe follow the session's quality band again; false when it has no outbound direction. */
    public boolean clearQualityBand(long pipe) throws CoreException {
        return band(pipe, null);
    }

    private boolean band(long pipe, QualityMonitor.Band b) throws CoreException {
        Pipe p = null;
        if (pipe >= 0) {
            p = pipes.get(pipe);
            if (p == null) throw CoreException.of(CoreException.Reason.INVALID);
            if (p.out() == null) return false;
        }
        if (quality == null) {
            if (b == null) return true;
            quality = new QualityMonitor(this);
        }
        quality.band(p, b);
        return true;
    }

    /** Sets the capacity floor; it reports at once when the capacity is below it. */
    public void setCapacityFloor(double rate, double ratio) {
        if (quality == null) quality = new QualityMonitor(this);
        quality.setFloor(rate, ratio);
    }




    /** The multipath layer; only where multipath state (an {@link Attach}, a decision) shows it is in use. */
    PathSession mp() {
        return (PathSession) path;
    }

    /** A multipath layer of this session over its current pipes. */
    PathSession multiPath() {
        return new PathSession(cfg, peer, role == Role.INITIATOR, feat, stats, pipes, lanes, streamsView, sched, ack, ctl, conn, tx, ka, life,
                outbox, now, new PathPort(), exporterValue, joinPsk);
    }

    /** A single-pipe path layer of this session. */
    PathLayer singlePath() {
        return new SinglePath(new SinglePort());
    }

    /** The single-pipe behaviour behind {@link SinglePath}. */
    private final class SinglePort implements SinglePath.Port {
        @Override public boolean wantsControl(Pipe p, Pipe.OutLane ol) {
            return ol.lane == 0 && (tx.controlPending(true) || tx.datagramsPending(p, ol));
        }
        @Override public void write(RecordWriter w, Pipe p, Pipe.OutLane ol) { tx.writeSingle(w, p, ol); }

        @Override public void onPipeAck(io.zmux.internal.wire.Frame.AckEntries it) throws WireException { ack.onSinglePipeAck(it, pipes.get(0), now); }
        @Override public void inboundEnded(boolean failed, boolean allEnded, Throwable cause) { singleInboundEnded(failed, allEnded, cause); }
        @Override public void outboundFailed(Throwable cause) { closeWith(SessionError.pipesLost(cause), null); }
        @Override public long srttMax() { return pipes.srttMax(); }
    }

    /** The session behind the multipath layer. */
    private final class PathPort implements PathSession.Port {
        @Override public CoreSessionPolicy sessionPolicy() { return sessionPolicy; }
        @Override public long now() { return now; }
        @Override public int firstBatchStreamCount() { return streams.count(); }
        @Override public StreamSend.FirstBatch firstBatch(int limit, Batch receipt) {
            return ack == null ? null : send.firstBatch(limit, tx::canSendNow, path, receipt);
        }
        @Override public void firstBatchAccepted(long own) {
            tx.earn(own);
        }
        @Override public void applyBatch(byte[] frames) { Session.this.applyBatch(frames); }
        @Override public long joinPaddingRoom() { return handshakeRoom(); }
        @Override public boolean profileBounded() { return pipes.shaping() != null; }
        @Override public CoreConfig.Policy policy() { return policy; }
        @Override public void emit(Event e) { Session.this.emit(e); }
        @Override public void failLocal(WireException e) { Session.this.failLocal(e); }
        @Override public void closeWith(SessionError e, CloseFrame fr) { Session.this.closeWith(e, fr); }
        @Override public void checkLingerDone() { Session.this.checkLingerDone(); }
        @Override public boolean isClosing() { return Session.this.isClosing(); }
        @Override public long lingerTimeout() { return Session.this.lingerTimeout(); }
        @Override public long sendBufferLimit() { return Session.this.sendBufferLimit(); }
        @Override public SessionState state() { return state; }

        @Override
        public void capacityChanged() {
            capacity.changed();
            if (quality != null && quality.floorSet()) quality.checkFloor();
        }

        @Override
        public void memberHealth(Pipe pipe, io.zmux.PipeHealth before, io.zmux.PipeHealth after) {
            if (path instanceof PathSession mp) capacity.healthChanged(mp, pipe, before, after);
        }

        @Override
        public void suspended(long graceDeadline) {
            preSuspend = state;
            state = SessionState.SUSPENDED;
            stats.suspended(now);
            grace = graceDeadline;
            for (var observer : suspensionObservers) observer.accept(graceDeadline);
        }

        @Override
        public void resumed() {
            stats.resumed(now);
            state = preSuspend;
            if (life.goAwaySent() || life.goAwayReceived()) state = SessionState.DRAINING;
            grace = Time.NEVER;
            ka.activity(now);
        }
    }

    /** The session behind the stream components and the path layer's view of the streams. */
    private final class StreamsPort implements StreamTable.Port, StreamSend.Port, StreamRecv.Port, StreamsView {
        @Override public void memoryCommand(Runnable command) { onOwner.accept(command); }

        @Override public long now() { return now; }
        @Override public void wantWrite() {
            deadlines.changed(ProtocolTimeline.Reason.PACING.bit() | ProtocolTimeline.Reason.SHAPING.bit()
                    | ProtocolTimeline.Reason.KEEPALIVE.bit());
            pipes.wakeWriters();
        }
        @Override public SessionError error() { return err; }
        @Override public boolean established() { return peerSeen; }
        @Override public TransportParams peer() { return peer; }
        @Override public Features features() { return feat; }
        @Override public boolean responder() { return role == Role.RESPONDER; }
        @Override public void reset(Stream st, long code) { send.reset(st, code); }
        @Override public void rejectSend(Stream st) { send.reject(st); }
        @Override public void stopRecv(Stream st, long code) { recv.stopRecv(st, code); }
        @Override public void unreadDrained() {
            if (state == SessionState.CLOSING) life.checkReads(now);
        }
        @Override public void duplicateReceived(long n) {
            stats.duplicateReceived(n);
            Pipe p = rx.rxPipe();
            if (p != null && p.mp() != null) p.mp().ret.duplicate(n);
        }
        @Override public void discarded(Stream st) {
            if (path instanceof PathSession mp) mp.retention.discarded(st);
            lanes.unbind(st);
            if (pipes.shaping() != null) pipes.shaping().stream(io.zmux.shaping.EventKind.STREAM_END, st.tab.id());
        }
        @Override public void opened(Stream st) {
            dg.releaseHeld(st);
            if (pipes.shaping() != null) pipes.shaping().stream(io.zmux.shaping.EventKind.STREAM_OPEN, st.tab.id());
        }
        @Override public long sendBuffer() { return sessionSendBuffer(); }
        @Override public void unretain(long n) { path.unretain(n); }

        @Override public Stream get(long id) { return streams.get(id); }
        @Override public boolean pinId(Stream st) {
            if (st.tab.idSet()) return true;
            if (!streams.idAvailable(Stream.typeIndex(st.bidi))) return false;
            try { streams.pinId(st); return true; } catch (CoreException e) { return false; }
        }
        @Override public Iterable<Stream> live() { return streams.live(); }
        @Override public boolean streamsBlockedAtLimit(int t) { return streams.streamsBlockedAtLimit(t); }
        @Override public void releaseAcked(Stream st, long upTo) { send.release(st, upTo); }
        @Override public void sendTerminal(Stream st, int sendState) { send.sendTerminal(st, sendState); }
    }

    /** The session behind the lanes, the record receiver and the transmitter. */
    private final class IoPort implements Lanes.Port, RecordReceiver.Port, Transmitter.Port {
        @Override public long now() { return now; }
        @Override public SessionState state() { return state; }
        @Override public boolean established() { return peerSeen; }
        @Override public boolean familyConfirmed() { return peer.has(TransportParams.FAMILY); }

        @Override public boolean datagramAckDue() { return nextDatagramAck() <= now; }

        @Override public void writeDatagramAcks(RecordWriter writer) {
            if (dg.feedback.reportPending()) dg.feedback.write(writer, now, feedbackInboundPipes(), feedbackRtt);
        }
        @Override public boolean closing() { return isClosing(); }
        @Override public Features features() { return feat; }
        @Override public TransportParams peer() { return peer; }
        @Override public boolean urgentLane(Stream st) { return path.urgentLane(st); }
        @Override public void failLocal(WireException e) { Session.this.failLocal(e); }
        @Override public void recheckCloseWait() { Session.this.recheckCloseWait(); }

        @Override public Gates gates() { return gates; }
        @Override public void peerParams(TransportParams tp) throws WireException { processPeerParams(tp); }
        @Override public void dispatch(Pipe p, io.zmux.internal.wire.Frame f, ReadBuf buf) throws WireException { Session.this.dispatch(p, f, buf); }

        @Override
        public void closingFrame(Pipe p, io.zmux.internal.wire.Frame f, ReadBuf buf) {
            try {
                dispatch(p, f, buf);
            } catch (WireException ignored) {
                // The session is closing already.
            }
        }

        @Override public void charge() { Session.this.charge(); }

        @Override
        public void recorded(Pipe p, Pipe.InLane il, long posBytes, boolean elicit, boolean pipePing, boolean pingOnly, boolean traffic,
                             boolean activity) {
            Session.this.recorded(p, il, posBytes, elicit, pipePing, pingOnly, traffic, activity);
        }

        @Override public long srtt() { return Session.this.srtt(); }
        @Override public AckEngine ack() { return ack; }
        @Override public Pacer pacer() { return pacer; }
        @Override public PathLayer path() { return path; }
        @Override public void closeSent() { closeSentOn(); }
        @Override public void closeRead() { checkLingerDone(); }
        @Override public void closeWaitDone() { endCloseWait(); }
    }
}
