package io.zmux.internal.core;

import io.zmux.internal.wire.TransportError;

/** The per-attachment state of a multipath session. */
final class Attach {
    // Outbound pipe states.
    static final int OUT_NONE = 0, OUT_JOINING = 1, OUT_ACTIVE = 2, OUT_SUSPECT = 3, OUT_DRAINING = 4, OUT_FAILED = 5,
            OUT_CLOSED = 6;
    static final PipeInfo.Out[] OUT_STATES = PipeInfo.Out.values();
    // Inbound pipe states.
    static final int IN_NONE = 0, IN_JOINING = 1, IN_OPEN = 2, IN_ENDED = 3;
    static final PipeInfo.In[] IN_STATES = PipeInfo.In.values();

    final boolean joinerLocal;
    /** The direction of a join pipe from the joiner: 0 up, 1 down, 2 bidirectional. */
    final int dir;
    final long fate;
    private int topologyExcluded;
    private long peerAuthorization;

    int topologyExcluded() { return topologyExcluded; }
    void topologyExcluded(int mask) { topologyExcluded = mask; }
    long peerAuthorization() { return peerAuthorization; }
    void peerAuthorization(long generation) { peerAuthorization = generation; }
    /**
     * The carrier's record mode: set by the pipe's Join handshake, and for pipe 0 copied once from the
     * configuration ({@link CoreConfig#firstPipeMode()}), which is immutable. The two are one value, not
     * two sources: the configuration states pipe 0's mode for the session's
     * life (it still counts for the weakest mode after pipe 0 is gone, and answers for a single-path
     * session with no attachment), and nothing writes pipe 0's copy later, so they cannot drift.
     */
    private io.zmux.internal.join.Mode mode;
    /** The Join handshake is running: nothing but it on the carrier. */
    private boolean handshaking;
    private byte[] binding;
    private io.zmux.internal.join.Joiner joiner;
    private io.zmux.internal.join.Framing joinFraming;
    private boolean tpSkipped;
    private long suspectFloor;
    /** The pipe's own dead timeout: 0 follows the carrier, then the session; negative: stall-based failure off. */
    private long deadTimeout;
    /** The carrier reports its own failure (CarrierInfo.ownFailure): its dead timeout defaults to off. */
    private boolean ownFailure;
    /** What a pipe keeps from when it turned suspect, once the settings were replaced (held: kept is set). */
    private boolean keptHeld;
    private long keptFloor, keptDead;
    /** The pipe's probe interval k, 0: off, -1: the session's; its probe jitter, NaN: the session's. */
    private long probeInterval;
    private double probeJitter;
    /** The RTT of the pipe's outbound direction: the same estimator as its OutDir's once the pipe has one (Pipe shares it). */
    private RttEstimator rtt = new RttEstimator();

    RttEstimator rtt() { return rtt; }

    /** Takes r as the pipe's one estimator (Pipe shares it with the outbound direction). */
    void useRtt(RttEstimator r) { rtt = r; }

    // The per-component state of the attachment; each part is written only by its component.
    final PathHealth.Slot health;
    final HealthProjection.Slot projection = new HealthProjection.Slot();
    final Joins.Slot joining;
    final PathSession.Link link;
    final Tiers.Slot tier;
    final Probation.Slot prob;
    final PathScheduler.Slot sched;
    final Retention.Slot ret;
    final Resume.Slot res;

    Attach(boolean joinerLocal, int dir, long fate, long suspectFloor, long deadTimeout, long probeInterval, double probeJitter, int out, int in, boolean firstRecord,
           long now) {
        this.joinerLocal = joinerLocal;
        this.dir = dir;
        this.fate = fate;
        this.suspectFloor = suspectFloor;
        this.deadTimeout = deadTimeout;
        this.probeInterval = probeInterval;
        this.probeJitter = probeJitter;
        health = new PathHealth.Slot(out, in);
        joining = new Joins.Slot(now);
        if (out != OUT_JOINING && in != IN_JOINING) joined(now);
        link = new PathSession.Link(now, firstRecord);
        tier = new Tiers.Slot();
        prob = new Probation.Slot();
        sched = new PathScheduler.Slot();
        ret = new Retention.Slot();
        res = new Resume.Slot();
    }

    private Long joinedAt;

    java.util.OptionalLong joinedAt() {
        return joinedAt == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(joinedAt);
    }

    void joined(long now) { joinedAt = now; }

    /** The Group asked for this join; 0 for the default. */
    private long group;

    long group() { return group; }

    void group(long g) { group = g; }

    /** The Group the join's key exchange used (the one asked for, or the default's pick); 0 before one ran, and for pipe 0. */
    private long usedGroup;

    long usedGroup() { return usedGroup; }

    void usedGroup(long g) { usedGroup = g; }

    void startHandshake(io.zmux.internal.join.Mode m, byte[] b) { mode = m; binding = b; handshaking = true; }
    void endHandshake() { handshaking = false; joiner = null; binding = null; verified = null; }

    /** A Reply that verified and is not yet acted on: the attachment is still joining. */
    private io.zmux.internal.join.ReplyPayload verified;
    io.zmux.internal.join.ReplyPayload verified() { return verified; }
    void verified(io.zmux.internal.join.ReplyPayload r) { verified = r; }
    void mode(io.zmux.internal.join.Mode m) { mode = m; }
    io.zmux.internal.join.Mode mode() { return mode; }
    boolean handshaking() { return handshaking; }
    /** A handshake that still runs; a failed one stays handshaking, since nothing else was allowed on its carrier. */
    boolean joining() { return handshaking && health.out() != OUT_FAILED && health.in() != IN_ENDED; }
    byte[] binding() { return binding; }
    io.zmux.internal.join.Joiner joiner() { return joiner; }
    io.zmux.internal.join.Framing joinFraming() { return joinFraming; }
    void joiner(io.zmux.internal.join.Joiner j, io.zmux.internal.join.Framing f) { joiner = j; joinFraming = f; }
    boolean tpSkipped() { return tpSkipped; }
    void tpSkipped(boolean v) { tpSkipped = v; }

    int out() {
        return health.out();
    }

    int in() {
        return health.in();
    }

    boolean retired() {
        return health.retired();
    }

    /** The attachment failed, closed or is draining: no direction will carry data again. */
    boolean ended(boolean hasOut) {
        int out = health.out();
        if (hasOut) return out == OUT_DRAINING || out == OUT_FAILED || out == OUT_CLOSED;
        return health.in() == IN_ENDED;
    }

    boolean liveOut() {
        int out = health.out();
        return out == OUT_JOINING || out == OUT_ACTIVE || out == OUT_SUSPECT || out == OUT_DRAINING;
    }

    long effPrio() {
        return tier.sendPrio() != null ? tier.sendPrio() : tier.peerPrio();
    }

    boolean effStandby() {
        return tier.sendStandby() != null ? tier.sendStandby() : tier.peerStandby();
    }

    /** min_rtt_p, or the smoothed initial value before a sample. */
    long minRtt() {
        return rtt.has() ? rtt.min() : rtt.smoothed();
    }

    long suspectFloor() { return suspectFloor; }
    long deadTimeout() { return deadTimeout; }
    long probeInterval() { return probeInterval; }
    double probeJitter() { return probeJitter; }
    boolean ownFailure() { return ownFailure; }
    void ownFailure(boolean on) { ownFailure = on; }
    boolean keptHeld() { return keptHeld; }
    long keptFloor() { return keptFloor; }
    long keptDead() { return keptDead; }

    /** Keeps the floor and the dead timeout in force (dead negative: off) while the pipe stays suspect. */
    void kept(long floor, long dead) { keptHeld = true; keptFloor = floor; keptDead = dead; }

    /** Drops what was kept. */
    void unkept() { keptHeld = false; }

    /** Replaces the pipe's own liveness settings. */
    void liveness(long suspectFloor, long deadTimeout, long probeInterval, double probeJitter) {
        this.suspectFloor = suspectFloor;
        this.deadTimeout = deadTimeout;
        this.probeInterval = probeInterval;
        this.probeJitter = probeJitter;
    }

    /** Whether bytes sent at sent may give estimator samples. */
    boolean sampleOk(long sent) {
        return !health.noSample() || sent > health.noSampleAt();
    }

    /** The start of an ack stall. */
    long tref() {
        long t = Math.max(link.lastAdvance(), link.liveAt());
        if (link.unacked() && link.oldestAt() > t) t = link.oldestAt();
        return t;
    }

    long progress() {
        return Math.max(link.lastAdvance(), link.lastRecord());
    }

    /** Whether a graceful self-removal puts its PIPE_ABANDON on its own pipe. */
    boolean selfGraceful(boolean hasOut) {
        return health.abandonCode() == TransportError.NO_ERROR && hasOut && liveOut() && !health.drainSuspect();
    }
}
