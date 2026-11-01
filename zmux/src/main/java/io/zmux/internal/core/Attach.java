package io.zmux.internal.core;

import io.zmux.internal.wire.TransportError;

/** The per-attachment state of a multipath session (MULTIPATH.md, SCHEDULER.md). */
final class Attach {
    // Outbound pipe states (STATE_MACHINE.md 3.1).
    static final int OUT_NONE = 0, OUT_JOINING = 1, OUT_ACTIVE = 2, OUT_SUSPECT = 3, OUT_DRAINING = 4, OUT_FAILED = 5,
            OUT_CLOSED = 6;
    static final PipeInfo.Out[] OUT_STATES = PipeInfo.Out.values();
    // Inbound pipe states (STATE_MACHINE.md 3.2).
    static final int IN_NONE = 0, IN_JOINING = 1, IN_OPEN = 2, IN_ENDED = 3;
    static final PipeInfo.In[] IN_STATES = PipeInfo.In.values();

    final boolean joinerLocal;
    /** MULTIPATH 5.1 direction from the joiner: 0 up, 1 down, 2 bidirectional. */
    final int dir;
    final long fate;
    /** Whether the PIPE_JOIN travels out of band. */
    final boolean oob;
    final long suspectFloor;
    final long deadTimeout;
    /** The pipe's probe interval k, 0: off, -1: the session's; its probe jitter, NaN: the session's. */
    final long probeInterval;
    final double probeJitter;
    final RttEstimator rtt = new RttEstimator();

    // The per-component state of the attachment; each part is written only by its component.
    final PathHealth.Slot health;
    final Joins.Slot joining;
    final PathSession.Link link;
    final Tiers.Slot tier;
    final Probation.Slot prob;
    final PathScheduler.Slot sched;
    final Retention.Slot ret;
    final Resume.Slot res;

    Attach(boolean joinerLocal, int dir, long fate, boolean oob, long suspectFloor, long deadTimeout, long probeInterval, double probeJitter, int out, int in, boolean firstRecord,
           long now) {
        this.joinerLocal = joinerLocal;
        this.dir = dir;
        this.fate = fate;
        this.oob = oob;
        this.suspectFloor = suspectFloor;
        this.deadTimeout = deadTimeout;
        this.probeInterval = probeInterval;
        this.probeJitter = probeJitter;
        health = new PathHealth.Slot(out, in);
        joining = new Joins.Slot(now);
        link = new PathSession.Link(now, firstRecord);
        tier = new Tiers.Slot();
        prob = new Probation.Slot();
        sched = new PathScheduler.Slot();
        ret = new Retention.Slot();
        res = new Resume.Slot();
    }

    int out() {
        return health.out();
    }

    int in() {
        return health.in();
    }

    boolean retired() {
        return health.retired();
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

    /** Whether bytes sent at sent may give estimator samples (E2). */
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

    /** Whether a graceful self-removal puts its PIPE_ABANDON on its own pipe (MULTIPATH 8.1). */
    boolean selfGraceful(boolean hasOut) {
        return health.abandonCode() == TransportError.NO_ERROR && hasOut && liveOut() && !health.drainSuspect();
    }
}
