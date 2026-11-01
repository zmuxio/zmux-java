package io.zmux.internal.core;

import io.zmux.internal.wire.TransportParams;

/**
 * Suspension and resumption of a multipath session and the reinjection hold of new attachments (RESUME.md 4, 6).
 */
final class Resume {
    /** What Resume asks of the rest of the path layer and the session. */
    interface Port {
        /** Queues an event (with the session's quality floor check). */
        void emit(Event e);

        /** The session clock. */
        long now();

        /** The session leaves SUSPENDED (RESUME.md 4.4). */
        void resumed();

        /** The session state. */
        SessionState state();

        /** The session enters SUSPENDED with the grace deadline. */
        void suspended(long graceDeadline);
    }

    private final Port port;
    private final CoreConfig cfg;
    private final TransportParams peer;
    private final PipeTable pipes;
    private Throwable suspendCause;

    Resume(CoreConfig cfg, TransportParams peer, PipeTable pipes, Port port) {
        this.port = port;
        this.cfg = cfg;
        this.peer = peer;
        this.pipes = pipes;
    }

    /** Enters SUSPENDED after the last pipe was lost (RESUME.md 4). */
    void suspend(Throwable cause) {
        if (port.state() == SessionState.SUSPENDED) return;
        suspendCause = cause;
        port.suspended(Time.add(port.now(), graceDuration()));
        port.emit(Event.simple(Event.Kind.SUSPENDED));
    }

    long graceDuration() {
        long g = cfg.resume();
        if (peer.resumeGraceMillis > 0) {
            long pg = Time.millis(peer.resumeGraceMillis);
            if (pg < g || g == 0) g = pg;
        }
        return g;
    }

    /** Leaves SUSPENDED once an ACTIVE outbound and an OPEN inbound pipe exist (RESUME.md 4.4). */
    void checkResumed() {
        if (port.state() != SessionState.SUSPENDED) return;
        boolean act = false, open = false;
        for (Pipe p : pipes.all()) {
            if (p.mp() == null || p.mp().health.retired()) continue;
            if (p.out() != null && p.mp().health.out() == Attach.OUT_ACTIVE) act = true;
            if (p.in() != null && p.mp().health.in() == Attach.IN_OPEN) open = true;
        }
        if (!act || !open) return;
        port.resumed();
        port.emit(Event.simple(Event.Kind.RESUMED));
        pipes.wakeWriters();
    }

    void startReinjectHold(Pipe p) {
        Attach a = p.mp();
        a.res.held1 = true;
        a.res.holdSent = 0;
        a.res.holdUntil = Time.add(port.now(), a.rtt.smoothed());
    }

    boolean reinjectRoom(Pipe p) {
        Attach a = p.mp();
        if (!a.res.held1 || port.now() >= a.res.holdUntil) {
            a.res.held1 = false;
            return true;
        }
        return a.res.holdSent < cfg.spreadChunk();
    }

    void endReinjectHolds() {
        for (Pipe p : pipes.all()) {
            if (p.mp() != null && p.mp().res.held1) {
                p.mp().res.held1 = false;
                pipes.wakeWriters();
            }
        }
    }

    /** Why the session suspended (for the resume timeout error). */
    Throwable cause() {
        return suspendCause;
    }

    /** n reinjected bytes went on p during its reinjection hold. */
    void resent(Pipe p, long n) {
        p.mp().res.holdSent += n;
    }

    /** The reinjection hold of one attachment (RESUME.md). */
    static final class Slot {
        private long holdUntil;
        private boolean held1;
        private long holdSent;

        Slot() {
        }

        long holdUntil() {
            return holdUntil;
        }

        boolean held1() {
            return held1;
        }

        long holdSent() {
            return holdSent;
        }
    }
}
