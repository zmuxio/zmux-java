package io.zmux.internal.core;

import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameEncoder;
import io.zmux.carrier.VarInt;

/**
 * The ending of one session: the handshake deadline, the two-phase GOAWAY (EXTENSIONS.md
 * Section 3), the pending CONNECTION_CLOSE with its lane wait, and the linger timer (SPEC.md
 * Section 6.5). The session's state machine and what closing does to streams and pipes stay
 * with the session; this component owns the facts and deadlines they are decided from.
 */
final class Lifecycle {
    /** A CONNECTION_CLOSE to send (SPEC.md Section 4.6). */
    record CloseFrame(boolean app, long code, long frameType, byte[] reason) {
        static CloseFrame of(boolean app, long code, long frameType, byte[] reason) {
            return new CloseFrame(app, code, frameType, reason == null ? new byte[0] : reason);
        }
    }

    // timers
    private long handshake;
    private long linger = Time.NEVER;
    private long laneWait = Time.NEVER;

    // CONNECTION_CLOSE
    private CloseFrame close;
    private boolean closePending;
    private boolean waitLanes;
    private boolean closedEmitted;

    // GOAWAY
    private byte[] reason = new byte[0];
    private long code;
    private final long[] sentWater = new long[2];
    private int phase;
    private final long[] recvWater = new long[2];
    private long recvCode;
    private final long[] maxOffered = new long[2];
    private boolean recheck;
    private boolean sent;
    private boolean received;
    private boolean closeWhenDone;

    Lifecycle(long handshakeDeadline) {
        handshake = handshakeDeadline;
    }

    // ---- timers ----

    void established() {
        handshake = Time.NEVER;
    }

    /** True once when the handshake deadline passed at t. */
    boolean handshakeDue(long t) {
        if (t < handshake) return false;
        handshake = Time.NEVER;
        return true;
    }

    /** True once when the linger timer expired at t. */
    boolean lingerDue(long t) {
        if (t < linger) return false;
        linger = Time.NEVER;
        return true;
    }

    /** True once when the wait for lanes to drain before CONNECTION_CLOSE expired at t. */
    boolean laneWaitDue(long t) {
        if (!waitLanes || t < laneWait) return false;
        waitLanes = false;
        return true;
    }

    void linger(long until) {
        linger = until;
    }

    long deadline() {
        long t = Math.min(handshake, linger);
        if (waitLanes) t = Math.min(t, laneWait);
        return t;
    }

    /** CLOSED: no deadline remains. */
    void clearTimers() {
        handshake = linger = laneWait = Time.NEVER;
    }

    // ---- CONNECTION_CLOSE ----

    /** Queues fr; laneWaitUntil, unless NEVER, holds it until the lanes drained (MULTIPATH.md 2.5). */
    void closeQueued(CloseFrame fr, long laneWaitUntil) {
        close = fr;
        closePending = true;
        linger = Time.NEVER;
        if (laneWaitUntil != Time.NEVER) {
            waitLanes = true;
            laneWait = laneWaitUntil;
        }
    }

    boolean closePending() {
        return closePending;
    }

    /** CONNECTION_CLOSE is held until the lanes drained. */
    boolean waitingLanes() {
        return waitLanes;
    }

    /** The lanes drained, or the peer's CONNECTION_CLOSE arrived: stop holding. */
    void lanesDone() {
        waitLanes = false;
    }

    /** Every live outbound pipe took CONNECTION_CLOSE; linger until lingerUntil. */
    void closeSent(long lingerUntil) {
        closePending = false;
        linger = lingerUntil;
    }

    /** Writes the queued CONNECTION_CLOSE; nothing follows it. False when it did not fit. */
    boolean writeClose(RecordWriter w) {
        CloseFrame c = close;
        int n = FrameEncoder.closeLength(c.app(), c.code(), c.frameType(), c.reason().length);
        w.end();
        if (!w.frame(n, false, bb -> FrameEncoder.close(bb, c.app(), c.code(), c.frameType(), c.reason()))) return false;
        w.close();
        return true;
    }

    /** True the first time: the CLOSED event is emitted once. */
    boolean markClosedEmitted() {
        if (closedEmitted) return false;
        closedEmitted = true;
        return true;
    }

    // ---- GOAWAY ----

    /** A graceful shutdown was asked for with code and reason. */
    void shutdownRequested(long code, byte[] reason) {
        closeWhenDone = true;
        this.code = code;
        this.reason = reason == null ? new byte[0] : reason;
    }

    boolean closeWhenDone() {
        return closeWhenDone;
    }

    /** The shutdown may close now: asked for, and not between the two GOAWAYs. */
    boolean shutdownReady() {
        return closeWhenDone && phase != 1;
    }

    long shutdownCode() {
        return code;
    }

    byte[] shutdownReason() {
        return reason;
    }

    /** A stream went away while shutting down: re-check at the end of the round. */
    void streamGone() {
        if (closeWhenDone) recheck = true;
    }

    boolean takeRecheck() {
        boolean r = recheck;
        recheck = false;
        return r;
    }

    /** Sends the first GOAWAY at the current stream limits; false when one was sent already. */
    boolean beginGoAway(long bidiLimit, long uniLimit) {
        if (sent) return false;
        sent = true;
        phase = 1;
        sentWater[0] = bidiLimit;
        sentWater[1] = uniLimit;
        return true;
    }

    boolean goAwaySent() {
        return sent;
    }

    long sentWater(int t) {
        return sentWater[t];
    }

    /** A peer stream of type t with index idx - 1 was offered to the application. */
    void offered(int t, long idx) {
        if (idx > maxOffered[t]) maxOffered[t] = idx;
    }

    /**
     * The fence ping returned: phase 2 (EXTENSIONS.md Section 3.3). False when not in phase 1;
     * otherwise lowered reports whether the watermarks dropped to what was offered.
     */
    boolean finalGoAway(boolean[] lowered) {
        if (phase != 1) return false;
        phase = 2;
        for (int t = 0; t < 2; t++) {
            if (maxOffered[t] < sentWater[t]) {
                sentWater[t] = maxOffered[t];
                lowered[0] = true;
            }
        }
        return true;
    }

    boolean writeGoAway(RecordWriter w) {
        long b0 = sentWater[0], b1 = sentWater[1], c = code;
        int n = 4 + VarInt.length(b0) + VarInt.length(b1) + VarInt.length(c);
        if (!w.frame(n, true, bb -> FrameEncoder.goAway(bb, b0, b1, c))) return false;
        w.logFrame(FrameDesc.of(FrameDesc.Kind.GOAWAY));
        return true;
    }

    /** A received GOAWAY; false when it lowers nothing. */
    boolean onGoAway(Frame f) {
        boolean lowered = false;
        if (!received) {
            received = true;
            recvWater[0] = f.maxBidi;
            recvWater[1] = f.maxUni;
            lowered = true;
        } else {
            if (f.maxBidi < recvWater[0]) {
                recvWater[0] = f.maxBidi;
                lowered = true;
            }
            if (f.maxUni < recvWater[1]) {
                recvWater[1] = f.maxUni;
                lowered = true;
            }
        }
        if (lowered) recvCode = f.code;
        return lowered;
    }

    boolean goAwayReceived() {
        return received;
    }

    long recvWater(int t) {
        return recvWater[t];
    }

    /** The received GOAWAY watermarks [maxBidi, maxUni, code], or null. */
    long[] peerGoAway() {
        return received ? new long[] {recvWater[0], recvWater[1], recvCode} : null;
    }
}
