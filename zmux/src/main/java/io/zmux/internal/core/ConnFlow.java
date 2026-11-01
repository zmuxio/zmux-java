package io.zmux.internal.core;

/**
 * Session-level flow control (SPEC.md Sections 8.2 and 8.3): the receive window, its released
 * and held credit, the buffered count, and the send credit with the send-buffer sums. The
 * caller supplies the buffer cap and memory pressure, which depend on the stream table and the
 * memory budget; a due MAX_DATA is reported through the injected callback.
 */
final class ConnFlow {
    private final Runnable creditDue;

    // receiving
    private long win;
    private long adv;
    private long rel;
    private long recv;
    private long buffered;
    private long held;
    private long blockSeen;
    private boolean peerBlock;
    private Tuner tune;

    // sending
    private long sendMax;
    private long sent;
    private long blocked;
    private long creditAt;
    private long sendHeld;
    private long smallHeld;
    private int bigCount;

    ConnFlow(long initialWindow, Runnable creditDue) {
        this.creditDue = creditDue;
        win = initialWindow;
        adv = initialWindow;
    }

    /** min(W/4, max(W/16, 256 KiB)). */
    static long updateThreshold(long w) {
        return Math.clamp(256 << 10, w / 16, w / 4); // w >= 0, so w/16 <= w/4
    }

    /** Whether a scope's released credit must be sent now (SPEC.md Section 8.3 rules 2 and 3). */
    static boolean creditDue(long c, long l, long w, long received, boolean blockedFrame) {
        if (c <= l) return false;
        long d = c - l;
        if (d >= updateThreshold(w)) return true;
        if (received >= l || blockedFrame) return true;
        return l - received < w / 4 && d >= Math.min(w / 4, 32 << 10);
    }

    // ---- receiving ----

    long window() { return win; }
    long advertised() { return adv; }
    long received() { return recv; }
    long buffered() { return buffered; }
    boolean peerBlocked() { return peerBlock; }
    Tuner tuner() { return tune; }

    /** The tuner, started once a quarter of the window was received; null before. */
    Tuner tunerIfDue() {
        if (tune == null && recv >= win / 4) tune = new Tuner();
        return tune;
    }

    /**
     * Counts newly received session bytes; false when they exceed the advertised limit
     * (FLOW_CONTROL_ERROR, SPEC.md Section 8.2).
     */
    boolean received(long n) {
        recv += n;
        return recv <= adv;
    }

    /** Accounts n bytes stored in a stream buffer; the only function that raises the buffered count (C1). */
    void buffer(long n, long cap, boolean pressure) {
        if (n == 0) return;
        if (buffered < cap && !pressure) {
            long r = Math.min(n, cap - buffered);
            rel += r;
            held += n - r;
        } else {
            held += n;
        }
        buffered += n;
        checkCredit();
    }

    /** Accounts n buffered bytes consumed; the only function that lowers the buffered count (C1). */
    void unbuffer(long n, long cap, boolean pressure) {
        if (n == 0) return;
        if (n > buffered) throw new IllegalStateException("buffered bytes below zero");
        buffered -= n;
        if (held > 0) {
            if (buffered < cap && !pressure) {
                rel += held;
                held = 0;
            } else {
                long r = Math.min(n, held);
                rel += r;
                held -= r;
            }
        }
        checkCredit();
    }

    /** Releases session credit for bytes that are never buffered (discarded on arrival, a reset's gap). */
    void releaseUnbuffered(long n) {
        if (n == 0) return;
        rel += n;
        checkCredit();
    }

    void checkCredit() {
        if (creditDue(rel + win, adv, win, recv, peerBlock)) creditDue.run();
    }

    /** Raises the window to w (auto-tuning or the application's predicate). */
    void grow(long w) {
        if (w <= win) return;
        win = w;
        checkCredit();
    }

    /** A DATA_BLOCKED at v (v ≤ the advertised limit, checked by the caller); true when it repeats an older one. */
    boolean onDataBlocked(long v) {
        boolean stale = v <= blockSeen && blockSeen > 0;
        blockSeen = Math.max(blockSeen, v);
        if (tune != null) tune.blockedAt(v);
        if (v == adv) {
            peerBlock = true;
            checkCredit();
        }
        return stale;
    }

    /** The MAX_DATA value to send now. */
    long maxDataValue() {
        return rel + win;
    }

    /** Whether MAX_DATA v is worth sending (force: a copy is owed, MULTIPATH.md 6.5). */
    boolean maxDataWanted(long v, boolean force) {
        return v > adv || adv == 0 || force;
    }

    /** MAX_DATA v was written. */
    void maxDataSent(long v, long now) {
        if (tune != null && v > adv) tune.sent(adv, now, recv == adv);
        adv = v;
        peerBlock = false;
    }

    // ---- sending ----

    long sendMax() { return sendMax; }
    long sent() { return sent; }
    long creditAt() { return creditAt; }

    /** Unused session send credit. */
    long room() {
        return sendMax - Math.min(sendMax, sent);
    }

    /** The peer's initial limit (or an assumed one, SPEC.md 6.1.2). */
    void sendLimit(long v) {
        sendMax = v;
    }

    /** A MAX_DATA from the peer; false when it raises nothing. */
    boolean onMaxData(long v, long now) {
        if (v <= sendMax) return false;
        if (sent >= sendMax) creditAt = now;
        sendMax = v;
        return true;
    }

    void consumed(long n) {
        sent += n;
    }

    /** Everything is resent from offset zero under unknown limits (early data restart). */
    void restartSend() {
        sent = 0;
        sendMax = 0;
    }

    /** Marks the session blocked at its current limit; false when already marked there. */
    boolean markBlocked() {
        if (blocked == sendMax + 1) return false;
        blocked = sendMax + 1;
        return true;
    }

    /** Still blocked at the limit that was marked (DATA_BLOCKED is current). */
    boolean blockedAtLimit() {
        return blocked == sendMax + 1;
    }

    // ---- send-buffer sums ----

    long sendHeld() { return sendHeld; }
    long smallHeld() { return smallHeld; }
    int bigCount() { return bigCount; }

    /** Bytes of sendHeld that are leases not yet filled: room, not memory. */
    private long leased;

    void leased(long delta) {
        leased += delta;
    }

    /** The send-side bytes that occupy memory. */
    long sendMemory() {
        return sendHeld - Math.min(sendHeld, leased);
    }

    void held(long before, long after) {
        long bh = before >>> 2, ah = after >>> 2;
        sendHeld = sendHeld - Math.min(sendHeld, bh) + ah;
        if ((before & 2) != 0) smallHeld -= Math.min(smallHeld, bh);
        if ((after & 2) != 0) smallHeld += ah;
        if ((before & 1) != 0) bigCount--;
        if ((after & 1) != 0) bigCount++;
    }

    /** CLOSED: no buffer remains. */
    void clearBuffers() {
        buffered = sendHeld = smallHeld = leased = 0;
        bigCount = 0;
    }
}
