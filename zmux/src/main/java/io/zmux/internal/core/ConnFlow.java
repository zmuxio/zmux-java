package io.zmux.internal.core;

import io.zmux.internal.memory.SharedMemory;

/**
 * Session-level flow control: the receive window, its released
 * and held credit, the buffered count, and the send credit with the send-buffer sums. The
 * caller supplies the buffer cap and memory pressure, which depend on the stream table and the
 * memory budget; a due MAX_DATA is reported through the injected callback.
 */
final class ConnFlow {
    private final Runnable creditDue;
    private final SharedMemory.Receive budget;
    private final long sendFloor;
    private final Runnable historyRoom;
    private final OwnerQueue<SendHistory> historyWaiters = new OwnerQueue<>();
    private volatile boolean historyWaiting;
    // Sparse nodes leave with their waiting direction, even after a high stream-count watermark.
    private final java.util.concurrent.ConcurrentSkipListMap<Long, SendBacking> backingWaiters =
            new java.util.concurrent.ConcurrentSkipListMap<>();
    private long nextBacking;
    /** Backing capacity reserved per byte of the receive window. */
    static final long RECEIVE_COST = ReceiveLayout.CREDIT_COST;
    static final long MIN_INITIAL_WINDOW = 16L << 10;

    // receiving
    private long win;
    private long adv;
    private long rel;
    private long recv;
    private long buffered;
    private long settled;
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

    ConnFlow(long initialWindow, Runnable creditDue, SharedMemory.Receive budget, boolean multipath, Runnable historyRoom) {
        this.creditDue = creditDue;
        this.budget = budget;
        this.sendFloor = sendFloor(multipath);
        this.historyRoom = historyRoom;
        win = initialWindow;
        adv = initialWindow;
    }

    static SharedMemory.Receive reserveInitial(SharedMemory budget, long desired, boolean bidi, boolean multipath) {
        if (budget == null) return null;
        SharedMemory.Receive receive = budget.reserveReceive(Math.min(desired, MIN_INITIAL_WINDOW), desired, RECEIVE_COST,
                ReceiveLayout.WORKING, ReceiveLayout.LOAN_FLOOR,
                bidi ? ReceiveLayout.STREAM_CHARGE : 0, bidi ? sendFloor(multipath) : 0);
        if (receive == null) throw new SharedMemory.Unavailable();
        return receive;
    }

    long reservedBytes() { return budget == null ? 0 : budget.reservedBytes(); }

    boolean underPressure() { return budget != null && budget.pressure(); }

    void releaseReservation() {
        if (budget != null) budget.close();
    }

    long reserveStreams(long n, boolean bidi, boolean local) {
        return budget == null ? n : budget.reserveStreams(n, receiveDirectionCost(bidi, local), sendDirectionCost(bidi, local));
    }

    void releaseStreams(long n, boolean bidi, boolean local) {
        if (budget != null) budget.releaseStream(n * receiveDirectionCost(bidi, local), n * sendDirectionCost(bidi, local));
    }

    static long receiveDirectionCost(boolean bidi, boolean local) {
        return bidi || !local ? ReceiveLayout.STREAM_CHARGE : 0;
    }

    static long sendFloor(boolean multipath) {
        return SendBacking.FLOOR + (multipath ? SendHistory.FLOOR : 0);
    }

    private long sendDirectionCost(boolean bidi, boolean local) {
        return bidi || local ? sendFloor : 0;
    }

    SendBacking newBacking() { return new SendBacking(this, budget, sendFloor, nextBacking++); }

    // Read under the account's lock. Payload pin callbacks can update this wait off the owner.
    private final java.util.function.LongSupplier backingThreshold = () -> {
        long first = historyWaiting ? SendHistory.PAGE_CHARGE : Long.MAX_VALUE;
        for (SendBacking backing : backingWaiters.values()) {
            long requested = backing.waitingBytes();
            if (requested != 0) first = Math.min(first, requested);
        }
        return first;
    };

    boolean reserveOptional(long bytes) { return budget == null || budget.reserveView(bytes); }

    void releaseOptional(long bytes) {
        if (budget != null) budget.releaseView(bytes);
    }

    void waitHistory(SendHistory history, boolean waiting) {
        if (waiting) historyWaiters.add(history.roomLink);
        else history.roomLink.remove();
        boolean pending = !historyWaiters.isEmpty();
        if (pending != historyWaiting) {
            historyWaiting = pending;
            updateBackingWait();
        }
    }

    void historyFloorFreed(SendHistory history) {
        history.ready();
        historyRoom.run();
    }

    void backingWaitChanged(SendBacking backing) {
        if (budget == null) return;
        // A physical release can clear the request while the owner publishes its index entry.
        boolean waiting;
        do {
            waiting = backing.waitingBytes() != 0;
            if (waiting) backingWaiters.put(backing.id(), backing);
            else backingWaiters.remove(backing.id(), backing);
        } while (waiting == (backing.waitingBytes() == 0));
        updateBackingWait();
    }

    void backingRoom() {
        if (historyWaiting && budget.viewRoom() >= SendHistory.PAGE_CHARGE) {
            while (!historyWaiters.isEmpty()) historyWaiters.first().value().ready();
            historyRoom.run();
        }
        for (SendBacking backing : backingWaiters.values()) backing.ready();
        if (historyWaiting || !backingWaiters.isEmpty()) updateBackingWait();
    }

    private void updateBackingWait() {
        if (budget != null) budget.waitForBacking(backingThreshold);
    }

    void onRoom(Runnable ready) {
        if (budget != null) budget.onRoom(ready);
    }

    /** min(W/4, max(W/16, 256 KiB)). */
    static long updateThreshold(long w) {
        return Math.clamp(256 << 10, w / 16, w / 4); // w >= 0, so w/16 <= w/4
    }

    /** Whether a scope's released credit must be sent now. */
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
     * (FLOW_CONTROL_ERROR).
     */
    boolean received(long n) {
        if (n > adv - recv) return false;
        recv += n;
        return true;
    }

    /**
     * Accounts n bytes stored in a stream buffer; the only function that raises the buffered count. Below the
     * buffer cap and without memory pressure the session credit of a byte is released as it is buffered (a
     * MUST of SPEC.md 8.3 rule 4, so that one unread stream cannot hold back the others); a byte at or above the
     * cap, or under pressure, holds its credit until it is consumed ({@link #unbuffer}), never longer.
     */
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

    /** Accounts n buffered bytes consumed; the only function that lowers the buffered count. */
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

    /** Physically retired offsets cannot require storage again, including arrival history. */
    void retire(long n) {
        if (n == 0) return;
        settled += n;
        checkCredit();
    }

    void checkCredit() {
        if (creditDue(proposedMaxData(), adv, win, recv, peerBlock)) creditDue.run();
    }

    /** Raises the window to w (auto-tuning or the application's predicate). */
    void grow(long w) {
        w = Math.min(w, io.zmux.internal.wire.VarInt.MAX);
        if (w <= win) return;
        if (budget != null) w = Math.min(w, budget.commit(Math.max(win, adv - settled), w));
        if (w == win) return;
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
        long proposed = proposedMaxData();
        if (budget == null) return proposed;
        // SPEC.md §8.3 rule 4 permits deferring credit under memory pressure. The
        // account records insufficient backing as the same pressure used by buffer/unbuffer.
        // Credit includes offsets inside gaps as well as offsets above the highest received.
        long granted = budget.commit(Math.max(win, adv - settled), proposed - settled);
        // Previously advertised credit remains valid even if the current proposal is smaller.
        //noinspection MathClampMigration
        return Math.max(adv, Math.min(proposed, settled + granted));
    }

    private long proposedMaxData() {
        return Math.min(rel + win, io.zmux.internal.wire.VarInt.MAX);
    }

    /** Whether MAX_DATA v is worth sending (force: a copy is owed). */
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

    /** The peer's initial limit (or an assumed one). */
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

    /** The send-side bytes that occupy memory. */
    long sendMemory() {
        return sendHeld - Math.min(sendHeld, leased);
    }

    void held(long before, long after) {
        held(before, after, 0);
    }

    /** Changes the filled and unfilled portions of one send direction together. */
    void held(long before, long after, long leaseChange) {
        leased += leaseChange;
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
