package io.zmux.internal.core;

/**
 * The control frames waiting to be written (SPEC.md Section 9.2): the session-level flags
 * (MAX_DATA, MAX_STREAMS, DATA_BLOCKED, STREAMS_BLOCKED, GOAWAY) and the streams with
 * pending stream-level frames, in the order they were first queued. The stream list is
 * intrusive and doubly linked through each stream's {@link Entry}: queueing and removal
 * are O(1) and allocate nothing.
 */
final class ControlQueue {
    // Stream-level frame bits.
    static final int MAX_STREAM_DATA = 1, STOP = 2, RESET = 4, PRIORITY = 8, BLOCKED = 16;

    /** One stream's place in the queue; its fields are written only by ControlQueue. */
    static final class Entry {
        private final Stream st;
        private Entry prev, next;
        private int bits;
        private boolean linked;

        Entry(Stream st) {
            this.st = st;
        }
    }

    private final Runnable wantWrite;

    private boolean maxData;
    private boolean forceMaxData;
    private final boolean[] maxStreams = new boolean[2];
    private final boolean[] streamsBlocked = new boolean[2];
    private boolean dataBlocked;
    private boolean goAway;

    private Entry head, tail;

    /** wantWrite runs whenever a stream-level frame is queued. */
    ControlQueue(Runnable wantWrite) {
        this.wantWrite = wantWrite;
    }

    // ---- session-level flags ----

    boolean maxData() { return maxData; }
    boolean maxDataForced() { return forceMaxData; }
    boolean maxStreams(int t) { return maxStreams[t]; }
    boolean streamsBlocked(int t) { return streamsBlocked[t]; }
    boolean dataBlocked() { return dataBlocked; }
    boolean goAway() { return goAway; }

    void wantMaxData() { maxData = true; }
    /** MAX_DATA is due even when its value did not grow (a lost frame, MULTIPATH.md 7.4). */
    void forceMaxData() { maxData = true; forceMaxData = true; }
    void maxDataDone() { maxData = false; }
    void forceMaxDataDone() { forceMaxData = false; }
    void wantMaxStreams(int t) { maxStreams[t] = true; }
    void maxStreamsDone(int t) { maxStreams[t] = false; }
    void wantStreamsBlocked(int t) { streamsBlocked[t] = true; }
    void streamsBlockedDone(int t) { streamsBlocked[t] = false; }
    void wantDataBlocked() { dataBlocked = true; }
    void dataBlockedDone() { dataBlocked = false; }
    void wantGoAway() { goAway = true; }
    void goAwayDone() { goAway = false; }

    /** Whether any control frame is pending. */
    boolean pending() {
        return maxData || maxStreams[0] || maxStreams[1] || dataBlocked || streamsBlocked[0] || streamsBlocked[1] || goAway
                || head != null;
    }

    /** Whether a frame that is written after stream data (blocked reports, stream frames) is pending. */
    boolean pendingAfterData() {
        return dataBlocked || streamsBlocked[0] || streamsBlocked[1] || head != null;
    }

    // ---- stream-level frames ----

    /** Marks a pending stream-level frame of st and asks for a write. */
    void stream(Stream st, int bit) {
        Entry e = st.cq;
        if (!e.linked) {
            e.linked = true;
            e.prev = tail;
            e.next = null;
            if (tail == null) head = e;
            else tail.next = e;
            tail = e;
        }
        e.bits |= bit;
        wantWrite.run();
    }

    /** Whether st has the frame bit pending. */
    static boolean has(Stream st, int bit) {
        return (st.cq.bits & bit) != 0;
    }

    /** The pending frame bits of st. */
    static int bits(Stream st) {
        return st.cq.bits;
    }

    /** Clears bit of st; the stream stays queued until {@link #settle}. */
    static void done(Stream st, int bit) {
        st.cq.bits &= ~bit;
    }

    /** Removes st from the queue once it has nothing pending. */
    void settle(Stream st) {
        if (st.cq.bits == 0) remove(st);
    }

    /** Drops every pending frame of st. */
    void remove(Stream st) {
        Entry e = st.cq;
        e.bits = 0;
        if (!e.linked) return;
        e.linked = false;
        if (e.prev == null) head = e.next;
        else e.prev.next = e.next;
        if (e.next == null) tail = e.prev;
        else e.next.prev = e.prev;
        e.prev = null;
        e.next = null;
    }

    /** The first queued stream, or null. */
    Stream first() {
        return head == null ? null : head.st;
    }

    /** The stream queued after st, or null; read it before st may leave the queue. */
    static Stream next(Stream st) {
        Entry n = st.cq.next;
        return n == null ? null : n.st;
    }

    /** Whether a queued stream has bit pending. */
    boolean anyStream(int bit) {
        for (Entry e = head; e != null; e = e.next) if ((e.bits & bit) != 0) return true;
        return false;
    }

    /** Forgets the stream list (CLOSED). */
    void clear() {
        for (Entry e = head; e != null; ) {
            Entry n = e.next;
            e.linked = false;
            e.prev = null;
            e.next = null;
            e = n;
        }
        head = tail = null;
    }
}
