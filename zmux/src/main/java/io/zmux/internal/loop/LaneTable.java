package io.zmux.internal.loop;

import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.Stream;
import io.zmux.internal.core.StreamError;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * The loop side of a session's stream lanes: which lanes exist, the write leases they hold,
 * and the stream commands that must order with lane data. On the session's loop only.
 */
public final class LaneTable {
    private final SessionRuntime driver;

    LaneTable(SessionRuntime driver) {
        this.driver = driver;
    }


    /** A stream's two lanes; either is null when the stream lacks that part. */
    public record StreamLanes(Stream stream, SendLane send, RecvLane recv) {}

    private final java.util.IdentityHashMap<Stream, StreamLanes> map = new java.util.IdentityHashMap<>();
    private SendLane leased;

    /** The lanes of st, created on first use; on the loop. */
    public StreamLanes of(Stream st) {
        StreamLanes l = map.get(st);
        if (l != null) return l;
        l = new StreamLanes(st, st.hasSend() ? new SendLane(driver, st) : null, st.hasRecv() ? new RecvLane(driver, st) : null);
        map.put(st, l);
        if (driver.finished()) {
            if (l.send() != null) l.send().terminate(CoreException.session(driver.terminal()));
            if (l.recv() != null) l.recv().sessionEnded(CoreException.session(driver.terminal()));
        } else {
            if (l.recv() != null) l.recv().pump();
            if (l.send() != null) l.send().drain();
        }
        return l;
    }

    /** Ends st's lanes for a local stop of its receiving part; on the loop. */
    public void stopReceiving(Stream st, long code) {
        StreamLanes l = map.get(st);
        if (l != null && l.recv() != null) l.recv().close();
        driver.core.receiving().stop(st, code);
    }

    /** Moves written data into the core before a command that must follow it; on the loop. */
    public void flushSending(Stream st) {
        StreamLanes l = map.get(st);
        if (l != null && l.send() != null) l.send().drain();
    }

    /** Resets st's sending part after the data written before; on the loop. */
    public void resetSending(Stream st, long code) {
        flushSending(st);
        driver.core.sending().reset(st, code);
        endSending(st);
    }

    private void endSending(Stream st) {
        endSending(st, map.get(st));
    }

    private void endSending(Stream st, StreamLanes l) {
        if (l == null || l.send() == null) return;
        CoreException e = driver.core.sending().writeError(st);
        l.send().terminate(e != null ? e : CoreException.stream(StreamError.of(
                StreamError.Kind.CLOSED_LOCALLY, -1)));
        l.send().drain();
    }

    /** Releases st (handle closed or collected): both lanes end, then the core releases it; on the loop. */
    public void releaseStream(Stream st) {
        StreamLanes l = map.remove(st);
        if (l != null && l.recv() != null) l.recv().close();
        driver.pickers.forget(st);
        driver.core.streams().release(st);
        endSending(st, l);
    }

    /** Declines st before reading; on the loop. */
    public void rejectStream(Stream st) {
        StreamLanes l = map.get(st);
        if (l != null && l.recv() != null) l.recv().close();
        flushSending(st);
        driver.core.streams().reject(st);
        endSending(st);
    }

    /** Hands st's sending to loop-run code: written data moves into the core, no lease is granted again; on the loop. */
    public void hostOwnSend(Stream st) {
        StreamLanes l = of(st);
        if (l.send() != null) l.send().hostTakeover();
    }

    /** Calls watcher (on the loop) with a failed end of st's receiving lane; null removes it. */
    public void watchRecvEnd(Stream st, Consumer<CoreException> watcher) {
        StreamLanes l = watcher == null ? map.get(st) : of(st);
        if (l != null && l.recv() != null) l.recv().watchEnd(watcher);
    }

    /** Calls watcher (on the loop) with the outcome that ends st's sending lane; null removes it. */
    public void watchSendEnd(Stream st, Consumer<CoreException> watcher) {
        StreamLanes l = watcher == null ? map.get(st) : of(st);
        if (l != null && l.send() != null) l.send().watchEnd(watcher);
    }

    /** Makes loop-run code st's only reader: reader (null: none) runs after each push; on the loop. */
    public RecvLane hostOwnRecv(Stream st, Runnable reader) {
        StreamLanes l = of(st);
        if (l.recv() != null) l.recv().hostReader(reader);
        return l.recv();
    }

    void listLease(SendLane lane) {
        lane.nextLeased = leased;
        leased = lane;
    }

    /** Loop rounds that drove this session; lanes date their activity by it. */
    private long round;
    /** Lanes whose writer waits while the share is held by other lanes' leases. */
    private final ArrayList<SendLane> starved = new ArrayList<>();
    private final boolean[] reclaimed = new boolean[1];

    long round() { return round; }

    /** A waiting writer found no lease; idle leases of other lanes are reclaimed for it. */
    void leaseShort(SendLane lane) {
        if (!starved.contains(lane)) starved.add(lane);
    }

    /**
     * Only while a writer waits for lease: leases idle since before the previous round go
     * back to their streams' share, then the waiting lanes ask again. Without competition
     * nothing is reclaimed, so an idle lane's next write does not wait for the loop.
     */
    void reclaimLeases() {
        round++;
        if (starved.isEmpty()) return;
        starved.removeIf(l -> !l.starved());
        if (starved.isEmpty()) return;
        reclaimed[0] = false;
        SendLane prev = null;
        for (SendLane l = leased; l != null; ) {
            SendLane next = l.nextLeased;
            if (l.reclaimIdle(round, reclaimed)) {
                prev = l;
            } else {
                if (prev == null) leased = next; else prev.nextLeased = next;
                l.nextLeased = null;
            }
            l = next;
        }
        if (reclaimed[0]) {
            List<SendLane> s = new ArrayList<>(starved);
            starved.clear();
            for (SendLane l : s) l.drain();
        }
        if (leased == null) starved.clear();
        else if (!starved.isEmpty()) driver.markDirty();
    }

    /** The session ended: every lane ends with outcome; on the loop. */
    void sessionEnded(CoreException outcome) {
        for (StreamLanes l : map.values()) {
            if (l.send() != null) l.send().terminate(outcome);
            if (l.recv() != null) l.recv().sessionEnded(outcome);
        }
        leased = null;
        starved.clear();
    }
}
