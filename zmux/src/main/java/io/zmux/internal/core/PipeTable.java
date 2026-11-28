package io.zmux.internal.core;

import java.util.ArrayList;
import java.util.List;

/**
 * The session's pipes: an ordered list (attachment order, which every scan over the pipes
 * follows) and an index by pipe ID. Owned by the session's loop; carrier commands and
 * wake-ups go through the {@link Outbox}.
 */
public final class PipeTable {
    private final ArrayList<Pipe> list = new ArrayList<>(1);
    /** Record shaping; null until a shaper is set. */
    private Shaping shaping;

    Shaping shaping() { return shaping; }

    /** Installs record shaping (the session, once). */
    void shaping(Shaping s) { shaping = s; }
    private final LongMap<Pipe> byId = new LongMap<>(4);
    private final Outbox outbox;

    private final java.util.function.LongConsumer carrierReplaced;

    PipeTable(Outbox outbox, java.util.function.LongConsumer carrierReplaced) {
        this.outbox = outbox;
        this.carrierReplaced = carrierReplaced;
    }

    /** The pipe with the ID, or null. */
    Pipe get(long id) {
        return byId.get(id);
    }

    /** Every pipe in attachment order; change it only through {@link #add} and {@link #remove}. */
    List<Pipe> all() {
        return list;
    }

    /** Moves on every change to the pipes, their outbound lanes or a pipe's outbound failure: what a scan over them depends on. */
    private int version;

    /** The pipe set's change count; equal across two reads exactly when no pipe, outbound lane or outbound direction came, went or failed. */
    int version() { return version; }

    /** An outbound lane joined a pipe, or a pipe's outbound direction failed. */
    void changed() { version++; }

    private long nextMember;

    SessionCapacity.Member member(String candidate, int expected, int attachments) {
        if (nextMember == -1L) throw new IllegalStateException("member IDs exhausted");
        return new SessionCapacity.Member(new io.zmux.MemberId(++nextMember), candidate, expected, attachments);
    }

    void add(Pipe p) {
        if (p.member() == null) {
            p.member(member("", SessionCapacity.configuredDirections(p), 1));
        }
        p.member().joined(io.zmux.PipeId.of(p.id));
        version++;
        list.add(p);
        Pipe previous = byId.get(p.id);
        if (previous != null) retireOutbound(previous);
        byId.put(p.id, p);
        if (shaping != null && p.out() != null) shaping.of(p);
    }

    void remove(Pipe p) {
        Shaping.pipeDown(p);
        version++;
        list.remove(p);
        retireOutbound(p);
        if (byId.get(p.id) == p) byId.remove(p.id);
        if (!goneW.isEmpty()) goneW.wakeEvery(outbox);
    }

    void clear() {
        version++;
        for (Pipe pipe : list) retireOutbound(pipe);
        list.clear();
        byId.clear();
        if (!goneW.isEmpty()) goneW.wakeEvery(outbox);
    }

    /** Releases outbound backing while retaining terminal pipe snapshots. */
    void releaseOutbound() {
        for (Pipe pipe : list) retireOutbound(pipe);
    }

    private static void retireOutbound(Pipe pipe) {
        if (pipe.out() == null) return;
        pipe.out().cancelDeadlines();
        pipe.out().snd.clearHistory();
        for (Pipe.OutLane lane : pipe.out().lanes) lane.retire();
    }

    /** The waiters for a pipe's removal. */
    private final WaitList goneW = new WaitList();

    /** Registers w until the next removal of a pipe. */
    void waitRemoved(Waiter w) { goneW.add(w); }

    /** Withdraws a registration of {@link #waitRemoved}. */
    void cancelRemovedWait(Waiter w) { goneW.remove(w); }

    /** Attaches pipe 0 in both directions; the driver then polls it for the first record. */
    public void attachBootstrap(CarrierInfo in, CarrierInfo out) {
        Pipe p = new Pipe(0, bootstrapIn(), new Pipe.OutDir(out), null, out.kind());
        p.label(out.label() != null ? out.label() : in != null ? in.label() : null);
        p.variant(out.variant());
        add(p);
    }

    /** The bootstrap pipe's inbound side: its first record carries the peer's transport parameters. */
    private static Pipe.InDir bootstrapIn() {
        return new Pipe.InDir(new Pipe.InLane(0, true, false));
    }

    // ---- wake-ups ----

    private Runnable sharedWriter;

    /** Registers the host's shared-work notification. The notification does not run IO. */
    public void sharedWriter(Runnable ready) {
        sharedWriter = ready;
    }

    /** One attachment's reservation identity, invalidated on replacement. */
    public static final class Writer implements AutoCloseable {
        private final Runnable changed;
        private final Runnable channel;
        private Runnable record;

        Writer(Runnable changed, Runnable record, Runnable channel) {
            this.changed = changed;
            this.record = record;
            this.channel = channel;
        }

        public boolean current() { return record != null; }

        void changed() { if (current()) changed.run(); }
        void record() { if (current()) record.run(); }
        void channel() { if (current()) channel.run(); }

        void invalidate() { record = null; }

        @Override public void close() { invalidate(); }
    }

    /** Registers the writer of an outbound lane. */
    public Writer writer(long id, long lane, Runnable record, Runnable channel) {
        Pipe p = get(id);
        Pipe.OutLane l = p == null || p.out() == null ? null : p.out().lane(lane);
        Writer writer = new Writer(this::wakeWriters, record, channel);
        if (l != null) l.writer(writer);
        else writer.invalidate();
        return writer;
    }

    /** Transfers the prepaid history of an attached outbound lane to its core lifetime. */
    public void attachHistory(long id, long lane, io.zmux.internal.buffer.WriteWorkspace backing) {
        Pipe p = get(id);
        Pipe.OutLane l = p == null || p.out() == null ? null : p.out().lane(lane);
        if (l != null) l.log.attach(backing);
    }

    /** Publishes session-wide work, coalesced by the host until a writer can claim it. */
    void wakeWriters() {
        if (sharedWriter != null) sharedWriter.run();
    }

    /** Wakes the writers of every lane of an outbound pipe. */
    void wake(Pipe p) {
        for (Pipe.OutLane lane : p.out().lanes) lane.requestWrite();
    }

    /** Wakes the writers of a pipe (a carrier was attached). */
    public void wakeWriters(long id) {
        Pipe p = get(id);
        if (p != null && p.out() != null) wake(p);
    }

    // ---- carriers ----

    /** The number of attachments. */
    public int count() { return list.size(); }

    /** The attachments' IDs. */
    public void ids(List<Long> dst) {
        for (Pipe p : list) dst.add(p.id);
    }

    /** The open outbound lanes of a pipe, lane 0 first. */
    public void outLanes(long pipeId, List<Long> dst) {
        Pipe p = get(pipeId);
        if (p == null || p.out() == null) return;
        for (Pipe.OutLane l : p.out().lanes) if (!l.st.refused()) dst.add(l.lane);
    }

    /** Records the admission value of the carrier that joined a pipe (opaque to zmux). */
    public void setAdmitted(long pipeId, Object v) {
        Pipe p = get(pipeId);
        if (p != null) p.admitted(v);
    }

    /** The admission value of a pipe's carrier; null without one or once the pipe is gone. */
    public Object admitted(long pipeId) {
        Pipe p = get(pipeId);
        return p == null ? null : p.admitted();
    }

    public void setCarrierConn(long pipeId, Object c) {
        Pipe p = get(pipeId);
        if (p == null) return;
        if (p.conn() != null && p.conn() != c) carrierReplaced.accept(pipeId);
        p.conn(c);
    }

    /** The kind of a pipe's carrier, null when undeclared or the pipe is gone. */
    public io.zmux.CarrierKind carrier(long pipeId) {
        Pipe p = get(pipeId);
        return p == null ? null : p.carrier();
    }

    /** The immutable wire fate group chosen when a pipe was reserved or joined. */
    public long fateGroup(long id) throws CoreException {
        Pipe p = get(id);
        if (p == null) throw CoreException.of(CoreException.Reason.INVALID);
        return p.mp() == null ? 0 : p.mp().fate;
    }

    /** Shuts down the write side of every outbound lane not shut yet. */
    void shutdownOut() {
        for (Pipe p : list) {
            if (p.out() == null) continue;
            for (Pipe.OutLane l : p.out().lanes) {
                if (Lanes.shut(l)) {
                    outbox.action(new Action(Action.Kind.SHUTDOWN, p.id, l.lane, Action.DIR_OUT));
                }
            }
        }
    }

    /** Closes every pipe's carriers gracefully. */
    void closeAll() {
        for (Pipe p : list) closeCarrier(p, Action.Kind.CLOSE);
    }

    void abortAll() {
        for (Pipe p : list) closeCarrier(p, Action.Kind.ABORT);
    }

    private void closeCarrier(Pipe p, Action.Kind kind) {
        if (p.requestCarrierClose()) outbox.action(new Action(kind, p.id, 0, Action.DIR_BIDI));
    }

    // ---- queries ----

    /** Whether some outbound pipe still takes a CONNECTION_CLOSE. */
    boolean hasLiveOutbound() {
        for (Pipe p : list) if (canCarryClose(p)) return true;
        return false;
    }

    /** An outbound pipe that still takes a CONNECTION_CLOSE: not failed, not shut. */
    private static boolean canCarryClose(Pipe p) {
        if (p.out() == null || p.out().failed()) return false;
        Pipe.OutLane l = p.out().lane(0);
        return l != null && !l.st.shut() && (p.mp() == null || (p.mp().liveOut() && !p.mp().health.retired() && !p.mp().handshaking()));
    }

    /** Whether CONNECTION_CLOSE went out on every outbound pipe that takes one. */
    boolean closeSentEverywhere() {
        for (Pipe p : list) {
            if (!canCarryClose(p)) continue;
            Pipe.OutLane l = p.out().lane(0);
            if (l != null && !l.tx.closeSent()) return false;
        }
        return true;
    }

    /**
     * Whether every live outbound pipe that took CONNECTION_CLOSE has written it: a failed
     * carrier no longer counts.
     */
    boolean closeWritten() {
        for (Pipe p : list) {
            if (p.out() == null || p.out().failed()) continue;
            Pipe.OutLane l = p.out().lane(0);
            if (l != null && l.tx.closeSent() && !l.tx.closeDone()) return false;
        }
        return true;
    }

    /**
     * Whether each outbound pipe that took CONNECTION_CLOSE finished writing it and shut all
     * of its live write sides (FIN or close_notify out, not only requested).
     */
    boolean closeShut() {
        for (Pipe p : list) {
            if (p.out() == null || p.out().failed()) continue;
            Pipe.OutLane l = p.out().lane(0);
            if (l != null && l.tx.closeSent() && !closeComplete(p)) return false;
        }
        return true;
    }

    /** Releases carriers whose protocol linger ended, independently of the application read drain. */
    void endLingeringPipes() {
        for (Pipe p : list) {
            Pipe.InDir in = p.in();
            // SPEC §6.5: EOF and CONNECTION_CLOSE both complete the inbound wait.
            if (in != null && !in.ended() && !in.closeRead()) continue;
            if (!closeComplete(p)) continue;
            if (in != null) in.end();
            closeCarrier(p, Action.Kind.CLOSE);
        }
    }

    /** Whether this pipe has no more close bytes to write or outbound lanes to shut. */
    private static boolean closeComplete(Pipe p) {
        if (p.out() == null || p.out().failed()) return true;
        Pipe.OutLane first = p.out().lanes.getFirst();
        boolean sentClose = first.tx.closeSent();
        if (sentClose ? !first.tx.closeDone() : canCarryClose(p)) return false;
        for (Pipe.OutLane lane : p.out().lanes) {
            if (!lane.st.refused() && (sentClose || lane.st.shut()) && !lane.tx.shutDone()) return false;
        }
        return true;
    }

    /** Whether every inbound direction ended. */
    boolean inboundEnded() {
        for (Pipe p : list) if (p.in() != null && !p.in().ended()) return false;
        return true;
    }

    /** The largest smoothed RTT over outbound pipes with a sample (single pipe). */
    long srttMax() {
        long m = 0;
        for (Pipe p : list) {
            if (p.out() != null && p.out().snd.rtt().has() && p.out().snd.rtt().smoothed() > m) m = p.out().snd.rtt().smoothed();
        }
        return m;
    }

    /** pipe_min_rtt of an attachment, or -1. */
    long minRtt(Pipe p) {
        if (p == null || p.mp() == null) return -1;
        if (p.out() != null && p.mp().rtt().has()) return p.mp().rtt().min();
        long best = -1;
        for (Pipe q : list) {
            if (q == p || q.mp() == null || q.out() == null || !q.mp().rtt().has()) continue;
            if (p.mp().fate != 0 && (q.mp().fate != p.mp().fate || q.mp().joinerLocal != p.mp().joinerLocal)) continue;
            best = Math.max(best, q.mp().rtt().min());
        }
        if (best < 0 && p.mp().fate != 0) {
            for (Pipe q : list) if (q != p && q.mp() != null && q.out() != null && q.mp().rtt().has()) best = Math.max(best, q.mp().rtt().min());
        }
        return best;
    }

    /** Whether stream directions can spread: several working outbound pipes, or one with several lanes. */
    boolean spreadable() {
        int usable = 0;
        boolean independent = false;
        for (Pipe p : list) {
            if (p.out() != null && !p.out().failed()) {
                usable++;
                if (p.out().lanes.size() > 1) independent = true;
            }
        }
        return usable > 1 || independent;
    }
}
