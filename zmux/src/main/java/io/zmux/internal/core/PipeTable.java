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

    PipeTable(Outbox outbox) {
        this.outbox = outbox;
    }

    /** The pipe with the ID, or null. */
    Pipe get(long id) {
        return byId.get(id);
    }

    /** Every pipe in attachment order; change it only through {@link #add} and {@link #remove}. */
    List<Pipe> all() {
        return list;
    }

    void add(Pipe p) {
        list.add(p);
        byId.put(p.id, p);
    }

    void remove(Pipe p) {
        list.remove(p);
        if (byId.get(p.id) == p) byId.remove(p.id);
    }

    void clear() {
        list.clear();
        byId.clear();
    }

    /** Attaches pipe 0 in both directions; the driver then polls it for the first record. */
    public void attachBootstrap(CarrierInfo in, CarrierInfo out, Waiter writerWaiter) {
        Pipe p = new Pipe(0, bootstrapIn(), new Pipe.OutDir(out, writerWaiter), null, out.kind());
        p.label(out.label() != null ? out.label() : in != null ? in.label() : null);
        add(p);
        outbox.wake(writerWaiter);
    }

    /** Replaces both directions of the bootstrap pipe after the carrier rejected its early data. */
    void restartBootstrap(Pipe p, CarrierInfo info) {
        p.restart(bootstrapIn(), new Pipe.OutDir(info, p.out().sig));
    }

    /** The bootstrap pipe's inbound side: its first record carries the peer's transport parameters. */
    private static Pipe.InDir bootstrapIn() {
        return new Pipe.InDir(new Pipe.InLane(0, true, false, false));
    }

    // ---- wake-ups ----

    /** Wakes the writers of every outbound pipe. */
    void wakeWriters() {
        for (Pipe p : list) if (p.out() != null) wake(p);
    }

    /** Wakes the writers of every lane of an outbound pipe. */
    void wake(Pipe p) {
        outbox.wake(p.out().sig);
        for (int i = 1; i < p.out().lanes.size(); i++) outbox.wake(p.out().lanes.get(i).st.sig());
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
        if (p != null) p.conn(c);
    }

    /** The kind of a pipe's carrier, null when undeclared or the pipe is gone. */
    public io.zmux.CarrierKind carrier(long pipeId) {
        Pipe p = get(pipeId);
        return p == null ? null : p.carrier();
    }

    /** The first record of an in-band join starts with the PIPE_JOIN the listener processed. */
    public void skipJoinFrame(long pipeId) {
        Pipe p = get(pipeId);
        if (p != null && p.in() != null) RecordReceiver.skipJoin(p.in().lanes.getFirst());
    }

    /** Installs the actual carrier properties before the reserved pipe performs I/O. */
    public void attachReserved(long id, CarrierInfo in, CarrierInfo out) throws CoreException {
        Pipe pipe = get(id);
        if (pipe == null || pipe.mp() != null && pipe.mp().health.retired()) throw CoreException.of(CoreException.Reason.PIPE_REFUSED);
        if (pipe.mp() == null || !pipe.mp().oob || !pipe.mp().joinerLocal || in == null && out == null
                || (pipe.in() == null) != (in == null) || (pipe.out() == null) != (out == null)) {
            throw CoreException.of(CoreException.Reason.INVALID);
        }
        if (pipe.out() != null) Lanes.rebind(pipe.out(), out);
        CarrierInfo info = out != null ? out : in;
        pipe.carrier(info.kind());
        pipe.label(info.label());
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

    /** Aborts the carriers of every pipe. */
    void abortAll() {
        for (Pipe p : list) outbox.action(new Action(Action.Kind.ABORT, p.id, 0, Action.DIR_BIDI));
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
        return l != null && !l.st.shut() && (p.mp() == null || p.mp().liveOut() && !p.mp().health.retired());
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
        if (p.out() != null && p.mp().rtt.has()) return p.mp().rtt.min();
        long best = -1;
        for (Pipe q : list) {
            if (q == p || q.mp() == null || q.out() == null || !q.mp().rtt.has()) continue;
            if (p.mp().fate != 0 && (q.mp().fate != p.mp().fate || q.mp().joinerLocal != p.mp().joinerLocal)) continue;
            best = Math.max(best, q.mp().rtt.min());
        }
        if (best < 0 && p.mp().fate != 0) {
            for (Pipe q : list) if (q != p && q.mp() != null && q.out() != null && q.mp().rtt.has()) best = Math.max(best, q.mp().rtt.min());
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
