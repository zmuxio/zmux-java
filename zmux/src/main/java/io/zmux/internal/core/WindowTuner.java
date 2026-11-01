package io.zmux.internal.core;

/**
 * Receive-window auto-tuning (SPEC.md Section 8.3 rule 6): per-scope delivery-rate samples,
 * the RTT they are measured over, and growth of the stream and session windows within the
 * memory allowance, optionally through the application's predicate.
 */
public final class WindowTuner {
    /** What the tuner asks of the session. */
    interface Port {
        /** Multipath: RTTs come from the pipes the data arrived on. */
        boolean multipath();

        /** pipe_min_rtt of the pipe the current input arrived on, or -1 (multipath only). */
        long inputPipeMinRtt();

        /** max(session window cap, S) (SPEC.md Section 8.3 rule 4). */
        long bufferCap();

        /** The largest session window the memory budget allows. */
        long allowance();

        /** Memory pressure, or a closing session: no growth. */
        boolean pressure();

        boolean closing();
    }

    private final CoreConfig cfg;
    private final ConnFlow conn;
    private final Port port;
    private long carrierMin;
    private final Tuner.MinFilter loopMin = new Tuner.MinFilter();
    private boolean paused;
    private long proposal;
    private boolean asking;

    WindowTuner(CoreConfig cfg, ConnFlow conn, Port port) {
        this.cfg = cfg;
        this.conn = conn;
        this.port = port;
    }

    boolean on() {
        return !cfg.noAutotune();
    }

    /** The bootstrap carrier's minimum RTT, 0 when it reports none. */
    void carrierMinRtt(long d) {
        carrierMin = d;
    }

    long carrierMinRtt() {
        return carrierMin;
    }

    /** A loop RTT sample (QX_PING, PIPE_ACK). */
    void loopRtt(long rtt, long now) {
        loopMin.update(rtt, now);
    }

    private long rtt(Tuner t) {
        if (port.multipath()) return t.maxRttSeen();
        if (carrierMin > 0) return carrierMin;
        return loopMin.has() ? loopMin.value() : -1;
    }

    private void scopeData(Tuner t, long off, long now) {
        boolean multi = port.multipath();
        if (multi) t.seenRtt(port.inputPipeMinRtt());
        t.arrived(off, now, multi ? null : loopMin, Math.max(Tuner.RATE_FLOOR, rtt(t)));
    }

    private long grow(Tuner t, long window, long maxWindow, long allowance) {
        if (t == null || t.intervals() == 0 || paused || port.pressure()) return window;
        double r = t.maxRate();
        long span = cfg.latencyBudget();
        long rtt = rtt(t);
        if (rtt > 0 && Tuner.K * rtt > span) span = Tuner.K * rtt;
        double target = r * span / 1e9;
        if (target <= window) return window;
        long w = (long) target;
        if (maxWindow > 0) w = Math.min(w, maxWindow);
        w = Math.min(w, allowance);
        return Math.max(w, window);
    }

    /** Stream data up to end arrived on st. */
    void data(Stream st, long end, long now) {
        if (!on()) return;
        StreamRecv.RecvPart r = st.rcv;
        if (r.tune() == null && end >= r.window() / 4) StreamRecv.startTuning(st);
        if (r.tune() != null) scopeData(r.tune(), end, now);
        Tuner ct = conn.tunerIfDue();
        if (ct != null) scopeData(ct, conn.received(), now);
        long cp = port.bufferCap();
        if (conn.buffered() >= cp) paused = true;
        else if (paused && conn.buffered() < cp / 2) paused = false;
    }

    /** The application consumed data. */
    void consumed() {
        if (paused && conn.buffered() < port.bufferCap() / 2) paused = false;
    }

    /** Before MAX_STREAM_DATA of st: grows its window, and the session's along with it. */
    void streamUpdate(Stream st) {
        StreamRecv.RecvPart r = st.rcv;
        if (!on() || r.tune() == null) return;
        long maxW = cfg.streamWindowCap();
        if (r.maxWin() > 0 && (maxW == 0 || r.maxWin() < maxW)) maxW = r.maxWin();
        long w = grow(r.tune(), r.window(), maxW, port.allowance());
        if (w > r.window()) {
            StreamRecv.growWindow(st, w);
            long cw = w + w / 2;
            if (cfg.connWindowCap() > 0) cw = Math.min(cw, cfg.connWindowCap());
            growConn(Math.min(cw, port.allowance()));
        }
    }

    /** Before MAX_DATA: grows the session window. */
    void connUpdate() {
        if (!on() || conn.tuner() == null) return;
        growConn(grow(conn.tuner(), conn.window(), cfg.connWindowCap(), port.allowance()));
    }

    /** Raises the session window to w, or proposes it to the application's predicate. */
    void growConn(long w) {
        if (w <= conn.window()) return;
        if (cfg.windowPredicate()) {
            if (w > proposal) proposal = w;
            return;
        }
        conn.grow(w);
    }

    /** The byte increase corresponding to proposal w. */
    public long increase(long w) {
        return Math.max(0, w - conn.window());
    }

    /** A pending session window growth for the application's predicate: the proposed window, or -1. */
    public long proposal() {
        if (proposal <= conn.window() || asking) return -1;
        asking = true;
        return proposal;
    }

    /** Applies the predicate's answer for proposal w. */
    public void applyProposal(long w, boolean ok) {
        asking = false;
        if (proposal != w) return;
        proposal = 0;
        if (ok && !port.closing() && !port.pressure() && w > conn.window() && w <= port.allowance()) conn.grow(w);
    }
}
