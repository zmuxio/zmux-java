package io.zmux.internal.core;


/**
 * Stateless estimates over the pipes of a multipath session that several path components share: one-way delay, PTO,
 * carrier openness, rates and ready bytes.
 */
final class PathMetrics {
    static final long NOTSENT_LOWAT = 32 << 10;

    private final PipeTable pipes;
    private final AckEngine ack;
    private final Features feat;
    private Pipe openAs;
    /** Scripted measurements may be supplied by the deterministic contract runner. */
    private java.util.function.LongFunction<Measurement> measured;
    record Measurement(long srtt, long variation, long eta, long deviation) {}

    void measurements(java.util.function.LongFunction<Measurement> source) { measured = source; }

    Measurement measured(Pipe p) { return measured == null ? null : measured.apply(p.id); }

    long srtt(Pipe p) {
        Measurement m = measured(p);
        return m == null ? p.mp().rtt().smoothed() : m.srtt();
    }

    long variation(Pipe p) {
        Measurement m = measured(p);
        return m == null ? p.mp().rtt().rttvar() : m.variation();
    }

    long deviation(Pipe p) {
        Measurement m = measured(p);
        return m == null ? p.mp().rtt().rttvar() : m.deviation();
    }

    PathMetrics(PipeTable pipes, AckEngine ack, Features feat) {
        this.pipes = pipes;
        this.ack = ack;
        this.feat = feat;
    }

    /** OWD_p. */
    long owd(Pipe p) {
        Attach a = p.mp();
        if (!a.rtt().has()) return a.rtt().smoothed() / 2;
        long m = a.rtt().min();
        for (Pipe q : pipes.all()) {
            if (q.mp() != null && q.out() != null && q.mp().liveOut() && q.mp().rtt().has() && q.mp().rtt().min() < m) m = q.mp().rtt().min();
        }
        return a.rtt().min() - m / 2;
    }

    /** Whether an outbound pipe is usable for data; probOk lets pipes in probation or revalidation count. */
    boolean pipeUsable(Pipe p, boolean probOk) {
        Attach a = p.mp();
        if (a == null || p.out() == null || p.out().failed() || a.health.out() != Attach.OUT_ACTIVE || a.health.retired()) return false;
        return (a.topologyExcluded() & Action.DIR_OUT) == 0 && !a.ret.penalized() && (probOk || (!a.prob.probOn() && !a.prob.reval()));
    }

    /** PTO_p (RFC 9002 6.2.1 with the peer's max_ack_delay). */
    long pto(Pipe p) {
        long d = Time.add(srtt(p), Math.max(Time.mul(variation(p), 4), Time.MILLISECOND));
        return Time.add(d, ack != null ? ack.peerDelay() : 25 * Time.MILLISECOND);
    }

    /** [live outbound, live inbound] pipe counts. */
    int[] liveCounts() {
        int out = 0, in = 0;
        for (Pipe p : pipes.all()) {
            Attach a = p.mp();
            if (a == null || a.health.retired()) continue;
            if (p.out() != null && a.liveOut()) out++;
            if (p.in() != null && (a.health.in() == Attach.IN_JOINING || a.health.in() == Attach.IN_OPEN)) in++;
        }
        return new int[] {out, in};
    }

    double rateOf(Pipe p) {
        return p.out().snd.re().value();
    }

    /**
     * Q_p: over p's outbound lanes, the bytes of the batch whose write call is
     * in progress plus the carrier's unsent bytes where the adapter reports them. Bytes are
     * assigned to a pipe only when written, so nothing waits ahead of the carrier.
     */
    long queueBytes(Pipe p) {
        if (p.out() == null) return 0;
        long q = 0;
        for (Pipe.OutLane ol : p.out().lanes) q = Time.add(q, ol.tx.queued());
        return q;
    }

    /** Q_p of the arrival estimate, at least the low-water mark while the carrier refuses bytes. */
    double queued(Pipe p) {
        double q = queueBytes(p);
        if (!open(p)) q = Math.max(q, NOTSENT_LOWAT);
        return q;
    }

    /** Whether a pipe's carrier takes bytes now (its lane 0). */
    boolean open(Pipe p) {
        if (p.out() == null || p.out().failed()) return false;
        return laneOpen(p, p.out().lanes.getFirst());
    }

    /** An accepting lane whose current write owns its next write opportunity. */
    boolean writingOnAcceptingLane(Pipe p) {
        if (p.out() == null || p.out().failed() || Pacer.held(p)) return false;
        var tx = p.out().lanes.getFirst().tx;
        return tx.writing() && !tx.notWritable();
    }

    /**
     * Whether lane ol of p takes bytes now: its carrier accepts them and the limiter does not hold p. While the write
     * stall check evaluates p (openAs), its carrier counts as accepting them.
     */
    boolean laneOpen(Pipe p, Pipe.OutLane ol) {
        if (Pacer.held(p)) return false;
        if (openAs == p) return true;
        return !ol.tx.notWritable() && !ol.tx.writing();
    }

    /** The pipe evaluated as if its carrier took bytes, or null; set by the write stall check only. */
    void openAs(Pipe p) {
        openAs = p;
    }

    /** A stream direction's data ready within its own credit and not yet committed (the preamble counts). */
    long readyBytes(Stream st) {
        StreamSend.SendPart sp = st.snd;
        long off = Math.max(sp.sent(), sp.q().base());
        long end = sp.sendEnd();
        if (end <= off) return 0;
        long n = end - off;
        if (st.tab.idSet()) n = Math.min(n, sp.maxData() - Math.min(sp.maxData(), off));
        else if (feat.preamble()) n += st.preSize();
        return n;
    }
}
