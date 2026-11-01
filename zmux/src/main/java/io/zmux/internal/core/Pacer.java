package io.zmux.internal.core;

import java.util.List;

/**
 * Pacing of the outbound pipes of a session with acknowledgement (SCHEDULER.md Section 9.1):
 * one {@link Limiter} per pipe whose carrier is not queue-bound. Owned by the session's loop;
 * wake-ups go through {@link Port}.
 */
final class Pacer {
    /** What Pacer asks of the session. */
    interface Port {
        /** Every outbound pipe may write again. */
        void wakeAllWriters();

        /** The pipe may write again. */
        void wakePipe(Pipe p);
    }

    private final Port port;
    private final List<Pipe> pipes;
    private final Limiter.Params params;

    Pacer(CoreConfig cfg, long peerThreshold, long peerDelay, List<Pipe> pipes, Port port) {
        this.port = port;
        this.pipes = pipes;
        this.params = new Limiter.Params(cfg.latencyBudget(), peerThreshold, peerDelay, cfg.spreadChunk());
    }

    /** Whether the pipe's limiter currently holds stream data back. */
    static boolean held(Pipe p) {
        return p.out() != null && p.out().pace.lim != null && p.out().pace.lim.held();
    }

    private static boolean paced(Pipe p) {
        return p.out() != null && !p.out().lanes.getFirst().st.info().queueBound();
    }

    /** Bytes in flight as the limiter counts them. */
    private static long inFlight(Pipe p) {
        AckEngine.Send ps = p.out().snd;
        long raw = ps.elicitEnd() - Math.min(ps.elicitEnd(), ps.cum());
        long held = heldBytes(p);
        Limiter lim = p.out().pace.lim;
        return lim != null ? raw - Math.min(raw, lim.leftOut(held)) : raw;
    }

    /** Bytes acknowledged on their own lanes beyond the cumulative point (N14). */
    private static long heldBytes(Pipe p) {
        Pipe.OutDir d = p.out();
        if (d.lanes.size() < 2) return 0;
        long unacked = 0;
        for (Pipe.OutLane l : d.lanes) {
            for (int i = 0; i < l.segs.size(); i++) {
                long a = l.segs.a(i), b = l.segs.b(i);
                unacked += b - Math.clamp(l.ack.acked(), a, b);
            }
        }
        long beyond = d.snd.so() - Math.min(d.snd.so(), d.snd.cum());
        return beyond - Math.min(beyond, unacked);
    }

    private static long srttOf(Pipe p) {
        if (p.mp() != null) return p.mp().rtt.smoothed();
        return p.out().snd.rtt().has() ? p.out().snd.rtt().smoothed() : 333 * Time.MILLISECOND;
    }

    private static Limiter limiterOf(Pipe p, long now) {
        if (!paced(p)) {
            p.out().pace.lim = null;
            return null;
        }
        if (p.out().pace.lim == null) p.out().pace.lim = new Limiter(now);
        return p.out().pace.lim;
    }

    /** Stream data p may take now; -1 when p is not paced. */
    long room(Pipe p, long now) {
        Limiter l = limiterOf(p, now);
        if (l == null) return -1;
        long r = l.room(now, inFlight(p), p.out().snd.re().value(), srttOf(p), params);
        return Math.min(r, 1L << 40);
    }

    /** An acknowledgement advanced p's cumulative point. */
    void onAck(Pipe p, long rtt, long rttSentAt, double rate, boolean roundEnd, long now) {
        Limiter l = limiterOf(p, now);
        if (l == null) return;
        AckEngine.Send ps = p.out().snd;
        long held = heldBytes(p);
        l.heldMin.update(now, held, l.baseOr(srttOf(p)));
        var ev = new Limiter.Ack(rtt, rttSentAt, rate, roundEnd, ps.cum(), Math.max(ps.elicitEnd(), ps.cum()), l.leftOut(held));
        boolean was = l.held();
        l.onAck(now, ev, ps.re(), srttOf(p), params);
        if (was && !l.held()) port.wakeAllWriters();
    }

    /** A batch of bytes positions went out on p. */
    void onSent(Pipe p, long bytes, long now) {
        Limiter l = p.out().pace.lim;
        if (l == null) return;
        long srtt = srttOf(p);
        double rate = p.out().snd.re().value();
        l.settle(now, bytes, rate, srtt, params);
        l.refresh(now, inFlight(p), rate, srtt, params);
    }

    /** A QX_PING measured rtt before the bootstrap pipe has an acknowledgement sample. */
    void onPing(Pipe p, long rtt, long now) {
        if (p == null || p.out() == null || p.mp() != null || p.out().snd.rtt().has()) return;
        Limiter l = limiterOf(p, now);
        if (l != null) l.pingSample(now, rtt, p.out().snd.re(), srttOf(p), params);
    }

    /** The earliest moment a held pipe may send again; dataPending is whether any stream data waits. */
    long deadline(boolean dataPending, long now) {
        if (!dataPending) return Time.NEVER;
        long t = Time.NEVER;
        for (Pipe p : pipes) {
            if (p.out() != null && p.out().pace.lim != null && p.out().pace.lim.held()) {
                // The limiter release, never sooner than one granularity from now, competes for the earliest wake-up.
                long release = Math.max(p.out().pace.lim.release(), Time.add(now, Limiter.GRANULARITY));
                t = Math.min(t, release);
            }
        }
        return t;
    }

    /** Releases the pipes whose hold ends at t. */
    void onTimeout(long t) {
        for (Pipe p : pipes) {
            if (p.out() == null) continue;
            Limiter l = p.out().pace.lim;
            if (l == null || !l.held() || l.release() > t) continue;
            l.refresh(t, inFlight(p), p.out().snd.re().value(), srttOf(p), params);
            if (!l.held()) {
                port.wakePipe(p);
                port.wakeAllWriters();
            }
        }
    }

    /** The pacing limiter of an outbound pipe; null while none applies. */
    static final class Slot {
        private Limiter lim;

        Slot() {
        }

        Limiter lim() {
            return lim;
        }
    }
}
