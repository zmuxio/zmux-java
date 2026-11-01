package io.zmux.internal.core;

import io.zmux.shaping.Capability;
import io.zmux.shaping.Event;
import io.zmux.shaping.EventKind;
import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.RecordView;
import io.zmux.shaping.Reply;
import io.zmux.shaping.Shaper;
import io.zmux.shaping.ShaperFactory;
import io.zmux.shaping.ShapingLimits;
import io.zmux.shaping.ShapingStats;
import java.time.Duration;
import java.util.Set;

/**
 * Record shaping: a {@link Shaper} per outbound pipe proposes record sizes and timing; this
 * class executes the proposals within the protocol's limits and the configured bounds.
 * Nothing exists until a shaper is set: the paths that consult it test {@link PipeTable#shaping}
 * (and the pipe's state) for null. Owner thread only.
 */
final class Shaping {
    /** What shaping needs of the session. */
    interface Port {
        long now();
        /** The peer's max_record_size, at most 65535. */
        int peerRecordLimit();
        /** Whether the peer is a zmux endpoint. */
        boolean peerZmux();
        long bytesSent();
        /** Stream data waits to be scheduled. */
        boolean dataWaiting();
    }

    /** PADDING a ratio limit allows whatever the bytes sent: one largest record. */
    static final long STARTUP_ALLOWANCE = RecordPlan.MAX_RECORD_LENGTH;
    /** Drivers arm no gate timer earlier than this. */
    static final long GRANULARITY = Time.MILLISECOND;

    private final Port port;
    private final PipeTable pipes;
    /** The limits, -1 (NaN for the ratio): unset. */
    private final long maxDelay, maxPauseTotal, maxPaddingBytes, maxInjected;
    private final double maxPaddingRatio;
    private ShaperFactory factory;
    /** Bumped by setShaper: a pipe's instance of an older generation is replaced. */
    private long gen;
    // Counters.
    private long paddingBytes, injected, injectDropped, clamped, pauseTotal, faults;
    /**
     * Send times of the latest records that are limited toward a plain QMux peer (SPEC.md
     * Section 13.2): PADDING-only records and QX_PING requests.
     */
    private final long[] marks = new long[16];
    private int mi;

    Shaping(Port port, PipeTable pipes, ShapingLimits lim, long outstandingPingAt) {
        this.port = port;
        this.pipes = pipes;
        maxDelay = lim.maxDelay().map(Shaping::nanos).orElse(-1L);
        maxPauseTotal = lim.maxPauseTotal().map(Shaping::nanos).orElse(-1L);
        maxPaddingBytes = lim.maxPaddingBytes().orElse(-1);
        maxInjected = lim.maxInjected().orElse(-1);
        maxPaddingRatio = lim.maxPaddingRatio().orElse(Double.NaN);
        java.util.Arrays.fill(marks, Long.MIN_VALUE);
        if (outstandingPingAt != Long.MIN_VALUE) mark(outstandingPingAt);
    }

    /** A decision was reduced to a limit. */
    void clamp() { clamped++; }

    /** Grows whenever a counter changes (every counter only grows). */
    long signature() {
        return paddingBytes + injected + injectDropped + clamped + pauseTotal + faults;
    }

    ShapingStats stats() {
        return new ShapingStats(paddingBytes, injected, injectDropped, clamped, Duration.ofNanos(pauseTotal), faults);
    }

    void setFactory(ShaperFactory f) {
        factory = f;
        gen++;
        for (Pipe p : pipes.all()) if (p.out() != null) of(p);
        pipes.wakeWriters();
    }

    boolean setPipe(long id, Shaper x) {
        Pipe p = pipes.get(id);
        if (p == null || p.out() == null) return false;
        install(p, x);
        pipes.wakeWriters(id);
        return true;
    }

    /** The pipe's active state, installing the factory's instance when it has none of the current generation. */
    PipeShape of(Pipe p) {
        PipeShape ps = p.shape();
        if (ps == null || ps.gen != gen) {
            Shaper x = null;
            if (factory != null) {
                try { x = factory.create(new io.zmux.shaping.ShapedPipe(p.id, p.carrier(), writeOverhead(p))); } catch (RuntimeException e) { faults++; }
            }
            ps = install(p, x);
        }
        return ps.sh == null ? null : ps;
    }

    /** The write overhead of p's outbound carrier (lane 0). */
    private static int writeOverhead(Pipe p) {
        var out = p.out();
        return out == null || out.lanes.isEmpty() ? 0 : out.lanes.get(0).st.info().writeOverhead();
    }

    private PipeShape install(Pipe p, Shaper x) {
        PipeShape old = p.shape();
        if (old != null && old.sh != null) old.event(EventKind.REPLACED, 0, 0, false, 0, 0);
        PipeShape ps = new PipeShape(p, x, gen);
        p.shape(ps);
        if (x == null) return ps;
        Set<Capability> caps;
        try { caps = x.capabilities(); } catch (RuntimeException e) { faults++; ps.reset(); return ps; }
        ps.plan = caps.contains(Capability.PLAN);
        ps.recordIn = caps.contains(Capability.RECORD_IN);
        ps.streams = caps.contains(Capability.STREAMS);
        ps.event(EventKind.START, 0, 0, false, 0, 0);
        return ps;
    }

    /** Bounds a pause by the limits and returns its end. */
    long pause(long d) {
        long now = port.now();
        if (d <= 0) return now;
        long max = maxDelay;
        if (max >= 0 && d > max) { d = max; clamped++; }
        long total = maxPauseTotal;
        if (total >= 0 && d > total - Math.min(pauseTotal, total)) { d = total - Math.min(pauseTotal, total); clamped++; }
        pauseTotal += d;
        return Time.add(now, d);
    }

    private static long nanos(Duration d) {
        try { return d.toNanos(); } catch (ArithmeticException e) { return Long.MAX_VALUE; }
    }

    /** Bounds n bytes of PADDING by the limits and counts them. */
    int allowPad(int n) {
        if (n <= 0) return 0;
        long u = n;
        long m = maxPaddingBytes;
        if (m >= 0 && paddingBytes + u > m) u = m - Math.min(paddingBytes, m);
        double r = maxPaddingRatio;
        if (!Double.isNaN(r)) {
            long sent = port.bytesSent();
            long other = sent - Math.min(sent, paddingBytes);
            long cap = Math.max((long) (r * other), STARTUP_ALLOWANCE);
            if (paddingBytes + u > cap) u = cap - Math.min(paddingBytes, cap);
        }
        if (u < n) clamped++;
        paddingBytes += u;
        return (int) u;
    }

    /** Whether a limited record (SPEC.md Section 13.2) may go now. */
    boolean allowed(long now) { return marks[mi] <= now - Time.SECOND; }

    void mark(long t) {
        marks[mi] = t;
        mi = (mi + 1) % marks.length;
    }

    /**
     * H_p of MULTIPATH.md 7.1: the longest a record of p without urgent frames may be held between building and
     * handing to the carrier, as bounded now: the smaller of maxDelay and what remains of maxPauseTotal, whichever
     * is set; 0 unshaped or with neither bound set.
     */
    long maxHold(Pipe p) {
        PipeShape ps = p.shape();
        if (ps == null || ps.sh == null) return 0;
        return holdBound(maxDelay, maxPauseTotal, pauseTotal);
    }

    /** The smaller of the set bounds (-1: unset): one delay, and the total less what was used of it (at least 0); 0 if neither. */
    static long holdBound(long maxDelay, long maxTotal, long used) {
        long rest = maxTotal >= 0 ? maxTotal - Math.min(used, maxTotal) : 0;
        if (maxDelay >= 0 && maxTotal >= 0) return Math.min(maxDelay, rest);
        return maxDelay >= 0 ? maxDelay : rest;
    }

    /** Whether the pipe's gate holds stream data now. */
    static boolean held(Pipe p, long now) {
        PipeShape ps = p.shape();
        if (ps != null && ps.sh != null && ps.held(now)) {
            ps.wake = true;
            return true;
        }
        return false;
    }

    /** The planned record of p waits for its delay: nothing goes out on the pipe but closing frames. */
    static boolean delaying(Pipe p, long now) {
        PipeShape ps = p.shape();
        return ps != null && ps.sh != null && ps.delaying(now);
    }

    /** An injection is due on lane 0 of p. */
    static boolean wants(Pipe p, long lane) {
        PipeShape ps = p.shape();
        return lane == 0 && ps != null && ps.injDue;
    }

    /** A due injection did not fit its batch: it leads the next one, before stream data. */
    static boolean deferred(PipeShape ps) { return ps.injDeferred && ps.injDue; }

    /**
     * Sends a due injection on lane 0: as PADDING of the open record, or as a record of PADDING only.
     * A record of PADDING only that does not fit the room left in a batch already holding bytes goes whole in the
     * next batch; only a limit shortens it.
     */
    void inject(RecordWriter w) {
        PipeShape ps = w.shape();
        if (ps == null || !ps.injDue || w.b().lane() != 0) return;
        int n = ps.injLen;
        ps.injDue = false;
        ps.injLen = 0;
        ps.injDeferred = false;
        if (w.openBytes() > 0 && ps.planned) {
            ps.extraPad += n;
            ps.event(EventKind.INJECTED, 0, n, false, 0, 0);
            return;
        }
        w.end();
        long now = port.now();
        if (w.closed() || !port.peerZmux() && !allowed(now) || maxInjected >= 0 && injected >= maxInjected) {
            injectDropped++;
            return;
        }
        int m = w.padRoom();
        if (n > m) {
            if (n <= w.recMax() && !w.batchEmpty()) { // the next batch takes it whole
                ps.injLen = n;
                ps.injDue = true;
                ps.injDeferred = true;
                return;
            }
            n = Math.max(m, 0);
            clamped++;
        }
        if ((n = allowPad(n)) == 0) { injectDropped++; return; }
        w.padRecord(n);
        mark(now);
        injected++;
        ps.event(EventKind.INJECTED, 0, n, false, 0, 0);
    }

    /** Reports a received record. */
    static void recordIn(Pipe p, long lane, int n, boolean padOnly) {
        PipeShape ps = p.shape();
        if (ps != null && ps.sh != null && ps.recordIn) ps.event(EventKind.RECORD_IN, lane, n, padOnly, 0, 0);
    }

    /** The earliest shaping timer. */
    long deadline() {
        long t = Time.NEVER, now = port.now();
        boolean waiting = port.dataWaiting();
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            if (ps == null || ps.sh == null) continue;
            t = Math.min(t, Math.min(ps.timers[0], ps.timers[1]));
            if (ps.injLen > 0 && !ps.injDue) t = Math.min(t, ps.injAt);
            if (ps.wake && (waiting || ps.delaying(now))) {
                long r = ps.hold;
                if (ps.rate > 0 && ps.tokens < ps.quantum()) {
                    r = Math.max(r, Time.add(ps.filled, (long) ((ps.quantum() - ps.tokens) * 1e9 / ps.rate) + 1));
                }
                long release = Math.max(r, Time.add(now, GRANULARITY)); // never sooner than one granularity
                t = Math.min(t, release);
            }
        }
        return t;
    }

    /** Runs the due shaping timers. */
    void timeout(long now) {
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            for (int i = 0; i < 2; i++) {
                if (ps == null || ps.sh == null) break;
                if (ps.timers[i] <= now) {
                    ps.timers[i] = Time.NEVER;
                    ps.event(EventKind.TIMER, 0, 0, false, i, 0);
                }
            }
            if (ps == null || ps.sh == null) continue;
            if (ps.injLen > 0 && !ps.injDue && ps.injAt <= now) {
                ps.injDue = true;
                pipes.wakeWriters(p.id);
            }
            if (ps.wake && !ps.held(now)) {
                ps.wake = false;
                pipes.wakeWriters(p.id);
            }
        }
    }

    /** Ends shaping of a pipe whose outbound direction failed. */
    static void pipeDown(Pipe p) {
        PipeShape ps = p.shape();
        if (ps != null && ps.sh != null) {
            ps.event(EventKind.PIPE_FAILED, 0, 0, false, 0, 0);
            ps.reset();
        }
    }

    /** Reports a stream event to the pipes that asked for it. */
    void stream(EventKind k, long id) {
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            if (ps != null && ps.sh != null && ps.streams) ps.event(k, 0, 0, false, 0, id);
        }
    }

    /** The shaping state of one outbound pipe. */
    final class PipeShape {
        final Pipe p;
        private Shaper sh;
        final long gen;
        private boolean plan, recordIn, streams;
        private long seq;
        /** The plan applies to the next record. */
        private boolean planned;
        final RecordPlan rp = new RecordPlan();
        private final Reply reply = new Reply();
        /** PADDING injected into the planned record. */
        private int extraPad;
        /** Stream data waits until then. */
        private long hold = Long.MIN_VALUE;
        private long rate, burst, filled;
        private double tokens;
        /** A writer waits for hold or tokens. */
        private boolean wake;
        private long injAt = Time.NEVER;
        private int injLen;
        private boolean injDue;
        /** The due injection waits for the next batch. */
        private boolean injDeferred;
        final long[] timers = {Time.NEVER, Time.NEVER};
        /** The pipe's weight in scheduler views; 0: none. */
        private double weight;

        PipeShape(Pipe p, Shaper sh, long gen) { this.p = p; this.sh = sh; this.gen = gen; }

        Shaping owner() { return Shaping.this; }

        /** The plan applies to the record being built. */
        boolean planned() { return planned; }

        /** An instance is installed. */
        boolean active() { return sh != null; }

        /** The weight in scheduler views; 0: none. */
        double weight() { return weight; }

        /** Drops the instance and every pending item. */
        void reset() {
            sh = null;
            planned = false;
            hold = Long.MIN_VALUE;
            rate = burst = 0;
            wake = false;
            injAt = Time.NEVER;
            injLen = 0;
            injDue = injDeferred = false;
            timers[0] = timers[1] = Time.NEVER;
            weight = 0;
        }

        private void fault() {
            faults++;
            reset();
        }

        void stop(boolean notify) {
            if (sh == null) return;
            if (notify) event(EventKind.STOPPED, 0, 0, false, 0, 0);
            reset();
        }

        void event(EventKind k, long lane, int n, boolean padOnly, int timer, long stream) {
            Shaper x = sh;
            if (x == null) return;
            reply.clear();
            try {
                x.onEvent(new Event(k, p.id, lane, port.now(), n, padOnly, timer, stream), reply);
            } catch (RuntimeException e) {
                fault();
                return;
            }
            if (k == EventKind.REPLACED || k == EventKind.STOPPED || k == EventKind.PIPE_FAILED) return;
            apply(reply);
        }

        /** Executes a reply. */
        void apply(Reply r) {
            long now = port.now();
            if (r.hasInject()) {
                injLen = 0; injAt = Time.NEVER; injDue = injDeferred = false;
                if (!r.injectCancelled()) {
                    injLen = Math.max(r.injectLength(), 1);
                    injAt = Time.add(now, Math.max(r.injectAfterNanos(), 0));
                }
            }
            if (r.hasGate()) {
                hold = pause(r.gatePauseNanos());
                rate = Math.max(r.gateRate(), 0);
                burst = r.gateBurst();
                filled = now;
                if (burst <= 0) burst = port.peerRecordLimit();
                tokens = burst;
                wake = true;
            }
            for (int i = 0; i < 2; i++) {
                if (!r.hasTimer(i)) continue;
                timers[i] = r.timerCancelled(i) ? Time.NEVER : Time.add(now, Math.max(r.timerAfterNanos(i), 0));
            }
            if (r.hasWeight()) {
                double w = r.weight();
                if (w > 0 && !Double.isInfinite(w)) weight = w;
            }
            if (r.stopRequested()) stop(true);
            pipes.wakeWriters(p.id);
        }

        /** The planned record waits for its delay. */
        boolean delaying(long now) {
            return planned && rp.delayNanos() > 0 && now < hold;
        }

        boolean held(long now) {
            if (now < hold) return true;
            if (rate == 0) return false;
            tokens = Math.min(burst, tokens + (double) rate * (now - filled) / 1e9);
            filled = now;
            return tokens < quantum();
        }

        /** The bucket level that releases a held pipe: the bytes of one granularity, at least one. */
        double quantum() {
            // At least one byte, even for an unset gate whose burst is 0.
            double perTick = Math.min((double) burst, (double) rate * GRANULARITY / 1e9);
            return Math.max(1, perTick);
        }

        /** Bounds the data room of the batch (cur, negative: none) by the gate and records the gate's share in w. */
        long room(RecordWriter w, long now, long cur) {
            if (held(now)) {
                wake = true;
                return 0;
            }
            if (rate == 0) return cur;
            long share = (long) tokens;
            w.shapeRoom(share);
            return cur < 0 || share < cur ? share : cur;
        }

        /** Data of the batch consumed gate tokens. */
        void spent(long n) {
            if (rate > 0) tokens -= n;
        }

        /** Plans the record the writer opens next; false when the instance ended. */
        boolean prep(RecordWriter w) {
            if (sh == null) return false;
            if (!planned) {
                rp.clear();
                extraPad = 0;
                if (plan) {
                    try {
                        sh.plan(new RecordView(p.id, w.b().lane(), seq, w.recMax(), w.writeStart(), port.now()), rp);
                    } catch (RuntimeException e) {
                        fault();
                        return false;
                    }
                }
                planned = true;
                if (rp.maxLength() > 0 && rp.maxLength() < RecordPlan.MIN_MAX_LENGTH) {
                    rp.maxLength(RecordPlan.MIN_MAX_LENGTH);
                    clamp();
                }
                // No padding target goes beyond a record, so that the targets cannot overflow.
                int r = w.recMax();
                if (rp.padTo() > r) { rp.padTo(r); clamp(); }
                if (rp.padMultiple() > r) { rp.padMultiple(r); clamp(); }
                if (rp.pad() > r) { rp.pad(r); clamp(); }
                if (!rp.reply().isEmpty()) {
                    apply(rp.reply());
                    if (sh == null) return false;
                }
                if (rp.delayNanos() > 0) {
                    hold = Math.max(hold, pause(rp.delayNanos()));
                    wake = true;
                }
            }
            if (delaying(port.now())) w.holdRecord();
            int m = rp.maxLength();
            w.cut(m > 0 && m < w.recMax() ? m : 0);
            return true;
        }

        /** The size the plan's targets (pad_to, pad_multiple) give a record of n frames bytes. */
        int target(int n) {
            int t = Math.max(n, rp.padTo());
            int b = rp.padMultiple();
            if (b > 0 && t % b != 0) t += b - t % b;
            return t;
        }

        /**
         * The most frames bytes the planned record may hold so that, padded to its target and
         * extra PADDING, it fits in total bytes; -1 when the target itself does not fit.
         */
        int framesBound(int total) {
            int b = total - extraPad();
            int m = rp.padMultiple();
            if (m > 0 && b > 0) b -= b % m;
            return b >= Math.max(rp.padTo(), 0) ? b : -1;
        }

        /** The PADDING the plan appends beyond its targets: pad and injections. */
        int extraPad() {
            return Math.max(rp.pad(), 0) + extraPad;
        }

        /** The record was closed with the plan applied. */
        void ended(RecordWriter w) {
            planned = false;
            seq++;
            w.cut(0);
            if (rp.flush()) w.flushBatch();
            if (rp.stop()) stop(true);
        }
    }
}
