package io.zmux.internal.core;

import io.zmux.shaping.Capability;
import io.zmux.shaping.Event;
import io.zmux.shaping.EventKind;
import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.RecordView;
import io.zmux.shaping.Effects;
import io.zmux.shaping.Shaper;
import io.zmux.shaping.ShaperFactory;
import io.zmux.shaping.ShapingLimits;
import io.zmux.shaping.ShapingMeters;
import io.zmux.shaping.ShapingStats;
import io.zmux.shaping.WireAccount;
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
        /** The session's own bytes sent: no padding, injections or copies. */
        long earnBytes();
        /** Stream data waits to be scheduled. */
        boolean dataWaiting();
    }

    /** Drivers arm no gate timer earlier than this. */
    static final long GRANULARITY = Time.MILLISECOND;

    private final Port port;
    private final PipeTable pipes;
    private final WireMeter account;
    /** The local limits under the budget; maxDelay is in nanoseconds, negative: no bound. */
    private boolean noPadding, noPause, noInjection;
    private long maxDelay;
    private ShaperFactory factory;
    /** Bumped by setShaper: a pipe's instance of an older generation is replaced. */
    private long gen;
    // Counters.
    private long stallSpent, injected, injectDropped, clamped, faults;
    /**
     * Send times of the latest records that are limited toward a plain QMux peer: PADDING-only records and QX_PING requests.
     */
    private final long[] marks = new long[16];
    private int mi;

    /** The account runs from the session's start, whenever shaping is first needed. */
    Shaping(Port port, PipeTable pipes, ShapingLimits inForce, long outstandingPingAt, long start) {
        this.port = port;
        this.pipes = pipes;
        account = new WireMeter(us(start));
        take(inForce);
        account.set(us(start), 0, inForce.wire());
        java.util.Arrays.fill(marks, Long.MIN_VALUE);
        if (outstandingPingAt != Long.MIN_VALUE) mark(outstandingPingAt);
    }

    private void take(ShapingLimits l) {
        noPadding = l.noPadding();
        noPause = l.noPause();
        noInjection = l.noInjection();
        maxDelay = l.maxDelay().isNegative() ? -1 : nanos(l.maxDelay());
    }

    /** Replaces the limits in force; the account carries over, widened or narrowed by the change of its initial value. */
    void limits(ShapingLimits inForce) {
        take(inForce);
        account.set(us(port.now()), port.earnBytes(), inForce.wire());
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            if (ps != null && ps.sh != null && ps.rateAsked != 0) ps.setRate();
        }
    }

    /** Microseconds of the session clock. */
    private static long us(long t) { return Math.max(t, 0) / 1000; }

    /** A decision was reduced to a limit. */
    void clamp() { clamped++; }

    /** Meters wire bytes of an action that accompanies a record. */
    void spend(long n) { if (n > 0) account.spend(n); }

    /** The session's readings. */
    ShapingMeters meters() {
        account.accrue(us(port.now()), port.earnBytes());
        return new ShapingMeters(account.left(), -1L, account.spent(), stallSpent / 1000, port.earnBytes());
    }

    /** Grows whenever a counter changes (every counter only grows). */
    long signature() {
        return account.spent() + injected + injectDropped + clamped + stallSpent + faults;
    }

    ShapingStats stats() {
        return new ShapingStats(account.spent(), Duration.ofNanos(stallSpent), injected, injectDropped, clamped, faults, 0);
    }

    boolean noPadding() { return noPadding; }

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
                try { x = factory.create(new io.zmux.shaping.ShapedPipe(p.id, p.carrier(), writeOverhead(p), p.variant())); } catch (RuntimeException e) { faults++; }
            }
            ps = install(p, x);
        }
        return ps.sh == null ? null : ps;
    }

    /** The write overhead of p's outbound carrier (lane 0). */
    private static int writeOverhead(Pipe p) {
        var out = p.out();
        return out == null || out.lanes.isEmpty() ? 0 : out.lanes.getFirst().st.info().writeOverhead();
    }

    private PipeShape install(Pipe p, Shaper x) {
        PipeShape old = p.shape();
        if (old != null && old.sh != null) old.settleHeld();
        if (old != null && old.sh != null) old.event(EventKind.REPLACED, 0, 0, false, 0);
        PipeShape ps = new PipeShape(p, x, gen);
        p.shape(ps);
        if (x == null) return ps;
        Set<Capability> caps;
        try { caps = x.capabilities(); } catch (RuntimeException e) { faults++; ps.reset(); return ps; }
        ps.plan = caps.contains(Capability.PLAN);
        ps.recordIn = caps.contains(Capability.RECORD_IN);
        ps.streams = caps.contains(Capability.STREAMS);
        ps.budget = caps.contains(Capability.BUDGET);
        ps.event(EventKind.START, 0, 0, false, 0);
        return ps;
    }

    /** Bounds a pause by the limits and adds what is left of it to the waits applied. */
    long pause(long d) {
        if (d <= 0) return 0;
        if (noPause) {
            clamped++;
            return 0;
        }
        if (maxDelay >= 0 && d > maxDelay) {
            d = maxDelay;
            clamped++;
        }
        addStall(d);
        return d;
    }

    private void addStall(long d) { stallSpent = d > Long.MAX_VALUE - stallSpent ? Long.MAX_VALUE : stallSpent + d; }

    private static long nanos(Duration d) {
        try { return d.toNanos(); } catch (ArithmeticException e) { return Long.MAX_VALUE; }
    }

    /** Whether a limited record may go now. */
    boolean allowed(long now) { return marks[mi] <= now - Time.SECOND; }

    void mark(long t) {
        marks[mi] = t;
        mi = (mi + 1) % marks.length;
    }

    /**
     * H_p, the record hold of failure detection: the longest a record of p without urgent frames may be held between building and
     * handing to the carrier, as bounded now; 0 unshaped by plans, or when nothing bounds one wait.
     */
    long maxHold(Pipe p) {
        PipeShape ps = p.shape();
        if (ps == null || ps.sh == null || !ps.plan || noPause) return 0;
        return Math.max(maxDelay, 0);
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
     * Sends a due injection on lane 0: as PADDING of the open record, or as a record of PADDING only within the limit on PADDING-only records toward a plain QMux peer. Either way it is spontaneous: what the limits or the account refuse is
     * dropped, never deferred. Only a record that does not fit a batch already holding bytes waits, whole, for the next
     * batch, and nothing is charged until it goes.
     */
    void inject(RecordWriter w) {
        PipeShape ps = w.shape();
        if (ps == null || !ps.injDue || w.b().lane() != 0) return;
        int len = ps.injLen;
        ps.injDue = false; // The slot stays until an answer changes it.
        ps.injDeferred = false;
        long now = port.now();
        boolean off = noInjection || noPadding;
        if (w.openBytes() > 0 && ps.planned) {
            if (off || !account.charge(us(now), port.earnBytes(), len)) {
                dropInject(ps);
                return;
            }
            ps.extraPad += len; // frames bytes, beside the plan's wire lengths
            ps.event(EventKind.INJECTED, 0, len, false, 0);
            return;
        }
        w.end();
        if (!port.peerZmux() && !allowed(now) || off) {
            dropInject(ps);
            return;
        }
        if (w.closed()) { // The record before it closed the batch: it leads the next one, uncharged.
            ps.injDue = true;
            ps.injDeferred = true;
            return;
        }
        boolean sizeField = w.framing() == Framing.BYTES;
        int over = writeOverhead(ps.p);
        int n = Math.max(WireLength.framesOf(len, sizeField, over), 1);
        int m = w.padRoom();
        if (n > m) {
            if (n <= w.recMax() && !w.batchEmpty()) { // the next batch takes it whole
                ps.injDue = true;
                ps.injDeferred = true;
                return;
            }
            n = Math.max(m, 0);
            clamped++;
        }
        if (n == 0 || !account.charge(us(now), port.earnBytes(), WireLength.wireOf(n, sizeField, ps.overHere(w)))) {
            dropInject(ps);
            return;
        }
        w.padRecord(n);
        mark(now);
        injected++;
        ps.event(EventKind.INJECTED, 0, n, false, 0);
    }

    /** An injection will not go. Whatever the answer leaves goes no sooner than a granularity later. */
    private void dropInject(PipeShape ps) {
        injectDropped++;
        ps.event(EventKind.INJECT_DROPPED, 0, 0, false, 0);
        long now = port.now();
        if (ps.injLen > 0) ps.injAt = Math.max(ps.injAt, Time.add(now, GRANULARITY));
    }

    /** Reports a received record. */
    static void recordIn(Pipe p, long lane, int n, boolean padOnly) {
        PipeShape ps = p.shape();
        if (ps != null && ps.sh != null && ps.recordIn) ps.event(EventKind.RECORD_IN, lane, n, padOnly, 0);
    }

    /** The earliest shaping timer. */
    long deadline() {
        long t = Time.NEVER, now = port.now();
        boolean waiting = port.dataWaiting();
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            if (ps == null || ps.sh == null) continue;
            t = Math.min(t, ps.wakeAt);
            if (ps.injLen > 0 && !ps.injDue) t = Math.min(t, ps.injAt);
            if (ps.wake && (waiting || ps.delaying(now))) {
                long r = ps.hold;
                TokenBucket b = ps.bucket;
                if (b.rate() > 0 && b.held()) {
                    long at = b.release();
                    r = Math.max(r, at < 0 || at > Long.MAX_VALUE / 1000 ? Time.NEVER : at * 1000);
                }
                long release = Math.max(r, Time.add(now, GRANULARITY)); // never sooner than one granularity
                t = Math.min(t, release);
            }
        }
        return t;
    }

    private static long unsignedMin(long a, long b) { return Long.compareUnsigned(a, b) < 0 ? a : b; }

    /** Runs the due shaping timers. */
    void timeout(long now) {
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            if (ps == null || ps.sh == null) continue;
            long at = ps.wakeAt;
            if (at <= now) {
                ps.event(EventKind.WAKE, 0, 0, false, 0);
                if (ps.sh == null) continue;
                if (ps.wakeAt == at) ps.wakeAt = Time.add(now, GRANULARITY); // Not replaced: not again at once.
            }
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
            ps.event(EventKind.PIPE_FAILED, 0, 0, false, 0);
            ps.reset();
        }
    }

    /** Reports a stream event to the pipes that asked for it. */
    void stream(EventKind k, long id) {
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            if (ps != null && ps.sh != null && ps.streams) ps.event(k, 0, 0, false, id);
        }
    }

    /** The frames bytes of a record of n frames bytes planned to these lengths on the carrier. */
    static int framesFor(int n, boolean sizeField, int over, int padTo, int padMultiple, int pad) {
        int l = WireLength.wireOf(n, sizeField, over);
        long t = Math.max(l, padTo);
        if (padMultiple > 0 && t % padMultiple != 0) t += padMultiple - t % padMultiple;
        t += Math.max(pad, 0);
        return t <= l ? n : Math.max(WireLength.framesOf(t, sizeField, over), n);
    }

    /** The shaping state of one outbound pipe. */
    final class PipeShape {
        final Pipe p;
        private Shaper sh;
        final long gen;
        private boolean plan, recordIn, streams, budget;
        private long seq;
        /** The plan applies to the next record. */
        private boolean planned;
        final RecordPlan rp = new RecordPlan();
        private final Effects effects = new Effects();
        /** PADDING injected into the planned record. */
        private int extraPad;
        /** How the plan's lengths read in frames bytes: the size field, the write overhead, the largest frames length. */
        private boolean sizeField;
        private int over, frameCut;
        /** Stream data waits until then. */
        private long hold = Long.MIN_VALUE;
        /** The latest end of a pause asked for: an answer repeats it, and a limit may have shortened that pause. */
        private long pauseAsked = Long.MIN_VALUE;
        /** The plan's padTo as asked, on the carrier. */
        private int wirePadTo;
        /** The rate as answered; the bucket holds the rate in force. */
        private long rateAsked;
        private final TokenBucket bucket = new TokenBucket();
        /** The rate gate holds data since then; NEVER: not held. */
        private long heldSince = Time.NEVER;
        /** A writer waits for hold or tokens. */
        private boolean wake;
        private long injAt = Time.NEVER;
        private int injLen;
        private boolean injDue;
        /** The due injection waits for the next batch. */
        private boolean injDeferred;
        private long wakeAt = Time.NEVER;
        /** The pipe's weight in scheduler views; 0: none. */
        private long weight;

        PipeShape(Pipe p, Shaper sh, long gen) { this.p = p; this.sh = sh; this.gen = gen; }

        Shaping owner() { return Shaping.this; }

        /** The plan applies to the record being built. */
        boolean planned() { return planned; }

        /** An instance is installed. */
        boolean active() { return sh != null; }

        /** The weight in scheduler views; 0: none. */
        long weight() { return weight; }

        /** Drops the instance and every pending item. */
        void reset() {
            settleHeld();
            sh = null;
            planned = false;
            hold = Long.MIN_VALUE;
            rateAsked = 0;
            bucket.rate(0);
            bucket.burst(0);
            heldSince = Time.NEVER;
            wake = false;
            injAt = Time.NEVER;
            injLen = 0;
            injDue = injDeferred = false;
            wakeAt = Time.NEVER;
            weight = 0;
        }

        private void fault() {
            faults++;
            reset();
        }

        void stop(boolean notify) {
            if (sh == null) return;
            if (notify) event(EventKind.STOPPED, 0, 0, false, 0);
            reset();
        }

        void event(EventKind k, long lane, int n, boolean padOnly, long stream) {
            Shaper x = sh;
            if (x == null) return;
            Effects e = effects;
            e.clear();
            try {
                x.onEvent(new Event(k, p.id, lane, port.now(), n, padOnly, stream, budget ? meters() : ShapingMeters.ZERO), e);
            } catch (RuntimeException failed) {
                fault();
                return;
            }
            if (k == EventKind.REPLACED || k == EventKind.STOPPED || k == EventKind.PIPE_FAILED) return;
            apply(e);
        }

        /** Executes effects. */
        void apply(Effects e) {
            long now = port.now();
            if (e.hasInject()) {
                injLen = 0; injAt = Time.NEVER; injDue = injDeferred = false;
                if (!e.injectNone()) {
                    injLen = Math.max(e.injectLength(), 1);
                    injAt = e.injectAtNanos();
                }
            }
            if (e.hasGate()) {
                holdUntil(e.gatePauseUntilNanos(), now);
                // Both are unsigned: what lies above a bound takes the bound.
                long r = unsignedMin(e.gateRate(), WireAccount.MAX_RATE);
                long b = TokenBucket.burstFloor(e.gateBurst(), port.peerRecordLimit());
                if (r != rateAsked || b != bucket.burst()) {
                    if (r != 0 && e.gateBurst() != 0 && Long.compareUnsigned(e.gateBurst(), RecordPlan.MIN_MAX_LENGTH) < 0) clamped++;
                    settleHeld();
                    rateAsked = r;
                    bucket.burst(b);
                    setRate();
                    bucket.reset(us(now));
                }
                wake = true;
            }
            if (e.hasWake()) wakeAt = e.wakeNone() ? Time.NEVER : e.wakeAtNanos();
            if (e.hasWeight() && e.weight() >= 0) weight = e.weight();
            if (e.stopRequested()) stop(true);
            pipes.wakeWriters(p.id);
        }

        /** Counts the time the rate gate has held data so far; the gate changes now. */
        private void settleHeld() {
            if (heldSince != Time.NEVER) addStall(Math.max(port.now() - heldSince, 0));
            heldSince = Time.NEVER;
        }

        /** Puts the rate asked in force, within the lower bound that the bound on one wait sets. */
        private void setRate() {
            long r = TokenBucket.floor(rateAsked, maxDelay < 0 ? -1 : maxDelay / 1000);
            if (r != rateAsked) clamped++;
            if (r != bucket.rate()) {
                // What has passed earns at the old rate and is held under the old rate.
                bucket.fill(us(port.now()));
                settleHeld();
            }
            bucket.rate(r);
        }

        /** Moves the end of the hold later, as far as the limits let it. */
        private void holdUntil(long until, long now) {
            long base = Math.max(now, hold), counted = Math.max(base, pauseAsked);
            if (until <= counted) return;
            hold = Time.add(base, pause(until - counted));
            pauseAsked = until;
        }

        /** The planned record waits for its delay. */
        boolean delaying(long now) {
            return planned && rp.delayNanos() > 0 && now < hold;
        }

        boolean held(long now) {
            if (now < hold) return true;
            if (bucket.rate() == 0) {
                settleHeld();
                return false;
            }
            bucket.fill(us(now));
            boolean h = bucket.held();
            if (h && heldSince == Time.NEVER) {
                heldSince = now;
            } else if (!h && heldSince != Time.NEVER) {
                addStall(now - heldSince);
                heldSince = Time.NEVER;
            }
            return h;
        }

        /** Bounds the data room of the batch (cur, negative: none) by the gate and records the gate's share in w. */
        long room(RecordWriter w, long now, long cur) {
            if (held(now)) {
                wake = true;
                return 0;
            }
            if (bucket.rate() == 0) return cur;
            long share = bucket.tokens();
            w.shapeRoom(share);
            return cur < 0 || share < cur ? share : cur;
        }

        /** Data of the batch consumed gate tokens. */
        void spent(long n) {
            if (bucket.rate() > 0) bucket.take(n);
        }

        /** Plans the record the writer opens next; false when the instance ended. */
        boolean prep(RecordWriter w) {
            if (sh == null) return false;
            if (!planned) {
                rp.clear();
                extraPad = 0;
                if (plan) {
                    try {
                        sh.plan(new RecordView(p.id, w.b().lane(), seq, w.recMax(), w.writeStart(), port.now(), budget ? meters() : ShapingMeters.ZERO), rp);
                    } catch (RuntimeException e) {
                        fault();
                        return false;
                    }
                }
                planned = true;
                wirePadTo = rp.padTo();
                fit(w, true);
                int r = WireLength.wireOf(w.recMax(), sizeField, over);
                if (rp.padMultiple() > r) { rp.padMultiple(r); clamp(); }
                if (rp.pad() > r) { rp.pad(r); clamp(); }
                if (!rp.effects().isEmpty()) {
                    apply(rp.effects());
                    if (sh == null) return false;
                }
                if (rp.delayNanos() > 0) {
                    long now = port.now();
                    hold = Math.max(hold, Time.add(now, pause(rp.delayNanos())));
                    wake = true;
                }
            }
            // A record planned in the middle of a write may open at the start of a later one, or the reverse.
            else if (over != overHere(w)) fit(w, false);
            if (delaying(port.now())) w.holdRecord();
            w.cut(frameCut > 0 && frameCut < w.recMax() ? frameCut : 0);
            return true;
        }

        /** The write overhead of a record w opens now. */
        private int overHere(RecordWriter w) { return w.writeStart() ? writeOverhead(p) : 0; }

        /** Reads the plan's lengths for a record w opens now; count: its clamps are counted, once for a record. */
        private void fit(RecordWriter w, boolean count) {
            sizeField = w.framing() == Framing.BYTES;
            over = overHere(w);
            frameCut = 0;
            int padTo = wirePadTo;
            if (rp.maxLength() > 0) {
                frameCut = WireLength.framesOf(rp.maxLength(), sizeField, over);
                if (frameCut < RecordPlan.MIN_MAX_LENGTH) {
                    frameCut = RecordPlan.MIN_MAX_LENGTH;
                    if (count) clamp();
                    if (padTo == rp.maxLength()) padTo = WireLength.wireOf(frameCut, sizeField, over);
                }
            }
            // No padding target goes beyond a record, so that the targets cannot overflow.
            int r = WireLength.wireOf(w.recMax(), sizeField, over);
            if (padTo > r) {
                padTo = r;
                if (count) clamp();
            }
            rp.padTo(padTo);
        }

        /** The frames bytes the plan gives a record of n frames bytes. */
        int target(int n) {
            return framesFor(n, sizeField, over, rp.padTo(), rp.padMultiple(), rp.pad());
        }

        /**
         * The most frames bytes the planned record may hold so that, padded to its target and
         * extra PADDING, it fits in total bytes; -1 when the target itself does not fit.
         */
        int framesBound(int total) {
            int b = total - extraPad;
            if (b < 0) return -1;
            long l = (long) WireLength.wireOf(b, sizeField, over) - Math.max(rp.pad(), 0);
            int m = rp.padMultiple();
            if (m > 0 && l > 0) l -= l % m;
            if (l < Math.max(rp.padTo(), 0)) return -1; // also a negative l
            int f = WireLength.framesOf(l, sizeField, over);
            return WireLength.wireOf(f, sizeField, over) > l ? f - 1 : f;
        }

        /** The head of the next record when this one is cut at cut frames bytes. */
        int cutCost(int cut) { return WireLength.wireOf(cut, sizeField, over) - cut; }

        /** The PADDING injected beyond the plan. */
        int extraPad() {
            return extraPad;
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
