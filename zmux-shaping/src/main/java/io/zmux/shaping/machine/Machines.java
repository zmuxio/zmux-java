package io.zmux.shaping.machine;

import io.zmux.shaping.Capability;
import io.zmux.shaping.Effects;
import io.zmux.shaping.Event;
import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.RecordView;
import io.zmux.shaping.Shaper;
import io.zmux.shaping.ShaperFactory;
import io.zmux.shaping.ShapingMeters;
import io.zmux.shaping.ShapingLimits;
import io.zmux.shaping.machine.Definition.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/** Factories of {@link Shaper}s that run the machines of a {@link Definition}, and the other JSON readers. */
public final class Machines {
    private Machines() {}

    private static final long NONE = -1;
    private static final long NANOS = 1000;

    /** A factory running d; its shared values are drawn here from the options' source. */
    public static ShaperFactory factory(Definition d, MachineOptions o) {
        Compiled c = new Compiled(d, Sampler.of(o), o);
        return pipe -> c.newShaper();
    }

    /** Reads a JSON definition and returns its factory. */
    public static ShaperFactory factory(String json, MachineOptions o) throws DefinitionException {
        return factory(Definition.parse(json, o.limits()), o);
    }

    /** Reads a pool policy; see {@link MachinePoolPolicy}. */
    public static MachinePoolPolicy poolPolicy(byte[] json, MachineOptions o) throws DefinitionException {
        o.limits().check(json.length);
        return MachinePoolPolicy.read(new String(json, StandardCharsets.UTF_8), o);
    }

    /**
     * Reads local limits: a JSON object with the optional members {@code wire} ({@code initial} up to 2^53, {@code per_mb} and
     * {@code per_sec} up to 2^43, integers), {@code max_delay} (microseconds up to 10^15; 0 the default, negative no bound)
     * and the switches {@code no_padding}, {@code no_pause} and {@code no_injection}. A wire of zeros allows no injection.
     */
    public static ShapingLimits limits(byte[] json, MachineOptions o) throws DefinitionException {
        o.limits().check(json.length);
        return DefinitionJson.limits(DefinitionJson.parse(new String(json, StandardCharsets.UTF_8)));
    }

    /** A definition with its shared values drawn: read only, shared by the pipes. */
    static final class Compiled {
        final Definition d;
        final long[] shared;
        final Set<Capability> caps = EnumSet.of(Capability.PLAN);
        private final MachineOptions o;

        Compiled(Definition d, Sampler compile, MachineOptions o) {
            this.d = d;
            this.o = o;
            shared = new long[d.sharedSlots];
            for (Value v : d.sites()) if (v.scope == Scope.SHARED) shared[v.slot] = compile.draw(v.segments);
            for (Machine m : d.machines) {
                for (State st : m.states()) {
                    for (Transition t : st.next()) {
                        switch (t.on()) {
                            case RECORD_IN, PADDING_IN -> caps.add(Capability.RECORD_IN);
                            case STREAM_OPEN, STREAM_END -> caps.add(Capability.STREAMS);
                            default -> { }
                        }
                        if (t.cond() != null && t.cond().what().metered()) caps.add(Capability.BUDGET);
                    }
                }
            }
        }

        MachineShaper newShaper() { return new MachineShaper(this, Sampler.of(o)); }
    }

    /** The shaper of one pipe. Times inside are microseconds of the session clock. */
    static final class MachineShaper implements Shaper {
        private final Compiled c;
        private final List<Machine> ms;
        private final Sampler s;
        private final int[] cur, gen;
        private final long[][] counters, timers;
        private final long[] injectAt, rate, burst, weight;
        private final int[] injectLen;
        private long pauseUntil, seq;
        /** Length of the triggering event. */
        private long len;
        private long now;
        private boolean stopped;
        /** The readings of this entrance. */
        private ShapingMeters meters = ShapingMeters.ZERO;
        private final long[] pipeKept;
        private final BitsOf pipeHas;
        /** The FIFO of triggers, four ints each: machine, trigger, slot, generation. */
        private int[] q = new int[64];
        private int qHead, qTail;
        /** The machine of the injection last reported; -1: none. */
        private int surfaced = -1;
        private long lastInjectAt = NONE, lastWake = NONE, lastUntil, lastRate, lastBurst, lastWeight;
        private int lastInjectLen;

        MachineShaper(Compiled c, Sampler s) {
            this.c = c;
            this.ms = c.d.machines;
            this.s = s;
            int n = ms.size();
            cur = new int[n];
            gen = new int[n];
            counters = new long[n][];
            timers = new long[n][];
            injectAt = new long[n];
            injectLen = new int[n];
            rate = new long[n];
            burst = new long[n];
            weight = new long[n];
            Arrays.fill(injectAt, NONE);
            for (int i = 0; i < n; i++) {
                cur[i] = ms.get(i).start();
                counters[i] = new long[ms.get(i).counters()];
                timers[i] = new long[ms.get(i).timers()];
                Arrays.fill(timers[i], NONE);
            }
            pipeKept = new long[c.d.pipeSlots];
            pipeHas = new BitsOf(c.d.pipeSlots);
        }

        @Override public Set<Capability> capabilities() { return c.caps; }

        int state(int machine) { return cur[machine]; }

        long[] counters(int machine) { return counters[machine]; }

        @Override public void plan(RecordView view, RecordPlan p) {
            now = view.nowNanos() / NANOS;
            meters = view.meters();
            long maxLen = 0, padTo = 0, multiple = 0, pad = 0, delay = 0;
            boolean flush = false;
            for (int i = 0; i < ms.size(); i++) {
                RecordRule r = ms.get(i).states().get(cur[i]).record();
                if (r == null) continue;
                long ml = 0;
                if (r.target() != null) {
                    ml = val(r.target());
                    padTo = Math.max(padTo, ml);
                } else if (r.maxLen() != null) {
                    ml = val(r.maxLen());
                }
                if (ml != 0 && (maxLen == 0 || ml < maxLen)) maxLen = ml;
                if (r.padTo() != null) padTo = Math.max(padTo, val(r.padTo()));
                if (r.padMultiple() != null) multiple = Math.max(multiple, val(r.padMultiple()));
                if (r.pad() != null) pad = Math.min(pad + val(r.pad()), Integer.MAX_VALUE);
                if (r.delay() != null) delay = Math.max(delay, val(r.delay()));
                flush |= r.flush();
            }
            p.maxLength((int) maxLen).padTo((int) padTo).padMultiple((int) multiple).pad((int) pad).delayNanos(delay * NANOS).flush(flush);
            fire(Trigger.RECORD_OUT, -1, 0, -1);
            seq++;
            p.stop(stopped);
            effects(p.effects());
        }

        @Override public void onEvent(Event e, Effects out) {
            now = e.nowNanos() / NANOS;
            meters = e.meters();
            switch (e.kind()) {
                case START -> {
                    for (int i = 0; i < ms.size(); i++) enter(i, ms.get(i).start());
                    drain();
                }
                case RECORD_IN -> fire(e.paddingOnly() ? Trigger.PADDING_IN : Trigger.RECORD_IN, -1, e.length(), -1);
                case INJECTED -> {
                    clearSurfaced();
                    fire(Trigger.INJECTED, -1, e.length(), -1);
                }
                case INJECT_DROPPED -> {
                    clearSurfaced();
                    if (surfaced >= 0) fire(Trigger.INJECT_DROPPED, -1, 0, surfaced);
                }
                case WAKE -> wake();
                case STREAM_OPEN -> fire(Trigger.STREAM_OPEN, -1, 0, -1);
                case STREAM_END -> fire(Trigger.STREAM_END, -1, 0, -1);
                default -> { return; } // replaced, stopped, pipe failed: no machine hears them
            }
            effects(out);
            if (stopped) out.stop();
        }

        private void clearSurfaced() {
            if (surfaced >= 0) injectAt[surfaced] = NONE;
        }

        private void wake() {
            List<long[]> due = new ArrayList<>();
            for (int m = 0; m < timers.length; m++) {
                for (int k = 0; k < timers[m].length; k++) {
                    if (timers[m][k] != NONE && timers[m][k] <= now) due.add(new long[] {timers[m][k], m, k});
                }
            }
            due.sort((a, b) -> a[0] != b[0] ? Long.compare(a[0], b[0]) : a[1] != b[1] ? Long.compare(a[1], b[1]) : Long.compare(a[2], b[2]));
            for (long[] d : due) {
                int m = (int) d[1], k = (int) d[2];
                if (timers[m][k] != d[0]) continue;
                timers[m][k] = NONE;
                fire(Trigger.TIMER, k, 0, m);
            }
        }

        @SuppressWarnings("EnumOrdinal") // the queue is an int array; the trigger is stored by ordinal and read back from TRIGGERS
        private void push(int m, Trigger t, int slot) {
            if (qTail + 4 > q.length) {
                int live = qTail - qHead;
                int[] to = live * 2 >= q.length ? new int[q.length * 2] : q;
                System.arraycopy(q, qHead, to, 0, live);
                q = to;
                qHead = 0;
                qTail = live;
            }
            q[qTail] = m;
            q[qTail + 1] = t.ordinal();
            q[qTail + 2] = slot;
            q[qTail + 3] = gen[m];
            qTail += 4;
        }

        private static final Trigger[] TRIGGERS = Trigger.values();

        private void fire(Trigger t, int slot, long n, int only) {
            len = n;
            if (only >= 0) push(only, t, slot);
            else for (int m = 0; m < ms.size(); m++) push(m, t, slot);
            drain();
        }

        private void drain() {
            while (qHead < qTail) {
                int m = q[qHead], t = q[qHead + 1], slot = q[qHead + 2], g = q[qHead + 3];
                qHead += 4;
                if (g == gen[m]) step(m, TRIGGERS[t], slot);
            }
            qHead = qTail = 0;
        }

        private void step(int m, Trigger on, int slot) {
            for (Transition t : ms.get(m).states().get(cur[m]).next()) {
                if (t.on() != on || t.slot() != slot) continue;
                if (t.cond() != null && !holds(m, t.cond())) continue;
                if (t.prob() != null && !s.prob(val(t.prob()))) continue;
                enter(m, t.to());
                return;
            }
        }

        private boolean holds(int m, Cond k) {
            long x = switch (k.what()) {
                case COUNTER -> counters[m][k.slot()];
                case LEN -> len;
                case SEQ -> seq;
                case WIRE_LEFT -> meters.wireLeft();
                case STALL_LEFT -> meters.stallLeft();
                case WIRE_SPENT -> meters.wireSpent();
                case SENT_BYTES -> meters.sentBytes();
                case WIRE_PPM -> meters.wirePpm();
            };
            return k.cmp().holds(x, val(k.value()));
        }

        private void enter(int m, int to) {
            cur[m] = to;
            gen[m]++;
            for (Action a : ms.get(m).states().get(to).enter()) {
                if (a.op() == Op.RESTART) {
                    enter(m, ms.get(m).start());
                    return;
                }
                act(m, a);
            }
            push(m, Trigger.ENTER, -1);
        }

        private void act(int m, Action a) {
            switch (a.op()) {
                case INJECT -> {
                    if (a.cancel()) {
                        injectAt[m] = NONE;
                    } else {
                        long after = val(a.after());
                        injectLen[m] = (int) val(a.len());
                        injectAt[m] = now + after;
                    }
                }
                case PAUSE -> pauseUntil = Math.max(pauseUntil, now + val(a.after()));
                case RATE -> {
                    rate[m] = val(a.rate());
                    burst[m] = a.burst() == null ? 0 : val(a.burst());
                }
                case TIMER -> timers[m][a.slot()] = a.cancel() ? NONE : now + val(a.after());
                case SET, ADD, SUB -> {
                    long[] ct = counters[m];
                    long was = ct[a.slot()], x = val(a.value());
                    long v = switch (a.op()) {
                        case SET -> x;
                        case ADD -> Long.compareUnsigned(was + x, was) < 0 ? -1L : was + x;
                        default -> Long.compareUnsigned(was, x) > 0 ? was - x : 0;
                    };
                    ct[a.slot()] = v;
                    if (v == 0 && (was != 0 || a.op() != Op.SET)) push(m, Trigger.ZERO, a.slot());
                }
                case WEIGHT -> {
                    long w = val(a.value());
                    if (w != 0) weight[m] = w;
                }
                case STOP -> stopped = true;
                default -> { }
            }
        }

        private long val(Value v) {
            return switch (v.scope) {
                case SHARED -> c.shared[v.slot];
                case PIPE -> {
                    if (!pipeHas.get(v.slot)) {
                        pipeKept[v.slot] = s.draw(v.segments);
                        pipeHas.set(v.slot);
                    }
                    yield pipeKept[v.slot];
                }
                case EACH, SESSION -> s.draw(v.segments);
            };
        }

        /** What differs from the last report. */
        private void effects(Effects out) {
            int best = -1;
            for (int m = 0; m < injectAt.length; m++) {
                if (injectAt[m] != NONE && (best < 0 || injectAt[m] < injectAt[best])) best = m;
            }
            surfaced = best;
            long at = best < 0 ? NONE : injectAt[best];
            int l = best < 0 ? 0 : injectLen[best];
            if (at != lastInjectAt || l != lastInjectLen) {
                if (best < 0) out.noInject();
                else out.inject(at * NANOS, l);
                lastInjectAt = at;
                lastInjectLen = l;
            }
            long wake = NONE;
            for (long[] ts : timers) for (long t : ts) if (t != NONE && (wake == NONE || t < wake)) wake = t;
            if (wake != lastWake) {
                if (wake == NONE) out.noWake();
                else out.wake(wake * NANOS);
                lastWake = wake;
            }
            long r = 0, b = 0;
            for (int m = 0; m < rate.length; m++) {
                if (rate[m] != 0 && (r == 0 || rate[m] < r)) { r = rate[m]; b = burst[m]; }
            }
            if (pauseUntil != lastUntil || r != lastRate || b != lastBurst) {
                out.gate(pauseUntil * NANOS, r, b);
                lastUntil = pauseUntil;
                lastRate = r;
                lastBurst = b;
            }
            long w = 0;
            for (long x : weight) if (x != 0 && (w == 0 || x < w)) w = x;
            if (w != lastWeight) {
                out.weight(w);
                lastWeight = w;
            }
        }
    }

    /** A fixed set of slots. */
    // A mutable bit set: a record would compare and expose its array as a value, which it is not.
    @SuppressWarnings("ClassCanBeRecord")
    private static final class BitsOf {
        private final long[] words;
        BitsOf(int n) { words = new long[(n + 63) / 64]; }
        boolean get(int i) { return (words[i >> 6] & 1L << i) != 0; }
        void set(int i) { words[i >> 6] |= 1L << i; }
    }
}
