package io.zmux.shaping.machine;

import io.zmux.shaping.Capability;
import io.zmux.shaping.Event;
import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.RecordView;
import io.zmux.shaping.Reply;
import io.zmux.shaping.Shaper;
import io.zmux.shaping.ShapingLimits;
import io.zmux.shaping.ShaperFactory;
import io.zmux.shaping.machine.Definition.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.random.RandomGenerator;

/** Factories of {@link Shaper}s that run the machines of a {@link Definition}. */
public final class Machines {
    private Machines() {}

    /**
     * A factory running d; d is checked against the options' limits. Every pipe runs d as
     * defined, whatever its {@link io.zmux.shaping.ShapedPipe} says, except for its write
     * overhead: the {@code max_len} and {@code pad_to} lengths of d are those of a carrier
     * write below the pipe, so the pipe's {@link io.zmux.shaping.ShapedPipe#writeOverhead()} is
     * taken off them on the first record of each write ({@link RecordView#writeStart()}); a
     * {@code max_len} that falls below {@link RecordPlan#MIN_MAX_LENGTH} is raised to it by the
     * session (and counted as clamped).
     */
    public static ShaperFactory factory(Definition d, Options o) throws DefinitionException {
        d.check(o.limits());
        Factory f = new Factory(d, o);
        return pipe -> f.newShaper(pipe.writeOverhead());
    }

    /** Reads a JSON definition and returns its factory. */
    public static ShaperFactory factory(String json, Options o) throws DefinitionException {
        var m = o.limits().maxBytes();
        if (SizeLimits.over(json.length(), m)) throw new DefinitionException("machine: " + json.length() + " bytes, limit " + m.getAsInt());
        return factory(Definition.parse(json), o);
    }

    /**
     * Reads a pool policy: a JSON object with the optional members {@code count} (a value),
     * {@code dial_after} (a list of values; entry {@code min(failures, length - 1)} applies)
     * and {@code retire} (a list of {@code {"metric": name, "above": value}}). Values have the
     * JSON form and validation of a definition's values; times are in seconds. Unknown members
     * and metrics are errors; scope {@code pipe} is accepted only in {@code retire}. The input
     * is bounded by the options' {@code maxBytes}; values are drawn from the options' source.
     * See {@link MachinePoolPolicy} for the decisions.
     */
    public static MachinePoolPolicy poolPolicy(byte[] json, Options o) throws DefinitionException {
        return MachinePoolPolicy.read(json, o);
    }

    /**
     * Reads session limits: a JSON object with the optional members {@code max_padding_bytes} and
     * {@code max_injected} (integers, 0 to 2^53), {@code max_padding_ratio} (0 to 1),
     * {@code max_pause_total} and {@code max_delay} (seconds, 0 to 1e9). An absent or null member is
     * no limit; 0 allows none. Unknown and duplicate members are errors. The input is bounded by the
     * options' {@code maxBytes}.
     */
    public static ShapingLimits sessionLimits(byte[] json, Options o) throws DefinitionException {
        var m = o.limits().maxBytes();
        if (SizeLimits.over(json.length, m)) throw new DefinitionException("limits: " + json.length + " bytes, limit " + m.getAsInt());
        return DefinitionJson.sessionLimits(new String(json, StandardCharsets.UTF_8));
    }

    private static final Trigger[] TIMERS = {Trigger.TIMER0, Trigger.TIMER1};
    private static final Trigger[] ZEROS = {Trigger.ZERO0, Trigger.ZERO1};
    private static final Trigger[] TRIGGERS = Trigger.values();

    private static final class Factory {
        final Definition d;
        final Options o;
        final IdentityHashMap<Value, Integer> idx = new IdentityHashMap<>();
        final Set<Capability> caps = EnumSet.of(Capability.PLAN);
        /** Triggers processed per event. */
        int bound;
        /** Factory-scoped values. */
        double[] kept;
        boolean[] has;

        Factory(Definition d, Options o) {
            this.d = d;
            this.o = o;
            for (Machine m : d.machines()) {
                bound += 4 * (m.states().size() + 1);
                for (State st : m.states()) {
                    Validation.values(st, v -> idx.putIfAbsent(v, idx.size()));
                    for (Transition t : st.next()) {
                        switch (t.on()) {
                            case RECORD_IN, PADDING_IN -> caps.add(Capability.RECORD_IN);
                            case STREAM_OPEN, STREAM_END -> caps.add(Capability.STREAMS);
                            default -> { }
                        }
                    }
                }
            }
            MachineShaper s = newShaper(0);
            double[] k = new double[idx.size()];
            boolean[] h = new boolean[idx.size()];
            for (Map.Entry<Value, Integer> e : idx.entrySet()) {
                if (e.getKey().scope() == Scope.SHARED) { k[e.getValue()] = s.draw(e.getKey()); h[e.getValue()] = true; }
            }
            kept = k;
            has = h;
        }

        MachineShaper newShaper(int overhead) {
            return new MachineShaper(this, Sampler.source(o), overhead);
        }
    }

    /** The shaper of one pipe. */
    private static final class MachineShaper extends Sampler implements Shaper {
        private final Factory f;
        private final int[] cur;
        private final double[][] ctr;
        /** Pipe-scoped values. */
        private final double[] kept;
        private final boolean[] has;
        private long seq;
        private Reply rep;
        private boolean stopping;
        /** Length of the triggering event. */
        private int n;
        private final ArrayDeque<long[]> q = new ArrayDeque<>();
        /** The pipe's write overhead. */
        private final int overhead;

        MachineShaper(Factory f, RandomGenerator r, int overhead) {
            super(r);
            this.f = f;
            this.overhead = Math.max(overhead, 0);
            int m = f.d.machines().size();
            cur = new int[m];
            ctr = new double[m][2];
            kept = new double[f.idx.size()];
            has = new boolean[f.idx.size()];
        }

        @Override public Set<Capability> capabilities() { return f.caps; }

        @Override public void plan(RecordView view, RecordPlan p) {
            for (int i = 0; i < cur.length; i++) {
                RecordRule rr = f.d.machines().get(i).states().get(cur[i]).record();
                if (rr == null) continue;
                int m = 0;
                if (rr.maxLen() != null) {
                    m = length(rr.maxLen());
                    if (p.maxLength() == 0 || m > 0 && m < p.maxLength()) p.maxLength(m);
                }
                if (rr.padTo() == rr.maxLen() && rr.padTo() != null) p.padTo(Math.max(p.padTo(), m)); // one draw for both fields
                else if (rr.padTo() != null) p.padTo(Math.max(p.padTo(), length(rr.padTo())));
                if (rr.padMultiple() != null) p.padMultiple(Math.max(p.padMultiple(), length(rr.padMultiple())));
                if (rr.pad() != null) p.pad((int) Math.min((long) p.pad() + length(rr.pad()), Integer.MAX_VALUE)); // lengths are at most 2^31-1
                if (rr.delay() != null) p.delayNanos(Math.max(p.delayNanos(), nanos(rr.delay())));
                if (rr.flush()) p.flush(true);
            }
            if (overhead > 0 && view.writeStart()) {
                // Below MIN_MAX_LENGTH: raised and counted by the session.
                if (p.maxLength() > 0) p.maxLength(Math.max(p.maxLength() - overhead, 1));
                p.padTo(Math.max(p.padTo() - overhead, 0));
            }
            rep = p.reply();
            stopping = false;
            fire(Trigger.RECORD_OUT, 0);
            seq++;
            if (stopping) p.stop(true); // The stop follows the record.
            rep = null;
        }

        @Override public void onEvent(Event e, Reply reply) {
            rep = reply;
            stopping = false;
            try {
                switch (e.kind()) {
                    case START -> {
                        for (int i = 0; i < cur.length; i++) enter(i, f.d.machines().get(i).start());
                        drain();
                    }
                    case RECORD_IN -> fire(e.paddingOnly() ? Trigger.PADDING_IN : Trigger.RECORD_IN, e.length());
                    case INJECTED -> fire(Trigger.INJECTED, e.length());
                    case TIMER -> fire(TIMERS[e.timer() & 1], 0);
                    case STREAM_OPEN -> fire(Trigger.STREAM_OPEN, 0);
                    case STREAM_END -> fire(Trigger.STREAM_END, 0);
                    default -> { }
                }
                if (stopping) reply.stop();
            } finally {
                rep = null;
            }
        }

        /** Offers t to every machine. */
        private void fire(Trigger t, int len) {
            n = len;
            for (int i = 0; i < cur.length; i++) q.add(new long[] {i, t.ordinal()});
            drain();
        }

        /** Processes pending triggers, at most the factory's bound per call. */
        private void drain() {
            for (int steps = 0; !q.isEmpty() && steps < f.bound; steps++) {
                long[] g = q.poll();
                step((int) g[0], TRIGGERS[(int) g[1]]);
            }
            q.clear();
        }

        private void step(int m, Trigger t) {
            State st = f.d.machines().get(m).states().get(cur[m]);
            for (Transition tr : st.next()) {
                if (tr.on() != t || tr.cond() != null && !cond(m, tr.cond())) continue;
                if (tr.prob() != null && r.nextDouble() >= sample(tr.prob())) continue;
                enter(m, tr.to());
                return;
            }
        }

        private void enter(int m, int to) {
            cur[m] = to;
            for (Action a : f.d.machines().get(m).states().get(to).enter()) act(m, a);
            q.add(new long[] {m, Trigger.ENTER.ordinal()});
        }

        private void act(int m, Action a) {
            Reply rp = rep;
            switch (a.op()) {
                case INJECT -> {
                    if (a.cancel()) rp.cancelInject();
                    else rp.inject(nanos(a.after()), length(a.len()));
                }
                case PAUSE -> rp.gate(nanos(a.after()), 0, 0);
                case RATE -> rp.gate(0, (long) bounded(Sampler.round(sample(a.rate())), Validation.MAX_COUNT), a.burst() == null ? 0 : length(a.burst()));
                case TIMER -> {
                    if (a.cancel()) rp.cancelTimer(a.slot());
                    else rp.timer(a.slot(), nanos(a.after()));
                }
                case SET, ADD, SUB -> {
                    double was = ctr[m][a.slot()], x = Sampler.round(sample(a.value())); // not negative: checked at load
                    double c = switch (a.op()) {
                        case SET -> x;
                        case ADD -> was + x;
                        default -> Math.max(was - x, 0);
                    };
                    ctr[m][a.slot()] = c;
                    if (c == 0 && (was != 0 || a.op() != Op.SET)) q.add(new long[] {m, ZEROS[a.slot()].ordinal()});
                }
                case WEIGHT -> rp.weight(sample(a.value()));
                case STOP -> stopping = true;
                case RESTART -> enter(m, f.d.machines().get(m).start());
            }
        }

        private boolean cond(int m, Cond c) {
            double x = switch (c.what()) {
                case COUNTER0 -> ctr[m][0];
                case COUNTER1 -> ctr[m][1];
                case LEN -> n;
                case SEQ -> seq;
            };
            double y = sample(c.value());
            return switch (c.cmp()) {
                case LT -> x < y;
                case LE -> x <= y;
                case EQ -> x == y;
                case NE -> x != y;
                case GE -> x >= y;
                case GT -> x > y;
            };
        }

        private int length(Value v) {
            double x = Sampler.round(sample(v));
            if (!(x > 0)) return 0;
            return (int) Math.min(x, Integer.MAX_VALUE);
        }

        private long nanos(Value v) {
            return (long) bounded(Sampler.round(sample(v) * 1e9), Long.MAX_VALUE / 2.0);
        }

        /** x within [-m, m], NaN as 0, so that a conversion to an integer is the same on every platform. */
        private static double bounded(double x, double m) {
            if (Double.isNaN(x)) return 0;
            return Math.max(Math.min(x, m), -m);
        }

        /** v, kept per its scope. */
        @Override double sample(Value v) {
            int i = f.idx.get(v);
            if (v.scope() == Scope.SHARED && f.has != null && f.has[i]) return f.kept[i];
            if (v.scope() == Scope.PIPE && has[i]) return kept[i];
            double x = draw(v);
            if (v.scope() == Scope.PIPE) { kept[i] = x; has[i] = true; }
            return x;
        }
    }
}
