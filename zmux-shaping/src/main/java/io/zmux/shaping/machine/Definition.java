package io.zmux.shaping.machine;

import io.zmux.shaping.GateRequirement;
import java.util.ArrayList;
import java.util.List;

/**
 * A set of machines that run side by side on each pipe, read from JSON: lengths are bytes on the carrier, times
 * microseconds, probabilities parts per million, and every number an integer.
 */
public final class Definition {
    final List<Machine> machines;
    final int pipeSlots, sharedSlots;
    final boolean includeControl;
    final GateRequirement gates;

    Definition(List<Machine> machines, boolean includeControl) {
        this.includeControl = includeControl;
        this.machines = List.copyOf(machines);
        GateRequirement required = GateRequirement.NONE;
        for (Machine m : machines) {
            for (State state : m.states) {
                for (Action action : state.enter) {
                    if (action.op == Op.RATE) required = required.combine(rateRequirement(action));
                }
            }
        }
        gates = required;
        int[] n = new int[2];
        for (Value v : sites()) {
            if (v.scope == Scope.PIPE) v.slot = n[0]++;
            else if (v.scope == Scope.SHARED) v.slot = n[1]++;
        }
        pipeSlots = n[0];
        sharedSlots = n[1];
    }

    private static GateRequirement rateRequirement(Action action) {
        // Burst zero resolves to the largest negotiated record. Zero-weight values are unreachable.
        long burst = 64;
        if (action.burst == null) burst = 65535;
        else {
            long[] values = action.burst.segments;
            for (int i = 0; i < values.length; i += 3) {
                if (values[i] == 0) continue;
                burst = Math.max(burst, values[i + 2]);
                if (values[i + 1] == 0) burst = Math.max(burst, 65535);
            }
        }
        long[] rates = action.rate.segments;
        GateRequirement result = GateRequirement.NONE;
        for (int i = 0; i < rates.length; i += 3) {
            if (rates[i] == 0 || rates[i + 2] == 0) continue;
            long lo = Math.max(rates[i + 1], 1);
            long wait = GateRequirement.waitMicros(lo, burst, 0);
            long nextQuantum = Math.ceilDiv(lo, 1000) * 1000;
            if (nextQuantum <= rates[i + 2]) wait = Math.max(wait, GateRequirement.waitMicros(nextQuantum, burst, 0));
            result = result.combine(new GateRequirement(wait, lo));
        }
        return result;
    }

    /** Reads a JSON object with {@code machines} and, optionally, {@code tables}. */
    public static Definition parse(String json) throws DefinitionException {
        return parse(json, SizeLimits.NONE);
    }

    /** Reads a definition under limits. */
    public static Definition parse(String json, SizeLimits limits) throws DefinitionException {
        limits.check(json.length());
        return DefinitionJson.definition(DefinitionJson.parse(json), limits);
    }

    /** Every value, in the order shared values are drawn. */
    List<Value> sites() {
        List<Value> out = new ArrayList<>();
        for (Machine m : machines) {
            for (State st : m.states) {
                addRule(out, st.record);
                addRule(out, st.datagram);
                for (Action a : st.enter) add(out, a.after, a.len, a.rate, a.burst, a.value);
                for (Transition t : st.next) add(out, t.cond == null ? null : t.cond.value, t.prob);
            }
        }
        return out;
    }

    private static void addRule(List<Value> out, RecordRule r) {
        if (r != null) add(out, r.target, r.maxLen, r.padTo, r.padMultiple, r.pad, r.delay);
    }

    private static void add(List<Value> out, Value... vs) {
        for (Value v : vs) if (v != null) out.add(v);
    }

    enum Scope { EACH, PIPE, SHARED, SESSION }

    /** One place a number is used: a constant, or a table flat as weight, lo, hi of each segment. */
    static final class Value {
        final long[] segments;
        final Scope scope;
        private final long lower;
        int slot;

        Value(long[] segments, Scope scope, long lower) {
            this.segments = segments;
            this.scope = scope;
            this.lower = lower;
        }

        /** The least bound of its segments. */
        long lower() { return lower; }

        long upper() {
            long hi = 0;
            for (int i = 2; i < segments.length; i += 3) hi = Math.max(hi, segments[i]);
            return hi;
        }
    }

    record Machine(int start, int counters, int timers, List<State> states) {}

    record State(RecordRule record, RecordRule datagram, List<Action> enter, List<Transition> next) {}

    record RecordRule(Value target, Value maxLen, Value padTo, Value padMultiple, Value pad, Value delay, boolean flush) {}

    enum Op { INJECT, PAUSE, RATE, TIMER, SET, ADD, SUB, WEIGHT, STOP, RESTART }

    /** An operation; fields its op does not use are null. */
    record Action(Op op, boolean cancel, int slot, Value after, Value len, Value rate, Value burst, Value value) {}

    /** What a transition reacts to; {@code TIMER} and {@code ZERO} carry a slot. */
    enum Trigger { RECORD_PLAN, DATAGRAM_PLAN, OUTBOUND, INJECTION_ACCEPTED, INJECTION_REJECTED, ENTER, STREAM_OPEN, STREAM_END, TIMER, ZERO }

    /** A transition; slot is -1 unless it reacts to a timer or a counter. */
    record Transition(Trigger on, int slot, int to, Cond cond, Value prob) {}

    enum Quantity {
        COUNTER, LEN, SEQ, WIRE_LEFT, STALL_LEFT, WIRE_SPENT, EARNED_BYTES, WIRE_PPM;

        /** A reading of the session's meters. */
        boolean metered() {
            return switch (this) {
                case COUNTER, LEN, SEQ -> false;
                case WIRE_LEFT, STALL_LEFT, WIRE_SPENT, EARNED_BYTES, WIRE_PPM -> true;
            };
        }
    }

    enum Comparison {
        LT, LE, EQ, NE, GE, GT;

        boolean holds(long x, long y) {
            int c = Long.compareUnsigned(x, y);
            return switch (this) {
                case LT -> c < 0;
                case LE -> c <= 0;
                case EQ -> c == 0;
                case NE -> c != 0;
                case GE -> c >= 0;
                case GT -> c > 0;
            };
        }
    }

    record Cond(Quantity what, int slot, Comparison cmp, Value value) {}
}
