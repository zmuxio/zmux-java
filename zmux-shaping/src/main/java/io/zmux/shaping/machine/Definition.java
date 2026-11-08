package io.zmux.shaping.machine;

import java.util.ArrayList;
import java.util.List;

/**
 * A set of machines that run side by side on each pipe, read from JSON: lengths are bytes on the carrier, times
 * microseconds, probabilities parts per million, and every number an integer.
 */
public final class Definition {
    final List<Machine> machines;
    final int pipeSlots, sharedSlots;

    Definition(List<Machine> machines) {
        this.machines = List.copyOf(machines);
        int[] n = new int[2];
        for (Value v : sites()) {
            if (v.scope == Scope.PIPE) v.slot = n[0]++;
            else if (v.scope == Scope.SHARED) v.slot = n[1]++;
        }
        pipeSlots = n[0];
        sharedSlots = n[1];
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
                RecordRule r = st.record;
                if (r != null) add(out, r.target, r.maxLen, r.padTo, r.padMultiple, r.pad, r.delay);
                for (Action a : st.enter) add(out, a.after, a.len, a.rate, a.burst, a.value);
                for (Transition t : st.next) add(out, t.cond == null ? null : t.cond.value, t.prob);
            }
        }
        return out;
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
    }

    record Machine(int start, int counters, int timers, List<State> states) {}

    record State(RecordRule record, List<Action> enter, List<Transition> next) {}

    record RecordRule(Value target, Value maxLen, Value padTo, Value padMultiple, Value pad, Value delay, boolean flush) {}

    enum Op { INJECT, PAUSE, RATE, TIMER, SET, ADD, SUB, WEIGHT, STOP, RESTART }

    /** An operation; fields its op does not use are null. */
    record Action(Op op, boolean cancel, int slot, Value after, Value len, Value rate, Value burst, Value value) {}

    /** What a transition reacts to; {@code TIMER} and {@code ZERO} carry a slot. */
    enum Trigger { RECORD_OUT, RECORD_IN, PADDING_IN, INJECTED, INJECT_DROPPED, ENTER, STREAM_OPEN, STREAM_END, TIMER, ZERO }

    /** A transition; slot is -1 unless it reacts to a timer or a counter. */
    record Transition(Trigger on, int slot, int to, Cond cond, Value prob) {}

    enum Quantity {
        COUNTER, LEN, SEQ, WIRE_LEFT, STALL_LEFT, WIRE_SPENT, SENT_BYTES, WIRE_PPM;

        /** A reading of the session's meters. */
        boolean metered() { return ordinal() >= WIRE_LEFT.ordinal(); }
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
