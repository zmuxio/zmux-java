package io.zmux.shaping.machine;

import java.util.List;
import java.util.Objects;

/**
 * A set of machines that run side by side on each pipe. Values are compared by identity:
 * one {@link Value} object used in two places is one sampled quantity.
 *
 * @param version must be {@link #VERSION}
 */
public record Definition(int version, List<Machine> machines) {
    /** The definition version this package reads. */
    public static final int VERSION = 1;

    public Definition {
        machines = machines == null ? List.of() : List.copyOf(machines);
    }

    /**
     * A state machine with two counters.
     *
     * @param start index into states
     */
    public record Machine(int start, List<State> states) {
        public Machine { states = states == null ? List.of() : List.copyOf(states); }
    }

    /**
     * A machine state.
     *
     * @param record applies to each record planned while the machine is in the state; null: none
     * @param enter runs in order when the state is entered
     * @param next is tried in order on each trigger; the first match moves
     */
    public record State(RecordRule record, List<Action> enter, List<Transition> next) {
        public State {
            enter = enter == null ? List.of() : List.copyOf(enter);
            next = next == null ? List.of() : List.copyOf(next);
        }
    }

    /** Record decisions; null values: none. Lengths in bytes, times in seconds. */
    public record RecordRule(Value maxLen, Value padTo, Value padMultiple, Value pad, Value delay, boolean flush) {}

    /** An operation; fields not used by its op are ignored. Slot is 0 or 1. */
    public record Action(Op op, boolean cancel, int slot, Value after, Value len, Value rate, Value burst, Value value) {
        public Action { Objects.requireNonNull(op, "op"); }
    }

    /** Moves the machine to {@code to} when {@code on} happens, {@code cond} holds and a draw falls below {@code prob} (null: 1). */
    public record Transition(Trigger on, int to, Value prob, Cond cond) {
        public Transition { Objects.requireNonNull(on, "on"); }
    }

    /** Compares a quantity with a value: what cmp value. */
    public record Cond(Quantity what, Comparison cmp, Value value) {
        public Cond {
            Objects.requireNonNull(what, "what");
            Objects.requireNonNull(cmp, "cmp");
        }
    }

    /**
     * A number: exactly one of constant, a uniform range {a, b}, or a distribution; then bounded
     * by min and max when set (null: unbounded).
     */
    public record Value(Double constant, double[] range, Dist dist, Scope scope, Double min, Double max) {
        public Value { if (scope == null) scope = Scope.EACH; }

        public static Value of(double x) { return new Value(x, null, null, Scope.EACH, null, null); }
    }

    /**
     * A distribution. Params by kind: uniform a b | loguniform a b (0 &lt; a &lt;= b) | exponential
     * rate | normal mean sd | lognormal mu sigma | weibull scale shape | pareto scale shape |
     * gamma shape scale | beta a b | geometric p | binomial n p (n integral, at most 2^53) | poisson mean (at most 2^53) | empirical:
     * points with weights | bag: points, drawn without replacement and refilled when empty |
     * mixture: of with weights.
     */
    public record Dist(DistKind kind, double[] params, double[] points, double[] weights, List<Value> of) {
        public Dist {
            Objects.requireNonNull(kind, "kind");
            if (params == null) params = new double[0];
            if (points == null) points = new double[0];
            if (weights == null) weights = new double[0];
            of = of == null ? List.of() : List.copyOf(of);
        }
    }

    /** The operations of an {@link Action}. */
    public enum Op {
        /** len bytes after {@code after} seconds; cancel: cancels the pending one. */
        INJECT("inject"),
        /** Stream data waits {@code after} seconds. */
        PAUSE("pause"),
        /** rate bytes per second, burst bytes; rate 0: no limit (a rate of none is a {@link #PAUSE}; the gate contract of all implementations). */
        RATE("rate"),
        /** Timer slot fires after {@code after} seconds; cancel: disarms it. */
        TIMER("timer"),
        /** Counter slot = value. */
        SET("set"),
        /** Counter slot += value. */
        ADD("add"),
        /** Counter slot -= value, not below 0. */
        SUB("sub"),
        /** Pipe weight = value. */
        WEIGHT("weight"),
        /** Shaping of the pipe ends. */
        STOP("stop"),
        /** The machine re-enters its start state. */
        RESTART("restart");

        final String id;
        Op(String id) { this.id = id; }
    }

    /** What a {@link Transition} reacts to. */
    public enum Trigger {
        /** A record was planned. */
        RECORD_OUT("record_out"),
        /** A record arrived. */
        RECORD_IN("record_in"),
        /** A record of PADDING only arrived. */
        PADDING_IN("padding_in"),
        /** An injection was sent. */
        INJECTED("injected"),
        TIMER0("timer0"),
        TIMER1("timer1"),
        /** Counter 0 reached 0. */
        ZERO0("zero0"),
        /** Counter 1 reached 0. */
        ZERO1("zero1"),
        /** Tried once after the state is entered. */
        ENTER("enter"),
        STREAM_OPEN("stream_open"),
        STREAM_END("stream_end");

        final String id;
        Trigger(String id) { this.id = id; }
    }

    /** What a {@link Cond} compares. */
    public enum Quantity {
        COUNTER0("counter0"), COUNTER1("counter1"),
        /** Bytes of the triggering record. */
        LEN("len"),
        /** Records planned on the pipe. */
        SEQ("seq");

        final String id;
        Quantity(String id) { this.id = id; }
    }

    /** How a {@link Cond} compares. */
    public enum Comparison {
        LT("lt"), LE("le"), EQ("eq"), NE("ne"), GE("ge"), GT("gt");

        final String id;
        Comparison(String id) { this.id = id; }
    }

    /** How long a sampled value is kept. */
    public enum Scope {
        /** Sampled at each use. */
        EACH("each"),
        /** Once per pipe. */
        PIPE("pipe"),
        /** Once per factory, shared by its pipes. */
        SHARED("shared");

        final String id;
        Scope(String id) { this.id = id; }
    }

    /** Distribution kinds. */
    public enum DistKind {
        UNIFORM("uniform", 2), LOGUNIFORM("loguniform", 2), EXPONENTIAL("exponential", 1), NORMAL("normal", 2),
        LOGNORMAL("lognormal", 2), WEIBULL("weibull", 2), PARETO("pareto", 2), GAMMA("gamma", 2), BETA("beta", 2),
        GEOMETRIC("geometric", 1), BINOMIAL("binomial", 2), POISSON("poisson", 1), EMPIRICAL("empirical", 0),
        BAG("bag", 0), MIXTURE("mixture", 0);

        final String id;
        final int params;
        DistKind(String id, int params) { this.id = id; this.params = params; }
    }

    /** Reads a JSON definition and validates it. */
    public static Definition parse(String json) throws DefinitionException {
        Definition d = DefinitionJson.read(json);
        d.validate();
        return d;
    }

    /** Throws the first error of this definition. */
    public void validate() throws DefinitionException {
        Validation.definition(this);
    }

    /** Validates this definition and bounds it by limits. */
    public void check(SizeLimits limits) throws DefinitionException {
        validate();
        Validation.size(this, limits);
    }
}
