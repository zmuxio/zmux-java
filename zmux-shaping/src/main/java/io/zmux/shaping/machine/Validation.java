package io.zmux.shaping.machine;

import io.zmux.shaping.machine.Definition.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** Static checks of a {@link Definition}. */
final class Validation {
    /** The largest Binomial trial count, and Poisson mean, accepted: 2^53, beyond which counts are not exact in a double. */
    static final double MAX_COUNT = 0x1p53;
    private Validation() {}

    static void definition(Definition d) throws DefinitionException {
        if (d.version() != Definition.VERSION) {
            throw new DefinitionException("machine: version " + d.version() + ", want " + Definition.VERSION);
        }
        if (d.machines().isEmpty()) throw new DefinitionException("machine: no machines");
        for (int i = 0; i < d.machines().size(); i++) {
            try { machine(d.machines().get(i)); }
            catch (DefinitionException e) { throw new DefinitionException("machine " + i, e); }
        }
    }

    private static void machine(Machine m) throws DefinitionException {
        int n = m.states().size();
        if (m.start() < 0 || m.start() >= n) throw new DefinitionException("start " + m.start() + " out of range");
        for (int i = 0; i < n; i++) {
            try { state(m.states().get(i), n); }
            catch (DefinitionException e) { throw new DefinitionException("state " + i, e); }
        }
        structure(m);
    }

    private static void state(State st, int n) throws DefinitionException {
        RecordRule r = st.record();
        if (r != null) {
            family(r.maxLen(), Family.LENGTH, "max_len");
            family(r.padTo(), Family.LENGTH, "pad_to");
            family(r.padMultiple(), Family.LENGTH, "pad_multiple");
            family(r.pad(), Family.LENGTH, "pad");
            family(r.delay(), Family.SECONDS, "delay");
        }
        for (int j = 0; j < st.enter().size(); j++) {
            try { action(st.enter().get(j)); }
            catch (DefinitionException e) { throw new DefinitionException("action " + j, e); }
        }
        for (int j = 0; j < st.next().size(); j++) {
            try { transition(st.next().get(j), n); }
            catch (DefinitionException e) { throw new DefinitionException("transition " + j, e); }
        }
    }

    private static void need(Action a, Value... vs) throws DefinitionException {
        for (Value v : vs) if (v == null) throw new DefinitionException(a.op().id + ": missing value");
    }

    /** Checks the operands of a; one its op does not require but present is checked by its family all the same. */
    private static void action(Action a) throws DefinitionException {
        if (a.slot() != 0 && a.slot() != 1) throw new DefinitionException("slot " + a.slot());
        switch (a.op()) {
            case INJECT -> { if (!a.cancel()) need(a, a.after(), a.len()); }
            case TIMER -> { if (!a.cancel()) need(a, a.after()); }
            case PAUSE -> need(a, a.after());
            case RATE -> need(a, a.rate());
            case SET, ADD, SUB, WEIGHT -> need(a, a.value());
            case STOP, RESTART -> { }
        }
        family(a.after(), Family.SECONDS, "after");
        family(a.len(), Family.LENGTH, "len");
        family(a.rate(), Family.RATE, "rate");
        family(a.burst(), Family.LENGTH, "burst");
        family(a.value(), a.op() == Op.WEIGHT ? Family.ANY : Family.COUNTER, "value");
    }

    private static void transition(Transition t, int n) throws DefinitionException {
        if (t.to() < 0 || t.to() >= n) throw new DefinitionException("target " + t.to() + " out of range");
        family(t.prob(), Family.PROB, "prob");
        if (t.cond() != null) {
            if (t.cond().value() == null) throw new DefinitionException("if: missing value");
            family(t.cond().value(), Family.ANY, "if");
        }
    }

    private static final double MAX_LENGTH = Integer.MAX_VALUE, MAX_SECONDS = 1e9;

    /**
     * The families of value positions: the range of the numbers written in a value (constant, range
     * ends, min, max, points, uniform and loguniform parameters, mixture components included), and the
     * least value it may take by {@link #lower} (NaN: any).
     */
    enum Family {
        LENGTH(0, MAX_LENGTH, 0), SECONDS(0, MAX_SECONDS, 0), RATE(0, MAX_COUNT, 0), COUNTER(0, MAX_COUNT, 0),
        PROB(0, 1, 0), ANY(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN),
        POOL_COUNT(1, MAX_COUNT, 1), POOL_SECONDS(0, MAX_SECONDS, 0), POOL_PROBE(0, Double.POSITIVE_INFINITY, 0),
        RULE_SECONDS(0, MAX_SECONDS, 0), RULE_COUNT(0, MAX_COUNT, 0);

        final double min, max, floor;
        Family(double min, double max, double floor) { this.min = min; this.max = max; this.floor = floor; }
    }

    /** Checks v (null: absent) as a value of family f at the position what. */
    static void family(Value v, Family f, String what) throws DefinitionException {
        if (v == null) return;
        try { value(v); }
        catch (DefinitionException e) { throw new DefinitionException(what, e); }
        DefinitionException[] bad = {null};
        walk(v, u -> { if (bad[0] == null) bad[0] = outOfFamily(u, f, what); });
        if (bad[0] != null) throw bad[0];
        double lo = lower(v);
        if (!Double.isNaN(f.floor) && !(lo >= f.floor)) {
            throw new DefinitionException(what + ": may draw " + (f.floor == 0 ? "negative" : "below " + fmt(f.floor))
                    + " (lower bound " + lo + "); needs a min of at least " + fmt(f.floor));
        }
        if (f == Family.PROB && !(upper(v) <= 1)) throw new DefinitionException(what + ": prob may draw above 1 (upper bound " + upper(v) + ")");
        if (f == Family.RATE && !rateOk(v)) throw new DefinitionException(what + ": rate may draw in (0, 1)");
    }

    private static String fmt(double x) { return x == Math.rint(x) && Math.abs(x) < 0x1p63 ? Long.toString((long) x) : Double.toString(x); }

    /** Whether no draw of v falls in (0, 1). */
    private static boolean rateOk(Value v) {
        if (v.constant() != null) {
            double x = lower(v);
            return x == 0 || x >= 1;
        }
        if (v.dist() != null && v.dist().kind() == DistKind.MIXTURE && v.min() == null && v.max() == null) {
            for (Value c : v.dist().of()) if (!rateOk(c)) return false;
            return true;
        }
        return lower(v) >= 1;
    }

    static void value(Value v) throws DefinitionException {
        if (v == null) return;
        int k = 0;
        if (v.constant() != null) {
            k++;
            if (!finite(v.constant())) throw new DefinitionException("value: not finite");
        }
        if (v.range() != null) {
            k++;
            double[] r = v.range();
            if (r.length != 2 || !finite(r[0]) || !finite(r[1]) || r[0] > r[1]) {
                throw new DefinitionException("value: range needs two finite numbers a <= b");
            }
        }
        if (v.dist() != null) {
            k++;
            dist(v.dist());
        }
        if (k != 1) throw new DefinitionException("value: exactly one of const, range, dist");
        if (v.min() != null && !finite(v.min()) || v.max() != null && !finite(v.max())) {
            throw new DefinitionException("value: min or max not finite");
        }
        if (v.min() != null && v.max() != null && v.min() > v.max()) throw new DefinitionException("value: min > max");
    }

    private static void dist(Dist d) throws DefinitionException {
        String kind = d.kind().id;
        double[] p = d.params();
        if (p.length != d.kind().params) {
            throw new DefinitionException("dist " + kind + ": " + p.length + " params, want " + d.kind().params);
        }
        for (double x : p) if (!finite(x)) throw new DefinitionException("dist " + kind + ": param not finite");
        boolean bad = switch (d.kind()) {
            case UNIFORM -> p[0] > p[1];
            case LOGUNIFORM -> p[0] <= 0 || p[0] > p[1];
            case EXPONENTIAL -> p[0] <= 0;
            case POISSON -> p[0] <= 0 || p[0] > MAX_COUNT;
            case NORMAL, LOGNORMAL -> p[1] < 0;
            case WEIBULL, PARETO, GAMMA, BETA -> p[0] <= 0 || p[1] <= 0;
            case GEOMETRIC -> p[0] <= 0 || p[0] > 1;
            case BINOMIAL -> p[0] < 0 || p[0] > MAX_COUNT || p[0] != Math.rint(p[0]) || p[1] < 0 || p[1] > 1;
            case EMPIRICAL -> d.points().length == 0 || d.weights().length != d.points().length || !weights(d.weights());
            case BAG -> d.points().length == 0;
            case MIXTURE -> {
                boolean b = d.of().isEmpty() || d.weights().length != d.of().size() || !weights(d.weights());
                for (Value v : d.of()) value(v);
                yield b;
            }
        };
        if (bad) throw new DefinitionException("dist " + kind + ": invalid parameters");
        for (double x : d.points()) if (!finite(x)) throw new DefinitionException("dist " + kind + ": point not finite");
    }

    private static boolean weights(double[] w) {
        double s = 0;
        for (double x : w) {
            if (!finite(x) || x < 0) return false;
            s += x;
        }
        return s > 0;
    }

    static boolean finite(double x) { return Double.isFinite(x); }

    /** The error of the first number written in u (its nested values aside) outside family f; null: none. */
    private static DefinitionException outOfFamily(Value u, Family f, String what) {
        for (double x : written(u)) {
            if (x < f.min) return new DefinitionException(what + ": " + x + (f.min == 0 ? " is negative" : " is below " + fmt(f.min)));
            if (x > f.max) return new DefinitionException(what + ": " + x + " above max " + fmt(f.max));
            if (f == Family.RATE && x > 0 && x < 1) return new DefinitionException(what + ": rate " + x + " in (0, 1)");
        }
        return null;
    }

    /** The numbers written in u itself: constant, range, min, max, distribution points and uniform bounds. */
    private static List<Double> written(Value u) {
        List<Double> xs = new ArrayList<>();
        if (u.constant() != null) xs.add(u.constant());
        if (u.range() != null) for (double x : u.range()) xs.add(x);
        if (u.min() != null) xs.add(u.min());
        if (u.max() != null) xs.add(u.max());
        if (u.dist() != null) {
            for (double x : u.dist().points()) xs.add(x);
            if (u.dist().kind() == DistKind.UNIFORM || u.dist().kind() == DistKind.LOGUNIFORM) for (double x : u.dist().params()) xs.add(x);
        }
        return xs;
    }

    /** Checks d against the size bounds of l; an unset bound is {@link Integer#MAX_VALUE}, which no size exceeds. */
    static void size(Definition d, SizeLimits l) throws DefinitionException {
        int maxMachines = l.maxMachines().orElse(Integer.MAX_VALUE), maxStates = l.maxStates().orElse(Integer.MAX_VALUE);
        int maxActions = l.maxActions().orElse(Integer.MAX_VALUE), maxTransitions = l.maxTransitions().orElse(Integer.MAX_VALUE);
        int maxPoints = l.maxPoints().orElse(Integer.MAX_VALUE);
        if (d.machines().size() > maxMachines) {
            throw new DefinitionException("machine: " + d.machines().size() + " machines, limit " + maxMachines);
        }
        for (int i = 0; i < d.machines().size(); i++) {
            Machine m = d.machines().get(i);
            if (m.states().size() > maxStates) {
                throw new DefinitionException("machine " + i + ": " + m.states().size() + " states, limit " + maxStates);
            }
            for (int j = 0; j < m.states().size(); j++) {
                State st = m.states().get(j);
                if (st.enter().size() > maxActions || st.next().size() > maxTransitions) {
                    throw new DefinitionException("machine " + i + ": state " + j + ": actions or transitions over limit");
                }
                boolean[] bad = {false};
                values(st, v -> {
                    Dist ds = v.dist();
                    if (ds != null && (ds.points().length > maxPoints || ds.of().size() > maxPoints)) bad[0] = true;
                });
                if (bad[0]) throw new DefinitionException("machine " + i + ": state " + j + ": distribution over limit");
            }
        }
    }

    /** Calls f for every value of st, nested ones included. */
    static void values(State st, Consumer<Value> f) {
        RecordRule r = st.record();
        if (r != null) for (Value v : new Value[] {r.maxLen(), r.padTo(), r.padMultiple(), r.pad(), r.delay()}) walk(v, f);
        for (Action a : st.enter()) for (Value v : new Value[] {a.after(), a.len(), a.rate(), a.burst(), a.value()}) walk(v, f);
        for (Transition t : st.next()) {
            walk(t.prob(), f);
            if (t.cond() != null) walk(t.cond().value(), f);
        }
    }

    static void walk(Value v, Consumer<Value> f) {
        if (v == null) return;
        f.accept(v);
        if (v.dist() != null) for (Value o : v.dist().of()) walk(o, f);
    }

    /**
     * Checks reachability, that the start state does not restart on entry (which would enter
     * it again without end), and that no injection with a zero lower bound on its delay lies on
     * a cycle of unconditional enter, injected or counter transitions.
     */
    private static void structure(Machine m) throws DefinitionException {
        int n = m.states().size();
        List<List<Integer>> all = new ArrayList<>(), fast = new ArrayList<>();
        for (int i = 0; i < n; i++) { all.add(new ArrayList<>()); fast.add(new ArrayList<>()); }
        for (int i = 0; i < n; i++) {
            State st = m.states().get(i);
            for (Transition t : st.next()) {
                all.get(i).add(t.to());
                switch (t.on()) {
                    case ENTER, INJECTED, ZERO0, ZERO1 -> { if (t.cond() == null) fast.get(i).add(t.to()); }
                    default -> { }
                }
            }
            for (Action a : st.enter()) {
                if (a.op() == Op.RESTART) {
                    all.get(i).add(m.start());
                    fast.get(i).add(m.start());
                }
            }
        }
        for (Action a : m.states().get(m.start()).enter()) {
            if (a.op() == Op.RESTART) throw new DefinitionException("state " + m.start() + ": restart on entering the start state");
        }
        boolean[] seen = reach(all, m.start());
        for (int i = 0; i < n; i++) if (!seen[i]) throw new DefinitionException("state " + i + " unreachable");
        for (int i = 0; i < n; i++) {
            for (Action a : m.states().get(i).enter()) {
                if (a.op() != Op.INJECT || a.cancel() || lower(a.after()) > 0) continue;
                for (int j : fast.get(i)) {
                    if (reach(fast, j)[i]) {
                        throw new DefinitionException("state " + i
                                + ": injection with zero delay on a cycle of enter, injected or counter transitions");
                    }
                }
            }
        }
    }

    private static boolean[] reach(List<List<Integer>> g, int from) {
        boolean[] seen = new boolean[g.size()];
        ArrayDeque<Integer> stack = new ArrayDeque<>();
        stack.push(from);
        seen[from] = true;
        while (!stack.isEmpty()) {
            for (int j : g.get(stack.pop())) {
                if (!seen[j]) { seen[j] = true; stack.push(j); }
            }
        }
        return seen;
    }

    /** An upper bound of the values v takes. */
    static double upper(Value v) {
        double x;
        if (v.constant() != null) x = v.constant();
        else if (v.range() != null) x = v.range()[1];
        else {
            Dist d = v.dist();
            double[] p = d.params();
            x = switch (d.kind()) {
                case UNIFORM, LOGUNIFORM -> p[1];
                case BETA -> 1;
                case BINOMIAL -> p[0];
                case GEOMETRIC -> p[0] == 1 ? 1 : Double.POSITIVE_INFINITY;
                case EMPIRICAL, BAG -> {
                    double y = Double.NEGATIVE_INFINITY;
                    for (double q : d.points()) y = Math.max(y, q);
                    yield y;
                }
                case MIXTURE -> {
                    double y = Double.NEGATIVE_INFINITY;
                    for (Value c : d.of()) y = Math.max(y, upper(c));
                    yield y;
                }
                default -> Double.POSITIVE_INFINITY;
            };
        }
        if (v.min() != null) x = Math.max(x, v.min());
        if (v.max() != null) x = Math.min(x, v.max());
        return x;
    }

    /** A lower bound of the values v takes: the one bound of the load checks (points and components of weight 0 count). */
    static double lower(Value v) {
        double x;
        if (v.constant() != null) x = v.constant();
        else if (v.range() != null) x = v.range()[0];
        else x = lower(v.dist());
        if (v.min() != null) x = Math.max(x, v.min());
        if (v.max() != null) x = Math.min(x, v.max());
        return x;
    }

    private static double lower(Dist d) {
        switch (d.kind()) {
            case UNIFORM, LOGUNIFORM, PARETO: return d.params()[0];
            case NORMAL: return Double.NEGATIVE_INFINITY;
            case GEOMETRIC: return 1;
            case EMPIRICAL, BAG: {
                double x = Double.POSITIVE_INFINITY;
                for (double p : d.points()) x = Math.min(x, p); // points of weight 0 count
                return x;
            }
            case MIXTURE: {
                double x = Double.POSITIVE_INFINITY;
                for (Value c : d.of()) x = Math.min(x, lower(c)); // components of weight 0 count
                return x;
            }
            default: return 0;
        }
    }
}
