package io.zmux.shaping.machine;

import static io.zmux.shaping.machine.DefinitionException.Category.*;

import io.zmux.shaping.ShapingBudget;
import io.zmux.shaping.ShapingLimits;
import io.zmux.shaping.WireAccount;
import io.zmux.shaping.machine.Definition.*;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Reads the JSON forms strictly: unknown and duplicate members are errors, names and enumerated values are
 * case-sensitive, null is absent, and every number is an integer in the integer form.
 */
final class DefinitionJson {
    private DefinitionJson() {}

    static final long P53 = 1L << 53;

    /** The range of a kind of number. */
    enum Family {
        LENGTH(0, Integer.MAX_VALUE), TIME(0, 1_000_000_000_000_000L), PPM(0, 1_000_000), WEIGHT(0, 0xFFFFFFFFL),
        COUNTER(0, P53), RATE(0, 1L << 43), POOL_COUNT(1, P53);

        final long lo, hi;
        Family(long lo, long hi) { this.lo = lo; this.hi = hi; }
    }

    static final Set<Scope> ANY_SCOPE = Set.of(Scope.EACH, Scope.PIPE, Scope.SHARED), NO_PIPE = Set.of(Scope.EACH, Scope.SHARED);

    /** Parses JSON text: objects as maps, arrays as lists, integers as Long or BigInteger, other numbers as Double. */
    static Object parse(String json) throws DefinitionException {
        return new Parser(json).document();
    }

    static DefinitionException err(DefinitionException.Category c, String what) {
        return new DefinitionException(c, what);
    }

    /** o with its null members left out, after failing on a member not in names. */
    static Map<String, Object> members(Object x, Set<String> names, String what) throws DefinitionException {
        if (!(x instanceof Map<?, ?> m)) throw err(TYPE, what);
        Map<String, Object> out = new LinkedHashMap<>();
        for (var e : m.entrySet()) {
            String k = (String) e.getKey();
            if (!names.contains(k)) throw err(UNKNOWN_MEMBER, what + "." + k);
            if (e.getValue() != null) out.put(k, e.getValue());
        }
        return out;
    }

    static Object required(Map<String, Object> d, String k, String what) throws DefinitionException {
        Object v = d.get(k);
        if (v == null) throw err(TYPE, what + "." + k);
        return v;
    }

    static boolean isInteger(Object x) { return x instanceof Long || x instanceof BigInteger; }

    /** An integer as a long; beyond a long, the nearest long, which every range refuses. */
    static long integer(Object x, String what) throws DefinitionException {
        if (x instanceof Long l) return l;
        if (x instanceof BigInteger b) return b.signum() < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
        throw err(TYPE, what);
    }

    static List<?> list(Object x, String what) throws DefinitionException {
        if (x instanceof List<?> l) return l;
        throw err(TYPE, what);
    }

    private static <E extends Enum<E>> E named(Class<E> type, Object x, String what) throws DefinitionException {
        if (x instanceof String s && s.equals(s.toLowerCase(Locale.ROOT))) {
            try {
                return Enum.valueOf(type, s.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw err(TYPE, what + " " + s);
            }
        }
        throw err(TYPE, what);
    }

    /**
     * What reading one candidate uses: its table sources, each read once at its first reference and shared by the
     * references, and its counters and timers against the bound.
     */
    static final class Tables {
        private final Map<String, Object> raw;
        private final SizeLimits limits;
        private final Map<String, Segments> parsed = new java.util.HashMap<>();
        private long slots;

        Tables(Map<String, Object> raw, SizeLimits limits) {
            this.raw = raw;
            this.limits = limits;
        }

        private Segments named(String name, String what) throws DefinitionException {
            Segments s = parsed.get(name);
            if (s != null) return s;
            Object t = raw.get(name);
            if (t == null) throw err(TABLE, name);
            s = Segments.of(segments(t, what));
            parsed.put(name, s);
            return s;
        }

        /** Counts a machine's slots before anything is allocated for them. */
        void useSlots(long n) throws DefinitionException {
            slots += n;
            if (limits.slotsOver(slots)) throw err(LIMIT, slots + " counters and timers");
        }
    }

    /** A checked list of segments with its bounds over every segment, those of weight 0 included; read only. */
    private record Segments(long[] flat, long lo, long hi) {
        static Segments of(long[] flat) {
            long lo = Long.MAX_VALUE, hi = 0;
            for (int i = 0; i < flat.length; i += 3) {
                lo = Math.min(lo, flat[i + 1]);
                hi = Math.max(hi, flat[i + 2]);
            }
            return new Segments(flat, lo, hi);
        }
    }

    /** A table's segments, checked. */
    static long[] segments(Object t, String what) throws DefinitionException {
        if (!(t instanceof List<?> l) || l.isEmpty()) throw err(TYPE, what + ": segments");
        long[] out = new long[3 * l.size()];
        long total = 0;
        for (int i = 0; i < l.size(); i++) {
            if (!(l.get(i) instanceof List<?> s) || s.size() != 3 || !isInteger(s.get(0)) || !isInteger(s.get(1)) || !isInteger(s.get(2))) {
                throw err(TYPE, what + ": segment");
            }
            long w = integer(s.get(0), what), lo = integer(s.get(1), what), hi = integer(s.get(2), what);
            if (w < 0 || w > 0xFFFFFFFFL || lo < 0 || lo > hi || hi > P53) throw err(RANGE, what + ": segment " + s);
            out[3 * i] = w;
            out[3 * i + 1] = lo;
            out[3 * i + 2] = hi;
            total += w;
        }
        if (total == 0) throw err(RANGE, what + ": total weight 0");
        return out;
    }

    static Value value(Object x, Family f, Tables tables, Set<Scope> scopes, String what) throws DefinitionException {
        if (isInteger(x)) {
            long v = integer(x, what);
            if (v < f.lo || v > f.hi) throw err(RANGE, what + ": " + x);
            return new Value(new long[] {1, v, v}, Scope.EACH, v);
        }
        if (!(x instanceof Map<?, ?>)) throw err(TYPE, what);
        Map<String, Object> d = members(x, Set.of("t", "scope"), what);
        Object t = required(d, "t", what);
        Scope scope = Scope.EACH;
        if (d.containsKey("scope")) scope = named(Scope.class, d.get("scope"), what + ": scope");
        if (!scopes.contains(scope)) throw err(SCOPE, what + ": " + d.get("scope"));
        Segments segs = t instanceof String name ? tables.named(name, what) : Segments.of(segments(t, what));
        if (segs.lo < f.lo || segs.hi > f.hi) throw err(RANGE, what + ": [" + segs.lo + "," + segs.hi + "]");
        return new Value(segs.flat, scope, segs.lo);
    }

    private static Value optional(Map<String, Object> d, String k, Family f, Tables tables) throws DefinitionException {
        return d.containsKey(k) ? value(d.get(k), f, tables, ANY_SCOPE, k) : null;
    }

    /** The tables of a root object: an object, read lazily. */
    @SuppressWarnings("unchecked")
    static Map<String, Object> tableSources(Object x) throws DefinitionException {
        if (x == null) return Map.of();
        if (x instanceof Map<?, ?> m) return (Map<String, Object>) m;
        throw err(TYPE, "tables");
    }

    static Definition definition(Object root, SizeLimits limits) throws DefinitionException {
        Map<String, Object> d = members(root, Set.of("machines", "tables"), "definition");
        return machines(d.get("machines"), new Tables(tableSources(d.get("tables")), limits));
    }

    static Definition machines(Object raw, Tables tables) throws DefinitionException {
        List<?> l = list(raw, "machines");
        if (l.isEmpty()) throw err(TYPE, "machines");
        List<Machine> ms = new ArrayList<>();
        for (Object m : l) ms.add(machine(m, tables));
        return new Definition(ms);
    }

    private static int count(Object x, String what) throws DefinitionException {
        if (x == null) return 0;
        if (!isInteger(x)) throw err(TYPE, what);
        long v = integer(x, what);
        if (v < 0 || v > Integer.MAX_VALUE) throw err(TYPE, what);
        return (int) v;
    }

    private static Machine machine(Object x, Tables tables) throws DefinitionException {
        Map<String, Object> d = members(x, Set.of("start", "counters", "timers", "states"), "machine");
        Object startRaw = required(d, "start", "machine");
        List<?> raw = list(required(d, "states", "machine"), "states");
        int start = count(startRaw, "machine start"), nc = count(d.get("counters"), "counters"), nt = count(d.get("timers"), "timers");
        tables.useSlots((long) nc + nt);
        if (raw.isEmpty()) throw err(TYPE, "states");
        int n = raw.size();
        if (start >= n) throw err(REFERENCE, "start");
        List<State> states = new ArrayList<>(n);
        for (Object s : raw) states.add(state(s, tables, n, nc, nt));
        Machine m = new Machine(start, nc, nt, List.copyOf(states));
        structure(m);
        return m;
    }

    private static State state(Object x, Tables tables, int n, int nc, int nt) throws DefinitionException {
        Map<String, Object> d = members(x, Set.of("record", "enter", "next"), "state");
        RecordRule record = d.containsKey("record") ? record(d.get("record"), tables) : null;
        List<Action> enter = new ArrayList<>();
        if (d.containsKey("enter")) for (Object a : list(d.get("enter"), "enter")) enter.add(action(a, tables, nc, nt));
        List<Transition> next = new ArrayList<>();
        if (d.containsKey("next")) for (Object t : list(d.get("next"), "next")) next.add(transition(t, tables, n, nc, nt));
        return new State(record, List.copyOf(enter), List.copyOf(next));
    }

    private static RecordRule record(Object x, Tables tables) throws DefinitionException {
        Map<String, Object> r = members(x, Set.of("target", "max_len", "pad_to", "pad_multiple", "pad", "delay", "flush"), "record");
        if (r.containsKey("target") && (r.containsKey("max_len") || r.containsKey("pad_to"))) throw err(CONFLICT, "target with max_len or pad_to");
        boolean flush = false;
        if (r.containsKey("flush")) {
            if (!(r.get("flush") instanceof Boolean b)) throw err(TYPE, "flush");
            flush = b;
        }
        return new RecordRule(optional(r, "target", Family.LENGTH, tables), optional(r, "max_len", Family.LENGTH, tables),
                optional(r, "pad_to", Family.LENGTH, tables), optional(r, "pad_multiple", Family.LENGTH, tables),
                optional(r, "pad", Family.LENGTH, tables), optional(r, "delay", Family.TIME, tables), flush);
    }

    private static int slot(Map<String, Object> d, int limit, String what) throws DefinitionException {
        Object s = d.get("slot");
        if (!isInteger(s)) throw err(TYPE, what + ".slot");
        long v = integer(s, what);
        if (v < 0 || v >= limit) throw err(REFERENCE, what + ".slot");
        return (int) v;
    }

    private static boolean cancel(Map<String, Object> d) throws DefinitionException {
        if (!d.containsKey("cancel")) return false;
        if (!Boolean.TRUE.equals(d.get("cancel"))) throw err(TYPE, "cancel");
        return true;
    }

    private static Value req(Map<String, Object> d, String k, Family f, Tables tables, String what) throws DefinitionException {
        return value(required(d, k, what), f, tables, ANY_SCOPE, k);
    }

    private static Action action(Object x, Tables tables, int nc, int nt) throws DefinitionException {
        if (!(x instanceof Map<?, ?> raw)) throw err(TYPE, "action");
        Op op = named(Op.class, raw.get("op"), "op");
        String id = op.name().toLowerCase(Locale.ROOT);
        switch (op) {
            case INJECT -> {
                Map<String, Object> d = members(x, Set.of("op", "cancel", "after", "len"), id);
                if (cancel(d)) {
                    if (d.containsKey("after") || d.containsKey("len")) throw err(UNKNOWN_MEMBER, "inject cancel");
                    return new Action(op, true, -1, null, null, null, null, null);
                }
                return new Action(op, false, -1, req(d, "after", Family.TIME, tables, id), req(d, "len", Family.LENGTH, tables, id), null, null, null);
            }
            case PAUSE -> {
                Map<String, Object> d = members(x, Set.of("op", "after"), id);
                return new Action(op, false, -1, req(d, "after", Family.TIME, tables, id), null, null, null, null);
            }
            case RATE -> {
                Map<String, Object> d = members(x, Set.of("op", "rate", "burst"), id);
                return new Action(op, false, -1, null, null, req(d, "rate", Family.RATE, tables, id), optional(d, "burst", Family.LENGTH, tables), null);
            }
            case TIMER -> {
                Map<String, Object> d = members(x, Set.of("op", "slot", "cancel", "after"), id);
                int s = slot(d, nt, id);
                if (cancel(d)) {
                    if (d.containsKey("after")) throw err(UNKNOWN_MEMBER, "timer cancel");
                    return new Action(op, true, s, null, null, null, null, null);
                }
                return new Action(op, false, s, req(d, "after", Family.TIME, tables, id), null, null, null, null);
            }
            case SET, ADD, SUB -> {
                Map<String, Object> d = members(x, Set.of("op", "slot", "value"), id);
                return new Action(op, false, slot(d, nc, id), null, null, null, null, req(d, "value", Family.COUNTER, tables, id));
            }
            case WEIGHT -> {
                Map<String, Object> d = members(x, Set.of("op", "value"), id);
                return new Action(op, false, -1, null, null, null, null, req(d, "value", Family.WEIGHT, tables, id));
            }
            default -> {
                members(x, Set.of("op"), id);
                return new Action(op, false, -1, null, null, null, null, null);
            }
        }
    }

    private static Transition transition(Object x, Tables tables, int n, int nc, int nt) throws DefinitionException {
        Map<String, Object> d = members(x, Set.of("on", "slot", "to", "if", "prob"), "transition");
        Object onRaw = required(d, "on", "transition"), toRaw = required(d, "to", "transition");
        if (!isInteger(toRaw)) throw err(TYPE, "to");
        long to = integer(toRaw, "to");
        if (to < 0 || to >= n) throw err(REFERENCE, "to");
        Trigger on = named(Trigger.class, onRaw, "on");
        int slot = -1;
        if (on == Trigger.TIMER || on == Trigger.ZERO) slot = slot(d, on == Trigger.TIMER ? nt : nc, onRaw.toString());
        else if (d.containsKey("slot")) throw err(UNKNOWN_MEMBER, "slot");
        Cond cond = null;
        if (d.containsKey("if")) {
            Map<String, Object> c = members(d.get("if"), Set.of("what", "slot", "cmp", "value"), "if");
            Quantity what = named(Quantity.class, required(c, "what", "if"), "if.what");
            int s = -1;
            if (what == Quantity.COUNTER) s = slot(c, nc, "if");
            else if (c.containsKey("slot")) throw err(UNKNOWN_MEMBER, "if.slot");
            Comparison cmp = named(Comparison.class, required(c, "cmp", "if"), "if.cmp");
            cond = new Cond(what, s, cmp, req(c, "value", Family.COUNTER, tables, "if"));
        }
        return new Transition(on, slot, (int) to, cond, optional(d, "prob", Family.PPM, tables));
    }

    /** Every state reachable; no cycle of steps within one event; none through the core without a wait. Linear in the machine. */
    private static void structure(Machine m) throws DefinitionException {
        List<State> states = m.states();
        int n = states.size();
        BitSet seen = new BitSet(n);
        int[][] all = graph(m, Set.of(Trigger.values()));
        int[] stack = new int[n];
        int top = 0;
        stack[top++] = m.start();
        seen.set(m.start());
        while (top > 0) {
            for (int to : all[stack[--top]]) if (!seen.get(to)) { seen.set(to); stack[top++] = to; }
        }
        if (seen.cardinality() != n) throw err(UNREACHABLE, "state " + seen.nextClearBit(0));
        if (!onCycle(graph(m, Set.of(Trigger.ENTER, Trigger.ZERO))).isEmpty()) throw err(CYCLE, "enter, zero or restart");
        BitSet busy = onCycle(graph(m, Set.of(Trigger.ENTER, Trigger.ZERO, Trigger.INJECTED, Trigger.INJECT_DROPPED, Trigger.TIMER)));
        for (int i = busy.nextSetBit(0); i >= 0; i = busy.nextSetBit(i + 1)) {
            for (Action a : states.get(i).enter()) {
                if ((a.op() == Op.INJECT || a.op() == Op.TIMER) && !a.cancel() && a.after().lower() == 0) throw err(ZERO_DELAY_CYCLE, "state " + i);
            }
        }
    }

    /** The edges of these triggers and of restarts, by state. */
    private static int[][] graph(Machine m, Set<Trigger> ons) {
        List<State> states = m.states();
        int[][] adj = new int[states.size()][];
        for (int i = 0; i < adj.length; i++) {
            State st = states.get(i);
            int[] to = new int[st.next().size() + 1];
            int n = 0;
            for (Transition t : st.next()) if (ons.contains(t.on())) to[n++] = t.to();
            for (Action a : st.enter()) {
                if (a.op() == Op.RESTART) { to[n++] = m.start(); break; }
            }
            adj[i] = java.util.Arrays.copyOf(to, n);
        }
        return adj;
    }

    /** The states on a cycle: strongly connected components without recursion, in time linear in the graph. */
    private static BitSet onCycle(int[][] adj) {
        int n = adj.length, clock = 0, calls = 0, open = 0;
        BitSet out = new BitSet(n), inOpen = new BitSet(n);
        int[] index = new int[n], low = new int[n], next = new int[n], call = new int[n], comp = new int[n];
        for (int s = 0; s < n; s++) {
            if (index[s] != 0) continue;
            index[s] = low[s] = ++clock;
            inOpen.set(s);
            comp[open++] = s;
            call[calls++] = s;
            while (calls > 0) {
                int v = call[calls - 1];
                if (next[v] < adj[v].length) {
                    int w = adj[v][next[v]++];
                    if (w == v) out.set(v);
                    else if (index[w] == 0) {
                        index[w] = low[w] = ++clock;
                        inOpen.set(w);
                        comp[open++] = w;
                        call[calls++] = w;
                    } else if (inOpen.get(w)) low[v] = Math.min(low[v], index[w]);
                    continue;
                }
                calls--;
                if (calls > 0) low[call[calls - 1]] = Math.min(low[call[calls - 1]], low[v]);
                if (low[v] != index[v]) continue;
                int first = open - 1;
                while (comp[first] != v) first--;
                for (int i = first; i < open; i++) {
                    inOpen.clear(comp[i]);
                    if (open - first > 1) out.set(comp[i]);
                }
                open = first;
            }
        }
        return out;
    }

    private static final Set<Scope> SESSION_SCOPE = Set.of(Scope.SESSION);
    private static final Set<String> WIRE = Set.of("initial", "per_mb", "per_sec");

    /** A {@code budget} object: an account drawn per session, and a bound on one wait. */
    static Budget budget(Object x, Tables tables) throws DefinitionException {
        Map<String, Object> d = members(x, Set.of("wire", "max_delay"), "budget");
        Value[] wire = null;
        if (d.containsKey("wire")) {
            Map<String, Object> w = members(d.get("wire"), WIRE, "wire");
            wire = new Value[] {wireValue(w, "initial", Family.COUNTER, tables), wireValue(w, "per_mb", Family.RATE, tables), wireValue(w, "per_sec", Family.RATE, tables)};
        }
        long maxDelay = 0;
        if (d.containsKey("max_delay")) {
            maxDelay = integer(d.get("max_delay"), "max_delay");
            if (maxDelay < 1 || maxDelay > Family.TIME.hi) throw err(RANGE, "max_delay");
        }
        return new Budget(wire, maxDelay);
    }

    private static Value wireValue(Map<String, Object> w, String k, Family f, Tables tables) throws DefinitionException {
        return w.containsKey(k) ? value(w.get(k), f, tables, SESSION_SCOPE, k) : null;
    }

    /** A parsed budget. wire is null when absent; maxDelay in microseconds, 0: none. */
    record Budget(Value[] wire, long maxDelay) {
        /** One session's budget: initial, per_mb, per_sec in order. A wire of zeros allows no injection. The caller holds s. */
        ShapingBudget draw(Sampler s) {
            WireAccount a = WireAccount.NONE;
            if (wire != null) a = new WireAccount(draw(s, wire[0]), draw(s, wire[1]), draw(s, wire[2]));
            return new ShapingBudget(a, maxDelay > 0 ? Duration.ofNanos(maxDelay * 1000) : null, !(wire != null && !a.isSet()));
        }

        private static long draw(Sampler s, Value v) { return v == null ? 0 : s.draw(v.segments); }
    }

    /** Local limits: {@code wire} of constants, {@code max_delay} (0 the default, negative no bound), and the switches. */
    static ShapingLimits limits(Object x) throws DefinitionException {
        Map<String, Object> d = members(x, Set.of("wire", "max_delay", "no_padding", "no_pause", "no_injection"), "limits");
        WireAccount a = WireAccount.NONE;
        boolean noInjection = flag(d, "no_injection");
        if (d.containsKey("wire")) {
            Map<String, Object> w = members(d.get("wire"), WIRE, "wire");
            a = new WireAccount(constant(w, "initial", Family.COUNTER), constant(w, "per_mb", Family.RATE), constant(w, "per_sec", Family.RATE));
            noInjection |= !a.isSet();
        }
        Duration maxDelay = ShapingLimits.DEFAULT_MAX_DELAY;
        if (d.containsKey("max_delay")) {
            long n = integer(d.get("max_delay"), "max_delay");
            if (n > Family.TIME.hi) throw err(RANGE, "max_delay");
            if (n < 0) maxDelay = null;
            else if (n > 0) maxDelay = Duration.ofNanos(n * 1000);
        }
        return new ShapingLimits(a, maxDelay, !flag(d, "no_padding"), !flag(d, "no_pause"), !noInjection);
    }

    private static long constant(Map<String, Object> w, String k, Family f) throws DefinitionException {
        if (!w.containsKey(k)) return 0;
        long n = integer(w.get(k), k);
        if (n < f.lo || n > f.hi) throw err(RANGE, k);
        return n;
    }

    private static boolean flag(Map<String, Object> d, String k) throws DefinitionException {
        if (!d.containsKey(k)) return false;
        if (d.get(k) instanceof Boolean b) return b;
        throw err(TYPE, k);
    }

    /** RFC 8259 JSON, with a duplicate member an error wherever it is. Nesting beyond any form read here is a syntax error. */
    private static final class Parser {
        private static final int MAX_DEPTH = 64;
        /** The digits of the largest integer here, 2^64 - 1. */
        private static final int MAX_DIGITS = 20;
        private static final BigInteger BEYOND = BigInteger.ONE.shiftLeft(64);
        private final String s;
        private int i, depth;

        Parser(String s) { this.s = s; }

        Object document() throws DefinitionException {
            Object v = value();
            ws();
            if (i != s.length()) throw error("trailing data");
            return v;
        }

        private DefinitionException error(String what) {
            return err(SYNTAX, what + " at offset " + i);
        }

        private void ws() {
            while (i < s.length()) {
                char c = s.charAt(i);
                if (c != ' ' && c != '\t' && c != '\n' && c != '\r') return;
                i++;
            }
        }

        private Object value() throws DefinitionException {
            ws();
            if (i >= s.length()) throw error("unexpected end");
            return switch (s.charAt(i)) {
                case '{' -> { enter(); Object o = obj(); depth--; yield o; }
                case '[' -> { enter(); Object a = arr(); depth--; yield a; }
                case '"' -> str();
                case 't' -> lit("true", Boolean.TRUE);
                case 'f' -> lit("false", Boolean.FALSE);
                case 'n' -> lit("null", null);
                default -> num();
            };
        }

        private void enter() throws DefinitionException {
            if (++depth > MAX_DEPTH) throw error("nesting too deep");
        }

        private Object lit(String word, Object v) throws DefinitionException {
            if (!s.startsWith(word, i)) throw error("invalid literal");
            i += word.length();
            return v;
        }

        private Map<String, Object> obj() throws DefinitionException {
            Map<String, Object> m = new LinkedHashMap<>();
            i++;
            ws();
            if (i < s.length() && s.charAt(i) == '}') { i++; return m; }
            while (true) {
                ws();
                if (i >= s.length() || s.charAt(i) != '"') throw error("want a member name");
                String k = str();
                ws();
                if (i >= s.length() || s.charAt(i) != ':') throw error("want ':'");
                i++;
                if (m.containsKey(k)) throw err(DUPLICATE_KEY, k);
                m.put(k, value());
                ws();
                if (i >= s.length()) throw error("unexpected end");
                char c = s.charAt(i++);
                if (c == '}') return m;
                if (c != ',') throw error("want ',' or '}'");
            }
        }

        private List<Object> arr() throws DefinitionException {
            List<Object> l = new ArrayList<>();
            i++;
            ws();
            if (i < s.length() && s.charAt(i) == ']') { i++; return l; }
            while (true) {
                l.add(value());
                ws();
                if (i >= s.length()) throw error("unexpected end");
                char c = s.charAt(i++);
                if (c == ']') return l;
                if (c != ',') throw error("want ',' or ']'");
            }
        }

        private String str() throws DefinitionException {
            StringBuilder b = new StringBuilder();
            i++;
            while (true) {
                if (i >= s.length()) throw error("unterminated string");
                char c = s.charAt(i++);
                if (c == '"') return b.toString();
                if (c < 0x20) throw error("control character in string");
                if (c != '\\') { b.append(c); continue; }
                if (i >= s.length()) throw error("unterminated string");
                char e = s.charAt(i++);
                switch (e) {
                    case '"', '\\', '/' -> b.append(e);
                    case 'b' -> b.append('\b');
                    case 'f' -> b.append('\f');
                    case 'n' -> b.append('\n');
                    case 'r' -> b.append('\r');
                    case 't' -> b.append('\t');
                    case 'u' -> {
                        if (i + 4 > s.length()) throw error("short escape");
                        int unit = 0;
                        for (int end = i + 4; i < end; i++) {
                            int h = hex(s.charAt(i));
                            if (h < 0) throw error("invalid escape");
                            unit = unit << 4 | h;
                        }
                        b.append((char) unit);
                    }
                    default -> throw error("invalid escape");
                }
            }
        }

        private static boolean digit(char c) { return c >= '0' && c <= '9'; }

        private static int hex(char c) {
            if (digit(c)) return c - '0';
            if (c >= 'a' && c <= 'f') return c - 'a' + 10;
            if (c >= 'A' && c <= 'F') return c - 'A' + 10;
            return -1;
        }

        private Number num() throws DefinitionException {
            int start = i;
            if (i < s.length() && s.charAt(i) == '-') i++;
            if (i >= s.length() || !digit(s.charAt(i))) throw error("invalid value");
            if (s.charAt(i) == '0') i++;
            else do i++; while (i < s.length() && digit(s.charAt(i)));
            boolean integral = true;
            if (i < s.length() && s.charAt(i) == '.') {
                integral = false;
                i++;
                if (i >= s.length() || !digit(s.charAt(i))) throw error("invalid number");
                while (i < s.length() && digit(s.charAt(i))) i++;
            }
            if (i < s.length() && (s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
                integral = false;
                i++;
                if (i < s.length() && (s.charAt(i) == '+' || s.charAt(i) == '-')) i++;
                if (i >= s.length() || !digit(s.charAt(i))) throw error("invalid number");
                while (i < s.length() && digit(s.charAt(i))) i++;
            }
            String t = s.substring(start, i);
            if (!integral) return Double.parseDouble(t);
            // More digits than any value here has: out of range whatever they say, and converting them costs n^2.
            if (t.length() - (t.charAt(0) == '-' ? 1 : 0) > MAX_DIGITS) return t.charAt(0) == '-' ? BEYOND.negate() : BEYOND;
            BigInteger b = new BigInteger(t);
            return b.bitLength() < 64 ? (Number) b.longValue() : b;
        }
    }
}
