package io.zmux.shaping.machine;

import io.zmux.shaping.ShapingLimits;
import io.zmux.shaping.machine.Definition.*;
import java.time.Duration;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads the JSON form of a {@link Definition}, strictly: unknown and duplicate members are errors,
 * names and enum values are case-sensitive, null is absent, integer members take only the JSON
 * integer form, and numbers outside binary64 are errors.
 */
final class DefinitionJson {
    private DefinitionJson() {}

    /**
     * Parses JSON text: objects as maps, arrays as lists, numbers in the integer form as Long (BigInteger
     * beyond a long), others as Double.
     */
    static Object parse(String json) throws DefinitionException {
        return new Parser(json).document();
    }

    private static final Set<String> ROOT = Set.of("version", "machines"), MACHINE = Set.of("start", "states"),
            STATE = Set.of("record", "enter", "next"), RECORD = Set.of("max_len", "pad_to", "pad_multiple", "pad", "delay", "flush"),
            TRANSITION = Set.of("on", "to", "prob", "if"), COND = Set.of("what", "cmp", "value"),
            VALUE = Set.of("const", "range", "dist", "scope", "min", "max");

    /** The members an action of each op may have, op included. */
    private static Set<String> actionFields(Op op) {
        return switch (op) {
            case INJECT -> Set.of("op", "cancel", "after", "len");
            case PAUSE -> Set.of("op", "after");
            case RATE -> Set.of("op", "rate", "burst");
            case TIMER -> Set.of("op", "cancel", "slot", "after");
            case SET, ADD, SUB -> Set.of("op", "slot", "value");
            case WEIGHT -> Set.of("op", "value");
            case STOP, RESTART -> Set.of("op");
        };
    }

    /** The members a distribution of each kind may have, kind included. */
    private static Set<String> distFields(DistKind k) {
        return switch (k) {
            case EMPIRICAL -> Set.of("kind", "points", "weights");
            case BAG -> Set.of("kind", "points");
            case MIXTURE -> Set.of("kind", "of", "weights");
            default -> Set.of("kind", "params");
        };
    }

    /** Fails on a non-null member of o not in names. */
    static void known(Map<String, Object> o, Set<String> names, String what) throws DefinitionException {
        for (var e : o.entrySet()) {
            if (e.getValue() != null && !names.contains(e.getKey())) {
                throw new DefinitionException(what + ": unknown field \"" + e.getKey() + "\"");
            }
        }
    }

    static Definition read(String json) throws DefinitionException {
        Map<String, Object> o = object(parse(json), "definition");
        known(o, ROOT, "definition");
        if (o.get("version") == null) throw new DefinitionException("definition: missing version");
        if (o.get("machines") == null) throw new DefinitionException("definition: no machines");
        List<Machine> machines = new ArrayList<>();
        List<Object> ms = list(o.get("machines"), "machines");
        for (int i = 0; i < ms.size(); i++) {
            try { machines.add(machine(object(ms.get(i), "machine"))); }
            catch (DefinitionException e) { throw new DefinitionException("machine " + i, e); }
        }
        return new Definition(integer(o.get("version"), "version"), machines);
    }

    private static Machine machine(Map<String, Object> o) throws DefinitionException {
        known(o, MACHINE, "machine");
        if (o.get("states") == null) throw new DefinitionException("machine: missing states");
        List<State> states = new ArrayList<>();
        List<Object> ss = list(o.get("states"), "states");
        for (int i = 0; i < ss.size(); i++) {
            try { states.add(state(object(ss.get(i), "state"))); }
            catch (DefinitionException e) { throw new DefinitionException("state " + i, e); }
        }
        return new Machine(integer(o.get("start"), "start"), states);
    }

    private static State state(Map<String, Object> o) throws DefinitionException {
        known(o, STATE, "state");
        RecordRule record = null;
        if (o.get("record") != null) {
            Map<String, Object> r = object(o.get("record"), "record");
            known(r, RECORD, "record");
            record = new RecordRule(value(r.get("max_len")), value(r.get("pad_to")), value(r.get("pad_multiple")),
                    value(r.get("pad")), value(r.get("delay")), bool(r.get("flush"), "flush"));
        }
        List<Action> enter = new ArrayList<>();
        List<Object> es = list(o.get("enter"), "enter");
        for (int i = 0; i < es.size(); i++) {
            try { enter.add(action(object(es.get(i), "action"))); }
            catch (DefinitionException e) { throw new DefinitionException("action " + i, e); }
        }
        List<Transition> next = new ArrayList<>();
        List<Object> ns = list(o.get("next"), "next");
        for (int i = 0; i < ns.size(); i++) {
            try { next.add(transition(object(ns.get(i), "transition"))); }
            catch (DefinitionException e) { throw new DefinitionException("transition " + i, e); }
        }
        return new State(record, enter, next);
    }

    private static Action action(Map<String, Object> o) throws DefinitionException {
        String id = string(o.get("op"), "op");
        Op op = null;
        for (Op x : Op.values()) if (x.id.equals(id)) op = x;
        if (op == null) throw new DefinitionException("unknown op \"" + id + "\"");
        known(o, actionFields(op), op.id);
        return new Action(op, bool(o.get("cancel"), "cancel"), integer(o.get("slot"), "slot"), value(o.get("after")),
                value(o.get("len")), value(o.get("rate")), value(o.get("burst")), value(o.get("value")));
    }

    private static Transition transition(Map<String, Object> o) throws DefinitionException {
        known(o, TRANSITION, "transition");
        String id = string(o.get("on"), "on");
        Trigger on = null;
        for (Trigger x : Trigger.values()) if (x.id.equals(id)) on = x;
        if (on == null) throw new DefinitionException("unknown trigger \"" + id + "\"");
        if (o.get("to") == null) throw new DefinitionException("transition: missing to");
        Cond cond = null;
        if (o.get("if") != null) {
            Map<String, Object> c = object(o.get("if"), "if");
            known(c, COND, "if");
            String w = string(c.get("what"), "what"), m = string(c.get("cmp"), "cmp");
            Quantity what = null;
            for (Quantity x : Quantity.values()) if (x.id.equals(w)) what = x;
            if (what == null) throw new DefinitionException("unknown quantity \"" + w + "\"");
            Comparison cmp = null;
            for (Comparison x : Comparison.values()) if (x.id.equals(m)) cmp = x;
            if (cmp == null) throw new DefinitionException("unknown comparison \"" + m + "\"");
            cond = new Cond(what, cmp, value(c.get("value")));
        }
        return new Transition(on, integer(o.get("to"), "to"), value(o.get("prob")), cond);
    }

    /** A value; null: absent. Checks its members, not its numbers. */
    static Value value(Object x) throws DefinitionException {
        if (x == null) return null;
        Map<String, Object> o = object(x, "value");
        known(o, VALUE, "value");
        Scope scope = Scope.EACH;
        if (o.get("scope") != null) {
            String id = string(o.get("scope"), "scope");
            scope = null;
            for (Scope s : Scope.values()) if (s.id.equals(id)) scope = s;
            if (scope == null) throw new DefinitionException("value: unknown scope \"" + id + "\"");
        }
        Dist dist = null;
        if (o.get("dist") != null) {
            Map<String, Object> d = object(o.get("dist"), "dist");
            String id = string(d.get("kind"), "kind");
            DistKind kind = null;
            for (DistKind k : DistKind.values()) if (k.id.equals(id)) kind = k;
            if (kind == null) throw new DefinitionException("dist: unknown kind \"" + id + "\"");
            known(d, distFields(kind), "dist " + id);
            List<Value> of = new ArrayList<>();
            for (Object v : list(d.get("of"), "of")) {
                Value c = value(v);
                if (c == null) throw new DefinitionException("dist " + id + ": want a value in of");
                of.add(c);
            }
            dist = new Dist(kind, numbers(d.get("params"), "params"), numbers(d.get("points"), "points"),
                    numbers(d.get("weights"), "weights"), of);
        }
        return new Value(number(o.get("const"), "const"), o.get("range") == null ? null : numbers(o.get("range"), "range"),
                dist, scope, number(o.get("min"), "min"), number(o.get("max"), "max"));
    }

    private static final Set<String> LIMITS = Set.of("max_padding_bytes", "max_padding_ratio", "max_injected", "max_pause_total", "max_delay");

    static ShapingLimits sessionLimits(String json) throws DefinitionException {
        Map<String, Object> o = object(parse(json), "limits");
        known(o, LIMITS, "limits");
        ShapingLimits l = ShapingLimits.none();
        Long n = count(o, "max_padding_bytes");
        if (n != null) l = l.withMaxPaddingBytes(n);
        n = count(o, "max_injected");
        if (n != null) l = l.withMaxInjected(n);
        Double r = number(o.get("max_padding_ratio"), "max_padding_ratio");
        if (r != null) {
            if (!(r >= 0 && r <= 1)) throw new DefinitionException("limits max_padding_ratio: " + r + " outside [0, 1]");
            l = l.withMaxPaddingRatio(r);
        }
        Duration d = seconds(o, "max_pause_total");
        if (d != null) l = l.withMaxPauseTotal(d);
        d = seconds(o, "max_delay");
        if (d != null) l = l.withMaxDelay(d);
        return l;
    }

    private static Long count(Map<String, Object> o, String k) throws DefinitionException {
        Long n = integerForm(o.get(k), "limits " + k);
        if (n == null) return null;
        if (n < 0) throw new DefinitionException("limits " + k + ": " + n + " is negative");
        if (n > (1L << 53)) throw new DefinitionException("limits " + k + ": " + n + " above max 2^53");
        return n;
    }

    private static Duration seconds(Map<String, Object> o, String k) throws DefinitionException {
        Double x = number(o.get(k), "limits " + k);
        if (x == null) return null;
        if (x < 0) throw new DefinitionException("limits " + k + ": " + x + " is negative");
        if (x > 1e9) throw new DefinitionException("limits " + k + ": " + x + " above max 1e9");
        return Duration.ofNanos((long) Sampler.round(x * 1e9));
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> object(Object x, String what) throws DefinitionException {
        if (x instanceof Map<?, ?> m) return (Map<String, Object>) m;
        throw new DefinitionException(what + ": want an object");
    }

    @SuppressWarnings("unchecked")
    static List<Object> list(Object x, String what) throws DefinitionException {
        if (x == null) return List.of();
        if (x instanceof List<?> l) return (List<Object>) l;
        throw new DefinitionException(what + ": want an array");
    }

    private static String string(Object x, String what) throws DefinitionException {
        if (x == null) return "";
        if (x instanceof String s) return s;
        throw new DefinitionException(what + ": want a string");
    }

    private static boolean bool(Object x, String what) throws DefinitionException {
        if (x == null) return false;
        if (x instanceof Boolean b) return b;
        throw new DefinitionException(what + ": want a boolean");
    }

    /** A JSON number of any form; null: absent. */
    static Double number(Object x, String what) throws DefinitionException {
        if (x == null) return null;
        if (x instanceof Number n) return n.doubleValue();
        throw new DefinitionException(what + ": want a number");
    }

    /** A JSON number in the integer form; null: absent. */
    static Long integerForm(Object x, String what) throws DefinitionException {
        if (x == null) return null;
        if (x instanceof Long l) return l;
        if (x instanceof BigInteger b) return b.signum() < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
        throw new DefinitionException(what + ": want an integer");
    }

    private static int integer(Object x, String what) throws DefinitionException {
        Long l = integerForm(x, what);
        if (l == null) return 0;
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) throw new DefinitionException(what + ": " + l + " out of range");
        return (int) (long) l;
    }

    private static double[] numbers(Object x, String what) throws DefinitionException {
        List<Object> l = list(x, what);
        double[] out = new double[l.size()];
        for (int i = 0; i < out.length; i++) {
            Double d = number(l.get(i), what);
            if (d == null) throw new DefinitionException(what + ": want a number");
            out[i] = d;
        }
        return out;
    }

    /** RFC 8259 JSON: objects as maps, arrays as lists, numbers as Double. */
    private static final class Parser {
        private final String s;
        private int i;

        Parser(String s) { this.s = s; }

        Object document() throws DefinitionException {
            Object v = value();
            ws();
            if (i != s.length()) throw error("trailing data");
            return v;
        }

        private DefinitionException error(String what) {
            return new DefinitionException("json: parse error: " + what + " at offset " + i);
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
            char c = s.charAt(i);
            return switch (c) {
                case '{' -> obj();
                case '[' -> arr();
                case '"' -> str();
                case 't' -> lit("true", Boolean.TRUE);
                case 'f' -> lit("false", Boolean.FALSE);
                case 'n' -> lit("null", null);
                default -> num();
            };
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
                if (m.containsKey(k)) throw error("duplicate member \"" + k + "\"");
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
                        try { b.append((char) Integer.parseInt(s.substring(i, i + 4), 16)); }
                        catch (NumberFormatException x) { throw error("invalid escape"); }
                        i += 4;
                    }
                    default -> throw error("invalid escape");
                }
            }
        }

        private static boolean digit(char c) { return c >= '0' && c <= '9'; }

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
            double d = Double.parseDouble(t);
            if (Double.isInfinite(d)) throw error("number " + t + " overflows binary64");
            if (!integral) return d;
            BigInteger b = new BigInteger(t);
            return b.bitLength() < 64 ? (Number) b.longValue() : b;
        }
    }
}
