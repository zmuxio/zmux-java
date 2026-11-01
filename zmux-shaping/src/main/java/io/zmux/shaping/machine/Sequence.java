package io.zmux.shaping.machine;

import io.zmux.shaping.machine.Definition.*;
import java.util.ArrayList;
import java.util.List;

/**
 * The compact form of a single machine that runs its steps in order, one record each, and
 * then stops shaping the pipe:
 *
 * <pre>
 * sequence := step ("," step)*
 * step     := dist ("/" modifier)*
 * modifier := "delay=" dist | "scope=" scope | "until=" guard | "inject=" count ":" dist
 * dist     := number | number "-" number | name "(" number ("," number)* ")"
 * scope    := "each" | "pipe" | "shared"
 * guard    := ("seq" | "len" | "counter0" | "counter1") ("&lt;" | "&lt;=" | "==" | "!=" | "&gt;=" | "&gt;") number
 * </pre>
 *
 * The step's dist is the record's frames length in bytes: shorter records are padded to it,
 * longer ones are cut at it. delay is in seconds. until ends the sequence after the record
 * when the guard holds. inject waits for the next received record, then sends count records
 * of PADDING only of the dist's length. name is a distribution kind with its params.
 */
public final class Sequence {
    private Sequence() {}

    /** Target placeholders: the following step, and the end. */
    private static final int NEXT = -1, END = -2;

    /** A state under construction. */
    private static final class Draft {
        RecordRule record;
        final List<Action> enter = new ArrayList<>();
        final List<Transition> next = new ArrayList<>();

        Draft(RecordRule record) { this.record = record; }
    }

    public static Definition parse(String s) throws DefinitionException {
        List<String> steps = split(s, ',');
        List<Draft> states = new ArrayList<>();
        List<int[]> links = new ArrayList<>(); // {step state, its last state}
        for (int i = 0; i < steps.size(); i++) {
            int at = states.size();
            try { parseStep(steps.get(i).strip(), at, states); }
            catch (DefinitionException e) { throw new DefinitionException("sequence: step " + i, e); }
            links.add(new int[] {at, states.size() - 1});
        }
        int end = states.size();
        Draft stop = new Draft(null);
        stop.enter.add(new Action(Op.STOP, false, 0, null, null, null, null, null));
        states.add(stop);
        for (int k = 0; k < links.size(); k++) {
            int next = k + 1 < links.size() ? links.get(k + 1)[0] : end;
            retarget(states.get(links.get(k)[1]), NEXT, next);
            retarget(states.get(links.get(k)[0]), END, end);
        }
        List<State> out = new ArrayList<>();
        for (Draft d : states) out.add(new State(d.record, d.enter, d.next));
        Definition d = new Definition(Definition.VERSION, List.of(new Machine(0, out)));
        d.validate();
        return d;
    }

    private static void retarget(Draft d, int from, int to) {
        for (int j = 0; j < d.next.size(); j++) {
            Transition t = d.next.get(j);
            if (t.to() == from) d.next.set(j, new Transition(t.on(), to, t.prob(), t.cond()));
        }
    }

    private static void parseStep(String s, int base, List<Draft> states) throws DefinitionException {
        List<String> parts = split(s, '/');
        Value l = parseDist(parts.getFirst());
        Value delay = null;
        Transition until = null;
        int count = 0;
        Value injLen = null;
        for (String p : parts.subList(1, parts.size())) {
            String t = p.strip();
            int eq = t.indexOf('=');
            if (eq < 0) throw new DefinitionException("modifier \"" + p + "\": want name=value");
            String k = t.substring(0, eq), v = t.substring(eq + 1);
            switch (k) {
                case "delay" -> delay = parseDist(v);
                case "scope" -> {
                    Scope sc = null;
                    for (Scope x : Scope.values()) if (x.id.equals(v)) sc = x;
                    if (sc == null) throw new DefinitionException("unknown scope \"" + v + "\"");
                    l = new Value(l.constant(), l.range(), l.dist(), sc, l.min(), l.max());
                    if (delay != null) delay = new Value(delay.constant(), delay.range(), delay.dist(), sc, delay.min(), delay.max());
                }
                case "until" -> until = new Transition(Trigger.RECORD_OUT, END, null, parseGuard(v));
                case "inject" -> {
                    int c = v.indexOf(':');
                    if (c < 0) throw new DefinitionException("inject \"" + v + "\": want count:dist");
                    try { count = Integer.parseInt(v.substring(0, c)); }
                    catch (NumberFormatException e) { count = -1; }
                    if (count < 0) throw new DefinitionException("inject count \"" + v.substring(0, c) + "\"");
                    injLen = parseDist(v.substring(c + 1));
                }
                default -> throw new DefinitionException("unknown modifier \"" + k + "\"");
            }
        }
        Draft st = new Draft(new RecordRule(l, l, null, null, delay, false));
        states.add(st);
        if (until != null) st.next.add(until);
        if (count == 0) {
            st.next.add(new Transition(Trigger.RECORD_OUT, NEXT, null, null));
            return;
        }
        int w = base + 1, a = base + 2, b = base + 3;
        st.next.add(new Transition(Trigger.RECORD_OUT, w, null, null));
        Draft wait = new Draft(null);
        wait.next.add(new Transition(Trigger.RECORD_IN, a, null, null));
        Draft arm = new Draft(null);
        arm.enter.add(new Action(Op.SET, false, 1, null, null, null, null, Value.of(count)));
        arm.next.add(new Transition(Trigger.ENTER, b, null, null));
        Draft send = new Draft(null);
        send.enter.add(new Action(Op.INJECT, false, 0, Value.of(0), injLen, null, null, null));
        send.enter.add(new Action(Op.SUB, false, 1, null, null, null, null, Value.of(1)));
        send.next.add(new Transition(Trigger.INJECTED, b, null, new Cond(Quantity.COUNTER1, Comparison.GT, Value.of(0))));
        send.next.add(new Transition(Trigger.INJECTED, NEXT, null, null));
        states.add(wait);
        states.add(arm);
        states.add(send);
    }

    private static final java.util.regex.Pattern NUMBER =
            java.util.regex.Pattern.compile("[+-]?(\\d+(\\.\\d*)?|\\.\\d+)([eE][+-]?\\d+)?");

    private static double parseNum(String s) throws DefinitionException {
        String t = s.strip();
        if (NUMBER.matcher(t).matches()) {
            double x = Double.parseDouble(t);
            if (Double.isFinite(x)) return x;
        }
        throw new DefinitionException("number \"" + s + "\"");
    }

    private static Value parseDist(String s) throws DefinitionException {
        s = s.strip();
        int open = s.indexOf('(');
        if (open >= 0) {
            if (!s.endsWith(")")) throw new DefinitionException("dist \"" + s + "\": missing )");
            String name = s.substring(0, open).strip();
            DistKind kind = null;
            for (DistKind k : DistKind.values()) if (k.id.equals(name)) kind = k;
            if (kind == null) throw new DefinitionException("dist: unknown kind \"" + name + "\"");
            String[] args = s.substring(open + 1, s.length() - 1).split(",", -1);
            double[] params = new double[args.length];
            for (int i = 0; i < args.length; i++) {
                try { params[i] = parseNum(args[i]); }
                catch (DefinitionException e) { throw new DefinitionException("dist \"" + s + "\"", e); }
            }
            Value v = new Value(null, null, new Dist(kind, params, null, null, null), Scope.EACH, null, null);
            Validation.value(v);
            return v;
        }
        int lead = Math.min(1, s.length());
        int dash = s.indexOf('-', lead);
        if (dash >= 0) {
            Value v = new Value(null, new double[] {parseNum(s.substring(0, dash)), parseNum(s.substring(dash + 1))}, null,
                    Scope.EACH, null, null);
            Validation.value(v);
            return v;
        }
        return Value.of(parseNum(s));
    }

    private static Cond parseGuard(String s) throws DefinitionException {
        String[] syms = {"<=", ">=", "==", "!=", "<", ">"};
        Comparison[] cmps = {Comparison.LE, Comparison.GE, Comparison.EQ, Comparison.NE, Comparison.LT, Comparison.GT};
        for (int k = 0; k < syms.length; k++) {
            int i = s.indexOf(syms[k]);
            if (i < 0) continue;
            double x;
            try { x = parseNum(s.substring(i + syms[k].length())); }
            catch (DefinitionException e) { throw new DefinitionException("guard \"" + s + "\"", e); }
            String w = s.substring(0, i).strip();
            for (Quantity q : Quantity.values()) if (q.id.equals(w)) return new Cond(q, cmps[k], Value.of(x));
            throw new DefinitionException("guard \"" + s + "\": unknown quantity \"" + w + "\"");
        }
        throw new DefinitionException("guard \"" + s + "\": no comparison");
    }

    /** Cuts s at sep outside parentheses. */
    private static List<String> split(String s, char sep) {
        List<String> out = new ArrayList<>();
        int depth = 0, start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == sep && depth == 0) { out.add(s.substring(start, i)); start = i + 1; }
        }
        out.add(s.substring(start));
        return out;
    }
}
