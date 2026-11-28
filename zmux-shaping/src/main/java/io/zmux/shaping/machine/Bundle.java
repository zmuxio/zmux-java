package io.zmux.shaping.machine;

import static io.zmux.shaping.machine.DefinitionException.Category.*;
import static io.zmux.shaping.machine.DefinitionJson.err;

import io.zmux.CarrierKind;
import io.zmux.PoolPolicy;
import io.zmux.carrier.HandshakeShaper;
import io.zmux.shaping.ShaperFactory;
import io.zmux.shaping.ShapingBudget;
import io.zmux.shaping.machine.Definition.Scope;
import io.zmux.shaping.machine.Definition.Value;
import io.zmux.shaping.machine.DefinitionJson.Family;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * A configuration bundle: for each kind of carrier the first candidate whose {@code requires} are all known, with its
 * handshake plans, machines, budget and pool policy. Anything wrong in what is read fails the whole bundle. A kind with no
 * entry, and a pipe of no declared kind, takes {@code default}.
 */
public final class Bundle {
    public static final int SCHEMA = 2;
    private static final List<String> KINDS = List.of("default", "websocket", "streaming", "split", "exchange", "quic", "http3", "webtransport");
    private static final Map<String, Set<String>> HANDSHAKES = Map.of(
            "establish", Set.of("hello", "reply", "finish"),
            "join", Set.of("hello", "hello_early", "reply", "confirm"));

    private final Map<String, Kind> kinds;
    private final Map<String, Integer> selected;
    private final List<String> unshaped;

    private Bundle(Map<String, Kind> kinds, Map<String, Integer> selected, List<String> unshaped) {
        this.kinds = kinds;
        this.selected = selected;
        this.unshaped = unshaped;
    }

    /** One message's plan as values. */
    private record Unit(Value padTo, Value padMultiple, Value pad, Value delay) {
        List<Value> values() {
            List<Value> out = new ArrayList<>(4);
            for (Value v : new Value[] {padTo, padMultiple, pad, delay}) if (v != null) out.add(v);
            return out;
        }
    }

    /** A selected candidate before its shared values are drawn. */
    record Candidate(Map<String, Unit> units, Definition machines, DefinitionJson.Budget budget, MachinePoolPolicy.Spec pool) {
        private static final Set<String> FIELDS = Set.of("requires", "handshakes", "machines", "budget", "pool", "include_control");

        static Candidate read(Object c, DefinitionJson.Tables tables) throws DefinitionException {
            Map<String, Object> d = DefinitionJson.members(c, FIELDS, "candidate");
            Map<String, Unit> units = d.containsKey("handshakes") ? handshakes(d.get("handshakes"), tables) : Map.of();
            boolean includeControl = DefinitionJson.flag(d, "include_control");
            Definition machines = d.containsKey("machines") ? DefinitionJson.machines(d.get("machines"), tables, includeControl) : null;
            DefinitionJson.Budget budget = d.containsKey("budget") ? DefinitionJson.budget(d.get("budget"), tables) : null;
            MachinePoolPolicy.Spec pool = d.containsKey("pool") ? MachinePoolPolicy.Spec.read(d.get("pool"), tables) : null;
            var profile = budget != null && budget.profile() != null ? budget.profile() : io.zmux.shaping.ProfileLimits.LIGHTWEIGHT;
            if (machines != null && !machines.gates.fits(profile.maxAddedDelay().toNanos() / 1000, 0)) {
                throw err(CONFLICT, "rate and burst exceed max_added_delay");
            }
            return new Candidate(units, machines, budget, pool);
        }

        private static Map<String, Unit> handshakes(Object x, DefinitionJson.Tables tables) throws DefinitionException {
            if (!(x instanceof Map<?, ?> hs)) throw err(TYPE, "handshakes");
            Map<String, Unit> out = new TreeMap<>(); // byte order of "handshake/message": the names are ASCII, and '/' sorts below them
            for (var e : hs.entrySet()) {
                Set<String> names = HANDSHAKES.get((String) e.getKey());
                if (names == null) continue;
                if (!(e.getValue() instanceof Map<?, ?> msgs)) throw err(TYPE, (String) e.getKey());
                for (var m : msgs.entrySet()) {
                    if (!names.contains((String) m.getKey())) continue;
                    Map<String, Object> u = DefinitionJson.members(m.getValue(), Set.of("pad_to", "pad_multiple", "pad", "delay"), "message");
                    out.put(e.getKey() + "/" + m.getKey(), new Unit(unit(u, "pad_to", Family.LENGTH, tables), unit(u, "pad_multiple", Family.LENGTH, tables),
                            unit(u, "pad", Family.LENGTH, tables), unit(u, "delay", Family.TIME, tables)));
                }
            }
            return out;
        }

        private static Value unit(Map<String, Object> u, String k, Family f, DefinitionJson.Tables tables) throws DefinitionException {
            return u.containsKey(k) ? DefinitionJson.value(u.get(k), f, tables, DefinitionJson.NO_PIPE, k) : null;
        }

        /** Draws the shared values in order: handshakes, machines, pool. */
        Kind compile(Sampler compile, MachineOptions o) {
            @SuppressWarnings({"IdentityHashMapUsage", "ValueClassIdentity"}) // keys are told apart by identity: equal values of different declarations are distinct; the Long values are not compared
            Map<Value, Long> kept = new java.util.IdentityHashMap<>();
            for (Unit u : units.values()) {
                for (Value v : u.values()) {
                    if (v.scope == Scope.SHARED) {
                        kept.put(v, compile.draw(v.segments));
                    }
                }
            }
            Machines.Compiled machineCode = machines == null ? null : new Machines.Compiled(machines, compile, o);
            MachinePoolPolicy policy = null;
            // Its own source for what it draws at run time: the policy and the handshake hook each hold a lock of their own.
            if (pool != null) policy = new MachinePoolPolicy(pool, compile, Sampler.of(o));
            // Its own source and lock for the draws of each session.
            Sampler session = budget == null ? null : Sampler.of(o);
            return new Kind(this, kept, machineCode, policy, compile, session);
        }
    }

    /** A selected candidate compiled. Its hook may be called concurrently. */
    static final class Kind {
        private final Candidate c;
        private final Map<Value, Long> kept;
        final Machines.Compiled machines;
        final MachinePoolPolicy pool;
        private final Sampler sampler, session;

        Kind(Candidate c, Map<Value, Long> kept, Machines.Compiled machines, MachinePoolPolicy pool, Sampler sampler, Sampler session) {
            this.c = c;
            this.kept = kept;
            this.machines = machines;
            this.pool = pool;
            this.sampler = sampler;
            this.session = session;
        }

        List<Long> sharedInOrder() {
            List<Long> values = new ArrayList<>();
            for (Unit unit : c.units.values()) {
                for (Value value : unit.values()) {
                    if (value.scope == Scope.SHARED) values.add(kept.get(value));
                }
            }
            if (machines != null) for (long value : machines.shared) values.add(value);
            if (pool != null) values.addAll(pool.sharedInOrder());
            return values;
        }

        private long val(Value v) {
            return v == null ? 0 : v.scope == Scope.SHARED ? kept.get(v) : sampler.draw(v.segments);
        }

        /** One session's budget; null without one. */
        Supplier<ShapingBudget> budget() {
            if (c.budget == null) return null;
            return () -> {
                synchronized (session) { return c.budget.draw(session); }
            };
        }

        /** The plan of a message, null without an entry. */
        HandshakeShaper.UnitPlan plan(String handshake, String message) {
            Unit u = c.units.get(handshake + "/" + message);
            if (u == null) return null;
            synchronized (sampler) {
                int to = (int) val(u.padTo), multiple = (int) val(u.padMultiple), pad = (int) val(u.pad);
                long delay = val(u.delay);
                if (c.budget != null && c.budget.profile() != null) {
                    delay = Math.min(delay, c.budget.profile().maxAddedDelay().toNanos() / 1000);
                }
                return new HandshakeShaper.UnitPlan(to, multiple, pad, delay == 0 ? null : Duration.ofNanos(delay * 1000));
            }
        }
    }

    /** Reads a bundle. */
    public static Bundle parse(byte[] json, MachineOptions o) throws DefinitionException {
        o.limits().check(json.length);
        Map<String, Object> d = DefinitionJson.members(DefinitionJson.parse(new String(json, StandardCharsets.UTF_8)),
                Set.of("schema", "tables", "kinds"), "bundle");
        if (!Long.valueOf(SCHEMA).equals(d.get("schema"))) throw err(SCHEMA_VERSION, "expected shaping schema " + SCHEMA);
        Map<String, Object> tables = DefinitionJson.tableSources(d.get("tables"));
        if (!(d.get("kinds") instanceof Map<?, ?> raw)) throw err(TYPE, "kinds");
        if (raw.get("default") == null) throw err(NO_DEFAULT, "kinds.default");
        Map<String, Kind> kinds = new LinkedHashMap<>();
        Map<String, Integer> selected = new LinkedHashMap<>();
        List<String> unshaped = new ArrayList<>();
        for (String kind : KINDS) {
            Object cands = raw.get(kind);
            if (cands == null) continue;
            List<?> list = DefinitionJson.list(cands, kind);
            boolean chosen = false;
            for (int i = 0; i < list.size() && !chosen; i++) {
                if (!(list.get(i) instanceof Map<?, ?> c)) throw err(TYPE, "candidate");
                Object rq = c.get("requires");
                if (rq != null) {
                    if (!(rq instanceof List<?> names)) throw err(TYPE, "requires");
                    boolean known = true;
                    for (Object n : names) {
                        if (!(n instanceof String s)) throw err(TYPE, "requires");
                        known &= o.features().contains(s);
                    }
                    if (!known) continue;
                }
                kinds.put(kind, Candidate.read(c, new DefinitionJson.Tables(tables, o.limits())).compile(Sampler.of(o), o));
                selected.put(kind, i);
                chosen = true;
            }
            if (!chosen) {
                if (!o.allowUnshaped()) throw err(NO_CANDIDATE, kind);
                unshaped.add(kind);
                selected.put(kind, null);
            }
        }
        return new Bundle(kinds, selected, List.copyOf(unshaped));
    }

    /** The index of the candidate selected for each kind in the bundle; null for a kind without one. */
    public Map<String, Integer> selected() { return java.util.Collections.unmodifiableMap(selected); }

    /** The kinds without a usable candidate. */
    public List<String> unshaped() { return unshaped; }

    private Kind of(CarrierKind k) { return kinds.get(resolve(k == null ? "" : k.toString())); }

    /** The name of the entry a carrier of this kind uses. */
    String resolve(String kind) {
        return kinds.containsKey(kind) ? kind : "default";
    }

    Kind kind(String name) { return kinds.get(name); }

    /** The plans of handshake messages; a message without an entry has none. */
    public HandshakeShaper handshakeShaper() {
        return view -> {
            Kind k = of(view.kind());
            return k == null ? null : k.plan(view.handshake(), view.message());
        };
    }

    /** The shapers of pipes; a kind without machines is not shaped. */
    public ShaperFactory shaperFactory() {
        var gates = kinds.values().stream().filter(k -> k.machines != null).map(k -> k.machines.d.gates)
                .reduce(io.zmux.shaping.GateRequirement.NONE, io.zmux.shaping.GateRequirement::combine);
        return new ShaperFactory() {
            @Override public io.zmux.shaping.GateRequirement gateRequirement() { return gates; }
            @Override public HandshakeShaper handshakeShaper() { return Bundle.this.handshakeShaper(); }
            @Override public io.zmux.shaping.Shaper create(io.zmux.shaping.ShapedPipe pipe) {
                Kind k = of(pipe.carrier());
                return k == null || k.machines == null ? null : k.machines.newShaper();
            }
        };
    }

    /**
     * The budget of a kind, for {@link io.zmux.ZmuxConfig.Builder#shapingBudget(java.util.function.Supplier)} or {@link io.zmux.ZmuxSession#setShaper}: it
     * draws the values of scope session at each call; null without a budget.
     */
    public Supplier<ShapingBudget> budget(CarrierKind kind) {
        Kind k = of(kind);
        return k == null ? null : k.budget();
    }

    /** The pool policy of a kind; null: the pool keeps its own behaviour. */
    public PoolPolicy poolPolicy(CarrierKind kind) {
        Kind k = of(kind);
        return k == null ? null : k.pool;
    }
}
