package io.zmux.shaping.machine;

import static io.zmux.shaping.machine.DefinitionException.Category.*;
import static io.zmux.shaping.machine.DefinitionJson.err;

import io.zmux.PipeId;
import io.zmux.PipeLiveness;
import io.zmux.PoolPolicy;
import io.zmux.shaping.machine.Definition.Scope;
import io.zmux.shaping.machine.Definition.Value;
import io.zmux.shaping.machine.DefinitionJson.Family;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link PoolPolicy} read from a JSON object with the optional members {@code count}, {@code dial_after} (a list; entry
 * {@code min(failures, length - 1)} applies), {@code dial_jitter}, {@code retire} (a list of {@code {"metric": name,
 * "above": value, "jitter": ppm}}), {@code probe_interval}, {@code probe_jitter} and, for table names, {@code tables}. Values
 * have the form of a definition's; times are microseconds and a jitter parts per million, below 10^6, that only lowers a value.
 *
 * <p>Metrics of a member: {@code age}, {@code idle}, {@code pipes}, {@code bytes_sent}, {@code bytes_received} and
 * {@code bytes}. A member is retired, in view order and once, when a metric is strictly above its rule's threshold. Scope
 * {@code shared} draws once per policy, {@code pipe} (only in {@code retire}) once per member while it is in the view,
 * {@code each} at each decision. A rule's jitter is drawn once per member; the dial spacing's once per wait, which starts
 * when the failures change or a new dial started. A drawn probe interval outside 0 and [1 s, 10^15 us] makes the decision
 * invalid, and the pool keeps its own behaviour. A decision does not block.
 */
public final class MachinePoolPolicy implements PoolPolicy {
    private static final long SECOND_MICROS = 1_000_000;

    /** A member as the metrics see it; times in microseconds, sizes unsigned. */
    record Metrics(long id, long age, long idle, long pipes, long bytesSent, long bytesReceived) {}

    /** A decision before it is mapped to the pool; null: keep the pool's own. Times in microseconds. */
    record Result(Long count, Long dialAfter, List<Long> retire, Long recheck, Long probeInterval, Long probeJitter) {}

    enum Metric {
        AGE, IDLE, PIPES, BYTES_SENT, BYTES_RECEIVED, BYTES;

        boolean timed() { return this == AGE || this == IDLE; }

        long of(Metrics m) {
            return switch (this) {
                case AGE -> m.age();
                case IDLE -> m.idle();
                case PIPES -> m.pipes();
                case BYTES_SENT -> m.bytesSent();
                case BYTES_RECEIVED -> m.bytesReceived();
                case BYTES -> {
                    long sum = m.bytesSent() + m.bytesReceived();
                    yield Long.compareUnsigned(sum, m.bytesSent()) < 0 ? -1L : sum;
                }
            };
        }
    }

    record Rule(Metric metric, Value above, long jitter) {}

    /** The members of a {@code pool} object, checked; every value is one place a number is used. */
    record Spec(Value count, List<Value> dialAfter, long dialJitter, List<Rule> rules, Value probeInterval, Long probeJitter) {
        private static final Set<String> FIELDS = Set.of("count", "dial_after", "dial_jitter", "retire", "probe_interval", "probe_jitter");

        static Spec read(Object raw, DefinitionJson.Tables tables) throws DefinitionException {
            Map<String, Object> d = DefinitionJson.members(raw, FIELDS, "pool");
            Set<Scope> free = DefinitionJson.NO_PIPE;
            Value count = d.containsKey("count") ? DefinitionJson.value(d.get("count"), Family.POOL_COUNT, tables, free, "count") : null;
            List<Value> dial = new ArrayList<>();
            if (d.containsKey("dial_after")) {
                for (Object v : DefinitionJson.list(d.get("dial_after"), "dial_after")) dial.add(DefinitionJson.value(v, Family.TIME, tables, free, "dial_after"));
            }
            Long dialJitter = jitter(d, "dial_jitter");
            if (dialJitter != null && dial.isEmpty()) throw err(CONFLICT, "dial_jitter without dial_after");
            List<Rule> rules = new ArrayList<>();
            if (d.containsKey("retire")) {
                for (Object r : DefinitionJson.list(d.get("retire"), "retire")) {
                    Map<String, Object> q = DefinitionJson.members(r, Set.of("metric", "above", "jitter"), "retire");
                    Metric metric = metric(DefinitionJson.required(q, "metric", "retire"));
                    Value above = DefinitionJson.value(DefinitionJson.required(q, "above", "retire"), metric.timed() ? Family.TIME : Family.COUNTER, tables, DefinitionJson.ANY_SCOPE, "above");
                    Long j = jitter(q, "jitter");
                    rules.add(new Rule(metric, above, j == null ? 0 : j));
                }
            }
            Value probe = d.containsKey("probe_interval") ? DefinitionJson.value(d.get("probe_interval"), Family.TIME, tables, free, "probe_interval") : null;
            Long probeJitter = jitter(d, "probe_jitter");
            if (probeJitter != null && probe == null) throw err(CONFLICT, "probe_jitter without probe_interval");
            return new Spec(count, dial, dialJitter == null ? 0 : dialJitter, rules, probe, probeJitter);
        }

        private static Metric metric(Object x) throws DefinitionException {
            if (x instanceof String s) {
                for (Metric m : Metric.values()) if (m.name().toLowerCase(java.util.Locale.ROOT).equals(s)) return m;
            }
            throw err(TYPE, "metric");
        }

        private static Long jitter(Map<String, Object> d, String k) throws DefinitionException {
            if (!d.containsKey(k)) return null;
            if (!DefinitionJson.isInteger(d.get(k))) throw err(TYPE, k);
            long v = DefinitionJson.integer(d.get(k), k);
            if (v < 0 || v >= 1_000_000) throw err(RANGE, k);
            return v;
        }

        /** The shared values in the order they are drawn. */
        List<Value> sites() {
            List<Value> out = new ArrayList<>();
            if (count != null) out.add(count);
            out.addAll(dialAfter);
            for (Rule r : rules) out.add(r.above);
            if (probeInterval != null) out.add(probeInterval);
            return out;
        }
    }

    private final Spec spec;
    private final Sampler sampler;
    /** Shared values, by the value's place. */
    @SuppressWarnings({"IdentityHashMapUsage", "ValueClassIdentity"}) // keys are told apart by identity: equal values of different declarations are distinct; the Long values are not compared
    private final Map<Value, Long> shared = new java.util.IdentityHashMap<>();
    /** Per member, by rule: the word of its jitter and its kept threshold. */
    private final HashMap<Long, MemberState> members = new HashMap<>();
    private boolean waiting;
    private long waitWord;
    private int waitFailures;
    private Long waitSince;

    private static final class MemberState {
        final long[] words, kept;
        final boolean[] has;

        MemberState(int rules) {
            words = new long[rules];
            kept = new long[rules];
            has = new boolean[rules];
        }
    }

    /** A policy with its shared values drawn from compile, its other values from sampler. */
    MachinePoolPolicy(Spec spec, Sampler compile, Sampler sampler) {
        this.spec = spec;
        this.sampler = sampler;
        for (Value v : spec.sites()) if (v.scope == Scope.SHARED) shared.put(v, compile.draw(v.segments));
    }

    /** The shared values in the order they were drawn. */
    List<Long> sharedInOrder() {
        List<Long> out = new ArrayList<>();
        for (Value v : spec.sites()) if (v.scope == Scope.SHARED) out.add(shared.get(v));
        return out;
    }

    static MachinePoolPolicy read(String json, MachineOptions o) throws DefinitionException {
        Object root = DefinitionJson.parse(json);
        Map<String, Object> tables = Map.of();
        if (root instanceof Map<?, ?> m && m.get("tables") != null) {
            tables = DefinitionJson.tableSources(m.get("tables"));
            Map<Object, Object> rest = new java.util.LinkedHashMap<>(m);
            rest.remove("tables");
            root = rest;
        }
        Sampler s = Sampler.of(o);
        return new MachinePoolPolicy(Spec.read(root, new DefinitionJson.Tables(tables, o.limits())), s, s);
    }

    private long val(Value v) {
        if (v.scope == Scope.SHARED) return shared.get(v);
        return sampler.draw(v.segments);
    }

    @Override
    public Decision decide(View view) {
        List<Metrics> ms = new ArrayList<>(view.members().size());
        boolean connections = false;
        for (Member m : view.members()) {
            connections |= m.pipeId() == null;
            long id = m.pipeId() == null ? m.fateGroup() : m.pipeId().value();
            ms.add(new Metrics(id, micros(m.age()), micros(m.idle()), m.pipes(), m.bytesSent(), m.bytesReceived()));
        }
        Result r = decide(view.failures(), view.sinceLastDial() == null ? null : micros(view.sinceLastDial()), ms);
        if (r.probeInterval() != null && r.probeInterval() != 0 && r.probeInterval() < SECOND_MICROS) {
            throw new IllegalArgumentException("pool probe_interval " + r.probeInterval() + " us outside 0 and [1 s, 10^15 us]");
        }
        Decision d = connections ? Decision.KEEP.withRetireConnections(new LinkedHashSet<>(r.retire()))
                : Decision.KEEP.withRetirePipes(r.retire().stream().map(PipeId::of).collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new)));
        if (r.count() != null) d = d.withCount((int) Math.min(r.count(), Integer.MAX_VALUE));
        if (r.dialAfter() != null) d = d.withDialAfter(Duration.ofNanos(r.dialAfter() * 1000));
        if (r.recheck() != null) d = d.withRecheck(Duration.ofNanos(r.recheck() * 1000));
        if (r.probeInterval() != null || r.probeJitter() != null) {
            PipeLiveness.Builder l = PipeLiveness.builder();
            if (r.probeInterval() != null) {
                if (r.probeInterval() == 0) l.noProbeInterval(); else l.probeInterval(Duration.ofNanos(r.probeInterval() * 1000));
            }
            if (r.probeJitter() != null) l.probeJitter(r.probeJitter() / 1e6);
            d = d.withLiveness(l.build());
        }
        return d;
    }

    /**
     * Decides for members in view order after failures consecutive failed dials, sinceDial microseconds after the last dial
     * started (null: none yet). The recheck is the least time until an age or idle rule of a member not retired fires (1 us
     * at its threshold) or until the dial spacing passes; null when neither applies.
     */
    synchronized Result decide(int failures, Long sinceDial, List<Metrics> view) {
        members.keySet().removeIf(id -> view.stream().noneMatch(m -> m.id() == id));
        Long c = spec.count == null ? null : val(spec.count);
        Long after = null;
        if (!spec.dialAfter.isEmpty()) {
            long a = val(spec.dialAfter.get(Math.clamp(failures, 0, spec.dialAfter.size() - 1)));
            if (spec.dialJitter != 0) {
                boolean fresh = !waiting || failures != waitFailures || (sinceDial != null && (waitSince == null || sinceDial < waitSince));
                if (fresh) {
                    waitWord = sampler.next();
                    waiting = true;
                }
                a = Sampler.jitter(a, spec.dialJitter, waitWord);
            }
            waitFailures = failures;
            waitSince = sinceDial;
            after = a;
        }
        Long probe = spec.probeInterval == null ? null : val(spec.probeInterval);
        long recheck = Long.MAX_VALUE;
        if (after != null && sinceDial != null && after > sinceDial) recheck = after - sinceDial;
        List<Long> retire = new ArrayList<>();
        for (Metrics m : view) {
            MemberState st = members.computeIfAbsent(m.id(), id -> {
                MemberState fresh = new MemberState(spec.rules.size());
                for (int i = 0; i < fresh.words.length; i++) if (spec.rules.get(i).jitter != 0) fresh.words[i] = sampler.next();
                return fresh;
            });
            long until = Long.MAX_VALUE;
            boolean retired = false;
            for (int i = 0; i < spec.rules.size(); i++) {
                Rule rule = spec.rules.get(i);
                long t;
                if (rule.above.scope == Scope.PIPE) {
                    if (!st.has[i]) {
                        st.kept[i] = sampler.draw(rule.above.segments);
                        st.has[i] = true;
                    }
                    t = st.kept[i];
                } else {
                    t = val(rule.above);
                }
                t = Sampler.jitter(t, rule.jitter, st.words[i]);
                long x = rule.metric.of(m);
                if (Long.compareUnsigned(x, t) > 0) {
                    if (!retire.contains(m.id())) retire.add(m.id());
                    retired = true;
                    break;
                }
                // until starts at Long.MAX_VALUE and only ever takes values of at least 1, so the bounds are ordered.
                if (rule.metric.timed()) until = Math.clamp(t - x, 1, until);
            }
            if (!retired && !retire.contains(m.id())) recheck = Math.min(recheck, until);
        }
        return new Result(c, after, retire, recheck == Long.MAX_VALUE ? null : recheck, probe, spec.probeJitter);
    }

    private static long micros(Duration d) { return d.toNanos() / 1000; }
}
