package io.zmux.shaping.machine;

import io.zmux.PoolPolicy;
import io.zmux.shaping.machine.Definition.Scope;
import io.zmux.shaping.machine.Definition.Value;
import io.zmux.shaping.machine.Validation.Family;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToDoubleFunction;

/**
 * A {@link PoolPolicy} read by {@link Machines#poolPolicy}.
 *
 * <p>Each decision sets the count from {@code count} (rounded half away from zero; its lower bound must be at least 1,
 * a draw above 2^53 makes the decision invalid), the dial spacing from the {@code dial_after} entry for the number of
 * consecutive failures (the last entry for any higher number; its lower bound must be at least 0), and retires, in view order and once each, the
 * members for which the metric of any {@code retire} rule is strictly greater than its
 * threshold. Metrics of a member: {@code age} and {@code idle} in seconds, {@code pipes} (1
 * per pipe, the pipes on a connection), {@code bytes_sent}, {@code bytes_received} and
 * {@code bytes} (their sum). Values have the families and load checks of a definition's values
 * (count 1 to 2^53; dial_after, and the thresholds of age and idle, 0 to 1e9 s; other thresholds 0 to 2^53). Values
 * of scope {@code shared} are drawn once per policy, of scope {@code pipe} once per member ID
 * while the member is in the view, of scope {@code each} at each decision. An absent member of
 * the JSON keeps the pool's own behaviour. A decision does not block and does not fail.
 *
 * <p>A jitter j (a JSON number, 0 &lt;= j &lt; 1) only lowers a value, by a random fraction of up
 * to j: a rule's {@code jitter} scales its threshold by 1 - j u, u drawn once per member and rule
 * when the member first appears in the view; {@code dial_jitter} scales the dial spacing by 1 - j u,
 * u drawn once per wait (a wait starts when the failures change, or a new dial started).
 * A null jitter is absent, which is 0.
 * {@code probe_interval} (seconds, not negative; 0 turns the idle probes off) and {@code probe_jitter}
 * pass through to the probe settings of the pipes a pool dials next; a draw outside 0 and [1, 1e9] s
 * makes the decision invalid: the pool rejects and reports it, and keeps its own behaviour.
 */
public final class MachinePoolPolicy implements PoolPolicy {
    /** A member as the metrics see it. */
    record Metrics(long id, double age, double idle, double pipes, double bytesSent, double bytesReceived) {}

    /** A decision before it is mapped to the pool; null: keep the pool's own. */
    record Result(Double count, Double dialAfter, List<Long> retire, Double recheck, Double probeInterval, Double probeJitter) {}

    /** The metrics a {@code retire} rule may name. */
    private static final Map<String, ToDoubleFunction<Metrics>> METRICS = Map.of(
            "age", Metrics::age,
            "idle", Metrics::idle,
            "pipes", Metrics::pipes,
            "bytes_sent", Metrics::bytesSent,
            "bytes_received", Metrics::bytesReceived,
            "bytes", m -> m.bytesSent() + m.bytesReceived());

    private static final Set<String> FIELDS = Set.of("count", "dial_after", "dial_jitter", "retire", "probe_interval", "probe_jitter");
    private static final Set<String> RULE_FIELDS = Set.of("metric", "above", "jitter");

    /** timed: the metric grows with time (age, idle), so the rule gives a recheck. */
    private record Rule(ToDoubleFunction<Metrics> metric, Value above, boolean timed, double jitter) {}

    private final Value count;
    private final List<Value> dialAfter;
    private final double dialJitter;
    private final List<Rule> rules;
    private final Value probeInterval;
    private final Double probeJitter;
    private final Draw draw;
    private final IdentityHashMap<Value, Double> shared = new IdentityHashMap<>();
    private final HashMap<Long, IdentityHashMap<Value, Double>> members = new HashMap<>();
    /** Per member, the draw u of each rule's jitter (rule order), drawn when it first appears. */
    private final HashMap<Long, double[]> ruleDraws = new HashMap<>();
    /** The current dial wait: its draw (NaN: none yet), and the failures and spacing it started from. */
    private double waitDraw = Double.NaN;
    private int waitFailures;
    private Double waitSince;

    private MachinePoolPolicy(Value count, List<Value> dialAfter, double dialJitter, List<Rule> rules, Value probeInterval,
                              Double probeJitter, Options o) {
        this.count = count;
        this.dialAfter = List.copyOf(dialAfter);
        this.dialJitter = dialJitter;
        this.rules = List.copyOf(rules);
        this.probeInterval = probeInterval;
        this.probeJitter = probeJitter;
        this.draw = new Draw(o);
    }

    static MachinePoolPolicy read(byte[] json, Options o) throws DefinitionException {
        var m = o.limits().maxBytes();
        if (SizeLimits.over(json.length, m)) throw new DefinitionException("pool: " + json.length + " bytes, limit " + m.getAsInt());
        Map<String, Object> spec = DefinitionJson.object(DefinitionJson.parse(new String(json, StandardCharsets.UTF_8)), "pool");
        DefinitionJson.known(spec, FIELDS, "pool");
        Value count = memberFree(spec.get("count"), Family.POOL_COUNT, "count");
        List<Value> dialAfter = new ArrayList<>();
        for (Object x : DefinitionJson.list(spec.get("dial_after"), "pool dial_after")) {
            if (x == null) throw new DefinitionException("pool dial_after: want a value");
            dialAfter.add(memberFree(x, Family.POOL_SECONDS, "dial_after " + dialAfter.size()));
        }
        List<Rule> rules = new ArrayList<>();
        for (Object x : DefinitionJson.list(spec.get("retire"), "pool retire")) {
            Map<String, Object> r = DefinitionJson.object(x, "pool retire");
            DefinitionJson.known(r, RULE_FIELDS, "pool retire");
            if (!(r.get("metric") instanceof String name)) throw new DefinitionException("pool retire: want a metric name");
            ToDoubleFunction<Metrics> metric = METRICS.get(name);
            if (metric == null) throw new DefinitionException("pool retire: unknown metric \"" + name + "\"");
            if (r.get("above") == null) throw new DefinitionException("pool retire " + name + ": missing above");
            boolean timed = name.equals("age") || name.equals("idle");
            Value above = value(r.get("above"), timed ? Family.RULE_SECONDS : Family.RULE_COUNT, "retire " + name + " above");
            Double j = jitter(r, "jitter", "pool retire " + name);
            rules.add(new Rule(metric, above, timed, j == null ? 0 : j));
        }
        Double dialJitter = jitter(spec, "dial_jitter", "pool");
        if (dialJitter != null && dialAfter.isEmpty()) throw new DefinitionException("pool dial_jitter: needs dial_after");
        Value probeInterval = memberFree(spec.get("probe_interval"), Family.POOL_PROBE, "probe_interval");
        Double probeJitter = jitter(spec, "probe_jitter", "pool");
        if (probeJitter != null && probeInterval == null) throw new DefinitionException("pool probe_jitter: needs probe_interval");
        return new MachinePoolPolicy(count, dialAfter, dialJitter == null ? 0 : dialJitter, rules, probeInterval, probeJitter, o);
    }

    /** The jitter o[k]: a JSON number, at least 0 and below 1; absent or null: null. */
    private static Double jitter(Map<String, Object> o, String k, String what) throws DefinitionException {
        Double j = DefinitionJson.number(o.get(k), what + " " + k);
        if (j != null && !(j >= 0 && j < 1)) throw new DefinitionException(what + " " + k + ": want a number in [0, 1)");
        return j;
    }

    private static Value value(Object x, Family f, String what) throws DefinitionException {
        try {
            Value v = DefinitionJson.value(x);
            Validation.family(v, f, what);
            return v;
        } catch (DefinitionException e) {
            throw new DefinitionException("pool", e);
        }
    }

    /** A value without scope {@code pipe}, nested ones included. */
    private static Value memberFree(Object x, Family f, String what) throws DefinitionException {
        Value v = value(x, f, what);
        boolean[] pipe = {false};
        Validation.walk(v, u -> pipe[0] |= u.scope() == Scope.PIPE);
        if (pipe[0]) throw new DefinitionException("pool " + what + ": scope pipe only in retire");
        return v;
    }

    @Override
    public synchronized Decision decide(View view) {
        List<Metrics> ms = new ArrayList<>(view.members().size());
        for (Member m : view.members()) {
            ms.add(new Metrics(m.id(), seconds(m.age()), seconds(m.idle()), m.pipes(), unsigned(m.bytesSent()), unsigned(m.bytesReceived())));
        }
        Double since = view.sinceLastDial() == null ? null : seconds(view.sinceLastDial());
        Result r = decide(view.failures(), since, ms);
        // Draws only a decision can judge: the pool rejects the decision, keeps its own behaviour and reports it.
        if (r.count() != null && r.count() > Validation.MAX_COUNT) throw new IllegalArgumentException("pool count " + r.count() + " above 2^53");
        if (r.probeInterval() != null && !(r.probeInterval() == 0 || r.probeInterval() >= 1 && r.probeInterval() <= 1e9)) {
            throw new IllegalArgumentException("pool probe_interval " + r.probeInterval() + " s outside 0 and [1, 1e9]");
        }
        Decision d = Decision.KEEP.withRetire(new LinkedHashSet<>(r.retire()));
        if (r.count() != null) d = d.withCount((int) Math.min(r.count(), Integer.MAX_VALUE));
        if (r.dialAfter() != null) d = d.withDialAfter(Duration.ofNanos((long) Sampler.round(r.dialAfter() * 1e9)));
        if (r.recheck() != null) d = d.withRecheck(Duration.ofNanos(Math.max(1, (long) Math.ceil(r.recheck() * 1e9))));
        // A policy file's 0 turns the idle probes off.
        if (r.probeInterval() != null) {
            d = r.probeInterval() == 0 ? d.disableIdleProbes() : d.withProbeInterval(Duration.ofNanos((long) Sampler.round(r.probeInterval() * 1e9)));
        }
        if (r.probeJitter() != null) d = d.withProbeJitter(r.probeJitter());
        return d;
    }

    /**
     * Decides for members in view order after failures consecutive failed dials, sinceDial
     * seconds after the last dial started (null: none yet). The recheck is the least time until
     * an age or idle rule of a member not retired fires (1 ns at its threshold) or until the dial
     * spacing passes; null when neither applies.
     */
    synchronized Result decide(int failures, Double sinceDial, List<Metrics> view) {
        members.keySet().removeIf(id -> view.stream().noneMatch(m -> m.id() == id));
        ruleDraws.keySet().removeIf(id -> view.stream().noneMatch(m -> m.id() == id));
        for (Metrics m : view) ruleDraws.computeIfAbsent(m.id(), id -> {
            double[] u = new double[rules.size()];
            for (int i = 0; i < u.length; i++) if (rules.get(i).jitter() > 0) u[i] = draw.uniform();
            return u;
        });
        Double c = count == null ? null : Sampler.round(draw.sample(count, null));
        Double after = null;
        if (!dialAfter.isEmpty()) {
            int k = Math.clamp(failures, 0, dialAfter.size() - 1);
            after = Math.min(draw.sample(dialAfter.get(k), null), 1e9); // not negative: checked at load
            if (dialJitter > 0) {
                boolean fresh = Double.isNaN(waitDraw) || failures != waitFailures
                        || sinceDial != null && (waitSince == null || sinceDial < waitSince);
                if (fresh) waitDraw = draw.uniform();
                after *= 1 - dialJitter * waitDraw;
            }
            waitFailures = failures;
            waitSince = sinceDial;
        }
        double recheck = Double.POSITIVE_INFINITY;
        if (after != null && sinceDial != null && after - sinceDial > 0) recheck = after - sinceDial;
        List<Long> retire = new ArrayList<>();
        for (Metrics m : view) {
            double until = Double.POSITIVE_INFINITY;
            boolean retired = false;
            double[] u = ruleDraws.get(m.id());
            for (int i = 0; i < rules.size(); i++) {
                Rule rule = rules.get(i);
                double x = rule.metric().applyAsDouble(m), t = draw.sample(rule.above(), m.id()) * (1 - rule.jitter() * u[i]);
                if (x > t) {
                    if (!retire.contains(m.id())) retire.add(m.id());
                    retired = true;
                    break;
                }
                if (rule.timed()) {
                    double headroom = Math.max(t - x, 1e-9); // strictly positive: a recheck is never immediate
                    until = Math.min(until, headroom);
                }
            }
            if (!retired && !retire.contains(m.id())) recheck = Math.min(recheck, until);
        }
        Double probe = probeInterval == null ? null : draw.sample(probeInterval, null); // judged by the decision
        return new Result(c, after, retire, recheck == Double.POSITIVE_INFINITY ? null : recheck, probe, probeJitter);
    }

    private static double seconds(Duration d) { return d.toNanos() / 1e9; }

    private static double unsigned(long x) { return x >= 0 ? x : x + 0x1p64; }

    /** Draws with the policy's source, keeping values per scope. */
    private final class Draw extends Sampler {
        private IdentityHashMap<Value, Double> member;

        Draw(Options o) { super(Sampler.source(o)); }

        /** A uniform draw from [0, 1). */
        double uniform() { return r.nextDouble(); }

        double sample(Value v, Long id) {
            member = id == null ? null : members.computeIfAbsent(id, k -> new IdentityHashMap<>());
            return sample(v);
        }

        @Override
        double sample(Value v) {
            IdentityHashMap<Value, Double> keep = switch (v.scope()) {
                case SHARED -> shared;
                case PIPE -> member;
                case EACH -> null;
            };
            if (keep == null) return draw(v);
            Double x = keep.get(v);
            if (x == null) keep.put(v, x = draw(v));
            return x;
        }
    }
}
