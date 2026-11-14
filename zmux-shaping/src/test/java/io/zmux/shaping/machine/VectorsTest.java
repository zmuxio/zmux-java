package io.zmux.shaping.machine;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.shaping.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.random.RandomGenerator;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/** The shared cases of draws, bundles and machines, from the test resources, or the directory of {@code ZMUX_SHAPING_VECTORS}. */
class VectorsTest {
    @SuppressWarnings("unchecked")
    static Map<String, Object> file(String name) throws Exception {
        String dir = System.getenv().getOrDefault("ZMUX_SHAPING_VECTORS", "../zmux/src/test/resources/spec/shaping");
        Path p = Path.of(dir, name);
        assertTrue(Files.exists(p), "no shared cases at " + p);
        return (Map<String, Object>) DefinitionJson.parse(Files.readString(p));
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> cases(Map<String, Object> m, String k) { return (List<Map<String, Object>>) m.get(k); }

    @SuppressWarnings("unchecked")
    static Map<String, Object> obj(Object o) { return (Map<String, Object>) o; }

    static DefinitionJson.Tables tables(Object x) throws DefinitionException {
        return new DefinitionJson.Tables(DefinitionJson.tableSources(x), SizeLimits.NONE);
    }

    static long num(Object x) { return x instanceof BigInteger b ? b.longValue() : x == null ? 0 : (Long) x; }

    /** Counts the words it gives. */
    static final class Source implements RandomGenerator {
        long state;
        long[] list;
        int n;

        Source(Map<String, Object> spec) {
            if (spec.containsKey("seed")) state = Long.parseUnsignedLong((String) spec.get("seed"), 16);
            else list = ((List<?>) spec.get("u")).stream().mapToLong(x -> Long.parseUnsignedLong((String) x, 16)).toArray();
        }

        @Override public long nextLong() {
            if (list != null) return list[n++];
            n++;
            state += 0x9E3779B97F4A7C15L;
            long z = state;
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }
    }

    static Stream<DynamicTest> each(List<Map<String, Object>> list, String section, Case c) {
        int[] i = {0};
        return list.stream().map(v -> {
            String name = section + "/" + (v.get("name") != null ? v.get("name") : "#" + i[0]);
            i[0]++;
            return DynamicTest.dynamicTest(name, () -> c.run(v));
        });
    }

    interface Case { void run(Map<String, Object> v) throws Exception; }

    @TestFactory Stream<DynamicTest> draw() throws Exception {
        Map<String, Object> f = file("draw.json");
        return Stream.of(
                each(cases(f, "splitmix64"), "splitmix64", v -> {
                    Source s = new Source(Map.of("seed", v.get("seed")));
                    for (Object w : (List<?>) v.get("first")) assertEquals(w, String.format("%016x", s.nextLong()));
                }),
                each(cases(f, "values"), "values", v -> {
                    Source s = new Source(obj(v.get("source")));
                    long[] segs = DefinitionJson.segments(v.get("t"), "t");
                    for (Object w : (List<?>) v.get("draws")) assertEquals(num(w), new Sampler(s).draw(segs));
                    assertEquals(num(v.get("consumed")), s.n);
                }),
                each(cases(f, "prob"), "prob", v -> {
                    Source s = new Source(obj(v.get("source")));
                    assertEquals(v.get("result"), new Sampler(s).prob(num(v.get("p"))));
                    assertEquals(num(v.get("consumed")), s.n);
                }),
                each(cases(f, "jitter"), "jitter", v -> {
                    Source s = new Source(obj(v.get("source")));
                    assertEquals(num(v.get("result")), new Sampler(s).jitter(num(v.get("value")), num(v.get("jitter_ppm"))));
                    assertEquals(num(v.get("consumed")), s.n);
                }),
                each(cases(f, "compile_order"), "compile_order", v -> {
                    Source s = new Source(obj(v.get("source")));
                    var c = Bundle.Candidate.read(v.get("candidate"), tables(v.get("tables")));
                    var kind = c.compile(new Sampler(s), MachineOptions.defaults().withNewSource(() -> s));
                    assertEquals(((List<?>) v.get("shared")).stream().map(VectorsTest::num).toList(), compiledShared(kind));
                    assertEquals(num(v.get("consumed")), s.n);
                })).flatMap(x -> x);
    }

    /** Shared values in order, the pool's after the others'. */
    static List<Long> compiledShared(Bundle.Kind k) {
        List<Long> out = new ArrayList<>(k.shared);
        if (k.pool != null) out.addAll(k.pool.sharedInOrder());
        return out;
    }

    static String category(DefinitionException e) { return e.category().id(); }

    @TestFactory Stream<DynamicTest> format() throws Exception {
        Map<String, Object> f = file("format.json");
        return Stream.concat(each(cases(f, "bundles"), "bundles", v -> {
            MachineOptions o = MachineOptions.defaults().withFeatures(Set.copyOf(((List<?>) v.get("features")).stream().map(x -> (String) x).toList()))
                    .withAllowUnshaped(Boolean.TRUE.equals(v.get("allow_unshaped")));
            byte[] raw = ((String) v.get("bundle")).getBytes(StandardCharsets.UTF_8);
            if (v.get("error") != null) {
                assertEquals(v.get("error"), category(assertThrows(DefinitionException.class, () -> Bundle.parse(raw, o))));
                return;
            }
            Bundle b = Bundle.parse(raw, o);
            Map<String, Integer> want = new HashMap<>();
            obj(v.get("selected")).forEach((k, x) -> want.put(k, x == null ? null : (int) num(x)));
            assertEquals(want, b.selected());
            assertEquals(v.get("unshaped"), b.unshaped());
        }), each(cases(f, "handshake"), "handshake", v -> {
            byte[] raw = ((String) v.get("bundle")).getBytes(StandardCharsets.UTF_8);
            Bundle b = Bundle.parse(raw, MachineOptions.defaults());
            String resolved = b.resolve((String) v.get("kind"));
            assertEquals(v.get("resolved"), resolved);
            // One kind compiled alone from the case's source.
            var kinds = obj(obj(DefinitionJson.parse((String) v.get("bundle"))).get("kinds"));
            Object cand = ((List<?>) kinds.get(resolved)).get(0);
            Source s = new Source(obj(v.get("source")));
            var kind = Bundle.Candidate.read(cand, tables(null)).compile(new Sampler(s), MachineOptions.defaults().withNewSource(() -> s));
            assertEquals(((List<?>) v.get("shared")).stream().map(VectorsTest::num).toList(), kind.shared);
            for (Map<String, Object> call : cases(v, "calls")) {
                var p = kind.plan((String) call.get("handshake"), (String) call.get("message"));
                var want = obj(call.get("plan"));
                if (want == null) {
                    assertNull(p, call.toString());
                } else {
                    assertEquals(num(want.get("pad_to")), p.padTo(), call.toString());
                    assertEquals(num(want.get("pad_multiple")), p.padMultiple(), call.toString());
                    assertEquals(num(want.get("pad")), p.pad(), call.toString());
                    assertEquals(num(want.get("delay")) * 1000, p.delay() == null ? 0 : p.delay().toNanos(), call.toString());
                }
                assertEquals(num(call.get("consumed")), s.n, call.toString());
            }
        }));
    }

    @TestFactory Stream<DynamicTest> machine() throws Exception {
        Map<String, Object> f = file("machine.json");
        return Stream.concat(each(cases(f, "run"), "run", VectorsTest::run), each(cases(f, "invalid"), "invalid", v -> {
            Object tables = obj(v.get("definition")).get("tables");
            assertEquals(v.get("error"), category(assertThrows(DefinitionException.class,
                    () -> DefinitionJson.machines(obj(v.get("definition")).get("machines"), tables(tables != null ? tables : v.get("tables"))))));
        }));
    }

    static void run(Map<String, Object> v) throws Exception {
        Source s = new Source(obj(v.get("source")));
        Definition d = DefinitionJson.machines(obj(v.get("definition")).get("machines"), tables(v.get("tables")));
        var compiled = new Machines.Compiled(d, new Sampler(s), MachineOptions.defaults().withNewSource(() -> s));
        var want = obj(v.get("compile"));
        List<Long> shared = new ArrayList<>();
        for (long x : compiled.shared) shared.add(x);
        assertEquals(((List<?>) want.get("shared")).stream().map(VectorsTest::num).toList(), shared);
        assertEquals(num(want.get("consumed")), s.n);
        Set<String> caps = new TreeSet<>();
        for (Capability c : compiled.caps) caps.add(c.name().toLowerCase(Locale.ROOT));
        assertEquals(new TreeSet<>((List<?>) want.get("caps")), caps);
        List<Machines.MachineShaper> pipes = new ArrayList<>();
        for (int i = 0; i < num(v.get("pipes")); i++) pipes.add(compiled.newShaper());
        int machines = d.machines.size();
        for (Map<String, Object> st : cases(v, "steps")) {
            var sh = pipes.get((int) num(st.get("pipe")));
            long now = num(st.get("now")) * 1000;
            String op = (String) st.get("op");
            Effects e;
            if (op.equals("plan")) {
                RecordPlan p = new RecordPlan();
                sh.plan(new RecordView(io.zmux.PipeId.BOOTSTRAP, 0, 0, 65535, true, now, meters(st)), p);
                var w = obj(st.get("plan"));
                String what = st.toString();
                assertEquals(num(w.get("max_len")), p.maxLength(), what);
                assertEquals(num(w.get("pad_to")), p.padTo(), what);
                assertEquals(num(w.get("pad_multiple")), p.padMultiple(), what);
                assertEquals(num(w.get("pad")), p.pad(), what);
                assertEquals(num(w.get("delay")) * 1000, p.delayNanos(), what);
                assertEquals(w.get("flush"), p.isFlush(), what);
                assertEquals(w.get("stop"), p.isStop(), what);
                e = p.effects();
            } else {
                e = new Effects();
                int len = (int) num(st.get("len"));
                Event base = switch (op) {
                    case "start" -> event(EventKind.START, now, 0, false);
                    case "record_in" -> event(EventKind.RECORD_IN, now, len, false);
                    case "padding_in" -> event(EventKind.RECORD_IN, now, len, true);
                    case "injected" -> event(EventKind.INJECTED, now, len, false);
                    case "inject_dropped" -> event(EventKind.INJECT_DROPPED, now, 0, false);
                    case "wake" -> event(EventKind.WAKE, now, 0, false);
                    case "stream_open" -> event(EventKind.STREAM_OPEN, now, 0, false);
                    case "stream_end" -> event(EventKind.STREAM_END, now, 0, false);
                    default -> throw new AssertionError(op);
                };
                sh.onEvent(new Event(base.kind(), io.zmux.PipeId.BOOTSTRAP, 0, now, len, base.paddingOnly(), 0, meters(st)), e);
            }
            assertEquals(st.get("effects"), effects(e), st.toString());
            assertEquals(num(st.get("consumed")), s.n, st.toString());
            List<Long> state = new ArrayList<>();
            List<List<String>> ctrs = new ArrayList<>();
            for (int m = 0; m < machines; m++) {
                state.add((long) sh.state(m));
                List<String> c = new ArrayList<>();
                for (long x : sh.counters(m)) c.add(Long.toUnsignedString(x));
                ctrs.add(c);
            }
            assertEquals(st.get("state"), state, st.toString());
            List<List<String>> wantC = new ArrayList<>();
            for (Object m : (List<?>) st.get("counters")) wantC.add(((List<?>) m).stream().map(Object::toString).toList());
            assertEquals(wantC, ctrs, st.toString());
        }
    }

    private static ShapingMeters meters(Map<String, Object> step) {
        var m = step.get("meters") == null ? Map.<String, Object>of() : obj(step.get("meters"));
        return new ShapingMeters(m.containsKey("wire_left") ? num(m.get("wire_left")) : -1L, m.containsKey("stall_left") ? num(m.get("stall_left")) : -1L,
                num(m.get("wire_spent")), 0, num(m.get("sent_bytes")));
    }

    @TestFactory Stream<DynamicTest> poolPolicy() throws Exception {
        return each(cases(file("pool_policy.json"), "decide"), "decide", v -> {
            Source s = new Source(obj(v.get("source")));
            var policy = new MachinePoolPolicy(MachinePoolPolicy.Spec.read(v.get("policy"), tables(v.get("tables"))), new Sampler(s), new Sampler(s));
            var compile = obj(v.get("compile"));
            assertEquals(((List<?>) compile.get("shared")).stream().map(VectorsTest::num).toList(), policy.sharedInOrder());
            assertEquals(num(compile.get("consumed")), s.n);
            for (Map<String, Object> st : cases(v, "steps")) {
                List<MachinePoolPolicy.Metrics> view = new ArrayList<>();
                if (st.get("members") != null) {
                    for (Map<String, Object> m : cases(st, "members")) {
                        view.add(new MachinePoolPolicy.Metrics(num(m.get("id")), num(m.get("age")), num(m.get("idle")), num(m.get("pipes")), num(m.get("bytes_sent")), num(m.get("bytes_received"))));
                    }
                }
                var r = policy.decide((int) num(st.get("failures")), st.get("since_dial") == null ? null : num(st.get("since_dial")), view);
                var w = obj(st.get("want"));
                String what = st.toString();
                assertEquals(optional(w.get("count")), r.count(), what);
                assertEquals(optional(w.get("dial_after")), r.dialAfter(), what);
                assertEquals(((List<?>) w.get("retire")).stream().map(VectorsTest::num).toList(), r.retire(), what);
                assertEquals(optional(w.get("probe_interval")), r.probeInterval(), what);
                assertEquals(optional(w.get("probe_jitter")), r.probeJitter(), what);
                assertEquals(optional(w.get("recheck")), r.recheck(), what);
                assertEquals(num(st.get("consumed")), s.n, what);
            }
        });
    }

    private static Long optional(Object x) { return x == null ? null : num(x); }

    private static Map<String, Object> wire(WireAccount a) {
        return Map.of("initial", a.initial(), "per_mb", a.perMb(), "per_sec", a.perSec());
    }

    /** A bound in the vectors' microseconds; none is -1. */
    private static long micros(java.time.Duration d) { return d == null ? -1 : d.toNanos() / 1000; }

    @TestFactory Stream<DynamicTest> localLimits() throws Exception {
        return each(cases(file("budget.json"), "parse"), "parse", v -> {
            if (v.get("error") != null) {
                assertEquals(v.get("error"), category(assertThrows(DefinitionException.class, () -> DefinitionJson.limits(v.get("limits")))));
                return;
            }
            ShapingLimits l = DefinitionJson.limits(v.get("limits"));
            Map<String, Object> got = new HashMap<>();
            got.put("wire", wire(l.wire()));
            // The format writes the default bound as 0.
            Object asked = obj(v.get("limits")).get("max_delay");
            boolean unset = asked == null || num(asked) == 0;
            got.put("max_delay", unset && ShapingLimits.DEFAULT_MAX_DELAY.equals(l.maxDelay()) ? 0L : micros(l.maxDelay()));
            got.put("no_padding", !l.padding());
            got.put("no_pause", !l.pause());
            got.put("no_injection", !l.injection());
            assertEquals(v.get("want"), got);
        });
    }

    private static WireAccount account(Object x) {
        var a = obj(x);
        return new WireAccount(num(a.get("initial")), num(a.get("per_mb")), num(a.get("per_sec")));
    }

    @TestFactory Stream<DynamicTest> merges() throws Exception {
        return each(cases(file("budget.json"), "merge"), "merge", v -> {
            var local = obj(v.get("local"));
            var config = obj(v.get("config"));
            long lm = num(local.get("max_delay")), bm = num(config.get("max_delay"));
            ShapingLimits l = ShapingLimits.defaults().withWire(account(local.get("wire")));
            if (lm < 0) l = l.withNoMaxDelay(); else if (lm > 0) l = l.withMaxDelay(java.time.Duration.ofNanos(lm * 1000));
            ShapingLimits in = l.under(new ShapingBudget(account(config.get("wire")), bm > 0 ? java.time.Duration.ofNanos(bm * 1000) : null, true));
            var want = obj(v.get("want"));
            assertEquals(account(want.get("wire")), in.wire(), v.toString());
            assertEquals(Boolean.TRUE.equals(want.get("no_injection")), !in.injection(), v.toString());
            assertEquals(num(want.get("max_delay")), micros(in.maxDelay()), v.toString());
        });
    }

    @TestFactory Stream<DynamicTest> sessionBudgets() throws Exception {
        return each(cases(file("format.json"), "budget"), "budget", v -> {
            Source s = new Source(obj(v.get("source")));
            var b = DefinitionJson.budget(v.get("budget"), tables(v.get("tables")));
            Sampler sampler = new Sampler(s);
            for (Map<String, Object> d : cases(v, "draws")) {
                ShapingBudget got = b.draw(sampler);
                assertEquals(account(d.get("wire")), got.wire(), d.toString());
                assertEquals(d.get("no_injection"), !got.injection(), d.toString());
                assertEquals(num(d.get("max_delay")), got.maxDelay() == null ? 0 : micros(got.maxDelay()), d.toString());
                assertEquals(num(d.get("consumed")), s.n, d.toString());
            }
        });
    }

    static Event event(EventKind k, long now, int len, boolean padOnly) { return new Event(k, io.zmux.PipeId.BOOTSTRAP, 0, now, len, padOnly, 0); }

    /** Effects in the form of the cases; times in microseconds. */
    static Map<String, Object> effects(Effects e) {
        Map<String, Object> m = new HashMap<>();
        if (e.isInjectSet()) {
            Map<String, Object> i = e.isInjectNone() ? null : new HashMap<>(Map.of("at", e.injectAtNanos() / 1000, "len", (long) e.injectLength()));
            m.put("inject", i);
        }
        if (e.isWakeSet()) m.put("wake", e.isWakeNone() ? null : (Object) (e.wakeAtNanos() / 1000));
        if (e.isGateSet()) m.put("gate", new HashMap<>(Map.of("pause_until", e.gatePauseUntilNanos() / 1000, "rate", e.gateRate(), "burst", e.gateBurst())));
        if (e.isWeightSet()) m.put("weight", e.weight());
        if (e.isStopRequested()) m.put("stop", true);
        return m;
    }
}
