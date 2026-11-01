package io.zmux.shaping.machine;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.shaping.*;
import io.zmux.shaping.machine.Definition.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;
import java.util.random.RandomGeneratorFactory;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class MachinesTest {
    static final Path CONTRACT = Path.of("..", "zmux", "src", "test", "resources", "shaping", "contract.json");

    static Options seeded() {
        Supplier<RandomGenerator> s = () -> RandomGeneratorFactory.of("L64X128MixRandom").create(12);
        return Options.defaults().withNewSource(s);
    }

    static Value cv(double x) { return Value.of(x); }
    static Value range(double a, double b, Scope s, Double min, Double max) { return new Value(null, new double[] {a, b}, null, s, min, max); }
    static State rec(RecordRule r, List<Transition> next) { return new State(r, List.of(), next); }
    static RecordRule pad(Value v) { return new RecordRule(null, null, null, v, null, false); }
    static Definition def(Machine... ms) { return new Definition(1, List.of(ms)); }
    static Machine m(State... st) { return new Machine(0, List.of(st)); }
    static Event ev(EventKind k) { return new Event(k, 0, 0, 0, 0, false, 0, 0); }

    /** The plans of the first n records of a pipe. */
    static List<RecordPlan> plans(Definition d, Options o, int n) throws DefinitionException {
        Shaper s = Machines.factory(d, o).create(new io.zmux.shaping.ShapedPipe(0, null));
        s.onEvent(ev(EventKind.START), new Reply());
        List<RecordPlan> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            RecordPlan p = new RecordPlan();
            s.plan(new RecordView(0, 0, i, 16382, true, 0), p);
            out.add(p);
        }
        return out;
    }

    static String json(Object o) {
        if (o == null) return "null";
        if (o instanceof String s) return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
        if (o instanceof Map<?, ?> m) {
            StringJoiner j = new StringJoiner(",", "{", "}");
            m.forEach((k, v) -> j.add(json(k) + ":" + json(v)));
            return j.toString();
        }
        if (o instanceof List<?> l) {
            StringJoiner j = new StringJoiner(",", "[", "]");
            for (Object v : l) j.add(json(v));
            return j.toString();
        }
        return String.valueOf(o);
    }

    /** Whether a vector expects success: its ok member, else no error member. */
    static boolean wantOk(Map<?, ?> v) { return v.get("ok") != null ? (Boolean) v.get("ok") : v.get("error") == null; }

    /** Checks that e has the vector's error substring, case-insensitively. */
    static void assertError(Map<?, ?> v, Exception e) {
        if (v.get("error") == null) return;
        String want = ((String) v.get("error")).toLowerCase(java.util.Locale.ROOT);
        assertTrue(e.getMessage().toLowerCase(java.util.Locale.ROOT).contains(want), "want \"" + want + "\" in: " + e.getMessage());
    }

    static int num(Map<?, ?> m, String k) { Object v = m.get(k); return v == null ? 0 : ((Number) v).intValue(); }

    @SuppressWarnings("unchecked")
    @Test void defaultSourceSeedsFullState() {
        byte[] a = Sampler.freshSeed();
        assertEquals(48, a.length);
        assertFalse(Arrays.equals(a, Sampler.freshSeed()));
        // Every seed byte reaches the state: same seed repeats, any single-byte change diverges.
        // (Bit 0 of the LCG additive word is forced odd, so flip the top bit of each byte.)
        long first = Sampler.seeded(a).nextLong();
        assertEquals(first, Sampler.seeded(a.clone()).nextLong());
        for (int i = 0; i < a.length; i++) {
            byte[] b = a.clone();
            b[i] ^= (byte) 0x80;
            RandomGenerator x = Sampler.seeded(a), y = Sampler.seeded(b);
            boolean differs = false;
            for (int k = 0; k < 8 && !differs; k++) differs = x.nextLong() != y.nextLong();
            assertTrue(differs, "seed byte " + i + " ignored");
        }
    }

    @TestFactory Collection<DynamicTest> contractDefinitions() throws Exception {
        var root = (Map<?, ?>) DefinitionJson.parse(Files.readString(CONTRACT));
        List<DynamicTest> out = new ArrayList<>();
        for (Object o : (List<?>) root.get("definitions")) {
            var v = (Map<?, ?>) o;
            out.add(DynamicTest.dynamicTest((String) v.get("name"), () -> {
                Definition d = null;
                DefinitionException err = null;
                try {
                    d = v.get("sequence") != null ? Sequence.parse((String) v.get("sequence"))
                            : Definition.parse(v.get("raw") != null ? (String) v.get("raw") : json(v.get("json")));
                }
                catch (DefinitionException e) { err = e; }
                if (!wantOk(v)) {
                    assertNotNull(err, "no error");
                    assertError(v, err);
                    return;
                }
                if (err != null) throw err;
                var want = (List<?>) v.get("plans");
                if (want == null) return;
                var got = plans(d, seeded(), want.size());
                for (int i = 0; i < want.size(); i++) {
                    var w = (Map<?, ?>) want.get(i);
                    RecordPlan p = got.get(i);
                    String g = p.maxLength() + " " + p.padTo() + " " + p.padMultiple() + " " + p.pad() + " " + p.delayNanos() + " " + p.flush() + " " + p.stop();
                    String e = num(w, "max_len") + " " + num(w, "pad_to") + " " + num(w, "pad_multiple") + " " + num(w, "pad") + " "
                            + (w.get("delay_ns") == null ? 0 : ((Number) w.get("delay_ns")).longValue()) + " "
                            + Boolean.TRUE.equals(w.get("flush")) + " " + Boolean.TRUE.equals(w.get("stop"));
                    assertEquals(e, g, "plan " + i);
                }
            }));
        }
        return out;
    }

    @Test void counterTransition() throws Exception {
        var d = def(m(new State(pad(cv(1)), List.of(new Action(Op.SET, false, 0, null, null, null, null, cv(2))),
                        List.of(new Transition(Trigger.RECORD_OUT, 1, null, null))),
                new State(pad(cv(1)), List.of(new Action(Op.SUB, false, 0, null, null, null, null, cv(1))),
                        List.of(new Transition(Trigger.ZERO0, 2, null, null), new Transition(Trigger.RECORD_OUT, 1, null, null))),
                new State(null, null, null)));
        assertEquals(List.of(1, 1, 0, 0, 0), plans(d, Options.defaults(), 5).stream().map(RecordPlan::pad).toList());
    }

    @Test void scopeBoundsAndSeeds() throws Exception {
        var pipe = range(0, 1000, Scope.PIPE, null, null);
        var use = range(0, 1000, Scope.EACH, null, null);
        var bounded = range(0, 1000, Scope.EACH, 10.0, 20.0);
        var d = def(m(rec(pad(pipe), null)), m(rec(new RecordRule(null, use, bounded, null, null, false), null)));
        var ps = plans(d, seeded(), 20);
        boolean varies = false;
        for (var p : ps) {
            assertEquals(ps.getFirst().pad(), p.pad(), "pipe-scoped value changed");
            varies |= p.padTo() != ps.getFirst().padTo();
            assertTrue(p.padMultiple() >= 10 && p.padMultiple() <= 20, "block " + p.padMultiple());
        }
        assertTrue(varies, "per-use value constant");
        var again = plans(d, seeded(), 20);
        for (int i = 0; i < ps.size(); i++) assertEquals(ps.get(i).padTo(), again.get(i).padTo(), "same seed, different plans");
    }

    @Test void sharedScopeIsOnePerFactory() throws Exception {
        var v = range(0, 1e6, Scope.SHARED, null, null);
        var f = Machines.factory(def(m(rec(pad(v), null))), Options.defaults());
        RecordPlan a = new RecordPlan(), b = new RecordPlan();
        f.create(new io.zmux.shaping.ShapedPipe(0, null)).plan(new RecordView(0, 0, 0, 100, true, 0), a);
        f.create(new io.zmux.shaping.ShapedPipe(1, null)).plan(new RecordView(1, 0, 0, 100, true, 0), b);
        assertEquals(a.pad(), b.pad());
    }

    @Test void enterActionsFillTheReply() throws Exception {
        var d = def(m(new State(null, List.of(new Action(Op.INJECT, false, 0, cv(0.001), cv(9), null, null, null),
                new Action(Op.RATE, false, 0, null, null, cv(100), cv(50), null),
                new Action(Op.TIMER, false, 1, cv(0.003), null, null, null, null),
                new Action(Op.WEIGHT, false, 0, null, null, null, null, cv(2))), null)));
        var r = new Reply();
        Machines.factory(d, Options.defaults()).create(new io.zmux.shaping.ShapedPipe(0, null)).onEvent(ev(EventKind.START), r);
        assertTrue(r.hasInject() && !r.injectCancelled() && r.injectAfterNanos() == 1_000_000 && r.injectLength() == 9);
        assertTrue(r.hasGate() && r.gateRate() == 100 && r.gateBurst() == 50 && r.gatePauseNanos() == 0);
        assertTrue(!r.hasTimer(0) && r.hasTimer(1) && r.timerAfterNanos(1) == 3_000_000);
        assertTrue(r.hasWeight() && r.weight() == 2 && !r.stopRequested());
        d = def(m(new State(null, List.of(new Action(Op.INJECT, true, 0, null, null, null, null, null),
                new Action(Op.TIMER, true, 0, null, null, null, null, null)), null)));
        r = new Reply();
        Machines.factory(d, Options.defaults()).create(new io.zmux.shaping.ShapedPipe(0, null)).onEvent(ev(EventKind.START), r);
        assertTrue(r.hasInject() && r.injectCancelled() && r.hasTimer(0) && r.timerCancelled(0) && !r.hasGate() && !r.hasWeight());
    }

    @Test void bagDrawsWithoutReplacement() throws Exception {
        var v = new Value(null, null, new Dist(DistKind.BAG, null, new double[] {1, 2, 3}, null, null), Scope.EACH, null, null);
        var ps = plans(def(m(rec(pad(v), null))), seeded(), 6);
        for (int k = 0; k < 6; k += 3) {
            Set<Integer> seen = new HashSet<>();
            for (var p : ps.subList(k, k + 3)) seen.add(p.pad());
            assertEquals(3, seen.size());
        }
    }

    /** Binomial trials and the Poisson mean are bounded by 2^53; the samplers take O(log n) steps up to it. */
    @Test void negativeCapableAmountsNeedMin() throws Exception {
        var normal = new Dist(DistKind.NORMAL, new double[] {100, 50}, null, null, null);
        Value bare = new Value(null, null, normal, Scope.EACH, null, null), neg = new Value(null, null, normal, Scope.EACH, -1.0, null),
                ok = new Value(null, null, normal, Scope.EACH, 0.0, null), rateOk = new Value(null, null, normal, Scope.EACH, 1.0, null);
        for (Value[] rb : new Value[][] {{bare, null}, {neg, null}, {ok, null}, {rateOk, bare}, {rateOk, neg}}) {
            var d = def(m(new State(null, List.of(new Action(Op.RATE, false, 0, null, null, rb[0], rb[1], null)), null)));
            assertThrows(DefinitionException.class, () -> Machines.factory(d, Options.defaults()));
        }
        Machines.factory(def(m(new State(null, List.of(new Action(Op.RATE, false, 0, null, null, rateOk, ok, null)), null))), Options.defaults());
        assertThrows(DefinitionException.class, () -> Machines.factory(def(m(rec(pad(bare), null))), Options.defaults()));
    }

    @Test void countDistributionBounds() throws Exception {
        double max = 0x1p53;
        Object[][] cases = {
            {DistKind.BINOMIAL, new double[] {max, 0.5}, true}, {DistKind.BINOMIAL, new double[] {2 * max, 0.5}, false},
            {DistKind.POISSON, new double[] {max}, true}, {DistKind.POISSON, new double[] {2 * max}, false},
        };
        for (Object[] c : cases) {
            var v = new Value(null, null, new Dist((DistKind) c[0], (double[]) c[1], null, null, null), Scope.EACH, null, 100.0);
            var d = def(m(rec(pad(v), null)));
            if ((Boolean) c[2]) {
                for (var p : plans(d, seeded(), 20)) assertEquals(100, p.pad());
            } else {
                assertThrows(DefinitionException.class, () -> Machines.factory(d, Options.defaults()));
            }
        }
    }

    /** The large-n samplers keep the mean and variance: Binomial(n, p) and Poisson(mean). */
    @Test void countDistributionMoments() throws Exception {
        Object[][] cases = {{DistKind.BINOMIAL, new double[] {100_000, 0.3}, 30_000.0, 21_000.0}, {DistKind.POISSON, new double[] {50_000}, 50_000.0, 50_000.0}};
        for (Object[] c : cases) {
            var v = new Value(null, null, new Dist((DistKind) c[0], (double[]) c[1], null, null, null), Scope.EACH, null, null);
            int n = 4000;
            double sum = 0, sq = 0;
            for (var p : plans(def(m(rec(pad(v), null))), seeded(), n)) { sum += p.pad(); sq += (double) p.pad() * p.pad(); }
            double mean = sum / n, var = (sq - n * mean * mean) / (n - 1), m = (double) c[2], vv = (double) c[3];
            assertTrue(Math.abs(mean - m) < 5 * Math.sqrt(vv / n), c[0] + " mean " + mean);
            assertTrue(Math.abs(var / vv - 1) < 0.15, c[0] + " variance " + var);
        }
    }

    @Test void distributionsStayInRange() throws Exception {
        Object[][] cases = {
            {DistKind.UNIFORM, new double[] {2, 3}}, {DistKind.LOGUNIFORM, new double[] {2, 3}}, {DistKind.EXPONENTIAL, new double[] {1}},
            {DistKind.NORMAL, new double[] {5, 1}}, {DistKind.LOGNORMAL, new double[] {0, 1}}, {DistKind.WEIBULL, new double[] {1, 2}},
            {DistKind.PARETO, new double[] {1, 2}}, {DistKind.GAMMA, new double[] {0.5, 1}}, {DistKind.BETA, new double[] {2, 2}},
            {DistKind.GEOMETRIC, new double[] {0.5}}, {DistKind.BINOMIAL, new double[] {4, 0.5}}, {DistKind.POISSON, new double[] {40}},
        };
        List<Dist> ds = new ArrayList<>();
        for (Object[] c : cases) ds.add(new Dist((DistKind) c[0], (double[]) c[1], null, null, null));
        ds.add(new Dist(DistKind.EMPIRICAL, null, new double[] {1, 2}, new double[] {0, 1}, null));
        ds.add(new Dist(DistKind.MIXTURE, null, null, new double[] {1}, List.of(cv(4))));
        for (Dist d : ds) {
            var v = new Value(null, null, d, Scope.EACH, d.kind() == DistKind.NORMAL ? 0.0 : null, null); // a pad needs min >= 0
            for (var p : plans(def(m(rec(pad(v), null))), seeded(), 50)) {
                int x = p.pad();
                boolean bad = switch (d.kind()) {
                    case UNIFORM, LOGUNIFORM -> x < 2 || x > 3;
                    case BETA -> x > 1;
                    case BINOMIAL -> x > 4;
                    case GEOMETRIC -> x < 1;
                    case EMPIRICAL -> x != 2;
                    case MIXTURE -> x != 4;
                    default -> x < 0;
                };
                assertFalse(bad, d.kind() + ": " + x);
            }
        }
    }

    @Test void sequenceInjectsAfterTheNextReceivedRecord() throws Exception {
        Shaper s = Machines.factory(Sequence.parse("10/inject=2:30, 20"), Options.defaults()).create(new io.zmux.shaping.ShapedPipe(0, null));
        s.onEvent(ev(EventKind.START), new Reply());
        RecordPlan p = new RecordPlan();
        s.plan(new RecordView(0, 0, 0, 100, true, 0), p);
        assertTrue(p.padTo() == 10 && !p.reply().hasInject());
        int n = 0;
        for (EventKind k = EventKind.RECORD_IN; ; k = EventKind.INJECTED) {
            Reply r = new Reply();
            s.onEvent(ev(k), r);
            if (!r.hasInject()) break;
            assertTrue(r.injectLength() == 30 && r.injectAfterNanos() == 0);
            n++;
        }
        assertEquals(2, n);
        p = new RecordPlan();
        s.plan(new RecordView(0, 0, 1, 100, true, 0), p);
        assertTrue(p.padTo() == 20 && p.stop() && !p.reply().stopRequested(), "the stop follows the record");
    }

    @Test void theWriteOverheadComesOffTheFirstRecordOfAWriteOnly() throws Exception {
        var f = Machines.factory(Sequence.parse("100, 100, 100"), Options.defaults());
        Shaper s = f.create(new io.zmux.shaping.ShapedPipe(0, null, 30));
        s.onEvent(ev(EventKind.START), new Reply());
        RecordPlan p = new RecordPlan();
        s.plan(new RecordView(0, 0, 0, 1000, true, 0), p);
        assertEquals(70, p.padTo());
        p = new RecordPlan();
        s.plan(new RecordView(0, 0, 1, 1000, false, 0), p);
        assertEquals(100, p.padTo(), "a later record of the same write");
        Shaper none = f.create(new io.zmux.shaping.ShapedPipe(1, null));
        none.onEvent(ev(EventKind.START), new Reply());
        p = new RecordPlan();
        none.plan(new RecordView(1, 0, 0, 1000, true, 0), p);
        assertEquals(100, p.padTo(), "no overhead: as defined");
    }

    @Test void sizeLimitsApply() throws Exception {
        var d = def(new Machine(0, List.of(new State(null, List.of(new Action(Op.WEIGHT, false, 0, null, null, null, null, cv(1)),
                new Action(Op.WEIGHT, false, 0, null, null, null, null, cv(1))), List.of(new Transition(Trigger.RECORD_OUT, 1, null, null))),
                new State(null, null, null))));
        assertThrows(DefinitionException.class, () -> Machines.factory(d, Options.defaults().withLimits(SizeLimits.NONE.withMaxStates(1))));
        assertThrows(DefinitionException.class, () -> Machines.factory(d, Options.defaults().withLimits(SizeLimits.NONE.withMaxActions(1))));
        assertNotNull(Machines.factory(d, Options.defaults()));
        assertThrows(DefinitionException.class, () -> Machines.factory(d, Options.defaults().withLimits(SizeLimits.NONE.withMaxMachines(0))),
                "a bound of 0 allows none");
        assertNotNull(Machines.factory(d, Options.defaults().withLimits(SizeLimits.NONE.withMaxTransitions(1))));
        assertThrows(IllegalArgumentException.class, () -> SizeLimits.NONE.withMaxPoints(-1));
        assertThrows(DefinitionException.class, () -> Machines.factory("{\"version\":1,\"machines\":[{\"states\":[{}]}]}",
                Options.defaults().withLimits(SizeLimits.NONE.withMaxBytes(8))));
    }

    @Test void streamTriggersAskForStreamEvents() throws Exception {
        var d = def(m(new State(null, null, List.of(new Transition(Trigger.STREAM_OPEN, 1, null, null))), rec(pad(cv(5)), null)));
        Shaper s = Machines.factory(d, Options.defaults()).create(new io.zmux.shaping.ShapedPipe(0, null));
        assertTrue(s.capabilities().contains(Capability.STREAMS) && !s.capabilities().contains(Capability.RECORD_IN));
        s.onEvent(ev(EventKind.START), new Reply());
        s.onEvent(new Event(EventKind.STREAM_OPEN, 0, 0, 0, 0, false, 0, 4), new Reply());
        RecordPlan p = new RecordPlan();
        s.plan(new RecordView(0, 0, 0, 100, true, 0), p);
        assertEquals(5, p.pad());
    }

    @Test void negativeAmountsFailTheDefinition() {
        String[] bad = {
            "{\"record\":{\"pad\":{\"const\":-1}}}",
            "{\"record\":{\"max_len\":{\"range\":[-5,10]}}}",
            "{\"record\":{\"delay\":{\"const\":1,\"min\":-1}}}",
            "{\"enter\":[{\"op\":\"inject\",\"after\":{\"const\":1},\"len\":{\"const\":-100}}]}",
            "{\"enter\":[{\"op\":\"pause\",\"after\":{\"const\":-0.5}}]}",
            "{\"enter\":[{\"op\":\"rate\",\"rate\":{\"const\":-1000}}]}",
            "{\"record\":{\"pad\":{\"dist\":{\"kind\":\"bag\",\"points\":[1,-2]}}}}",
        };
        for (String st : bad) {
            String json = "{\"version\":1,\"machines\":[{\"states\":[" + st + "]}]}";
            assertThrows(DefinitionException.class, () -> Machines.factory(json, Options.defaults()), st);
        }
        var nan = new Value(1.0, null, null, Scope.EACH, Double.NaN, null);
        assertThrows(DefinitionException.class, () -> Machines.factory(def(m(rec(pad(nan), null))), Options.defaults()));
    }

    @Test void padsOfSeveralMachinesSumWithoutOverflow() throws Exception {
        var big = cv(Integer.MAX_VALUE);
        var p = plans(def(m(rec(pad(big), null)), m(rec(pad(big), null))), seeded(), 1).getFirst();
        assertEquals(Integer.MAX_VALUE, p.pad());
    }
}
