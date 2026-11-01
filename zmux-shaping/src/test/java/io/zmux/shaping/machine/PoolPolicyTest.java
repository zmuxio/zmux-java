package io.zmux.shaping.machine;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.*;
import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.shaping.machine.MachinePoolPolicy.Metrics;
import io.zmux.shaping.machine.MachinePoolPolicy.Result;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.Timeout;

@Timeout(40)
class PoolPolicyTest {
    static final Path VECTORS = Path.of("..", "zmux", "src", "test", "resources", "shaping", "pool_policy.json");

    static MachinePoolPolicy policy(String json, Options o) throws DefinitionException {
        return Machines.poolPolicy(json.getBytes(StandardCharsets.UTF_8), o);
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> vectors() throws Exception {
        var root = (Map<String, Object>) DefinitionJson.parse(Files.readString(VECTORS));
        assertEquals(3L, root.get("version"));
        return root;
    }

    static Double dbl(Object x) { return x == null ? null : ((Number) x).doubleValue(); }

    static double num(Map<String, Object> o, String k) { return o.get(k) == null ? 0 : ((Number) o.get(k)).doubleValue(); }

    @SuppressWarnings("unchecked")
    @TestFactory Stream<DynamicTest> parseVectors() throws Exception {
        return ((List<Object>) vectors().get("parse")).stream().map(x -> (Map<String, Object>) x).map(c ->
                DynamicTest.dynamicTest((String) c.get("name"), () -> {
                    String json = c.get("raw") != null ? (String) c.get("raw") : MachinesTest.json(c.get("policy"));
                    if (MachinesTest.wantOk(c)) policy(json, MachinesTest.seeded());
                    else MachinesTest.assertError(c, assertThrows(DefinitionException.class, () -> policy(json, MachinesTest.seeded())));
                }));
    }

    @SuppressWarnings("unchecked")
    @TestFactory Stream<DynamicTest> decideVectors() throws Exception {
        return ((List<Object>) vectors().get("decide")).stream().map(x -> (Map<String, Object>) x).map(c ->
                DynamicTest.dynamicTest((String) c.get("name"), () -> {
                    MachinePoolPolicy p = policy(MachinesTest.json(c.get("policy")), MachinesTest.seeded());
                    List<Result> got = new ArrayList<>();
                    for (Object s : (List<Object>) c.get("steps")) {
                        var step = (Map<String, Object>) s;
                        List<Metrics> ms = new ArrayList<>();
                        for (Object m : (List<Object>) step.getOrDefault("members", List.of())) {
                            var o = (Map<String, Object>) m;
                            ms.add(new Metrics((long) num(o, "id"), num(o, "age"), num(o, "idle"), num(o, "pipes"),
                                    num(o, "bytes_sent"), num(o, "bytes_received")));
                        }
                        Result r = p.decide((int) num(step, "failures"), dbl(step.get("since_dial")), ms);
                        got.add(r);
                        var want = (Map<String, Object>) step.getOrDefault("want", Map.of());
                        for (var e : want.entrySet()) {
                            Object w = e.getValue();
                            switch (e.getKey()) {
                                case "count" -> assertEquals(dbl(w), r.count());
                                case "dial_after" -> assertEquals(dbl(w), r.dialAfter());
                                case "retire" -> assertEquals(((List<Object>) w).stream().map(i -> ((Number) i).longValue()).toList(), r.retire());
                                case "count_in" -> within((List<Object>) w, r.count());
                                case "dial_after_in" -> within((List<Object>) w, r.dialAfter());
                                case "recheck" -> {
                                    if (w == null) assertNull(r.recheck());
                                    else { assertNotNull(r.recheck()); assertEquals(dbl(w), r.recheck(), 1e-6); }
                                }
                                case "recheck_in" -> {
                                    within((List<Object>) w, r.recheck());
                                    assertTrue(r.recheck() > 0);
                                }
                                case "same_as" -> assertEquals(got.get(((Number) w).intValue()), r);
                                case "dial_after_same_as" -> assertEquals(got.get(((Number) w).intValue()).dialAfter(), r.dialAfter());
                                case "dial_after_redrawn_from" -> assertNotEquals(got.get(((Number) w).intValue()).dialAfter(), r.dialAfter(),
                                        "a dial delay drawn anew after a dial");
                                case "probe_interval" -> assertEquals(dbl(w), r.probeInterval());
                                case "probe_interval_in" -> within((List<Object>) w, r.probeInterval());
                                case "probe_jitter" -> assertEquals(dbl(w), r.probeJitter());
                                default -> fail("unknown expectation " + e.getKey());
                            }
                        }
                    }
                }));
    }

    static void within(List<Object> bounds, Number x) {
        assertNotNull(x);
        assertTrue(x.doubleValue() >= dbl(bounds.get(0)) && x.doubleValue() <= dbl(bounds.get(1)), "out of bounds: " + x);
    }

    static List<Metrics> members(long... ids) {
        List<Metrics> out = new ArrayList<>();
        for (long id : ids) out.add(new Metrics(id, 15, 0, 1, 0, 0));
        return out;
    }

    @Test void pipeScopeIsDroppedWhenTheMemberLeaves() throws Exception {
        MachinePoolPolicy p = policy("{\"retire\":[{\"metric\":\"age\",\"above\":{\"range\":[0,30],\"scope\":\"pipe\"}}]}", MachinesTest.seeded());
        List<Long> first = null;
        for (int i = 0; i < 64; i++) {
            List<Long> r = p.decide(0, null, members(1, 2, 3, 4, 5, 6, 7, 8)).retire();
            if (first == null) first = r;
            assertEquals(first, r);
        }
        // Leaving the view drops the thresholds; on return they are drawn again.
        boolean changed = false;
        for (int i = 0; i < 64 && !changed; i++) {
            p.decide(0, null, List.of());
            changed = !p.decide(0, null, members(1, 2, 3, 4, 5, 6, 7, 8)).retire().equals(first);
        }
        assertTrue(changed, "pipe-scoped thresholds survived the members leaving the view");
    }

    @Test void eachScopeRedrawsAndSharedKeeps() throws Exception {
        MachinePoolPolicy p = policy("{\"count\":{\"range\":[1,1000]},\"dial_after\":[{\"range\":[0,1000],\"scope\":\"shared\"}]}", MachinesTest.seeded());
        Set<Double> counts = new HashSet<>();
        Set<Double> afters = new HashSet<>();
        for (int i = 0; i < 32; i++) {
            Result r = p.decide(i, null, List.of());
            counts.add(r.count());
            afters.add(r.dialAfter());
        }
        assertTrue(counts.size() > 1);
        assertEquals(1, afters.size());
    }

    @Test void fixedSeedIsDeterministic() throws Exception {
        String json = "{\"count\":{\"dist\":{\"kind\":\"poisson\",\"params\":[2.5]},\"min\":1},"
                + "\"dial_after\":[{\"dist\":{\"kind\":\"weibull\",\"params\":[8,1.5]}}],"
                + "\"retire\":[{\"metric\":\"age\",\"above\":{\"dist\":{\"kind\":\"gamma\",\"params\":[5,4]},\"scope\":\"pipe\"}}]}";
        MachinePoolPolicy a = policy(json, MachinesTest.seeded()), b = policy(json, MachinesTest.seeded());
        for (int i = 0; i < 16; i++) {
            List<Metrics> ms = members(1, 2, 3, i + 10);
            assertEquals(a.decide(i, null, ms), b.decide(i, null, ms));
        }
    }

    @Test void decisionMapsToThePool() throws Exception {
        MachinePoolPolicy p = policy("{\"count\":{\"const\":2},\"dial_after\":[{\"const\":0.25}],"
                + "\"retire\":[{\"metric\":\"bytes\",\"above\":{\"const\":10}}]}", Options.defaults());
        var d = p.decide(new PoolPolicy.View(List.of(
                new PoolPolicy.Member(4, Duration.ofSeconds(1), Duration.ZERO, 1, 6, 5, true, false),
                new PoolPolicy.Member(5, Duration.ofSeconds(1), Duration.ZERO, 1, 10, 0, true, false)), 0, null));
        assertEquals(PoolPolicy.Decision.KEEP.withCount(2).withDialAfter(Duration.ofMillis(250)).withRetire(Set.of(4L)), d);
    }

    @Test void pipePoolHonoursCountAndAgeRetire() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(8)
                    .withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO)).build();
            var listener = Zmux.listener(ListenerConfig.of(cfg));
            var io = MemoryPipe.pair(Framing.BYTE_STREAM);
            listener.handle(io[1], JoinHint.inBand(java.net.InetSocketAddress.createUnresolved("bootstrap", 0))).closed();
            ZmuxSession client = Zmux.open(io[0], Role.CLIENT, cfg), server = listener.accept(Duration.ofSeconds(3));
            PipePool.Dial dial = attempt -> {
                var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.handle(p[1], JoinHint.inBand(java.net.InetSocketAddress.createUnresolved("pool", 0))).closed();
                return attempt.join(p[0]);
            };
            PoolPolicy policy = policy("{\"count\":{\"const\":2},\"retire\":[{\"metric\":\"age\",\"above\":{\"const\":0.3}}]}", Options.defaults());
            var run = client.paths().maintain(PipePool.of(dial).withInspectionInterval(Duration.ofMillis(5))
                    .withBackoff(new PipePool.Backoff(Duration.ofMillis(10), Duration.ofMillis(40), 0)).withPolicy(policy));
            await(() -> run.pipes().size() == 2);
            Set<Long> first = new HashSet<>();
            for (var pipe : run.pipes()) first.add(pipe.id());
            await(() -> run.pipes().size() == 2 && run.pipes().stream().noneMatch(pipe -> first.contains(pipe.id())));
            run.close();
            var result = run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertTrue(result.rotations() >= 2, "rotations " + result.rotations());
            assertEquals(2, result.pipes().size());
            client.close(); server.close(); client.awaitClosed(); server.awaitClosed(); listener.close();
        }
    }

    interface Checked { boolean get() throws Exception; }

    static void await(Checked c) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!c.get() && System.nanoTime() < until) Thread.sleep(3);
        assertTrue(c.get(), "condition did not become true");
    }

    @Test void probeIntervalBelowOneSecondIsRejectedNotRaised() throws Exception {
        MachinePoolPolicy p = policy("{\"probe_interval\":{\"const\":0.5}}", MachinesTest.seeded());
        assertEquals(0.5, p.decide(0, null, List.of()).probeInterval());
        // The pool's call fails, so the pool rejects and reports the decision and keeps its own behaviour.
        assertThrows(IllegalArgumentException.class, () -> p.decide(new PoolPolicy.View(List.of(), 0, null)));
    }

    @Test void countNeedsMinOfOne() throws Exception {
        for (String c : new String[] {"{\"dist\":{\"kind\":\"normal\",\"params\":[3,2]}}", "{\"dist\":{\"kind\":\"normal\",\"params\":[3,2]},\"min\":0}",
                "{\"dist\":{\"kind\":\"poisson\",\"params\":[3]}}", "{\"const\":0}"}) {
            assertThrows(DefinitionException.class, () -> policy("{\"count\":" + c + "}", MachinesTest.seeded()), c);
        }
        policy("{\"count\":{\"dist\":{\"kind\":\"normal\",\"params\":[3,2]},\"min\":1}}", MachinesTest.seeded());
    }
}
