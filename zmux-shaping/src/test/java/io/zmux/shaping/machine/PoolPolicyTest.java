package io.zmux.shaping.machine;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.PoolPolicy;
import io.zmux.shaping.machine.MachinePoolPolicy.Metrics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PoolPolicyTest {
    static MachinePoolPolicy policy(String json) throws DefinitionException {
        return Machines.poolPolicy(json.getBytes(StandardCharsets.UTF_8), Options.defaults());
    }

    static Metrics member(long id, long age, long idle, long pipes, long sent, long received) {
        return new Metrics(id, age, idle, pipes, sent, received);
    }

    @Test void countsSpacingsAndStrictlyAboveThresholds() throws Exception {
        var p = policy("{\"count\":2,\"dial_after\":[1000000,5000000],\"retire\":[{\"metric\":\"age\",\"above\":100},{\"metric\":\"bytes\",\"above\":10}]}");
        var r = p.decide(7, 3_000_000L, List.of(member(1, 100, 0, 1, 5, 5), member(2, 101, 0, 1, 0, 0), member(3, 0, 0, 1, 6, 5)));
        assertEquals(2, r.count());
        assertEquals(5_000_000, r.dialAfter());
        assertEquals(List.of(2L, 3L), r.retire());
        assertEquals(1, r.recheck(), "the kept member is at its threshold");
    }

    @Test void pipeScopeIsPerMemberWhileInTheView() throws Exception {
        var p = policy("{\"retire\":[{\"metric\":\"age\",\"above\":{\"t\":[[1,0,1000000]],\"scope\":\"pipe\"}}]}");
        List<Metrics> view = List.of(member(1, 500000, 0, 1, 0, 0), member(2, 500000, 0, 1, 0, 0), member(3, 500000, 0, 1, 0, 0), member(4, 500000, 0, 1, 0, 0));
        var first = p.decide(0, null, view).retire();
        for (int i = 0; i < 32; i++) assertEquals(first, p.decide(0, null, view).retire());
        boolean changed = false;
        for (int i = 0; i < 64 && !changed; i++) {
            p.decide(0, null, List.of());
            changed = !p.decide(0, null, view).retire().equals(first);
        }
        assertTrue(changed, "thresholds survived the members leaving the view");
    }

    @Test void aRuleJitterOnlyLowersAndHoldsPerMember() throws Exception {
        var p = policy("{\"retire\":[{\"metric\":\"age\",\"above\":1000000,\"jitter\":500000}]}");
        Long first = p.decide(0, 0L, List.of(member(1, 0, 0, 1, 0, 0))).recheck();
        assertTrue(first >= 500000 && first <= 1000000, "recheck " + first);
        assertEquals(first, p.decide(0, 0L, List.of(member(1, 0, 0, 1, 0, 0))).recheck());
    }

    @Test void aDialJitterHoldsWithinAWaitAndIsDrawnAgainForTheNext() throws Exception {
        var p = policy("{\"dial_after\":[{\"t\":[[1,1000000000,1000000000]],\"scope\":\"shared\"}],\"dial_jitter\":900000}");
        long a = p.decide(0, 5L, List.of()).dialAfter();
        assertEquals(a, p.decide(0, 6L, List.of()).dialAfter());
        boolean changed = false;
        for (int i = 0; i < 64 && !changed; i++) changed = p.decide(0, 1L, List.of()).dialAfter() != a;
        assertTrue(changed, "a new dial starts a new wait");
    }

    @Test void sharedKeepsAndEachDraws() throws Exception {
        var p = policy("{\"count\":{\"t\":[[1,1,1000000]]},\"dial_after\":[{\"t\":[[1,0,1000000]],\"scope\":\"shared\"}]}");
        Set<Long> counts = new java.util.HashSet<>(), afters = new java.util.HashSet<>();
        for (int i = 0; i < 32; i++) {
            var r = p.decide(i, null, List.of());
            counts.add(r.count());
            afters.add(r.dialAfter());
        }
        assertTrue(counts.size() > 1);
        assertEquals(1, afters.size());
    }

    @Test void decisionMapsToThePool() throws Exception {
        var p = policy("{\"count\":2,\"dial_after\":[250000],\"retire\":[{\"metric\":\"bytes\",\"above\":10}]}");
        var d = p.decide(new PoolPolicy.View(List.of(
                new PoolPolicy.Member(4, Duration.ofSeconds(1), Duration.ZERO, 1, 6, 5, true, false),
                new PoolPolicy.Member(5, Duration.ofSeconds(1), Duration.ZERO, 1, 10, 0, true, false)), 0, null));
        assertEquals(PoolPolicy.Decision.KEEP.withCount(2).withDialAfter(Duration.ofMillis(250)).withRetire(Set.of(4L)), d);
    }

    @Test void aProbeIntervalBelowOneSecondIsRejectedNotRaised() throws Exception {
        var p = policy("{\"probe_interval\":500000}");
        assertEquals(500000, p.decide(0, null, List.of()).probeInterval());
        assertThrows(IllegalArgumentException.class, () -> p.decide(new PoolPolicy.View(List.of(), 0, null)));
    }
}
