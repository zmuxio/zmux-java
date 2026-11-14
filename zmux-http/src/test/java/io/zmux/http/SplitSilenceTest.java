package io.zmux.http;

import io.zmux.PipeLiveness;
import io.zmux.Liveness;
import static org.junit.jupiter.api.Assertions.*;
import io.zmux.PipeAuth;
import io.zmux.PipeOptions;
import io.zmux.PipePool;
import io.zmux.PipeState;
import io.zmux.Role;
import io.zmux.Zmux;
import io.zmux.ZmuxPipe;
import io.zmux.ZmuxSession;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A split carrier through a proxy that goes silent toward the client only: the uplink still
 * carries bytes but nothing acknowledges them, so it fails within T_fail, the pool joins a new
 * pair, and the session closes promptly afterwards.
 */
@Timeout(90)
class SplitSilenceTest {
    static void await(long ms, String what, BooleanSupplier c) throws InterruptedException {
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ms);
        while (!c.getAsBoolean()) {
            assertTrue(System.nanoTime() < until, what + " not within " + ms + " ms");
            Thread.sleep(5);
        }
    }

    static io.zmux.PipeStats stats(ZmuxPipe p) {
        try { return p.stats().orElse(null); } catch (java.io.IOException closed) { return null; }
    }

    static boolean[] member(ZmuxSession s) {
        boolean up = false, down = false;
        for (ZmuxPipe p : s.pipes()) {
            if (p.id().value() == 0) continue;
            var st = stats(p);
            if (st == null) continue;
            up |= st.outbound() == PipeState.Outbound.ACTIVE;
            down |= st.inbound() == PipeState.Inbound.OPEN;
        }
        return new boolean[] {up, down};
    }

    /** A member whose uplink is active and has an acknowledgement, so an RTT sample; null: none. */
    static ZmuxPipe sampled(ZmuxSession s) {
        for (ZmuxPipe p : s.pipes()) {
            var st = p.id().value() == 0 ? null : stats(p);
            if (st != null && st.outbound() == PipeState.Outbound.ACTIVE && st.smoothedRtt() != null) return p;
        }
        return null;
    }

    static boolean gone(ZmuxPipe p) {
        var st = stats(p);
        return st == null || st.outbound() == PipeState.Outbound.FAILED || st.outbound() == PipeState.Outbound.CLOSED;
    }

    @Test void aSilencedDownlinkFailsTheUnacknowledgedUplinkAndThePoolRejoins() throws Exception {
        java.util.function.UnaryOperator<io.zmux.ZmuxConfig.Builder> mp = b -> b.maxPipes(4).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation()
                .resumeTimeout(Duration.ofSeconds(30)).liveness(Liveness.builder().probeInterval(Duration.ofSeconds(1)).build());
        try (var f = new Fixture(mp, b -> b, b -> b.streaming(), false, null);
             var px = new TestProxy(f.base, TestProxy.Profile.PASS); var c = f.client(b -> b)) {
            var s = Zmux.open(c.pipe(px.base, Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null), Role.CLIENT, f.config);
            var server = f.listener.accept(Duration.ofSeconds(5));
            var joins = new AtomicInteger();
            var pool = PipePool.join(a -> { joins.incrementAndGet(); return java.util.List.of(c.splitJoin(a.session(), px.base, Map.of(), null, a.options())); })
                    .withOptions(PipeOptions.builder().liveness(PipeLiveness.builder().probeInterval(Duration.ofSeconds(1)).build()).build())
                    .withBackoff(new io.zmux.Backoff(Duration.ofMillis(100), Duration.ofSeconds(1), 0));
            var run = s.maintainPipes(pool);
            // The silence reaches the responses streaming now only, so the member's downlink must be
            // open before it. Its uplink must also hold an RTT sample: without one PTO is the initial
            // estimate and T_fail about 8 s, beyond the bound below.
            await(5000, "pool member", () -> { var m = member(s); return m[0] && m[1] && sampled(s) != null; });
            ZmuxPipe p0 = s.pipes().stream().filter(p -> p.id().value() == 0).findFirst().orElseThrow();
            ZmuxPipe up = sampled(s);
            px.silenceResponses();
            long start = System.nanoTime();
            // k = 1 s, T_fail = 2 s at these round-trip times, and a margin.
            await(4500, "pipe 0 failure", () -> gone(p0));
            // T_fail of pipe 0, then T_dead = T_fail (resume) of the unacknowledged uplink.
            await(8000 - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start), "member uplink failure", () -> gone(up));
            await(25_000, "rejoin", () -> joins.get() >= 2);
            await(10_000, "both directions", () -> { var m = member(s); return m[0] && m[1]; });
            Fixture.exchange(s, server, 64 << 10);
            s.close();
            s.closed().get(5, TimeUnit.SECONDS);
            run.completion().toCompletableFuture().get(5, TimeUnit.SECONDS);
            server.closed().get(10, TimeUnit.SECONDS);
            run.close();
        }
    }
}
