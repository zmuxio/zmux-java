package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.MultipathConfig;
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
        for (ZmuxPipe p : s.paths().list()) {
            if (p.id() == 0) continue;
            var st = stats(p);
            if (st == null) continue;
            up |= st.outbound() == PipeState.Outbound.ACTIVE;
            down |= st.outbound() == null && st.inbound() == PipeState.Inbound.OPEN;
        }
        return new boolean[] {up, down};
    }

    static boolean gone(ZmuxPipe p) {
        var st = stats(p);
        return st == null || st.outbound() == PipeState.Outbound.FAILED || st.outbound() == PipeState.Outbound.CLOSED;
    }

    @Test void aSilencedDownlinkFailsTheUnacknowledgedUplinkAndThePoolRejoins() throws Exception {
        var mp = MultipathConfig.defaults().withMaxPipes(4).withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO)
                .withResumeTimeout(Duration.ofSeconds(30)).withProbeInterval(Duration.ofSeconds(1));
        try (var f = new Fixture(b -> b.multipath(mp), b -> b, b -> b.streaming(), false, null);
             var px = new TestProxy(f.base, TestProxy.Profile.PASS); var c = f.client(b -> b)) {
            var s = Zmux.open(c.pipe(px.base, Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null), Role.CLIENT, f.config);
            var server = f.listener.accept(Duration.ofSeconds(5));
            var joins = new AtomicInteger();
            var pool = PipePool.join(a -> { joins.incrementAndGet(); return c.splitJoin(a.session(), px.base, Map.of(), null, a.options()); })
                    .withOptions(PipeOptions.builder().probeInterval(Duration.ofSeconds(1)).build())
                    .withBackoff(new PipePool.Backoff(Duration.ofMillis(100), Duration.ofSeconds(1), 0));
            var run = s.paths().maintain(pool);
            // The silence reaches the responses streaming now only: the member's uplink must be
            // active and its downlink open before it.
            await(5000, "pool member", () -> { var m = member(s); return m[0] && m[1]; });
            ZmuxPipe p0 = s.paths().list().stream().filter(p -> p.id() == 0).findFirst().orElseThrow();
            ZmuxPipe up = s.paths().list().stream().filter(p -> p.id() != 0 && stats(p) != null && stats(p).outbound() == PipeState.Outbound.ACTIVE)
                    .findFirst().orElseThrow();
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
