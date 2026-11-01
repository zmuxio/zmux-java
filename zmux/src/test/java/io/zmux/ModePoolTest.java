package io.zmux;

import static io.zmux.CarrierSelectionTest.reaching;
import static io.zmux.CarrierSelectionTest.refused;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** PipePool.mode: members dialled through a carrier mode, upgraded make before break (API.md Section 4.8). */
@Timeout(60)
class ModePoolTest {
    private static ZmuxConfig config(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(4)
                .withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO)).build();
    }

    private static void await(BooleanSupplier done, String what) throws InterruptedException {
        long until = System.nanoTime() + 20_000_000_000L;
        while (!done.getAsBoolean()) {
            if (System.nanoTime() > until) fail(what);
            Thread.sleep(5);
        }
    }

    private static List<Long> ids(PipeMaintenance m) { return m.pipes().stream().map(ZmuxPipe::id).toList(); }

    @Test void membersStartOnTheRobustKindAndUpgradeMakeBeforeBreak() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            try (var l = Zmux.listener(ListenerConfig.of(cfg))) {
                var base = new AtomicInteger(); var ws = new AtomicInteger(); var ex = new AtomicInteger();
                var cache = new CarrierCache(Duration.ofHours(1), Duration.ofMillis(300), Duration.ofSeconds(10));
                cache.record("k", CarrierKind.WEBSOCKET, false, System.nanoTime());
                var pool = PipePool.mode(CarrierMode.auto(reaching(CarrierKind.WEBSOCKET, l, ws), reaching(CarrierKind.EXCHANGE, l, ex)),
                        ModePolicy.of(cache, "k").withSettle(Duration.ofMillis(200)));
                try (var s = Zmux.dial(CarrierMode.explicit(reaching(CarrierKind.STREAMING, l, base)), cfg, null);
                     var m = s.paths().maintain(pool)) {
                    await(() -> m.pipes().size() == 1, "first member");
                    long first = m.pipes().getFirst().id();
                    assertEquals(0, ws.get(), "the kind backing off is skipped");
                    assertEquals(1, ex.get());
                    // Once the backoff ends the preferred kind joins; the old member stays until it settled.
                    boolean[] both = new boolean[1];
                    await(() -> {
                        if (m.pipes().size() == 2) both[0] = true;
                        return m.pipes().size() == 1 && m.pipes().getFirst().id() != first;
                    }, "upgraded");
                    assertTrue(both[0], "make before break");
                    assertEquals(1, ws.get());
                    assertTrue(cache.usable("k", CarrierKind.WEBSOCKET));
                    assertEquals(CarrierKind.WEBSOCKET, s.stats().carrier());
                    Thread.sleep(300);
                    assertEquals(1, ws.get(), "no further upgrade from the most preferred kind");
                    assertEquals(1, ex.get());
                }
            }
        }
    }

    @Test void aFailedUpgradeKeepsTheMemberAndBacksTheKindOff() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            try (var l = Zmux.listener(ListenerConfig.of(cfg))) {
                var base = new AtomicInteger(); var ws = new AtomicInteger(); var ex = new AtomicInteger();
                var cache = new CarrierCache(Duration.ofHours(1), Duration.ofMillis(100), Duration.ofSeconds(10));
                var pool = PipePool.mode(CarrierMode.auto(refused(CarrierKind.WEBSOCKET, ws), reaching(CarrierKind.EXCHANGE, l, ex)),
                        ModePolicy.of(cache, "k")).withBackoff(new PipePool.Backoff(Duration.ofMillis(20), Duration.ofMillis(50), 0));
                try (var s = Zmux.dial(CarrierMode.explicit(reaching(CarrierKind.STREAMING, l, base)), cfg, null);
                     var m = s.paths().maintain(pool)) {
                    await(() -> m.pipes().size() == 1, "first member");
                    List<Long> first = ids(m);
                    await(() -> ws.get() >= 2, "upgrade retried after the backoff");
                    assertEquals(first, ids(m), "the old member stays");
                    assertEquals(1, ex.get());
                    var entry = cache.snapshot().entries().stream().filter(e -> e.kind() == CarrierKind.WEBSOCKET).findFirst().orElseThrow();
                    assertTrue(entry.backoff().compareTo(Duration.ofMillis(200)) >= 0, "backoff doubled: " + entry.backoff());
                    assertTrue(s.stats().carrierCandidates().stream().anyMatch(r -> r.joined() && r.outcome() == CandidateOutcome.REFUSED));
                }
            }
        }
    }

    @Test void aMemberLostWithinTheDropWindowCountsAgainstItsKind() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            try (var l = Zmux.listener(ListenerConfig.of(cfg))) {
                var base = new AtomicInteger(); var ex = new AtomicInteger();
                var cache = new CarrierCache();
                var pool = PipePool.mode(CarrierMode.auto(reaching(CarrierKind.EXCHANGE, l, ex)), ModePolicy.of(cache, "k"));
                try (var s = Zmux.dial(CarrierMode.explicit(reaching(CarrierKind.STREAMING, l, base)), cfg, null);
                     var m = s.paths().maintain(pool)) {
                    await(() -> m.pipes().size() == 1, "first member");
                    assertTrue(cache.usable("k", CarrierKind.EXCHANGE));
                    m.pipes().getFirst().abort();
                    await(() -> cache.backingOff("k", CarrierKind.EXCHANGE), "the drop recorded");
                }
            }
        }
    }

    @Test void invalidModesAreRejectedAndDialsCanBeWrapped() throws Exception {
        var ex = new AtomicInteger();
        Candidate c = refused(CarrierKind.EXCHANGE, ex);
        assertThrows(IllegalArgumentException.class, () -> CarrierMode.auto(c, refused(CarrierKind.EXCHANGE, ex)));
        assertThrows(IllegalArgumentException.class, () -> CarrierMode.auto(List.of()));
        assertThrows(IllegalArgumentException.class, () -> ModePolicy.defaults().withSettle(Duration.ofMillis(-1)));
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var l = Zmux.listener(ListenerConfig.of(cfg))) {
                var wrapped = new AtomicInteger();
                var cand = reaching(CarrierKind.WEBSOCKET, l, new AtomicInteger()).wrapDial(io -> { wrapped.incrementAndGet(); return io; });
                try (var s = Zmux.dial(CarrierMode.explicit(cand), cfg, null)) {
                    assertEquals(1, wrapped.get());
                    CarrierSelectionTest.echo(s, l);
                }
            }
        }
    }
}
