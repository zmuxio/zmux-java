package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Per-pipe probe interval and jitter, the since-last-traffic statistic, and pool redial. */
@Timeout(40)
class ProbeTest {
    static ZmuxConfig config(EventLoops loops, MultipathConfig m) {
        return ZmuxConfig.builder().eventLoops(loops).multipath(m.withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO)).build();
    }

    static MultipathConfig multipath() { return MultipathConfig.defaults().withMaxPipes(4); }

    @Test void probeSettingsAreValidated() {
        var b = PipeOptions.builder();
        assertThrows(IllegalArgumentException.class, () -> b.probeInterval(Duration.ofMillis(999)));
        b.probeInterval(Duration.ofSeconds(1));
        assertThrows(IllegalArgumentException.class, () -> b.probeJitter(-0.01));
        assertThrows(IllegalArgumentException.class, () -> b.probeJitter(1));
        assertThrows(IllegalArgumentException.class, () -> b.probeJitter(Double.NaN));
        b.probeJitter(0).probeJitter(0.99);
        assertThrows(IllegalArgumentException.class, () -> MultipathConfig.defaults().withProbeJitter(1));
        assertThrows(IllegalArgumentException.class, () -> MultipathConfig.defaults().withProbeJitter(-1));
        assertEquals(0.5, MultipathConfig.defaults().withProbeJitter(0.5).probeJitter());
        assertThrows(IllegalArgumentException.class, () -> PoolPolicy.Decision.KEEP.withProbeInterval(Duration.ofMillis(500)));
        assertThrows(IllegalArgumentException.class, () -> PoolPolicy.Decision.KEEP.withProbeJitter(1));
    }

    /** Waits until pipe's PIPE_PING count reaches n, at most ms. */
    static long pingsWithin(ZmuxPipe pipe, long ms) throws Exception {
        Thread.sleep(ms);
        return pipe.stats().orElseThrow().pingsSent();
    }

    @Test void aPipeProbesAtItsOwnIntervalWithinItsJitter() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            // The session's default without jitter: the bootstrap pipe's first wait is the whole 15 s.
            var cfg = config(loops, multipath().withProbeJitter(0));
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var client = Zmux.connect(listener.localAddress(), cfg);
                 var server = listener.accept(Duration.ofSeconds(5))) {
                var fast = client.paths().add(listener.localAddress(), PipeOptions.builder().probeInterval(Duration.ofSeconds(1)).build());
                var jittered = client.paths().add(listener.localAddress(),
                        PipeOptions.builder().probeInterval(Duration.ofSeconds(2)).probeJitter(0.9).build());
                ZmuxPipe bootstrap = client.paths().list().stream().filter(p -> p.id() == 0).findFirst().orElseThrow();
                long f0 = fast.stats().orElseThrow().pingsSent(), j0 = jittered.stats().orElseThrow().pingsSent(), b0 = bootstrap.stats().orElseThrow().pingsSent();
                Thread.sleep(6500);
                long f = fast.stats().orElseThrow().pingsSent() - f0, j = jittered.stats().orElseThrow().pingsSent() - j0;
                assertTrue(f >= 5 && f <= 8, "1 s interval: " + f + " probes in 6.5 s");
                // Each wait is drawn from [0.2 s, 2 s]: at least 3, at most 32 probes in 6.5 s.
                assertTrue(j >= 3 && j <= 33, "jittered 2 s interval: " + j + " probes in 6.5 s");
                assertTrue(bootstrap.stats().orElseThrow().pingsSent() - b0 <= 1, "the session's 15 s interval");
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void probesNeitherResetSinceLastTrafficNorKeepTheSessionAlive() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).timeouts(Timeouts.defaults().withIdle(Duration.ofSeconds(4)).disableKeepAlive())
                    .multipath(multipath().withProbeInterval(Duration.ofSeconds(1)).withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO)).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var client = Zmux.connect(listener.localAddress(), cfg);
                 var server = listener.accept(Duration.ofSeconds(5))) {
                long started = System.nanoTime();
                try (var s = client.openStream()) {
                    s.write(new byte[] {1}); s.finish();
                    try (var peer = server.acceptStream()) { assertArrayEquals(new byte[] {1}, SessionTest.readAll(peer)); }
                }
                ZmuxPipe pipe = client.paths().list().getFirst();
                assertTrue(pipe.stats().orElseThrow().sinceLastTraffic().toMillis() < 1000);
                Thread.sleep(2500);
                PipeStats st = pipe.stats().orElseThrow();
                assertTrue(st.pingsSent() >= 1, "probes were sent");
                assertTrue(st.sinceLastTraffic().toMillis() >= 2000, "probes do not count as traffic: " + st.sinceLastTraffic());
                // Only probes and their answers: the idle timeout still closes the session.
                SessionError e = client.closed().get(10, TimeUnit.SECONDS);
                assertInstanceOf(SessionError.IdleTimeout.class, e);
                assertTrue(System.nanoTime() - started >= TimeUnit.SECONDS.toNanos(4) - TimeUnit.MILLISECONDS.toNanos(200));
                server.closed().get(10, TimeUnit.SECONDS);
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void thePolicysDialAfterReplacesTheBackoffAndItsProbeSettingsReachNewPipes() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops, multipath());
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var client = Zmux.connect(listener.localAddress(), cfg);
                 var server = listener.accept(Duration.ofSeconds(5))) {
                var dials = new CopyOnWriteArrayList<Long>(); var options = new AtomicReference<PipeOptions>();
                PoolPolicy policy = view -> PoolPolicy.Decision.KEEP.withCount(1).withDialAfter(Duration.ofMillis(100)).withProbeInterval(Duration.ofSeconds(3)).withProbeJitter(0.25);
                PipePool pool = PipePool.of(attempt -> {
                    dials.add(System.nanoTime()); options.set(attempt.options());
                    if (dials.size() <= 2) throw new java.io.IOException("refused");
                    return attempt.join(listener.localAddress());
                }).withInspectionInterval(Duration.ofSeconds(30)).withBackoff(new PipePool.Backoff(Duration.ofSeconds(10), Duration.ofSeconds(20), 0))
                        .withPolicy(policy);
                try (var run = client.paths().maintain(pool)) {
                    PipePoolTest.await(() -> run.pipes().size() == 1);
                    assertEquals(3, dials.size());
                    for (int i = 1; i < 3; i++) {
                        long gap = TimeUnit.NANOSECONDS.toMillis(dials.get(i) - dials.get(i - 1));
                        assertTrue(gap >= 90 && gap < 2000, "dial spacing " + gap + " ms, not the 10 s backoff");
                    }
                    assertEquals(Duration.ofSeconds(3), options.get().probeInterval);
                    assertEquals(0.25, options.get().probeJitter);
                }
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void aPoolRedialsAsSoonAsItsPipeFails() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops, multipath());
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var client = Zmux.connect(listener.localAddress(), cfg);
                 var server = listener.accept(Duration.ofSeconds(5))) {
                var dials = new AtomicInteger();
                PipePool pool = PipePool.of(attempt -> { dials.incrementAndGet(); return attempt.join(listener.localAddress()); })
                        .withInspectionInterval(Duration.ofSeconds(30)).withBackoff(new PipePool.Backoff(Duration.ofMillis(200), Duration.ofSeconds(1), 0));
                try (var run = client.paths().maintain(pool)) {
                    PipePoolTest.await(() -> run.pipes().size() == 1);
                    Thread.sleep(300); // older than the backoff: the redial goes at once
                    long old = run.pipes().getFirst().id();
                    long cut = System.nanoTime();
                    run.pipes().getFirst().abort();
                    while (dials.get() < 2 && System.nanoTime() - cut < TimeUnit.SECONDS.toNanos(5)) Thread.sleep(1);
                    long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - cut);
                    assertTrue(dials.get() >= 2 && ms < 500, "redial " + ms + " ms after the failure, not at the 30 s inspection");
                    PipePoolTest.await(() -> run.pipes().size() == 1 && run.pipes().getFirst().id() != old);
                }
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void probeRecordsAreShapedAndADelayDoesNotMakeThePipeSuspect() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var planned = new AtomicInteger(); var delay = new java.util.concurrent.atomic.AtomicBoolean();
            io.zmux.shaping.ShaperFactory factory = pipe -> new io.zmux.shaping.Shaper() {
                @Override public Set<io.zmux.shaping.Capability> capabilities() { return Set.of(io.zmux.shaping.Capability.PLAN); }
                @Override public void plan(io.zmux.shaping.RecordView view, io.zmux.shaping.RecordPlan plan) {
                    planned.incrementAndGet(); if (delay.get()) plan.delayNanos(TimeUnit.MILLISECONDS.toNanos(600));
                }
                @Override public void onEvent(io.zmux.shaping.Event event, io.zmux.shaping.Reply reply) { }
            };
            var base = config(loops, multipath());
            var shaped = ZmuxConfig.builder().eventLoops(loops).shaper(factory)
                    .multipath(multipath().withProbeInterval(Duration.ofSeconds(1)).withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO)).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(base));
                 var client = Zmux.connect(listener.localAddress(), shaped);
                 var server = listener.accept(Duration.ofSeconds(5))) {
                ZmuxPipe pipe = client.paths().list().getFirst();
                delay.set(true);
                long p0 = pipe.stats().orElseThrow().pingsSent(), r0 = planned.get();
                Thread.sleep(5000);
                PipeStats st = pipe.stats().orElseThrow();
                long pings = st.pingsSent() - p0;
                assertTrue(pings >= 2, "probes: " + pings);
                assertTrue(planned.get() - r0 >= pings, "every probe record passes the shaper");
                assertEquals(0, st.suspectCount(), "the ack stall runs from the record's hand-over to the carrier");
                assertEquals(PipeState.Outbound.ACTIVE, st.outbound());
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }
}
