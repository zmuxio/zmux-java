package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Carriers through an intermediary that ends idle or old connections, swallows bytes, or refuses connections. */
@Timeout(60)
class RelayTest {
    static MultipathConfig multipath(int pipes) {
        return MultipathConfig.defaults().withMaxPipes(pipes).withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO);
    }

    static ZmuxConfig config(EventLoops loops, MultipathConfig m) { return ZmuxConfig.builder().eventLoops(loops).multipath(m).build(); }

    static PipePool through(TcpRelay relay, PipeOptions options) {
        return PipePool.of(attempt -> attempt.join(relay.address())).withOptions(options)
                .withInspectionInterval(Duration.ofSeconds(30)).withBackoff(new PipePool.Backoff(Duration.ofMillis(100), Duration.ofSeconds(1), 0));
    }

    static <T extends SessionEvent> T event(ZmuxSession s, Class<T> kind, java.util.function.Predicate<T> match, long ms) throws Exception {
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ms);
        while (true) {
            long left = until - System.nanoTime();
            if (left <= 0) fail("no " + kind.getSimpleName() + " within " + ms + " ms");
            SessionEvent e;
            try { e = s.nextEvent(Duration.ofNanos(left)); } catch (java.net.SocketTimeoutException timeout) { continue; }
            if (kind.isInstance(e) && match.test(kind.cast(e))) return kind.cast(e);
        }
    }

    @Test void probesBelowTheIdleLimitKeepPipesThroughAnIdleCuttingRelay() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops, multipath(4));
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var relay = new TcpRelay(listener.localAddress());
                 var client = Zmux.connect(listener.localAddress(), cfg); var server = listener.accept(Duration.ofSeconds(5))) {
                relay.idleLimitMs = 1500;
                var kept = through(relay, PipeOptions.builder().probeInterval(Duration.ofSeconds(1)).probeJitter(0.3).build());
                try (var run = client.paths().maintain(kept)) {
                    PipePoolTest.await(() -> run.pipes().size() == 1);
                    Thread.sleep(8000);
                    assertEquals(0, relay.idleCuts.get(), "probes every [0.7 s, 1 s] keep the 1.5 s idle limit at bay");
                    assertEquals(1, relay.accepted.get());
                }
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void aProbeIntervalAboveTheIdleLimitLosesThePipeAndThePoolRedials() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops, multipath(4));
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var relay = new TcpRelay(listener.localAddress());
                 var client = Zmux.connect(listener.localAddress(), cfg); var server = listener.accept(Duration.ofSeconds(5))) {
                relay.idleLimitMs = 1500;
                var lost = through(relay, PipeOptions.builder().probeInterval(Duration.ofSeconds(4)).build());
                try (var run = client.paths().maintain(lost)) {
                    PipePoolTest.await(() -> run.pipes().size() == 1);
                    long first = run.pipes().getFirst().id();
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
                    while (relay.idleCuts.get() == 0 && System.nanoTime() < until) Thread.sleep(5);
                    assertTrue(relay.idleCuts.get() > 0, "the relay ended the idle pipe");
                    long cut = System.nanoTime();
                    while (relay.accepted.get() < 2 && System.nanoTime() - cut < TimeUnit.SECONDS.toNanos(4)) Thread.sleep(2);
                    assertTrue(relay.accepted.get() >= 2, "redialled");
                    assertTrue(System.nanoTime() - cut < TimeUnit.SECONDS.toNanos(2), "redial follows the loss, not the 30 s inspection");
                    PipePoolTest.await(() -> run.pipes().size() == 1 && run.pipes().getFirst().id() != first);
                }
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void anIdlePipeIntoABlackHoleFailsWithinTheProbeIntervalAndTFail() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops, multipath(4));
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var relay = new TcpRelay(listener.localAddress());
                 var client = Zmux.connect(listener.localAddress(), cfg); var server = listener.accept(Duration.ofSeconds(5))) {
                var pipe = client.paths().add(relay.address(), PipeOptions.builder().probeInterval(Duration.ofSeconds(1)).build());
                Thread.sleep(300);
                relay.blackhole = true;
                long start = System.nanoTime();
                event(client, SessionEvent.OutboundPipeStateChanged.class,
                        e -> e.pipe() == pipe.id() && e.state() == PipeState.Outbound.FAILED, 8000);
                long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                assertTrue(ms <= 1000 + 2000 + 1500, "failed after " + ms + " ms (k = 1 s, T_fail = 2 s)");
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void cuttingTheLastPipeSuspendsAndThePoolResumesWithoutLossOrDuplication() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = config(loops, multipath(2).withResumeTimeout(Duration.ofSeconds(120)));
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var relay = new TcpRelay(listener.localAddress())) {
                var accepted = ex.submit((Callable<ZmuxSession>) listener::accept);
                var client = Zmux.connect(relay.address(), cfg); var server = accepted.get(5, TimeUnit.SECONDS);
                byte[] data = new byte[8 << 20]; new java.util.Random(5).nextBytes(data);
                var received = ex.submit(() -> SessionTest.readAll(server.acceptStream()));
                var decisions = new AtomicLong();
                PoolPolicy policy = view -> { decisions.incrementAndGet(); return PoolPolicy.Decision.KEEP.withCount(1).withDialAfter(Duration.ofMillis(200)); };
                try (var run = client.paths().maintain(through(relay, PipeOptions.defaults()).withPolicy(policy))) {
                    PipePoolTest.await(() -> run.pipes().size() == 1);
                    var stream = client.openStream();
                    var writer = ex.submit(() -> { stream.write(data); stream.finish(); return null; });
                    Thread.sleep(100);
                    var suspended = ex.submit(() -> event(client, SessionEvent.Suspended.class, e -> true, 10000));
                    relay.cutAll();
                    suspended.get(10, TimeUnit.SECONDS);
                    event(client, SessionEvent.Resumed.class, e -> true, 10000);
                    writer.get(20, TimeUnit.SECONDS);
                    assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(data),
                            MessageDigest.getInstance("SHA-256").digest(received.get(20, TimeUnit.SECONDS)), "no loss, no duplication");
                    assertTrue(relay.accepted.get() >= 3, "the pool redialled while suspended");
                    stream.close();
                }
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void aRelayRefusingConnectionsThroughTheGracePeriodEndsTheSessionAndThePool() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = config(loops, multipath(2).withResumeTimeout(Duration.ofSeconds(2)));
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var relay = new TcpRelay(listener.localAddress())) {
                var accepted = ex.submit((Callable<ZmuxSession>) listener::accept);
                var client = Zmux.connect(relay.address(), cfg); var server = accepted.get(5, TimeUnit.SECONDS);
                var run = client.paths().maintain(through(relay, PipeOptions.defaults()));
                PipePoolTest.await(() -> run.pipes().size() == 1);
                relay.refuse = true;
                relay.cutAll();
                SessionError e = client.closed().get(10, TimeUnit.SECONDS);
                assertInstanceOf(SessionError.ResumeTimeout.class, e);
                var result = run.completion().toCompletableFuture().get(5, TimeUnit.SECONDS);
                assertInstanceOf(SessionClosedException.class, result.cause(), "the pool stops with the session");
                assertTrue(relay.accepted.get() >= 4, "it kept dialling during the grace period: " + relay.accepted.get());
                server.closed().get(10, TimeUnit.SECONDS);
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void anAgeRuleWithJitterRotatesBeforeTheRelaysConnectionLimitWithoutStalling() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = config(loops, multipath(4));
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var relay = new TcpRelay(listener.localAddress());
                 var client = Zmux.connect(listener.localAddress(), cfg); var server = listener.accept(Duration.ofSeconds(5))) {
                relay.ageLimitMs = 4000;
                var thresholds = new ConcurrentHashMap<Long, Long>();
                PoolPolicy ages = view -> {
                    var retire = new java.util.HashSet<Long>();
                    for (var m : view.members()) {
                        long limit = thresholds.computeIfAbsent(m.id(), id -> (long) (2500 * (1 - 0.3 * ThreadLocalRandom.current().nextDouble())));
                        if (m.age().toMillis() > limit) retire.add(m.id());
                    }
                    return PoolPolicy.Decision.KEEP.withCount(1).withRetire(retire).withRecheck(Duration.ofMillis(50));
                };
                try (var run = client.paths().maintain(through(relay, PipeOptions.defaults()).withPolicy(ages))) {
                    PipePoolTest.await(() -> run.pipes().size() == 1 && run.pipes().getFirst().stats().orElseThrow().outbound() == PipeState.Outbound.ACTIVE);
                    client.paths().list().stream().filter(p -> p.id() == 0).findFirst().orElseThrow().close();
                    var echo = ex.submit(() -> {
                        try (var peer = server.acceptStream()) {
                            byte[] b = new byte[1];
                            for (java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(b); peer.read(bb.clear()) > 0;) peer.write(b);
                            peer.finish();
                        }
                        return null;
                    });
                    long worst = 0;
                    try (var s = client.openStream()) {
                        byte[] b = new byte[1];
                        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        while (System.nanoTime() < end) {
                            long t = System.nanoTime();
                            s.write(b);
                            assertEquals(1, s.read(java.nio.ByteBuffer.allocate(1)));
                            worst = Math.max(worst, System.nanoTime() - t);
                            Thread.sleep(20);
                        }
                        s.finish();
                        echo.get(5, TimeUnit.SECONDS);
                    }
                    assertEquals(0, relay.ageCuts.get(), "every pipe was replaced before the relay's 4 s limit");
                    assertTrue(relay.accepted.get() >= 4, "rotations: " + relay.accepted.get());
                    assertTrue(TimeUnit.NANOSECONDS.toMillis(worst) < 500, "worst round trip " + TimeUnit.NANOSECONDS.toMillis(worst) + " ms");
                }
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }
}
