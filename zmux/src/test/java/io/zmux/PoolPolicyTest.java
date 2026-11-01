package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(40)
class PoolPolicyTest {
    @Test void countOverridesThePoolAndClosesRetiredMembersFirst() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 8))) {
            var count = new AtomicInteger(3); var retire = new AtomicLong(-1);
            PoolPolicy policy = v -> PoolPolicy.Decision.KEEP.withCount(count.get()).withRetire(retire.get() < 0 ? Set.of() : Set.of(retire.get()));
            var run = f.client.paths().maintain(PipePoolTest.fast(f.dial()).withPolicy(policy));
            PipePoolTest.await(() -> run.pipes().size() == 3);
            var doomed = run.pipes().get(1);
            retire.set(doomed.id()); count.set(2);
            PipePoolTest.await(() -> run.pipes().size() == 2 && run.pipes().stream().noneMatch(p -> p.id() == doomed.id()));
            PipePoolTest.await(() -> PipePoolTest.isInactive(doomed));
            run.close();
            assertEquals(2, run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS).pipes().size());
        }
    }

    @Test void retireReplacesMakeBeforeBreak() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 8))) {
            var retire = new AtomicLong(-1); var seen = new ConcurrentLinkedQueue<PoolPolicy.View>();
            PoolPolicy policy = v -> {
                seen.add(v);
                return retire.get() < 0 ? PoolPolicy.Decision.KEEP : PoolPolicy.Decision.KEEP.withRetire(Set.of(retire.get()));
            };
            var run = f.client.paths().maintain(PipePoolTest.fast(f.dial()).withPolicy(policy));
            PipePoolTest.await(() -> run.pipes().size() == 1);
            long old = run.pipes().getFirst().id();
            PipePoolTest.await(() -> seen.stream().anyMatch(v -> v.members().size() == 1 && v.members().getFirst().usable()));
            retire.set(old);
            PipePoolTest.await(() -> run.pipes().size() == 1 && run.pipes().getFirst().id() != old);
            run.close();
            assertEquals(1, run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS).rotations());
        }
    }

    @Test void dialAfterSpacesDials() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 8))) {
            var dials = new ConcurrentLinkedQueue<Long>();
            PoolPolicy policy = v -> PoolPolicy.Decision.KEEP.withDialAfter(Duration.ofMillis(200));
            PipePool.Dial dial = attempt -> { dials.add(System.nanoTime()); return f.dial().open(attempt); };
            var run = f.client.paths().maintain(PipePoolTest.fast(dial).withBackoff(new PipePool.Backoff(Duration.ofMillis(10), Duration.ofSeconds(1), 0)).withCount(2).withPolicy(policy));
            PipePoolTest.await(() -> run.pipes().size() == 2);
            run.close();
            run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            var times = dials.stream().mapToLong(Long::longValue).toArray();
            assertEquals(2, times.length);
            assertTrue(times[1] - times[0] >= TimeUnit.MILLISECONDS.toNanos(190), "dials closer than dialAfter");
        }
    }

    @Test void recheckShorterThanTheInspectionIntervalIsHonouredAndFloored() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 8))) {
            var calls = new AtomicInteger(); var recheck = new java.util.concurrent.atomic.AtomicReference<>(Duration.ofMillis(20));
            PoolPolicy policy = v -> { calls.incrementAndGet(); return PoolPolicy.Decision.KEEP.withRecheck(recheck.get()); };
            var run = f.client.paths().maintain(PipePoolTest.fast(f.dial()).withCount(1).withInspectionInterval(Duration.ofSeconds(5)).withPolicy(policy));
            PipePoolTest.await(() -> run.pipes().size() == 1);
            int before = calls.get(); Thread.sleep(300); int n = calls.get() - before;
            assertTrue(n >= 5 && n <= 20, n + " decisions in 300 ms at a 20 ms recheck");
            recheck.set(Duration.ofNanos(1));
            Thread.sleep(50);
            before = calls.get(); Thread.sleep(300); n = calls.get() - before;
            assertTrue(n >= 10 && n <= 45, n + " decisions in 300 ms: the 10 ms floor does not hold");
            run.close();
            run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
        }
    }

    @Test void aThrowingPolicyKeepsThePoolsOwnBehaviour() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 8))) {
            var calls = new AtomicInteger();
            PoolPolicy policy = v -> { calls.incrementAndGet(); throw new IllegalStateException("policy failure"); };
            var run = f.client.paths().maintain(PipePoolTest.fast(f.dial()).withCount(2).withPolicy(policy));
            PipePoolTest.await(() -> run.pipes().size() == 2);
            run.close();
            var result = run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(2, result.pipes().size());
            assertTrue(calls.get() >= 1);
        }
    }

    @Test void aSuccessfulDialAlsoWaitsTheMinimumBackoffFromItsStart() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 8))) {
            var dials = new ConcurrentLinkedQueue<Long>();
            PipePool.Dial dial = attempt -> { dials.add(System.nanoTime()); return f.dial().open(attempt); };
            var run = f.client.paths().maintain(PipePoolTest.fast(dial).withCount(2)
                    .withBackoff(new PipePool.Backoff(Duration.ofMillis(300), Duration.ofSeconds(1), 0)));
            PipePoolTest.await(() -> run.pipes().size() == 2);
            run.close();
            run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            var times = dials.stream().mapToLong(Long::longValue).toArray();
            assertEquals(2, times.length);
            assertTrue(times[1] - times[0] >= TimeUnit.MILLISECONDS.toNanos(290), "a success did not space the next dial");
        }
    }

    @Test void aRejectedDecisionKeepsThePoolIsReportedAndTheRunGoesOn() throws Exception {
        var logger = java.util.logging.Logger.getLogger(PipeMaintenance.class.getName());
        var reports = new ConcurrentLinkedQueue<java.util.logging.LogRecord>();
        var handler = new java.util.logging.Handler() {
            @Override public void publish(java.util.logging.LogRecord r) { reports.add(r); }
            @Override public void flush() {}
            @Override public void close() {}
        };
        logger.addHandler(handler);
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 8))) {
            var calls = new AtomicInteger();
            // Decision refuses a negative count: the policy's answer is rejected.
            PoolPolicy policy = v -> { calls.incrementAndGet(); return PoolPolicy.Decision.KEEP.withCount(-1); };
            var run = f.client.paths().maintain(PipePoolTest.fast(f.dial()).withCount(2).withPolicy(policy));
            PipePoolTest.await(() -> run.pipes().size() == 2);
            int before = calls.get();
            PipePoolTest.await(() -> calls.get() > before + 2);
            assertFalse(reports.isEmpty(), "the rejection was not reported");
            assertInstanceOf(IllegalArgumentException.class, reports.peek().getThrown());
            run.close();
            var result = run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(2, result.pipes().size());
        } finally { logger.removeHandler(handler); }
    }
}
