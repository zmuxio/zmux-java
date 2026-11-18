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
            PoolPolicy policy = v -> PoolDecision.KEEP.withCount(count.get()).withRetirePipes(retire.get() < 0 ? Set.of() : Set.of(PipeId.of(retire.get())));
            var run = f.client.maintainPipes(PipePoolTest.fast(f.dial()).withPolicy(policy));
            PipePoolTest.await(() -> run.pipes().size() == 3);
            var doomed = run.pipes().get(1);
            retire.set(doomed.id().value()); count.set(2);
            PipePoolTest.await(() -> run.pipes().size() == 2 && run.pipes().stream().noneMatch(p -> p.id().value() == doomed.id().value()));
            PipePoolTest.await(() -> PipePoolTest.isInactive(doomed));
            run.close();
            assertEquals(2, run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS).pipes().size());
        }
    }

    @Test void retireReplacesMakeBeforeBreak() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 8))) {
            var retire = new AtomicLong(-1); var seen = new ConcurrentLinkedQueue<PoolView>();
            PoolPolicy policy = v -> {
                seen.add(v);
                return retire.get() < 0 ? PoolDecision.KEEP : PoolDecision.KEEP.withRetirePipes(Set.of(PipeId.of(retire.get())));
            };
            var run = f.client.maintainPipes(PipePoolTest.fast(f.dial()).withPolicy(policy));
            PipePoolTest.await(() -> run.pipes().size() == 1);
            long old = run.pipes().getFirst().id().value();
            PipePoolTest.await(() -> seen.stream().anyMatch(v -> v.members().size() == 1 && v.members().getFirst().usable()));
            retire.set(old);
            PipePoolTest.await(() -> run.pipes().size() == 1 && run.pipes().getFirst().id().value() != old);
            run.close();
            assertEquals(1, run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS).rotations());
        }
    }

    @Test void dialAfterSpacesDials() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 8))) {
            var dials = new ConcurrentLinkedQueue<Long>();
            PoolPolicy policy = v -> PoolDecision.KEEP.withDialAfter(Duration.ofMillis(200));
            PipePool.Dial dial = attempt -> { dials.add(System.nanoTime()); return f.dial().open(attempt); };
            var run = f.client.maintainPipes(PipePoolTest.fast(dial).withBackoff(new Backoff(Duration.ofMillis(10), Duration.ofSeconds(1), 0)).withCount(2).withPolicy(policy));
            PipePoolTest.await(() -> run.pipes().size() == 2);
            run.close();
            run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            var times = dials.stream().mapToLong(Long::longValue).toArray();
            assertEquals(2, times.length);
            assertTrue(times[1] - times[0] >= TimeUnit.MILLISECONDS.toNanos(190), "dials closer than dialAfter");
        }
    }

    /** The recheck's floor and the wait it leaves, as pure arithmetic: no clock, no count of looks in a window. */
    @Test void aRecheckIsFlooredAndCutsTheIntervalShort() {
        long tick = 1_000_000; // 1 ms in nanoseconds
        assertEquals(10 * tick, PoolDecision.KEEP.withRecheck(Duration.ofNanos(1)).recheckNanos(), "the 10 ms floor");
        assertEquals(20 * tick, PoolDecision.KEEP.withRecheck(Duration.ofMillis(20)).recheckNanos());
        assertEquals(0, PoolDecision.KEEP.recheckNanos(), "unset: no extra look");
        long interval = PipeMaintenance.INTERVAL;
        assertEquals(interval, PipeMaintenance.recheckWait(0, 0, 5 * tick), "no recheck: the maintenance interval");
        assertEquals(15 * tick, PipeMaintenance.recheckWait(20 * tick, 0, 5 * tick), "the time left of the recheck");
        assertEquals(0, PipeMaintenance.recheckWait(20 * tick, 0, 500 * tick), "overdue: look at once");
        assertEquals(interval, PipeMaintenance.recheckWait(30_000 * tick, 0, 5 * tick), "a long recheck adds nothing to the interval");
    }

    /** A running pool asks again by its recheck: reached within a generous bound, with no count of looks per window. */
    @Test void aShortRecheckAsksAgain() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 8))) {
            var calls = new AtomicInteger();
            PoolPolicy policy = v -> { calls.incrementAndGet(); return PoolDecision.KEEP.withRecheck(Duration.ofMillis(20)); };
            var run = f.client.maintainPipes(PipePoolTest.fast(f.dial()).withCount(1).withPolicy(policy));
            PipePoolTest.await(() -> run.pipes().size() == 1);
            int before = calls.get();
            PipePoolTest.await(() -> calls.get() >= before + 5);
            run.close();
            run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
        }
    }

    @Test void aThrowingPolicyKeepsThePoolsOwnBehaviour() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 8))) {
            var calls = new AtomicInteger();
            PoolPolicy policy = v -> { calls.incrementAndGet(); throw new IllegalStateException("policy failure"); };
            var run = f.client.maintainPipes(PipePoolTest.fast(f.dial()).withCount(2).withPolicy(policy));
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
            var run = f.client.maintainPipes(PipePoolTest.fast(dial).withCount(2)
                    .withBackoff(new Backoff(Duration.ofMillis(300), Duration.ofSeconds(1), 0)));
            PipePoolTest.await(() -> run.pipes().size() == 2);
            run.close();
            run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            var times = dials.stream().mapToLong(Long::longValue).toArray();
            assertEquals(2, times.length);
            assertTrue(times[1] - times[0] >= TimeUnit.MILLISECONDS.toNanos(290), "a success did not space the next dial");
        }
    }

    @Test void aRejectedDecisionKeepsThePoolIsReportedAndTheRunGoesOn() throws Exception {
        var logger = new CapturingLogger();
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(ZmuxConfig.builder().eventLoops(loops).maxPipes(8)
                .pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation().logger(logger).build())) {
            var calls = new AtomicInteger();
            // Decision refuses a negative count: the policy's answer is rejected.
            PoolPolicy policy = v -> { calls.incrementAndGet(); return PoolDecision.KEEP.withCount(-1); };
            var run = f.client.maintainPipes(PipePoolTest.fast(f.dial()).withCount(2).withPolicy(policy));
            PipePoolTest.await(() -> run.pipes().size() == 2);
            int before = calls.get();
            PipePoolTest.await(() -> calls.get() > before + 2);
            assertFalse(logger.entries.isEmpty(), "the rejection was not reported");
            assertInstanceOf(IllegalArgumentException.class, logger.entries.peek().thrown());
            run.close();
            var result = run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(2, result.pipes().size());
        }
    }

    @Test void aRejectionCallbackReceivesEveryRejectedDecisionInPlaceOfTheLogger() throws Exception {
        var logger = new CapturingLogger();
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(ZmuxConfig.builder().eventLoops(loops).maxPipes(8)
                .pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation().logger(logger).build())) {
            var calls = new AtomicInteger(); var rejected = new ConcurrentLinkedQueue<Throwable>();
            PoolPolicy policy = v -> switch (calls.incrementAndGet() % 3) {
                case 0 -> null;
                case 1 -> throw new AssertionError("an Error is a rejection too");
                default -> PoolDecision.KEEP.withRecheck(Duration.ZERO);
            };
            var run = f.client.maintainPipes(PipePoolTest.fast(f.dial()).withCount(1).withPolicy(policy).withDecisionRejected(rejected::add));
            PipePoolTest.await(() -> run.pipes().size() == 1 && rejected.size() >= 4);
            assertTrue(rejected.stream().anyMatch(e -> e instanceof AssertionError), "an Error other than a VM error is rejected");
            assertTrue(rejected.stream().anyMatch(e -> e instanceof NullPointerException), "a null decision is rejected");
            assertTrue(rejected.stream().anyMatch(e -> e instanceof IllegalArgumentException), "a zero recheck is rejected");
            assertFalse(logger.has("pool policy"), "the callback replaces the logger");
            run.close();
            assertEquals(1, run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS).pipes().size());
        }
    }
}
