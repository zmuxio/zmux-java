package io.zmux;

import io.zmux.carrier.CarrierHalfSource;
import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class ControllerLifecycleTest {
    private static final CarrierKind KIND = CarrierKind.STREAMING;
    private static final String KEY = "route";

    @Test void recoveryRanksAtOneClockReadingUnderEveryChoice() {
        for (var choice : SessionController.Choice.values()) {
            var clock = new ManualClock();
            var domain = new ClockDomain(clock);
            var cache = new CarrierCache(CarrierCache.Config.DEFAULT, domain,
                    new java.util.Random(0) { @Override public double nextDouble() { return 1; } });
            var layout = List.of(new AttachmentSpec(Directions.BOTH, PipeOptions.defaults()));
            CarrierOpener unused = (c, r) -> { throw new AssertionError("unexpected opener"); };
            var candidates = List.of("a", "b", "c", "d", "e", "f", "standby-a", "standby-b").stream().map(id ->
                    new SessionController.Candidate(id, id, KIND, "", id.equals("f") ? 1 : 0,
                            id.startsWith("standby-"), false, layout, unused)).toList();
            var options = new SessionController.Options("ordering-" + choice, candidates, choice,
                    SessionController.Limits.DEFAULT, cache, domain, Map.of());
            try (var controller = new SessionController(options)) {
                cache.report("c", KIND, "", CarrierCache.Outcome.USABLE);
                cache.report("e", KIND, "", CarrierCache.Outcome.FAILED);
                cache.report("f", KIND, "", CarrierCache.Outcome.FAILED);
                clock.advance(Duration.ofNanos(1));
                cache.report("d", KIND, "", CarrierCache.Outcome.FAILED);
                cache.report("standby-b", KIND, "", CarrierCache.Outcome.FAILED);
                clock.advance(Duration.ofNanos(1));
                cache.report("standby-a", KIND, "", CarrierCache.Outcome.FAILED);
                cache.report("standby-a", KIND, "", CarrierCache.Outcome.PERFORMANCE,
                        new CarrierCache.Observation(null, Duration.ZERO, Duration.ofMillis(1), 10_000_000L));
                var ranks = controller.ranked(List.of("standby-b", "standby-a", "f", "e", "d", "c", "b", "a"), true, clock.nanoTime());
                assertEquals(List.of("c", "a", "b", "e", "d", "f", "standby-a", "standby-b"),
                        ranks.stream().map(SessionController.Candidate::id).toList());
            }
        }
    }

    @Test void aliasesAndChangingOwnersKeepOneLiveRecoverySequence() {
        var domain = new ClockDomain(new ManualClock());
        var cache = new CarrierCache(CarrierCache.Config.DEFAULT, domain,
                new java.util.Random(0) { @Override public double nextDouble() { return 1; } });
        var layout = List.of(new AttachmentSpec(Directions.BOTH, PipeOptions.defaults()));
        CarrierOpener unused = (c, r) -> { throw new AssertionError("unexpected opener"); };
        var a = new SessionController.Candidate("a", KEY, KIND, "", 0, false, false, layout, unused);
        var b = new SessionController.Candidate("b", KEY, KIND, "", 0, false, false, layout, unused);
        var options = new SessionController.Options("aliases", List.of(a, b), SessionController.Choice.STATIC,
                SessionController.Limits.DEFAULT, cache, domain, Map.of());
        try (var controller = new SessionController(options)) {
            var old = controller.new Owner();
            var next = controller.new Owner();
            old.recoveryDemand(Set.of(a, b));
            try (var permit = cache.acquire(KEY, KIND, "", true, () -> 0).attempt()) {
                assertTrue(permit.start());
                old.recoveryDemand(Set.of(b));
                next.recoveryDemand(Set.of(a));
                old.recoveryDemand(Set.of());
                permit.terminal(false, true, domain.now()).join();
            }
            assertEquals(Duration.ofSeconds(1), cache.retryMetrics(KEY, KIND, "").recoverySampled());
            next.recoveryDemand(Set.of(a, b));
            next.recoveryDemand(Set.of(b));
            assertEquals(CarrierCache.Suppression.RECOVERY_WAIT, cache.admission(KEY, KIND, "", true));
            next.recoveryDemand(Set.of());
            assertNull(cache.admission(KEY, KIND, "", true));
        }
    }

    @Test void recoveryCadenceAdvancesAtEqualityAndDiscardsTheLastDemand() {
        var clock = new ManualClock();
        var cache = new CarrierCache(CarrierCache.Config.DEFAULT, new ClockDomain(clock),
                new java.util.Random(0) { @Override public double nextDouble() { return 1; } });
        var key = new CarrierCache.Key(KEY, KIND, "");
        var owner = new Object();
        cache.recoveryDemand(key, owner, true);
        cache.report(KEY, KIND, "", CarrierCache.Outcome.FAILED);
        assertNull(cache.acquire(KEY, KIND, "", false, () -> 0).attempt());
        for (int seconds : new int[]{1, 2, 4, 8, 16, 32, 60, 60}) {
            try (var permit = cache.acquire(KEY, KIND, "", true, () -> 0).attempt()) {
                assertTrue(permit.start());
                permit.terminal(false, true, clock.nanoTime()).join();
            }
            clock.advance(Duration.ofSeconds(seconds).minusNanos(1));
            assertEquals(CarrierCache.Suppression.RECOVERY_WAIT, cache.admission(KEY, KIND, "", true));
            clock.advance(Duration.ofNanos(1));
            assertNull(cache.admission(KEY, KIND, "", true));
        }
        cache.recoveryDemand(key, owner, false);
        assertNull(cache.retryMetrics(KEY, KIND, "").recoveryRetryAt());
    }

    @Test void obsoleteRecoveryReturnsTheReservationToAnotherControllerWithoutInvokingItsOpener() throws Exception {
        var clock = new ManualClock();
        var domain = new ClockDomain(clock);
        var cache = new CarrierCache(CarrierCache.Config.DEFAULT, domain);
        var member = new MemberSpec(KEY, List.of(new AttachmentSpec(Directions.SEND, PipeOptions.defaults())));
        var invoked = new CompletableFuture<Void>();
        CarrierOpener opener = (setup, request) -> {
            invoked.complete(null);
            throw new java.io.IOException("unexpected opener start");
        };
        var candidate = new SessionController.Candidate(KEY, KEY, KIND, "", 0, false, false, member.attachments(), opener);
        var candidates = List.of(candidate);
        var choice = SessionController.Choice.STATIC;
        cache.report(KEY, KIND, "", CarrierCache.Outcome.FAILED);
        clock.advance(Duration.ofSeconds(5));
        try (var loops = EventLoops.create(2);
             var fixture = new SessionTestSupport.Fixture(SessionTestSupport.configBuilder(loops, 4)
                     .resumeTimeout(Duration.ofMinutes(2)).build());
             var first = new SessionController(new SessionController.Options("obsolete-probe", candidates, choice, SessionController.Limits.DEFAULT.withMaximumOpeners(1), cache, domain, java.util.Map.of()));
             var second = new SessionController(new SessionController.Options("available-probe", candidates, choice, SessionController.Limits.DEFAULT.withMaximumOpeners(1), cache, domain, java.util.Map.of()))) {
            SessionTestSupport.leaveReceiveOnly(fixture);
            var observed = fixture.client.capacity();
            var request = new ChangeRequest(observed.policyRevision(), observed.observed(),
                    Duration.ofSeconds(10), List.of(member), Set.of(), 0);
            var admitted = CapacityChange.begin(fixture.client, request, first.new Owner(), candidate, true, () -> 0, null, null);
            var secondOwner = second.new Owner();
            assertNotNull(admitted.change());
            try (var change = admitted.change(); var lease = change.controllerLease()) {
                assertNotNull(lease);
                assertNull(secondOwner.acquire(candidate, true, member.attachments().size(), false, () -> 0).lease());
                change.cancel();
                change.done().get(5, TimeUnit.SECONDS);
                assertFalse(lease.started());
                assertFalse(invoked.isDone());
            }
            try (var lease = secondOwner.acquire(candidate, true, member.attachments().size(), false, () -> 0).lease()) {
                assertNotNull(lease);
                assertTrue(lease.start());
            }
        }
    }

    @Test void dialAfterRechecksReservationsAtOpenerEntry() {
        var clock = new ManualClock();
        var cache = new CarrierCache(CarrierCache.Config.DEFAULT, new ClockDomain(clock));
        cache.report(KEY, KIND, "", CarrierCache.Outcome.FAILED);
        clock.advance(Duration.ofSeconds(5));
        var after = new java.util.concurrent.atomic.AtomicLong();
        try (var reserved = cache.acquire(KEY, KIND, "", true, after::get).attempt()) {
            assertNotNull(reserved);
            after.set(Duration.ofSeconds(70).toNanos());
            assertFalse(reserved.start());
            assertEquals(after.get(), reserved.blockedUntil());
        }
        // A different candidate may share the cache key and keep its own schedule.
        try (var independent = cache.acquire(KEY, KIND, "", true, () -> 0).attempt()) {
            assertNotNull(independent);
        }
        clock.advance(Duration.ofSeconds(65).minusNanos(1));
        assertNull(cache.acquire(KEY, KIND, "", true, after::get).attempt());
        clock.advance(Duration.ofNanos(1));
        for (int i = 0; i < 2; i++) {
            try (var ordinary = cache.acquire(KEY, KIND, "", true, after::get).attempt()) {
                assertNotNull(ordinary);
                assertTrue(ordinary.start());
            }
            clock.advance(Duration.ofNanos(1));
        }
        var metrics = cache.retryMetrics(KEY, KIND, "");
        assertEquals(2, metrics.attemptedStarts());
        assertFalse(metrics.suppressions().containsKey(CarrierCache.Suppression.DIAL_AFTER));
    }

    @Test void closingUnsubscribesFromTheSharedCacheWhileAnOpenerStillHoldsItsSlot() {
        var cache = new CarrierCache();
        var candidate = new SessionController.Candidate(KEY, KEY, KIND, "", 0, false, false,
                List.of(new AttachmentSpec(Directions.BOTH, PipeOptions.defaults())),
                (setup, request) -> { throw new AssertionError("fixture holds the lease directly"); });
        var options = new SessionController.Options("cache-listener-removal", List.of(candidate),
                SessionController.Choice.STATIC, SessionController.Limits.DEFAULT.withMaximumOpeners(1), cache, null, java.util.Map.of());
        var controller = new SessionController(options);
        try (var lease = controller.new Owner().acquire(candidate, false, candidate.attachments().size(), false, () -> 0).lease()) {
            assertNotNull(lease);
            assertTrue(lease.start());
            controller.close();
            assertFalse(controller.done().isDone());
            var signal = controller.available();
            cache.report(KEY, KIND, "", CarrierCache.Outcome.FAILED);
            assertFalse(signal.isDone());
        } finally {
            controller.close();
        }
        assertTrue(controller.done().isDone());
    }

    @Test void topologyAndAuthorizationApplyToConfirmedAttachments() throws Exception {
        try (var loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 4))) {
            var session = f.client;
            var before = session.capacity();
            var specification = new MemberSpec(KEY, List.of(new AttachmentSpec(Directions.BOTH, PipeOptions.defaults())));
            try (var change = session.beginChange(new ChangeRequest(before.policyRevision(), before.observed(),
                    Duration.ofSeconds(10), List.of(specification), Set.of(), 0))) {
                var attempt = change.startMember(0, (setup, request) -> {
                    var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                    f.listener.adapter().handle(pair[1], JoinHint.inBand(null));
                    return new CarrierOpener.Opened(List.of(pair[0]));
                });
                var result = attempt.completion().get(5, TimeUnit.SECONDS);
                assertEquals(Attempt.Disposition.CONFIRMED, result.disposition());
                assertEquals(CapacityChange.Status.COMMITTED, change.commit().status());
                PipeId id = result.pipes().getFirst();
                var local = session.pipes().stream().filter(p -> p.id().equals(id)).findFirst().orElseThrow();
                session.setSessionPolicy(new SessionPolicy(1, 0, null, before.limits(), Map.of(KEY, Directions.RECEIVE)));
                assertEquals(DirectionHealth.Reason.TOPOLOGY_EXCLUDED, local.health().orElseThrow().send().reason());
                assertEquals(Directions.RECEIVE, session.capacity().members().stream()
                        .filter(member -> member.id().equals(result.member())).findFirst().orElseThrow().covered());
                var remote = f.server.capacity();
                f.server.setSessionPolicy(new SessionPolicy(1, 1, null, remote.limits()));
                assertTrue(f.server.capacity().members().stream().filter(member -> member.pipes().contains(id))
                        .allMatch(CapacitySnapshot.Member::draining));
                assertEquals(Directions.BOTH, f.server.capacity().covered());
            }
        }
    }

    @Test void memoryBootstrapDoesNotSpendTheListenerDeadlineWaitingForTheClient() throws Exception {
        try (var loops = EventLoops.create(1)) {
            var cfg = SessionTestSupport.config(loops, 3);
            var deadline = ListenerConfig.DEFAULT_FIRST_RECORD_TIMEOUT;
            try (var listener = Zmux.listener(ListenerConfig.of(cfg))) {
                var early = MemoryPipe.pair(Framing.BYTE_STREAM);
                var listening = new CompletableFuture<Void>();
                var reader = new PipeReader() {
                    @Override public void start(Sink sink) {
                        early[1].reader().start(sink);
                        listening.complete(null);
                    }
                    @Override public void close() { early[1].reader().close(); }
                };
                var handled = listener.adapter().handle(early[1].withHalves(reader, early[1].writer()),
                        JoinHint.inBand(TestAddr.peer("early")));
                listening.get(5, TimeUnit.SECONDS);
                ListenerStatsTest.expire(loops, deadline, handled);
                assertEquals(1, listener.stats().timedOut());
                var failure = assertThrows(SessionClosedException.class, () -> Zmux.open(early[0], Role.CLIENT, cfg));
                assertInstanceOf(SessionError.PipesLost.class, failure.error());
            }

            try (var listener = Zmux.listener(ListenerConfig.of(cfg))) {
                var bootstrap = new SessionTestSupport.MemoryBootstrap();
                bootstrap.firstRecord.thenRun(() -> bootstrap.admit(listener));
                loops.next().advanceTimersForTests(deadline.toNanos()).get(5, TimeUnit.SECONDS);
                assertEquals(0, listener.stats().timedOut());
                try (var client = Zmux.open(bootstrap.client(), Role.CLIENT, cfg);
                     var server = listener.accept(Duration.ofSeconds(5))) {
                    assertFalse(client.closed().isDone());
                    assertNotNull(server);
                    assertEquals(1, listener.stats().sessions());
                }
            }
        }
    }

    @Test void accountingSurvivesProtocolRetirementAndWaitsForIoRelease() throws Exception {
        var closing = new CompletableFuture<Void>();
        var release = new CompletableFuture<Void>();
        var source = new CarrierHalfSource("spare", KIND, "test", "");
        try (var loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(3)))
                    .pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).accounting(new AccountingPolicy(true, 1, true)).build();
            try (var fixture = new SessionTestSupport.Fixture(config)) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var writer = new PipeWriter() {
                    @Override public void write(OutboundBatch batch, Completion done) { pair[0].writer().write(batch, done); }

                    @Override public void close() {
                        closing.complete(null);
                        release.join();
                        pair[0].writer().close();
                    }
                };
                fixture.listener.adapter().handle(pair[1], JoinHint.inBand(null));
                var pipe = fixture.client.addPipe(pair[0].withHalves(pair[0].reader(), writer).withAccountingSources(source, source),
                        PipeOptions.defaults());
                var ticket = pipe.accounting().orElseThrow();
                try {
                    pipe.abort();
                    closing.get(5, TimeUnit.SECONDS);
                    assertFalse(ticket.completion().isDone());
                } finally {
                    release.complete(null);
                }
                var totals = ticket.completion().get(5, TimeUnit.SECONDS);
                assertEquals(pipe.id(), totals.pipe());
                assertEquals(source, totals.send().getFirst().source());
                assertTrue(totals.send().getFirst().records() > 0);
                assertEquals(0, totals.send().getFirst().activeBytes());
                assertTrue(totals.send().getFirst().retiredBytes() > 0);
                assertEquals(totals, ticket.snapshot());
            }
        }
    }

    private static final class ManualClock implements ClockDomain.Source {
        private long now;

        @Override public long nanoTime() { return now; }
        @Override public Instant wallTime() { return Instant.EPOCH.plusNanos(now); }
        @Override public Runnable schedule(long delayNanos, Runnable task) {
            throw new IllegalStateException("cache-only clock");
        }

        void advance(Duration duration) { now += duration.toNanos(); }
    }
}
