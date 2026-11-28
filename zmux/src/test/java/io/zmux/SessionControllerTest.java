package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static io.zmux.SessionTestSupport.awaitCapacity;

@Timeout(60)
class SessionControllerTest {
    @Test void qualityDecisionRequiresTheObservedGeneration() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        var domain = domain(clock);
        try (var loops = EventLoops.create(2); var fixture = new SessionTestSupport.Fixture(
                SessionTestSupport.configBuilder(loops, 4).clockDomain(domain).build());
             var controller = new SessionController(new SessionController.Options("quality-input", List.of(),
                     SessionController.Choice.STATIC, SessionController.Limits.DEFAULT, null, domain, Map.of()));
             var control = controller.manage(fixture.client, SessionTarget.empty())) {
            var cap = fixture.client.capacity();
            var member = cap.members().getFirst();
            var path = member.paths().getFirst();
            long generation = path.health().send().evidenceGeneration();
            var bad = new SessionTarget.QualityInput(control.snapshot().generation(), 0, member.id(), path.pipe(),
                    Direction.OUTBOUND, generation + 1, SessionTarget.QualityInput.Field.HEALTH, 0, 0, 0, null, 0);
            var replace = new SessionTarget.Replacement("quality", Set.of(member.id()), 0, List.of(bad));
            assertThrows(SessionController.StaleDecisionException.class,
                    () -> control.setTarget(new SessionTarget(1, List.of(), List.of(replace))));
            assertEquals(0, control.snapshot().targetRevision());

        }
    }

    @Test void releaseAndTakeoverCompareTheExactHandle() throws Exception {
        try (var loops = EventLoops.create(2); var fixture = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 4));
             var first = new SessionController(new SessionController.Options("handoff-a", List.of(), SessionController.Choice.STATIC,
                     SessionController.Limits.DEFAULT, null, null, Map.of()));
             var second = new SessionController(new SessionController.Options("handoff-b", List.of(), SessionController.Choice.STATIC,
                     SessionController.Limits.DEFAULT, null, null, Map.of()))) {
            var target = SessionTarget.empty();
            var original = first.manage(fixture.client, target);
            assertThrows(IllegalArgumentException.class, () -> first.takeOver(original, target));
            var next = second.takeOver(original, target);
            assertEquals(original.generation(), next.snapshot().predecessor());
            assertEquals(original.generation() + 1, next.generation());
            assertThrows(SessionController.StaleBindingException.class, original::release);
            assertThrows(SessionController.StaleBindingException.class, () -> first.takeOver(original, target));
            next.release();
            var again = first.manage(fixture.client, target);
            assertEquals(next.generation() + 1, again.generation());
            assertNull(again.snapshot().predecessor());
            again.release();
        }
    }

    @Test void stableMemberRegistrationAndSubsequentSharedFaultRearmHealthReset() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        var domain = domain(clock);
        var defaults = CarrierCache.Config.DEFAULT;
        var cache = new CarrierCache(new CarrierCache.Config(defaults.halfLife(), defaults.dropWindow(),
                defaults.performanceWindow(), Duration.ofMillis(10), defaults.backoff(), defaults.maxImportBytes(),
                defaults.maxImportEntries()), domain);
        String key = "stable";
        var kind = CarrierKind.STREAMING;
        var candidate = new SessionController.Candidate(key, key, kind, "", 0, false, false, List.of(DUPLEX),
                (setup, request) -> { throw new AssertionError("unexpected opener"); });
        try (var loops = EventLoops.create(2); var fixture = new SessionTestSupport.Fixture(
                SessionTestSupport.configBuilder(loops, 4).clockDomain(domain).noMaxIdleTimeout().build());
             var controller = new SessionController(new SessionController.Options("health-reset", List.of(candidate),
                     SessionController.Choice.STATIC, SessionController.Limits.DEFAULT, cache, domain, Map.of()))) {
            var feedback = new MemberFeedback(controller, fixture.client);
            var id = new MemberId(17);
            cache.report(key, kind, "", CarrierCache.Outcome.FAILED);
            fixture.client.run(op -> {
                feedback.observe(new io.zmux.internal.core.SessionCapacity.MemberHealth(id, key, Directions.BOTH,
                        Directions.BOTH, Directions.BOTH, domain.now(), false, false, null));
                return null;
            });
            clock.advance(Duration.ofMillis(10));
            assertEquals(0, cache.score(key, kind, "").cooldownLevel());
            cache.report(key, kind, "", CarrierCache.Outcome.FAILED);
            clock.advance(Duration.ofMillis(1));
            cache.report(key, kind, "", CarrierCache.Outcome.SHORT_DROP);
            clock.advance(Duration.ofMillis(10).minusNanos(1));
            assertEquals(2, cache.score(key, kind, "").cooldownLevel());
            clock.advance(Duration.ofNanos(1));
            assertEquals(0, cache.score(key, kind, "").cooldownLevel());
            cache.report(key, kind, "", CarrierCache.Outcome.FAILED);
            fixture.client.run(op -> {
                feedback.observe(new io.zmux.internal.core.SessionCapacity.MemberHealth(id, key, Directions.BOTH,
                        Directions.SEND, Directions.SEND, domain.now(), false, false, null));
                feedback.close();
                return null;
            });
            clock.advance(Duration.ofMillis(10));
            assertEquals(1, cache.score(key, kind, "").cooldownLevel());
        }
    }

    @Test void bootstrapAttributionSurvivesControllerReplacement() throws Exception {
        try (var loops = EventLoops.create(2)) {
            var config = SessionTestSupport.config(loops, 4);
            var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
            pair[0] = pair[0].withCarrier(CarrierKind.STREAMING).withVariant("original").withLabel("original route");
            try (var fixture = new SessionTestSupport.Fixture(config, pair)) {
                var session = fixture.client;
                var initial = session.capacity().members().getFirst();
                CarrierOpener unused = (setup, request) -> { throw new AssertionError("unexpected carrier open"); };
                var primary = new SessionController.Candidate("primary", "primary", CarrierKind.STREAMING, "original",
                        0, false, false, List.of(DUPLEX), unused);
                var other = new SessionController.Candidate("other", "other", CarrierKind.STREAMING, "original",
                        0, false, false, List.of(DUPLEX), unused);
                try (var first = new SessionController(new SessionController.Options(UUID.randomUUID().toString(),
                            List.of(primary), SessionController.Choice.STATIC, SessionController.Limits.DEFAULT.withMaximumOpeners(1), null, null, java.util.Map.of()));
                     var second = new SessionController(new SessionController.Options(UUID.randomUUID().toString(),
                            List.of(primary, other), SessionController.Choice.STATIC, SessionController.Limits.DEFAULT.withMaximumOpeners(1), null, null, java.util.Map.of()))) {
                    var target = SessionTarget.empty();
                    try (var control = first.manage(session, target)) {
                        assertThrows(IllegalStateException.class, () -> second.manage(session, target, primary.id()));
                        assertEquals("", session.capacity().members().getFirst().candidate());
                        control.release();
                        control.done().get(30, TimeUnit.SECONDS);
                    }
                    try (var control = first.manage(session, target, primary.id())) {
                        assertEquals(primary.id(), control.snapshot().capacity().members().getFirst().candidate());
                        control.release();
                        control.done().get(30, TimeUnit.SECONDS);
                    }
                    assertThrows(IllegalArgumentException.class, () -> second.manage(session, target, other.id()));
                    try (var control = second.manage(session, target, primary.id())) {
                        var member = control.snapshot().capacity().members().getFirst();
                        assertEquals(primary.id(), member.candidate());
                        assertEquals(initial.id(), member.id());
                        assertEquals(initial.pipes(), member.pipes());
                        assertEquals(initial.expected(), member.expected());
                        assertEquals(initial.covered(), member.covered());
                        var stats = session.pipes().getFirst().stats().orElseThrow();
                        assertEquals("original route", stats.label());
                        assertEquals(CarrierKind.STREAMING, stats.carrier());
                        assertEquals("original", stats.variant());
                    }
                }
            }
        }
    }

    private static final AttachmentSpec DUPLEX = new AttachmentSpec(Directions.BOTH, PipeOptions.defaults());

    @Test void aPreparedBackupCanBootstrapWhileTheFirstOpenerStillHoldsItsExecutionSlot() throws Exception {
        try (var loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            try (var listener = Zmux.listener(ListenerConfig.of(config))) {
                var firstEntered = new CompletableFuture<Void>();
                var releaseFirst = new CompletableFuture<Void>();
                var rejected = new DiscardProbe();
                var first = new SessionController.Candidate("first", "first", CarrierKind.STREAMING, "", 0, false, true,
                        List.of(DUPLEX), (setup, request) -> {
                            firstEntered.complete(null);
                            releaseFirst.join();
                            return new CarrierOpener.Opened(List.of(rejected.carrier()));
                        });
                var backup = new SessionController.Candidate("backup", "backup", CarrierKind.STREAMING, "", 1, false, true,
                        List.of(DUPLEX), (setup, request) -> {
                            firstEntered.join();
                            var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                            listener.adapter().handle(pair[1], JoinHint.inBand(null));
                            return new CarrierOpener.Opened(List.of(pair[0]));
                        });
                var options = new SessionController.Options(UUID.randomUUID().toString(), List.of(first, backup),
                        SessionController.Choice.STATIC, SessionController.Limits.DEFAULT.withMaximumOpeners(2).withAttemptTimeout(Duration.ofSeconds(45)), null, null, java.util.Map.of());
                var controller = new SessionController(options);
                try {
                    try (var session = controller.open(config, SessionTarget.empty()).session();
                         var peer = listener.accept(Duration.ofSeconds(30))) {
                        assertNotNull(peer);
                        controller.close();
                        assertFalse(controller.done().isDone());
                        assertThrows(IllegalStateException.class, () -> new SessionController(options));
                        assertFalse(session.isClosed());
                    } finally {
                        releaseFirst.complete(null);
                    }
                    rejected.await();
                    controller.done().get(30, TimeUnit.SECONDS);
                } finally {
                    controller.close();
                }
            }
        }
    }

    @Test void aNewTargetDiscardsLateCarriersAndPreservesTheOnlyReceiveDirection() throws Exception {
        try (var loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(3)))
                    .pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).resumeTimeout(Duration.ofMinutes(2)).build();
            try (var fixture = new SessionTestSupport.Fixture(config)) {
                var session = fixture.client;
                var receive = new AttachmentSpec(Directions.RECEIVE, PipeOptions.defaults());
                var before = session.capacity();
                var request = new ChangeRequest(before.policyRevision(), before.observed(), Duration.ofMinutes(1),
                        List.of(new MemberSpec("old", List.of(receive))), Set.of(), 0);
                MemberId original;
                try (var addition = session.beginChange(request)) {
                    var attempt = addition.startMember(0, (setup, open) -> {
                        var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                        fixture.listener.adapter().handle(pair[1], JoinHint.inBand(null));
                        return new CarrierOpener.Opened(List.of(pair[0]));
                    });
                    var result = attempt.completion().get(30, TimeUnit.SECONDS);
                    assertEquals(Attempt.Disposition.CONFIRMED, result.disposition(), () -> String.valueOf(result.error()));
                    assertEquals(CapacityChange.Status.COMMITTED, addition.commit().status());
                    original = result.member();
                }
                session.pipes().getFirst().abort();
                awaitCapacity(session, view -> view.covered() == Directions.RECEIVE);
                var entered = new CompletableFuture<Void>();
                var cancelled = new CompletableFuture<Void>();
                var returnCarrier = new CompletableFuture<Void>();
                var recoveryEntered = new CompletableFuture<Void>();
                var returnRecovery = new CompletableFuture<Void>();
                var openers = new java.util.concurrent.atomic.AtomicInteger();
                var discarded = new DiscardProbe();
                var recoveryDiscarded = new DiscardProbe();
                var old = new SessionController.Candidate("old", "old", CarrierKind.STREAMING, "", 0, false, false,
                        List.of(receive), (setup, open) -> { throw new AssertionError("old member is already present"); });
                var replacement = new SessionController.Candidate("extra", "extra", CarrierKind.QUIC, "", 0, false, false,
                        List.of(DUPLEX), (setup, open) -> {
                            if (openers.incrementAndGet() == 1) {
                                var registration = setup.onCancel(() -> cancelled.complete(null));
                                try {
                                    entered.complete(null);
                                    returnCarrier.join();
                                    return new CarrierOpener.Opened(List.of(discarded.carrier()));
                                } finally {
                                    registration.close();
                                }
                            }
                            recoveryEntered.complete(null);
                            returnRecovery.join();
                            return new CarrierOpener.Opened(List.of(recoveryDiscarded.carrier()));
                        });
                var options = new SessionController.Options(UUID.randomUUID().toString(), List.of(old, replacement),
                        SessionController.Choice.STATIC, SessionController.Limits.DEFAULT.withMaximumOpeners(1).withAttemptTimeout(Duration.ofMinutes(1)), null, null, java.util.Map.of());
                var desired = new SessionTarget(1, target(1, 1).groups(), List.of(new SessionTarget.Replacement("replace",
                        Set.of(original), 0)));
                var controller = new SessionController(options);
                try (controller) {
                    try (var control = controller.manage(session, desired)) {
                        try {
                            entered.get(30, TimeUnit.SECONDS);
                            control.setTarget(new SessionTarget(2, List.of(new SessionTarget.Group("group",
                                    List.of(new SessionTarget.Quota("quota", 1, Directions.RECEIVE, List.of("old"))), SessionTarget.Rotation.NONE)), List.of()));
                            cancelled.get(30, TimeUnit.SECONDS);
                        } finally {
                            returnCarrier.complete(null);
                        }
                        // Quotas govern planned changes. Recovery can still use extra for the missing send direction.
                        recoveryEntered.get(30, TimeUnit.SECONDS);
                        discarded.await();
                        var current = session.capacity();
                        assertEquals(Directions.RECEIVE, current.covered());
                        assertTrue(current.members().stream().anyMatch(member -> member.id().equals(original)
                                && member.covered() == Directions.RECEIVE));
                        assertTrue(current.members().stream().noneMatch(member -> member.candidate().equals("extra")));
                    } finally {
                        returnRecovery.complete(null);
                    }
                }
                controller.done().get(30, TimeUnit.SECONDS);
                assertEquals(2, openers.get());
                discarded.await();
                recoveryDiscarded.await();
            }
        }
    }

    @Test void theOwnerDeadlineFinishesTheAttemptBeforeItsOpenerAndIoLifetime() throws Exception {
        try (var loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(2)))
                    .pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noMaxIdleTimeout()
                    .liveness(Liveness.builder().noDeadTimeout().noProbeInterval().build()).build();
            try (var fixture = new SessionTestSupport.Fixture(config)) {
                var session = fixture.client;
                var capacity = session.capacity();
                var request = new ChangeRequest(capacity.policyRevision(), capacity.observed(), Duration.ofMinutes(1),
                        List.of(new MemberSpec("late", List.of(DUPLEX))), Set.of(), 0);
                var discarded = new DiscardProbe();
                var entered = new CompletableFuture<Void>();
                var proceed = new CompletableFuture<Void>();
                try (var change = session.beginChange(request)) {
                    var attempt = change.startMember(0, (setup, open) -> {
                        entered.complete(null);
                        proceed.join();
                        return new CarrierOpener.Opened(List.of(discarded.carrier()));
                    });
                    try {
                        entered.get(30, TimeUnit.SECONDS);
                        session.driver.loop().advanceTimersForTests(request.timeout().toNanos()).get(30, TimeUnit.SECONDS);
                        var result = attempt.completion().get(30, TimeUnit.SECONDS);
                        assertEquals(Attempt.Disposition.DEADLINE, result.disposition());
                        assertFalse(attempt.released().isDone());
                        change.done().get(30, TimeUnit.SECONDS);
                    } finally {
                        proceed.complete(null);
                    }
                    discarded.await();
                    attempt.released().get(30, TimeUnit.SECONDS);
                    assertEquals(Attempt.Disposition.DEADLINE, attempt.snapshot().result().disposition());
                }
            }
        }
    }

    @Test void aConfirmedRecoveryWithdrawsTheAlternateWhileItsOpenerIsStillRunning() throws Exception {
        try (var loops = EventLoops.create(2); var fixture = new SessionTestSupport.Fixture(
                SessionTestSupport.configBuilder(loops, 4).resumeTimeout(Duration.ofMinutes(2)).build())) {
            var confirmWinner = new CompletableFuture<Void>();
            var alternateEntered = new CompletableFuture<Void>();
            var alternateCancelled = new CompletableFuture<Void>();
            var returnAlternates = new CompletableFuture<Void>();
            var first = new SessionController.Candidate("winner", "winner", CarrierKind.STREAMING, "", 0, false, false,
                    List.of(DUPLEX), (setup, request) -> {
                        confirmWinner.join();
                        return SessionTestSupport.memoryOpener(fixture.listener, (spec, io) -> io).open(setup, request);
                    });
            var alternate = new SessionController.Candidate("alternate", "alternate", CarrierKind.STREAMING, "", 1, false, false,
                    List.of(DUPLEX), (setup, request) -> {
                        var registration = setup.onCancel(() -> alternateCancelled.complete(null));
                        try {
                            alternateEntered.complete(null);
                            returnAlternates.join();
                            return new CarrierOpener.Opened(List.of());
                        } finally {
                            registration.close();
                        }
                    });
            var quota = new SessionTarget.Quota("recovery", 1, Directions.BOTH, List.of(first.id(), alternate.id()));
            var target = new SessionTarget(1, List.of(new SessionTarget.Group("recovery", List.of(quota), SessionTarget.Rotation.NONE)), List.of());
            var options = new SessionController.Options(UUID.randomUUID().toString(), List.of(first, alternate),
                    SessionController.Choice.STATIC, SessionController.Limits.DEFAULT, null, null, java.util.Map.of());
            fixture.client.pipes().getFirst().abort();
            awaitCapacity(fixture.client, capacity -> capacity.covered() == Directions.NONE);
            try (var controller = new SessionController(options); var control = controller.manage(fixture.client, target)) {
                try {
                    alternateEntered.get(5, TimeUnit.SECONDS);
                    confirmWinner.complete(null);
                    alternateCancelled.get(5, TimeUnit.SECONDS);
                    assertEquals(Directions.BOTH, fixture.client.capacity().covered());
                } finally {
                    control.release();
                    confirmWinner.complete(null);
                    returnAlternates.complete(null);
                }
            }
        }
    }

    @Test void sendConfirmationKeepsTheDuplexAlternative() {
        var need = new SessionControl.RecoveryNeed(Directions.BOTH);
        assertTrue(need.includes(Directions.SEND));
        assertTrue(need.includes(Directions.BOTH));

        need.confirmed(Directions.SEND);
        assertEquals(Directions.RECEIVE, need.missing());
        assertTrue(need.includes(Directions.BOTH));
        assertFalse(need.includes(Directions.SEND));

        need.confirmed(Directions.BOTH);
        assertEquals(Directions.NONE, need.missing());
        assertFalse(need.includes(Directions.BOTH));
    }

    @Test void ownerRechecksRecoveryBeforeAReleasedOpenerCanWriteJoin() throws Exception {
        try (var loops = EventLoops.create(2); var fixture = new SessionTestSupport.Fixture(
                SessionTestSupport.configBuilder(loops, 4).resumeTimeout(Duration.ofMinutes(2)).build())) {
            var session = fixture.client;
            SessionTestSupport.leaveReceiveOnly(fixture);
            var returned = new CompletableFuture<Void>();
            var entered = new CompletableFuture<CarrierOpener.Request>();
            var carrier = new DiscardProbe();
            try (var recovery = SessionTestSupport.change(session, member("recovery", Directions.SEND), Set.of())) {
                var attempt = recovery.startControllerMember(0, (scope, request) -> {
                    entered.complete(request);
                    returned.join();
                    return new CarrierOpener.Opened(List.of(carrier.carrier()));
                }, Directions.SEND);
                try {
                    assertEquals(Directions.SEND, entered.get(30, TimeUnit.SECONDS).need());
                    SessionTestSupport.add(fixture, member("backup", Directions.SEND));
                    awaitCapacity(session, capacity -> capacity.covered() == Directions.BOTH);
                } finally {
                    returned.complete(null);
                }
                assertEquals(Attempt.Disposition.NO_LONGER_NEEDED, attempt.completion().get(30, TimeUnit.SECONDS).disposition());
                carrier.await();
                attempt.released().get(30, TimeUnit.SECONDS);
            }
        }
    }

    @Test void aNewTopologyRevisionAdmitsRecoveryWithoutChargingTheExcludedCandidate() throws Exception {
        String key = "route";
        var cache = new CarrierCache();
        var limits = new CapacityLimits(4);
        var denied = new SessionPolicy(1, 0, null, limits, Map.of(key, Directions.NONE));
        var entered = new CompletableFuture<CarrierOpener.Request>();
        var release = new CompletableFuture<Void>();
        var candidate = new SessionController.Candidate(key, key, CarrierKind.STREAMING, "", 0, false, false,
                member(key, Directions.SEND).attachments(), (scope, request) -> {
                    entered.complete(request);
                    release.join();
                    return new CarrierOpener.Opened(List.of(), new java.io.IOException("fixture stopped"));
                });
        var options = new SessionController.Options(UUID.randomUUID().toString(), List.of(candidate),
                SessionController.Choice.D160, SessionController.Limits.DEFAULT.withMaximumOpeners(1), cache, null, java.util.Map.of());
        try (var loops = EventLoops.create(2);
             var fixture = new SessionTestSupport.Fixture(SessionTestSupport.configBuilder(loops, limits.steadyAttachments())
                     .resumeTimeout(Duration.ofMinutes(2)).sessionPolicy(denied).build());
             var controller = new SessionController(options)) {
            SessionTestSupport.leaveReceiveOnly(fixture);
            try (var control = controller.manage(fixture.client, SessionTestSupport.sendRecoveryTarget(key))) {
                var blocked = SessionTestSupport.observe(control, view -> view.blocked() != SessionControl.Blocking.NONE);
                assertEquals(SessionControl.Blocking.TOPOLOGY_EXCLUDED, blocked.blocked());
                assertEquals(0, blocked.capacity().reserved());
                assertTrue(blocked.attempts().isEmpty());
                assertFalse(entered.isDone());
                var score = cache.score(key, candidate.kind(), candidate.variant());
                assertEquals(0, score.attempts());
                assertTrue(score.cooling().isEmpty());

                fixture.client.setSessionPolicy(new SessionPolicy(2, 0, null, limits, Map.of(key, Directions.SEND)));
                var request = entered.get(30, TimeUnit.SECONDS);
                assertEquals(CarrierOpener.Stage.JOIN, request.stage());
                assertEquals(Directions.SEND, request.need());
                var admitted = SessionTestSupport.observe(control, view -> !view.attempts().isEmpty());
                assertEquals(List.of(request.attempt()), admitted.attempts());
                assertEquals(2, admitted.capacity().policyRevision());
            } finally {
                release.complete(null);
            }
        }
    }

    static ClockDomain domain(SpliceHooksTest.ManualClock clock) {
        return domain(clock, ignored -> {});
    }

    static ClockDomain domain(SpliceHooksTest.ManualClock clock, java.util.function.LongConsumer scheduled) {
        return new ClockDomain(new ClockDomain.Source() {
            @Override public long nanoTime() { return clock.now(); }
            @Override public java.time.Instant wallTime() { return java.time.Instant.EPOCH.plusNanos(clock.now()); }
            @Override public Runnable schedule(long delay, Runnable task) {
                long at = clock.now() + delay;
                var cancel = clock.schedule(at, task);
                scheduled.accept(at);
                return cancel;
            }
        });
    }

    @Test void aRoundOfHookFailuresParksUntilANewTarget() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        var domain = domain(clock);
        var cache = new CarrierCache(CarrierCache.Config.DEFAULT, domain);
        var calls = new java.util.concurrent.LinkedBlockingQueue<String>();
        var layout = List.of(DUPLEX);
        var paths = new java.util.ArrayList<SessionController.Candidate>();
        for (String key : List.of("p", "q")) {
            paths.add(new SessionController.Candidate(key, key, CarrierKind.STREAMING, "", 0, false, false, layout,
                    (setup, request) -> { calls.add(key); throw new IllegalStateException("host callback"); }));
        }
        var quota = new SessionTarget.Quota("both", 1, Directions.BOTH, List.of("p", "q"));
        var group = new SessionTarget.Group("repair", List.of(quota), SessionTarget.Rotation.NONE);
        try (var loops = EventLoops.create(2); var fixture = new SessionTestSupport.Fixture(
                SessionTestSupport.configBuilder(loops, 4).clockDomain(domain).resumeTimeout(Duration.ofMinutes(2)).build());
             var controller = new SessionController(new SessionController.Options("neutral-round", paths,
                     SessionController.Choice.STATIC, SessionController.Limits.DEFAULT, cache, domain, Map.of()))) {
            fixture.client.pipes().getFirst().abort();
            awaitCapacity(fixture.client, c -> c.covered() == Directions.NONE);
            try (var control = controller.manage(fixture.client, new SessionTarget(1, List.of(group), List.of()))) {
                assertEquals("p", calls.poll(5, TimeUnit.SECONDS));
                assertEquals("q", calls.poll(5, TimeUnit.SECONDS));
                SessionTestSupport.observe(control, view -> view.blocked() == SessionControl.Blocking.NO_ELIGIBLE_CANDIDATE
                        && view.attempts().isEmpty());
                assertTrue(calls.isEmpty());
                control.setTarget(new SessionTarget(2, List.of(group), List.of()));
                assertEquals("p", calls.poll(5, TimeUnit.SECONDS));
                for (var path : paths) assertNull(cache.retryMetrics(path.cacheKey(), path.kind(), "").recoveryRetryAt());
            }
        }
    }

    @Test void singleCandidateRecoveryRunsAtTheSampledDeadline() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        var domain = domain(clock);
        var cache = new CarrierCache(CarrierCache.Config.DEFAULT, domain,
                new java.util.Random(0) { @Override public double nextDouble() { return 1; } });
        var calls = new java.util.concurrent.LinkedBlockingQueue<Long>();
        var hold = new CompletableFuture<Void>();
        var first = new java.util.concurrent.atomic.AtomicBoolean(true);
        var candidate = new SessionController.Candidate("only", "only", CarrierKind.STREAMING, "", 0, true, false,
                List.of(DUPLEX), (setup, request) -> {
                    calls.add(clock.now());
                    if (first.getAndSet(false)) return new CarrierOpener.Opened(List.of(), new java.io.IOException("dial failed"));
                    hold.join();
                    return new CarrierOpener.Opened(List.of());
                });
        try (var loops = EventLoops.create(2); var fixture = new SessionTestSupport.Fixture(
                SessionTestSupport.configBuilder(loops, 4).clockDomain(domain).resumeTimeout(Duration.ofMinutes(2)).build());
             var controller = new SessionController(new SessionController.Options("single-cadence", List.of(candidate),
                     SessionController.Choice.STATIC, SessionController.Limits.DEFAULT, cache, domain, Map.of()))) {
            fixture.client.pipes().getFirst().abort();
            awaitCapacity(fixture.client, c -> c.covered() == Directions.NONE);
            var quota = new SessionTarget.Quota("repair", 1, Directions.BOTH, List.of(candidate.id()));
            var target = new SessionTarget(1, List.of(new SessionTarget.Group("repair", List.of(quota), SessionTarget.Rotation.NONE)), List.of());
            try (var control = controller.manage(fixture.client, target)) {
                long at = calls.poll(5, TimeUnit.SECONDS);
                long due = at + Duration.ofSeconds(1).toNanos();
                SessionTestSupport.observe(control, view -> Long.valueOf(due).equals(view.nextRetry()));
                clock.advance(Duration.ofSeconds(1).minusNanos(1));
                assertNull(calls.peek());
                clock.advance(Duration.ofNanos(1));
                assertEquals(Long.valueOf(due), calls.poll(5, TimeUnit.SECONDS));
            } finally { hold.complete(null); }
        }
    }

    @Test void aSingleCoolingStandbyCanRecoverImmediately() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        var domain = domain(clock);
        var cache = new CarrierCache(CarrierCache.Config.DEFAULT, domain);
        var kind = CarrierKind.STREAMING;
        var called = new CompletableFuture<Long>();
        var release = new CompletableFuture<Void>();
        var candidate = new SessionController.Candidate("only", "only", kind, "", 0, true, false,
                List.of(DUPLEX), (setup, request) -> {
                    called.complete(clock.now());
                    release.join();
                    return new CarrierOpener.Opened(List.of());
                });
        cache.report(candidate.cacheKey(), kind, "", CarrierCache.Outcome.FAILED);
        var options = new SessionController.Options("single-recovery-cooldown", List.of(candidate),
                SessionController.Choice.STATIC, SessionController.Limits.DEFAULT, cache, domain, Map.of());
        try (var loops = EventLoops.create(2);
             var fixture = new SessionTestSupport.Fixture(SessionTestSupport.configBuilder(loops, 4)
                     .clockDomain(domain).resumeTimeout(Duration.ofMinutes(2)).build());
             var controller = new SessionController(options)) {
            fixture.client.pipes().getFirst().abort();
            awaitCapacity(fixture.client, capacity -> capacity.covered() == Directions.NONE);
            try (var control = controller.manage(fixture.client, SessionTarget.empty())) {
                try {
                    assertEquals(clock.now(), called.get(5, TimeUnit.SECONDS));
                } finally {
                    control.release();
                    release.complete(null);
                }
            }
        }
    }

    @Test void enteringSuspensionShortensAnAlreadyStartedAttempt() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        var domain = domain(clock);
        var entered = new CompletableFuture<CarrierOpener.Request>();
        var release = new CompletableFuture<Void>();
        var grace = Duration.ofSeconds(2);
        try (var loops = EventLoops.create(2);
             var fixture = new SessionTestSupport.Fixture(SessionTestSupport.configBuilder(loops, 4)
                     .clockDomain(domain).resumeTimeout(grace).build())) {
            CarrierOpener opener = (setup, request) -> {
                entered.complete(request);
                release.join();
                return new CarrierOpener.Opened(List.of());
            };
            var candidate = new SessionController.Candidate("attempt", "attempt", CarrierKind.STREAMING, "", 0, false, false,
                    List.of(DUPLEX), opener);
            var options = new SessionController.Options("shorten-running-attempt", List.of(candidate),
                    SessionController.Choice.STATIC, SessionController.Limits.DEFAULT, null, domain, Map.of());
            try (var controller = new SessionController(options)) {
                var session = fixture.client;
                var snapshot = session.capacity();
                var timeout = options.limits().attemptTimeout();
                var request = new ChangeRequest(snapshot.policyRevision(), snapshot.observed(), timeout,
                        List.of(candidate.member()), Set.of(), 0);
                var admitted = CapacityChange.begin(session, request, controller.new Owner(), candidate, false, () -> 0, null, null);
                try (var change = admitted.change()) {
                    try {
                        var attempt = change.startControllerMember(0, opener, null);
                        assertEquals(timeout, entered.get(5, TimeUnit.SECONDS).timeout());
                        session.pipes().getFirst().abort();
                        awaitCapacity(session, capacity -> capacity.covered() == Directions.NONE);
                        clock.advance(grace.minusNanos(2));
                        assertFalse(attempt.completion().isDone());
                        clock.advance(Duration.ofNanos(1));
                        assertEquals(Attempt.Disposition.DEADLINE, attempt.completion().get(5, TimeUnit.SECONDS).disposition());
                        assertFalse(session.isClosed());
                    } finally {
                        release.complete(null);
                    }
                }
            }
        }
    }

    @Test void managedDialAfterWaitsPastTheProbeWindowAndChargesARefusalAsOneFailure() throws Exception {
        String id = "scheduled";
        long initial = Duration.ofSeconds(70).toNanos();
        long updated = Duration.ofSeconds(75).toNanos();
        var retryArmed = new CompletableFuture<Void>();
        var clock = new SpliceHooksTest.ManualClock();
        var domain = domain(clock, at -> {
            if (at == updated) retryArmed.complete(null);
        });
        var cache = new CarrierCache(CarrierCache.Config.DEFAULT, domain);
        var kind = CarrierKind.STREAMING;
        var called = new CompletableFuture<Long>();
        var settled = new CompletableFuture<CarrierCache.CooldownStarted>();
        var subscribed = new CompletableFuture<Void>();
        cache.report(id, kind, "", CarrierCache.Outcome.FAILED);
        cache.cooldownEvents().subscribe(new java.util.concurrent.Flow.Subscriber<>() {
            @Override public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {
                subscription.request(1);
                subscribed.complete(null);
            }
            @Override public void onNext(CarrierCache.CooldownEvent event) {
                settled.complete((CarrierCache.CooldownStarted) event);
            }
            @Override public void onError(Throwable failure) { settled.completeExceptionally(failure); }
            @Override public void onComplete() { }
        });
        subscribed.get(5, TimeUnit.SECONDS);
        var candidate = new SessionController.Candidate(id, id, kind, "", 0, false, false,
                member(id, Directions.SEND).attachments(), (setup, request) -> {
                    called.complete(clock.now());
                    return new CarrierOpener.Opened(List.of(), new java.io.IOException("authenticated refusal",
                            new CarrierRetryException(Duration.ofMinutes(5))));
                });
        var options = new SessionController.Options(UUID.randomUUID().toString(), List.of(candidate),
                SessionController.Choice.D160, SessionController.Limits.DEFAULT.withMaximumOpeners(1), cache, domain, Map.of(id, initial));

        try (var loops = EventLoops.create(2);
             var fixture = new SessionTestSupport.Fixture(SessionTestSupport.configBuilder(loops, 4)
                     .clockDomain(domain).resumeTimeout(Duration.ofMinutes(2)).build());
             var controller = new SessionController(options)) {
            SessionTestSupport.leaveReceiveOnly(fixture);
            clock.advance(Duration.ofSeconds(5));
            try (var control = controller.manage(fixture.client, SessionTestSupport.sendRecoveryTarget(id))) {
                SessionTestSupport.observe(control, view -> view.blocked() == SessionControl.Blocking.DIAL_AFTER
                        && view.nextRetry() == initial);
                assertEquals(1L, cache.retryMetrics(id, kind, "").suppressions().get(CarrierCache.Suppression.DIAL_AFTER));
                control.setDialAfter(id, updated);
                SessionTestSupport.observe(control, view -> view.blocked() == SessionControl.Blocking.DIAL_AFTER
                        && view.nextRetry() == updated);
                // Publication of the blocked snapshot precedes registration of the retry timer.
                retryArmed.get(5, TimeUnit.SECONDS);
                assertEquals(1L, cache.retryMetrics(id, kind, "").suppressions().get(CarrierCache.Suppression.DIAL_AFTER));
                clock.advance(Duration.ofSeconds(67));
                var rescanned = control.awaitSnapshotChange(control.snapshot());
                control.setDialAfter(id, updated);
                rescanned.get(5, TimeUnit.SECONDS);
                assertEquals(1L, cache.retryMetrics(id, kind, "").suppressions().get(CarrierCache.Suppression.DIAL_AFTER));
                clock.advance(Duration.ofSeconds(3).minusNanos(1));
                assertFalse(called.isDone());
                clock.advance(Duration.ofNanos(updated - clock.now()));
                assertEquals(updated, called.get(5, TimeUnit.SECONDS));
                var fault = settled.get(5, TimeUnit.SECONDS);
                assertEquals(CarrierCache.CooldownReason.ATTEMPT, fault.reason());
                assertEquals(Duration.ofMinutes(2), fault.nominalWait());
                assertEquals(2, fault.level());
                // The cooldown is published before the attempt gives up its per-key reservation.
                var afterRelease = new CompletableFuture<CarrierCache.Suppression>();
                Runnable observeRelease = () -> {
                    var admission = cache.admission(id, kind, "", false);
                    if (admission != CarrierCache.Suppression.ALREADY_IN_FLIGHT) afterRelease.complete(admission);
                };
                var unsubscribe = cache.observeChanges(observeRelease);
                try {
                    observeRelease.run();
                    assertEquals(CarrierCache.Suppression.RETRY_AFTER, afterRelease.get(5, TimeUnit.SECONDS));
                } finally {
                    unsubscribe.run();
                }
            }
        }
    }

    private static MemberSpec member(String name, Directions direction) {
        return new MemberSpec(name, List.of(new AttachmentSpec(direction, PipeOptions.defaults())));
    }

    @Test void reconcilesARegisteredQuotaAndDrainsOnlyItsMembers() throws Exception {
        try (var loops = EventLoops.create(2); var fixture = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 3))) {
            var opened = new CompletableFuture<CarrierOpener.Request>();
            var candidate = new SessionController.Candidate("extra", "route", CarrierKind.QUIC, "", 0, false, false,
                    List.of(DUPLEX), (setup, request) -> {
                        var io = MemoryPipe.pair(Framing.BYTE_STREAM);
                        fixture.listener.adapter().handle(io[1], JoinHint.inBand(null));
                        opened.complete(request);
                        return new CarrierOpener.Opened(List.of(io[0]));
                    });
            var options = new SessionController.Options(UUID.randomUUID().toString(), List.of(candidate), SessionController.Choice.STATIC,
                    SessionController.Limits.DEFAULT.withMaximumOpeners(2).withAttemptTimeout(Duration.ofSeconds(45)), null, null, java.util.Map.of());
            try (var controller = new SessionController(options);
                 var control = controller.manage(fixture.client, target(1, 1))) {
                assertEquals(CarrierOpener.Stage.JOIN, opened.get(30, TimeUnit.SECONDS).stage());
                var added = awaitCapacity(fixture.client, snapshot -> snapshot.members().stream()
                        .anyMatch(member -> member.candidate().equals("extra") && member.covered() == Directions.BOTH));
                assertEquals(2, added.localAttachments());

                var current = control.snapshot();
                while (current.blocked() != SessionControl.Blocking.NONE || !current.attempts().isEmpty()
                        || current.capacity().localAttachments() != 2) {
                    current = control.awaitSnapshotChange(current).get(30, TimeUnit.SECONDS);
                }
                try (var cleanup = controller.new Owner().acquire(null, false, options.limits().maximumCleanupAttachments(), false, () -> 0).lease()) {
                    assertNotNull(cleanup);
                    var empty = target(2, 0);
                    control.setTarget(empty);
                    control.setTarget(empty);
                    assertThrows(IllegalArgumentException.class, () -> control.setTarget(target(1, 0)));
                    current = control.snapshot();
                    while (current.targetRevision() != 2 || current.blocked() != SessionControl.Blocking.CLEANUP) {
                        current = control.awaitSnapshotChange(current).get(30, TimeUnit.SECONDS);
                    }
                    assertTrue(current.capacity().members().stream().anyMatch(member -> member.candidate().equals("extra")
                            && member.covered() == Directions.BOTH && !member.draining()));
                }
                var drained = awaitCapacity(fixture.client, snapshot -> snapshot.members().stream()
                        .noneMatch(member -> member.candidate().equals("extra") && member.covered() != Directions.NONE));
                assertEquals(Directions.BOTH, drained.covered());
                assertEquals(1, drained.members().stream().filter(m -> m.candidate().isEmpty()).count());
                SessionEvent drainedEvent;
                do { drainedEvent = fixture.client.nextEvent(Duration.ofSeconds(5)); }
                while (!(drainedEvent instanceof SessionEvent.DrainCommitted));
                var committed = (SessionEvent.DrainCommitted) drainedEvent;
                assertEquals(control.generation(), committed.generation());
                assertEquals(2, committed.revision());
                assertEquals(Set.of(added.members().stream().filter(m -> m.candidate().equals("extra")).findFirst().orElseThrow().id()),
                        committed.draining());
                control.release();
                control.done().get(30, TimeUnit.SECONDS);
            }
        }
    }

    private static SessionTarget target(long revision, int members) {
        return new SessionTarget(revision, List.of(new SessionTarget.Group("group",
                List.of(new SessionTarget.Quota("quota", members, Directions.BOTH, List.of("extra"))), SessionTarget.Rotation.NONE)), List.of());
    }

}
