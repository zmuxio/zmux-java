package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.HandshakeShaper;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import java.io.IOException;
import java.net.SocketException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class ControllerSettlementTest {
    private static final CarrierKind KIND = CarrierKind.STREAMING;
    private static final AttachmentSpec DUPLEX = new AttachmentSpec(Directions.BOTH, PipeOptions.defaults());
    private static final Duration GUARD = Duration.ofSeconds(5);

    @SuppressWarnings("try") // The assertion observes shutdown before automatic resource cleanup.
    @Test void takeoverAndReleaseFinishOldWorkBeforeReturningProtocolReservations() throws Exception {
        for (boolean takeover : new boolean[] {true, false}) {
            var clock = new SpliceHooksTest.ManualClock();
            var domain = SessionControllerTest.domain(clock);
            var cache = new CarrierCache(CarrierCache.Config.DEFAULT, domain);
            var holdOpener = new CompletableFuture<Void>();
            var entered = new CompletableFuture<CarrierOpener.Request>();
            var candidate = candidate("old", 0, (setup, request) -> {
                entered.complete(request);
                holdOpener.join();
                return new CarrierOpener.Opened(List.of(), new SocketException("late opener failure"));
            });
            try (var loops = EventLoops.create(2); var fixture = fixture(loops, domain);
                 var oldDomain = controller("old-settlement", domain, cache, List.of(candidate));
                 var newDomain = controller("new-settlement", domain, cache, List.of())) {
                var session = fixture.client;
                var old = oldDomain.manage(session, target(candidate.id()));
                try {
                    var request = get(entered);
                    var before = SessionTestSupport.observe(old, snapshot -> snapshot.attempts().contains(request.attempt())
                            && snapshot.occupancy().bindingOpeners() == 1);
                    assertEquals(1, before.occupancy().domainCleanup());
                    var reserved = event(session, SessionEvent.BatchReserved.class, e -> e.generation() == old.generation());
                    var started = event(session, SessionEvent.AttemptStarted.class, e -> e.batch() == reserved.batch());
                    assertEquals(request.attempt(), started.attempt().id());
                    assertEquals(1, session.capacity().reserved());

                    if (takeover) {
                        var next = newDomain.takeOver(old, SessionTarget.empty());
                        assertEquals(0, next.snapshot().occupancy().bindingOpeners());
                        assertEquals(0, next.snapshot().occupancy().domainCleanup());
                    } else old.release();
                    assertEquals(0, session.capacity().reserved());
                    var ended = event(session, SessionEvent.AttemptEnded.class, e -> e.batch() == reserved.batch());
                    assertEquals(Attempt.Disposition.CANCELLED, ended.attempt().disposition());
                    assertEquals(old.generation(), ended.attempt().generation());
                    assertEquals(reserved.batch(), ended.attempt().batch());
                    var rollback = event(session, SessionEvent.BatchRolledBack.class, e -> e.batch() == reserved.batch());
                    assertEquals(old.generation(), rollback.generation());
                    if (takeover) {
                        var installed = event(session, SessionEvent.TargetChanged.class, e -> e.predecessor() != null);
                        assertEquals(old.generation() + 1, installed.generation());
                    }
                    oldDomain.close();
                    assertFalse(oldDomain.done().isDone());
                } finally {
                    holdOpener.complete(null);
                }
                get(oldDomain.done());
                assertEquals(0, cache.score(candidate.cacheKey(), KIND, "").attempts());
                assertEquals(0, cache.retryMetrics(candidate.cacheKey(), KIND, "").faults());
                assertNull(cache.retryMetrics(candidate.cacheKey(), KIND, "").recoveryRetryAt());
            }
        }
    }

    @Test void staggerAndDeadlineBelongToTheRecoveryTransaction() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        var domain = SessionControllerTest.domain(clock);
        var cache = new CarrierCache(CarrierCache.Config.DEFAULT, domain);
        var a = new CompletableFuture<CarrierOpener.Request>();
        var b = new CompletableFuture<CarrierOpener.Request>();
        var c = new CompletableFuture<CarrierOpener.Request>();
        var returnA = new CompletableFuture<Void>();
        var returnB = new CompletableFuture<Void>();
        var returnC = new CompletableFuture<Void>();
        var ends = new java.util.concurrent.LinkedBlockingQueue<SessionEvent.AttemptEnded>();
        var candidates = List.of(
                candidate("a", 0, hanging(a, returnA)),
                candidate("b", 1, (setup, request) -> {
                    b.complete(request);
                    returnB.join();
                    return new CarrierOpener.Opened(List.of(), new SocketException("only b failed"));
                }),
                candidate("c", 2, hanging(c, returnC)));
        try (var loops = EventLoops.create(2); var fixture = new SessionTestSupport.Fixture(SessionTestSupport.configBuilder(loops, 5)
                     .clockDomain(domain).resumeTimeout(Duration.ofMinutes(2)).tracer(new Tracer() {
                         @Override public void event(SessionId session, SessionEvent event) {
                             if (event instanceof SessionEvent.AttemptEnded ended) ends.add(ended);
                         }
                     }).build());
             var controller = controller("shared-race", domain, cache, candidates)) {
            var session = fixture.client;
            session.pipes().getFirst().abort();
            SessionTestSupport.awaitCapacity(session, view -> view.covered() == Directions.NONE);
            long origin = clock.now();
            Duration stagger = Duration.ofNanos(BootstrapRace.STAGGER_NANOS);
            var timeout = SessionController.Limits.DEFAULT.attemptTimeout();
            try (var control = controller.manage(session, SessionTarget.empty())) {
                var first = get(a);
                assertEquals(timeout, first.timeout());
                SessionTestSupport.observe(control, view -> Long.valueOf(origin + stagger.toNanos()).equals(view.nextRetry()));
                clock.advance(stagger);
                var second = get(b);
                assertEquals(timeout.minus(stagger), second.timeout());
                SessionTestSupport.observe(control, view -> view.attempts().containsAll(List.of(first.attempt(), second.attempt())));
                returnB.complete(null);
                long thirdDue = origin + 2 * stagger.toNanos();
                SessionTestSupport.observe(control, view -> view.attempts().equals(List.of(first.attempt()))
                        && Long.valueOf(thirdDue).equals(view.nextRetry()));
                assertFalse(c.isDone());
                clock.advance(stagger);
                var third = get(c);
                assertEquals(timeout.minus(stagger.multipliedBy(2)), third.timeout());

                var reserved = event(session, SessionEvent.BatchReserved.class, e -> e.generation() == control.generation());
                assertEquals(origin + timeout.toNanos(), reserved.deadline());
                var startC = event(session, SessionEvent.AttemptStarted.class, e -> e.attempt().id() == third.attempt());
                assertEquals(reserved.batch(), startC.batch());
                assertEquals(thirdDue, startC.at());
                clock.advance(timeout.minus(stagger.multipliedBy(2)));
                var results = new java.util.HashMap<Long, SessionEvent.AttemptEnded>();
                while (results.size() < 3) {
                    var ended = ends.poll(5, TimeUnit.SECONDS);
                    assertNotNull(ended);
                    results.put(ended.attempt().id(), ended);
                }
                for (var attempt : List.of(first, third)) {
                    var expired = results.get(attempt.attempt());
                    assertEquals(Attempt.Disposition.DEADLINE, expired.attempt().disposition());
                    assertEquals(reserved.deadline(), expired.at());
                    assertEquals(reserved.batch(), expired.batch());
                }
                assertEquals(0, session.capacity().reserved());
            } finally {
                returnA.complete(null);
                returnB.complete(null);
                returnC.complete(null);
            }
        }
    }

    @Test void stalePlansCannotAdvanceEpochOrChangeDialAfter() throws Exception {
        try (var loops = EventLoops.create(2); var fixture = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 4));
             var first = controller("old-epoch", ClockDomain.system(), null, List.of());
             var next = controller("new-epoch", ClockDomain.system(), null, List.of())) {
            var session = fixture.client;
            var old = first.manage(session, SessionTarget.empty());
            long epoch = session.capacity().epoch();
            var member = session.capacity().members().getFirst().id();
            var entered = new CompletableFuture<Void>();
            var release = new CompletableFuture<Void>();
            SessionControl installed;
            synchronized (old) {
                // Install the observer before the worker can publish this target's first plan.
                old.awaitSnapshotChange(old.snapshot()).thenAccept(view -> {
                    if (view.targetRevision() != 1) return;
                    entered.complete(null);
                    release.join();
                });
                old.setTarget(new SessionTarget(1, List.of(),
                        List.of(new SessionTarget.Replacement("epoch", Set.of(member), epoch + 1))));
            }
            try {
                get(entered);
                installed = next.takeOver(old, SessionTarget.empty());
            } finally {
                release.complete(null);
            }
            var stale = new Object();
            assertSame(stale, old.fence().submit(op -> {
                session.core.advanceEpoch();
                return null;
            }, stale));
            assertEquals(epoch, session.capacity().epoch());
            assertThrows(SessionController.StaleBindingException.class, () -> old.setDialAfter("unused", 42));
            assertEquals(old.generation() + 1, installed.generation());
        }
    }

    @Test void takeoverUsesOwnerRevisionEvenWithAnUnpublishedSnapshot() throws Exception {
        try (var loops = EventLoops.create(2); var fixture = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 4));
             var first = controller("revision-first", ClockDomain.system(), null, List.of());
             var next = controller("revision-next", ClockDomain.system(), null, List.of())) {
            var old = first.manage(fixture.client, SessionTarget.empty());
            var member = fixture.client.capacity().members().getFirst();
            var pipe = member.paths().getFirst();
            synchronized (old) {
                old.setTarget(new SessionTarget(1, List.of(), List.of()));
                assertEquals(0, old.snapshot().targetRevision());
                var stale = qualityTarget(old.generation(), 0, member, pipe);
                var valid = qualityTarget(old.generation(), 1, member, pipe);
                assertThrows(SessionController.StaleDecisionException.class, () -> next.takeOver(old, stale));
                assertEquals(valid.revision(), next.takeOver(old, valid).snapshot().targetRevision());
            }
        }
    }

    @Test void authenticatedReplyIsReportedBeforeConfirmShapingAndSurvivesTakeover() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        var domain = SessionControllerTest.domain(clock);
        var cache = new CarrierCache(CarrierCache.Config.DEFAULT, domain);
        var shaping = new CompletableFuture<Long>();
        var unblock = new CompletableFuture<Void>();
        try (var loops = EventLoops.create(2); var fixture = new SessionTestSupport.Fixture(
                SessionTestSupport.configBuilder(loops, 4).clockDomain(domain)
                        .handshakeShaper(view -> {
                            if (view.handshake().equals("join") && view.message().equals("confirm")) {
                                shaping.complete(clock.now());
                                unblock.join();
                            }
                            return HandshakeShaper.UnitPlan.NONE;
                        }).build())) {
            var candidate = candidate("proof", 0, SessionTestSupport.memoryOpener(fixture.listener, (attachment, io) -> io));
            try (var first = controller("proof-first", domain, cache, List.of(candidate));
                 var next = controller("proof-next", domain, cache, List.of())) {
                var old = first.manage(fixture.client, target(candidate.id()));
                try {
                    long at = get(shaping);
                    assertEquals(1, cache.score(candidate.cacheKey(), KIND, "").attempts());
                    clock.advance(Duration.ofSeconds(1));
                    assertEquals(Duration.ofNanos(clock.now() - at), cache.score(candidate.cacheKey(), KIND, "").age());
                    next.takeOver(old, SessionTarget.empty());
                    var ended = event(fixture.client, SessionEvent.AttemptEnded.class, e -> e.generation() == old.generation());
                    assertEquals(Attempt.Disposition.CANCELLED, ended.attempt().disposition());
                    assertEquals(1, cache.score(candidate.cacheKey(), KIND, "").attempts());
                } finally {
                    unblock.complete(null);
                }
            }
        }
    }

    @Test void aTransportFailureAfterReplyChargesCooldownWithoutAnotherHandshake() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        var domain = SessionControllerTest.domain(clock);
        var cache = new CarrierCache(CarrierCache.Config.DEFAULT, domain);
        var socketFailure = new SocketException("Confirm write failed");
        try (var loops = EventLoops.create(2); var fixture = fixture(loops, domain)) {
            var opener = SessionTestSupport.memoryOpener(fixture.listener, (attachment, io) -> {
                AtomicInteger writes = new AtomicInteger();
                return PipeIo.of(io.reader(), new PipeWriter() {
                    @Override public void write(io.zmux.carrier.OutboundBatch bytes, Completion done) {
                        if (writes.incrementAndGet() == 2) done.failed(socketFailure);
                        else io.writer().write(bytes, done);
                    }
                    @Override public void close() { io.writer().close(); }
                    @Override public void abort(Throwable error) { io.writer().abort(error); }
                });
            });
            var candidate = candidate("confirm-fault", 0, opener);
            try (var controller = controller("confirm-fault", domain, cache, List.of(candidate))) {
                controller.manage(fixture.client, target(candidate.id()));
                var ended = event(fixture.client, SessionEvent.AttemptEnded.class, e -> true);
                assertEquals(Attempt.Disposition.CONFIRMATION_FAILED, ended.attempt().disposition());
                assertEquals(1, cache.retryMetrics(candidate.cacheKey(), KIND, "").faults());
                var score = cache.score(candidate.cacheKey(), KIND, "");
                assertEquals(1, score.attempts());
                assertEquals(1, score.successWeight());
            }
        }
    }

    @Test void openerErrorIsTerminalAtTheCurrentClockCut() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        var domain = SessionControllerTest.domain(clock);
        var cache = new CarrierCache(CarrierCache.Config.DEFAULT, domain);
        var failure = new AssertionError("host opener");
        var candidate = candidate("assertion", 0, (setup, request) -> { throw failure; });
        try (var loops = EventLoops.create(2); var fixture = fixture(loops, domain);
             var controller = controller("opener-error", domain, cache, List.of(candidate))) {
            fixture.client.pipes().getFirst().abort();
            SessionTestSupport.awaitCapacity(fixture.client, view -> view.covered() == Directions.NONE);
            long origin = clock.now();
            controller.manage(fixture.client, SessionTarget.empty());
            var ended = event(fixture.client, SessionEvent.AttemptEnded.class, e -> true);
            assertEquals(origin, ended.at());
            assertEquals(Attempt.Disposition.HOOK_FAILED, ended.attempt().disposition());
            assertSame(failure, ended.attempt().error());
            assertNull(cache.retryMetrics(candidate.cacheKey(), KIND, "").recoveryRetryAt());
        }
    }

    @Test void bootstrapChecksPhysicalLayoutAndKeepsIdentityThroughUnknownBindings() throws Exception {
        var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
        pair[0] = pair[0].withCarrier(KIND);
        CarrierOpener unopened = (setup, request) -> { throw new AssertionError("unexpected opener"); };
        var duplex = candidate("first", 0, unopened);
        var split = new SessionController.Candidate(duplex.id(), duplex.cacheKey(), KIND, "", 0, false, true,
                List.of(new AttachmentSpec(Directions.SEND, PipeOptions.defaults()),
                        new AttachmentSpec(Directions.RECEIVE, PipeOptions.defaults())), unopened);
        var bootstrapOnly = new SessionController.Candidate(duplex.id(), duplex.cacheKey(), KIND, "", 0, false, true,
                List.of(), unopened);
        var incompatible = new SessionController.Candidate(duplex.id(), "different-key", KIND, "", 0, false, true,
                List.of(DUPLEX), unopened);
        var domain = ClockDomain.system();
        try (var loops = EventLoops.create(2);
             var fixture = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 4), pair);
             var first = controller("member-origin", domain, null, List.of(bootstrapOnly));
             var splitDomain = controller("member-split", domain, null, List.of(split));
             var unknown = controller("member-unknown", domain, null, List.of());
             var wrong = controller("member-incompatible", domain, null, List.of(incompatible))) {
            var session = fixture.client;
            var target = SessionTarget.empty();
            assertThrows(IllegalArgumentException.class, () -> splitDomain.manage(session, target, split.id()));
            assertEquals("", session.capacity().members().getFirst().candidate());
            var attributed = first.manage(session, target, duplex.id());
            var intermediate = unknown.takeOver(attributed, target);
            assertThrows(IllegalArgumentException.class, () -> wrong.takeOver(intermediate, target));
            assertEquals(duplex.id(), first.takeOver(intermediate, target).snapshot().capacity().members().getFirst().candidate());
        }
    }

    @Test void splitFaultsBeforeFullCoverageHaveDistinctPhysicalEpisodes() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        var domain = SessionControllerTest.domain(clock);
        var cache = new CarrierCache(CarrierCache.Config.DEFAULT, domain);
        var split = new SessionController.Candidate("split", "split", KIND, "", 0, false, false,
                List.of(new AttachmentSpec(Directions.SEND, PipeOptions.defaults()),
                        new AttachmentSpec(Directions.RECEIVE, PipeOptions.defaults())),
                (setup, request) -> { throw new AssertionError("unexpected opener"); });
        try (var loops = EventLoops.create(2); var fixture = fixture(loops, domain);
             var controller = controller("partial-fault", domain, cache, List.of(split))) {
            var carrier = new Object();
            var reused = new IOException("reused carrier cause");
            var first = new io.zmux.carrier.CarrierFaultException(new io.zmux.carrier.CarrierFaultException.Episode(carrier), reused);
            var second = new io.zmux.carrier.CarrierFaultException(new io.zmux.carrier.CarrierFaultException.Episode(carrier), reused);
            fixture.client.run(op -> {
                var feedback = new MemberFeedback(controller, fixture.client);
                var events = List.of(first, first, second);
                for (int i = 0; i < events.size(); i++) {
                    var error = events.get(i);
                    var episode = io.zmux.carrier.CarrierFaultException.episodeOf(error);
                    feedback.observe(new io.zmux.internal.core.SessionCapacity.MemberHealth(
                            new MemberId(41 + i), "split", Directions.BOTH,
                            Directions.NONE, Directions.NONE, domain.now(), true, true, episode));
                }
                return null;
            });
            assertEquals(2, cache.retryMetrics("split", KIND, "").faults());
        }
    }

    @Test void aSuppressionSurvivesReservationAndTakeoverUntilTheOpportunityEnds() {
        var clock = new SpliceHooksTest.ManualClock();
        var domain = SessionControllerTest.domain(clock);
        var cache = new CarrierCache(CarrierCache.Config.DEFAULT, domain);
        var candidate = candidate("suppressed", 0, (setup, request) -> { throw new AssertionError(); });
        var hostGate = new java.util.concurrent.atomic.AtomicLong(domain.now() + 1);
        try (var first = controller("suppression-first", domain, cache, List.of(candidate));
             var next = controller("suppression-next", domain, cache, List.of(candidate))) {
            var predecessor = first.new Owner();
            predecessor.recoveryDemand(Set.of(candidate));
            assertNull(predecessor.acquire(candidate, true, 1, false, hostGate::get).lease());
            hostGate.set(0);
            try (var reserved = predecessor.acquire(candidate, true, 1, false, hostGate::get).lease()) {
                assertNotNull(reserved);
                hostGate.set(domain.now() + 1);
                assertFalse(reserved.start());
            }
            var successor = next.new Owner();
            successor.recoveryDemand(Set.of(candidate));
            successor.inheritOpportunities(predecessor, Set.of(candidate));
            predecessor.recoveryDemand(Set.of());
            assertNull(successor.acquire(candidate, true, 1, false, hostGate::get).lease());
            assertEquals(1L, cache.retryMetrics(candidate.cacheKey(), KIND, "").suppressions().get(CarrierCache.Suppression.DIAL_AFTER));

            successor.recoveryDemand(Set.of());
            successor.recoveryDemand(Set.of(candidate));
            assertNull(successor.acquire(candidate, true, 1, false, hostGate::get).lease());
            assertEquals(2L, cache.retryMetrics(candidate.cacheKey(), KIND, "").suppressions().get(CarrierCache.Suppression.DIAL_AFTER));
            successor.recoveryDemand(Set.of());
        }
    }

    @Test void recoverySamplerCanReadCacheAndUsesTheFailureCommitTime() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        var domain = SessionControllerTest.domain(clock);
        var entered = new CompletableFuture<Void>();
        var sample = new CompletableFuture<Void>();
        var reference = new java.util.concurrent.atomic.AtomicReference<CarrierCache>();
        var config = CarrierCache.Config.DEFAULT;
        var cache = new CarrierCache(config, domain, new java.util.Random(0) {
            @Override public double nextDouble() {
                reference.get().score("sample", KIND, "");
                entered.complete(null);
                sample.join();
                return 0.5;
            }
        });
        reference.set(cache);
        var key = new CarrierCache.Key("sample", KIND, "");
        var demand = new Object();
        cache.recoveryDemand(key, demand, true);
        try (var permit = cache.acquire(key.key(), key.kind(), key.variant(), true, () -> 0).attempt()) {
            assertTrue(permit.start());
            long at = domain.now();
            var settled = permit.terminal(false, true, at);
            try {
                get(entered);
                clock.advance(Duration.ofMillis(200));
                assertEquals(CarrierCache.Suppression.ALREADY_IN_FLIGHT, cache.admission(key.key(), key.kind(), key.variant(), true));
            } finally {
                sample.complete(null);
            }
            get(settled);
            assertEquals(at + Duration.ofMillis(500).toNanos(),
                    cache.retryMetrics(key.key(), key.kind(), key.variant()).recoveryRetryAt());
        } finally {
            cache.recoveryDemand(key, demand, false);
        }
    }

    private static SessionTarget qualityTarget(long generation, long revision, CapacitySnapshot.Member member,
                                               CapacitySnapshot.Path pipe) {
        var input = new SessionTarget.QualityInput(generation, revision, member.id(), pipe.pipe(), Direction.OUTBOUND,
                pipe.health().send().evidenceGeneration(), SessionTarget.QualityInput.Field.HEALTH, 0, 0, 0, null, 0);
        return new SessionTarget(3, List.of(), List.of(new SessionTarget.Replacement("evidence", Set.of(member.id()), 0, List.of(input))));
    }

    private static CarrierOpener hanging(CompletableFuture<CarrierOpener.Request> start, CompletableFuture<Void> hold) {
        return (setup, request) -> {
            start.complete(request);
            hold.join();
            return new CarrierOpener.Opened(List.of());
        };
    }

    private static SessionController.Candidate candidate(String id, int tier, CarrierOpener opener) {
        return new SessionController.Candidate(id, id, KIND, "", tier, false, false, List.of(DUPLEX), opener);
    }

    private static SessionTarget target(String id) {
        var quota = new SessionTarget.Quota("duplex", 1, Directions.BOTH, List.of(id));
        return new SessionTarget(1, List.of(new SessionTarget.Group("g", List.of(quota), SessionTarget.Rotation.NONE)), List.of());
    }

    private static SessionController controller(String name, ClockDomain domain, CarrierCache cache, List<SessionController.Candidate> list) {
        return new SessionController(new SessionController.Options(name, list, SessionController.Choice.STATIC,
                SessionController.Limits.DEFAULT, cache, domain, Map.of()));
    }

    private static SessionTestSupport.Fixture fixture(EventLoops loops, ClockDomain domain) throws Exception {
        return new SessionTestSupport.Fixture(SessionTestSupport.configBuilder(loops, 5).clockDomain(domain)
                .resumeTimeout(Duration.ofMinutes(2)).build());
    }

    private static <T> T get(CompletableFuture<T> future) throws Exception {
        return future.get(GUARD.toNanos(), TimeUnit.NANOSECONDS);
    }

    private static <T extends SessionEvent> T event(ZmuxSession session, Class<T> type, Predicate<T> filter) throws IOException {
        for (;;) {
            var event = session.nextEvent(GUARD);
            if (type.isInstance(event) && filter.test(type.cast(event))) return type.cast(event);
        }
    }
}
