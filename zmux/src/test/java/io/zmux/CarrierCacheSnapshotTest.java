package io.zmux;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import org.junit.jupiter.api.Test;

import static io.zmux.CarrierCache.Outcome.*;
import static io.zmux.CarrierKind.QUIC;
import static org.junit.jupiter.api.Assertions.*;

class CarrierCacheSnapshotTest {
    private static final Instant ORIGIN = Instant.parse("2026-11-26T00:00:00Z");
    private static final String KEY = "node";
    private static final String VARIANT = "direct";
    private static final double TOLERANCE = 1e-12;

    private static final class Time {
        private Instant origin = ORIGIN;
        private Duration elapsed = Duration.ZERO;
        Instant now() { return origin.plus(elapsed); }
        long monotonic() { return elapsed.toNanos(); }
        void advance(Duration span) { elapsed = elapsed.plus(span); }
        void rollBack(Duration span) { origin = origin.minus(span); }
        CarrierCache open() {
            var c = CarrierCache.Config.DEFAULT;
            return open(new CarrierCache.Config(c.halfLife(), c.dropWindow(), c.performanceWindow(),
                    c.healthReset(), new Backoff(Duration.ofMinutes(1), Duration.ofHours(6), 0),
                    c.maxImportBytes(), c.maxImportEntries()), new Random(1));
        }
        CarrierCache open(CarrierCache.Config config, Random random) {
            return new CarrierCache(config, this::now, this::monotonic, random);
        }
    }

    @Test void decayAndPerformanceStayAnchoredAcrossImportAndReplay() {
        var time = new Time();
        var first = time.open();
        assertEquals(.125, first.score(KEY, QUIC, VARIANT).score(), TOLERANCE);
        first.report(KEY, QUIC, VARIANT, USABLE);
        first.report(KEY, QUIC, VARIANT, PERFORMANCE, new CarrierCache.Observation(time.now(), Duration.ZERO, Duration.ofMillis(50), 2_500_000));
        assertEquals(1.0 / 3, first.score(KEY, QUIC, VARIANT).score(), TOLERANCE);
        time.advance(Duration.ofSeconds(39));
        byte[] saved = first.export();
        var restarted = time.open();
        restarted.importSnapshot(saved);
        time.advance(Duration.ofSeconds(21));
        restarted.importSnapshot(saved);
        assertEquals((1 + Math.pow(2, -1.0 / 360)) / (2 + Math.pow(2, -1.0 / 360)) / 4,
                restarted.score(KEY, QUIC, VARIANT).score(), TOLERANCE);
        time.advance(Duration.ofMinutes(359));
        assertEquals(.5, restarted.score(KEY, QUIC, VARIANT).weight(), TOLERANCE);
        assertEquals(1, restarted.score(KEY, QUIC, VARIANT).attempts());
    }

    @Test void coolingUsesOneSampleAndASuccessKeepsTheFailureLevel() {
        var time = new Time();
        var cache = time.open();
        cache.report(KEY, QUIC, VARIANT, FAILED);
        byte[] saved = cache.export();
        assertEquals(Duration.ofMinutes(1), cache.score(KEY, QUIC, VARIANT).cooling().orElseThrow());
        time.advance(Duration.ofSeconds(21));
        cache.report(KEY, QUIC, VARIANT, USABLE);
        var copy = time.open();
        copy.importSnapshot(saved);
        assertEquals(Duration.ofSeconds(39), copy.score(KEY, QUIC, VARIANT).cooling().orElseThrow());
        time.advance(Duration.ofSeconds(39));
        copy.importSnapshot(saved);
        assertFalse(copy.backingOff(KEY, QUIC, VARIANT));
        cache.report(KEY, QUIC, VARIANT, FAILED);
        assertEquals(Duration.ofMinutes(2), cache.score(KEY, QUIC, VARIANT).nominalBackoff());
        time.advance(Duration.ofMinutes(10));
        cache.report(KEY, QUIC, VARIANT, RECOVERED, new CarrierCache.Observation(time.now(), Duration.ofMinutes(10), Duration.ZERO, 0));
        assertEquals(Duration.ZERO, cache.score(KEY, QUIC, VARIANT).nominalBackoff());
        assertEquals(3, cache.score(KEY, QUIC, VARIANT).attempts());
    }

    @Test void shortDropProfileHasAnOpenWindowAndUsesTheFlooredWaitAsItsBase() throws Exception {
        var time = new Time();
        var defaults = CarrierCache.Config.DEFAULT;
        var profile = defaults.withShortDropResilience();
        assertEquals(new Backoff(Duration.ofSeconds(30), Duration.ofMinutes(30), 0.2, 0.2), profile.backoff());
        var plain = time.open(new CarrierCache.Config(defaults.halfLife(), defaults.dropWindow(), defaults.performanceWindow(),
                defaults.healthReset(), profile.backoff(), defaults.maxImportBytes(), defaults.maxImportEntries()), new Random(0));
        var resilient = time.open(profile, new Random(0) {
            @Override public double nextDouble() { return 0; }
        });
        for (var span : new Duration[] { Duration.ZERO, Duration.ofMinutes(5), Duration.ofMinutes(5) }) {
            time.advance(span);
            plain.report(KEY, QUIC, VARIANT, SHORT_DROP);
            resilient.report(KEY, QUIC, VARIANT, SHORT_DROP);
        }
        assertEquals(Duration.ofMinutes(2), resilient.score(KEY, QUIC, VARIANT).nominalBackoff());
        time.advance(Duration.ofNanos(1));
        plain.report(KEY, QUIC, VARIANT, SHORT_DROP);
        var connection = new io.zmux.carrier.CarrierFaultException.Episode(new Object());
        var cooled = new java.util.concurrent.CompletableFuture<CarrierCache.CooldownStarted>();
        var subscribed = new java.util.concurrent.CompletableFuture<Void>();
        resilient.cooldownEvents().subscribe(new java.util.concurrent.Flow.Subscriber<>() {
            @Override public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
                subscribed.complete(null);
            }
            @Override public void onNext(CarrierCache.CooldownEvent event) {
                if (event instanceof CarrierCache.CooldownStarted started) cooled.complete(started);
            }
            @Override public void onError(Throwable failure) { cooled.completeExceptionally(failure); }
            @Override public void onComplete() { }
        });
        subscribed.get(5, java.util.concurrent.TimeUnit.SECONDS);
        resilient.reportFault(KEY, QUIC, VARIANT, SHORT_DROP, time.now(), connection);
        resilient.reportFault(KEY, QUIC, VARIANT, SHORT_DROP, time.now(), connection);
        assertEquals(Duration.ofMinutes(8), cooled.get(5, java.util.concurrent.TimeUnit.SECONDS).sampledWait());
        assertEquals(Duration.ofMinutes(4), plain.score(KEY, QUIC, VARIANT).nominalBackoff());
        var raised = resilient.score(KEY, QUIC, VARIANT);
        assertEquals(4, raised.cooldownLevel());
        assertEquals(Duration.ofMinutes(10), raised.nominalBackoff());
        assertEquals(Duration.ofMinutes(8), raised.cooling().orElseThrow());
        resilient.report(KEY, QUIC, VARIANT, FAILED);
        assertEquals(Duration.ofMinutes(20), resilient.score(KEY, QUIC, VARIANT).nominalBackoff());
        time.advance(profile.healthReset());
        resilient.report(KEY, QUIC, VARIANT, RECOVERED,
                new CarrierCache.Observation(time.now(), profile.healthReset(), Duration.ZERO, 0));
        resilient.report(KEY, QUIC, VARIANT, SHORT_DROP);
        assertEquals(Duration.ofSeconds(30), resilient.score(KEY, QUIC, VARIANT).nominalBackoff());
    }

    @Test void coolingLevelSurvivesExpiryAndImportWithoutDelayingRecovery() {
        var time = new Time();
        var cache = time.open();
        cache.report(KEY, QUIC, VARIANT, FAILED);
        cache.report(KEY, QUIC, VARIANT, FAILED);
        time.advance(Duration.ofSeconds(5));
        byte[] snapshot = cache.export();
        var imported = time.open();
        imported.importSnapshot(snapshot);
        assertEquals(2, imported.score(KEY, QUIC, VARIANT).cooldownLevel());
        try (var permit = imported.acquire(KEY, QUIC, VARIANT, true, () -> 0).attempt()) { assertNotNull(permit); }

        String json = new String(snapshot, StandardCharsets.UTF_8);
        var older = time.open();
        older.importSnapshot(json.replace("\"level\":2,", "").getBytes(StandardCharsets.UTF_8));
        assertEquals(0, older.score(KEY, QUIC, VARIANT).cooldownLevel());
        assertEquals(Duration.ofSeconds(115), older.score(KEY, QUIC, VARIANT).cooling().orElseThrow());
        older.report(KEY, QUIC, VARIANT, FAILED);
        assertEquals(1, older.score(KEY, QUIC, VARIANT).cooldownLevel());
        assertInvalid(imported, imported.export(), CarrierCache.SnapshotError.VALUE,
                json.replace("\"level\":2", "\"level\":-1"));

        time.advance(Duration.ofSeconds(115));
        var expired = time.open();
        expired.importSnapshot(imported.export());
        assertFalse(expired.backingOff(KEY, QUIC, VARIANT));
        assertEquals(2, expired.score(KEY, QUIC, VARIANT).cooldownLevel());
        expired.report(KEY, QUIC, VARIANT, FAILED);
        assertEquals(3, expired.score(KEY, QUIC, VARIANT).cooldownLevel());
    }

    @Test void positiveCooldownLevelRequiresAnAnchor() {
        var time = new Time();
        var cache = time.open();
        cache.report(KEY, QUIC, VARIANT, FAILED);
        byte[] saved = cache.export();
        String json = new String(saved, StandardCharsets.UTF_8).replace(
                "\"cooling\":{\"at\":\"2026-11-26T00:00:00Z\"", "\"cooling\":{\"at\":\"0001-01-01T00:00:00Z\"");
        assertInvalid(cache, saved, CarrierCache.SnapshotError.VALUE, json);
    }

    @Test void committedCooldownEventsAndMetricsReflectOneEpisode() throws Exception {
        var time = new Time();
        var cache = time.open();
        var events = new java.util.concurrent.LinkedBlockingQueue<CarrierCache.CooldownEvent>();
        var subscribed = new java.util.concurrent.CompletableFuture<Void>();
        cache.cooldownEvents().subscribe(new java.util.concurrent.Flow.Subscriber<>() {
            @Override public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
                subscribed.complete(null);
            }
            @Override public void onNext(CarrierCache.CooldownEvent event) { events.add(event); }
            @Override public void onError(Throwable failure) { subscribed.completeExceptionally(failure); }
            @Override public void onComplete() { }
        });
        var seconds = java.util.concurrent.TimeUnit.SECONDS;
        subscribed.get(5, seconds);
        var connection = new io.zmux.carrier.CarrierFaultException.Episode(new Object());
        for (int i = 0; i < 2; i++) cache.reportFault(KEY, QUIC, VARIANT, SHORT_DROP, time.now(), connection);

        var key = new CarrierCache.Key(KEY, QUIC, VARIANT);
        var minute = Duration.ofMinutes(1);
        assertEquals(new CarrierCache.CooldownStarted(key, 1, 1, CarrierCache.CooldownReason.SHORT_DROP,
                0, minute, minute, minute.toNanos()), events.poll(5, seconds));
        assertEquals(1, cache.score(KEY, QUIC, VARIANT).cooldownLevel());
        time.advance(Duration.ofSeconds(5));
        try (var probe = cache.acquire(KEY, QUIC, VARIANT, true, () -> 0).attempt()) {
            assertNotNull(probe);
            assertNull(cache.acquire(KEY, QUIC, VARIANT, true, () -> 0).attempt());
            assertTrue(probe.start());
        }
        var metrics = cache.retryMetrics(KEY, QUIC, VARIANT);
        assertEquals(new CarrierCache.RetryMetrics(1, 1, Duration.ZERO, Duration.ZERO, null,
                java.util.Map.of(),
                java.util.Optional.of(Duration.ofSeconds(5)), Duration.ofSeconds(55)), metrics);

        var reset = cache.config().healthReset();
        var before = reset.minusNanos(1);
        time.advance(before.minusSeconds(5));
        cache.report(KEY, QUIC, VARIANT, RECOVERED, new CarrierCache.Observation(time.now(), before, Duration.ZERO, 0));
        assertEquals(1, cache.score(KEY, QUIC, VARIANT).cooldownLevel());
        time.advance(Duration.ofNanos(1));
        cache.report(KEY, QUIC, VARIANT, RECOVERED, new CarrierCache.Observation(time.now(), reset, Duration.ZERO, 0));
        assertEquals(new CarrierCache.CooldownReset(key, 1, reset.toNanos()), events.poll(5, seconds));
        assertEquals(0, cache.score(KEY, QUIC, VARIANT).cooldownLevel());
        assertEquals(Duration.ZERO, cache.retryMetrics(KEY, QUIC, VARIANT).remaining());
    }

    @Test void rollbackAndReexportPreserveTheAgeOfHistoryAndPerformance() {
        var publisherTime = new Time();
        var publisher = publisherTime.open();
        publisher.report(KEY, QUIC, VARIANT, USABLE);
        publisher.report(KEY, QUIC, VARIANT, PERFORMANCE,
                new CarrierCache.Observation(null, Duration.ZERO, Duration.ofMillis(1), 0));
        publisherTime.advance(Duration.ofSeconds(39));
        byte[] saved = publisher.export();

        var receivingTime = new Time();
        receivingTime.rollBack(Duration.ofHours(1));
        var receiver = receivingTime.open();
        receiver.importSnapshot(saved);
        receivingTime.advance(Duration.ofSeconds(20));
        assertEquals(Duration.ofSeconds(59), receiver.score(KEY, QUIC, VARIANT).age());
        byte[] relayed = receiver.export();

        var copy = receivingTime.open();
        copy.importSnapshot(relayed);
        assertEquals(receiver.score(KEY, QUIC, VARIANT), copy.score(KEY, QUIC, VARIANT));
        receivingTime.advance(Duration.ofSeconds(1));
        receiver.importSnapshot(saved); // An equal origin timestamp must leave the original local anchor intact.
        var expired = receiver.score(KEY, QUIC, VARIANT);
        assertEquals(expired, copy.score(KEY, QUIC, VARIANT));
        assertEquals(Duration.ofMinutes(1), expired.age());
        double weight = Math.pow(2, -1.0 / 360);
        assertEquals(weight, expired.weight(), TOLERANCE);
        assertEquals((1 + weight) / (2 + weight) / 4, expired.score(), TOLERANCE);
    }

    @Test void allThreeImportErrorsAreAtomic() {
        var time = new Time();
        var cache = time.open();
        cache.report(KEY, QUIC, VARIANT, USABLE);
        byte[] before = cache.export();
        String json = new String(before, StandardCharsets.UTF_8);
        assertInvalid(cache, before, CarrierCache.SnapshotError.VERSION, "{\"version\":1}");
        assertInvalid(cache, before, CarrierCache.SnapshotError.FORMAT, "{");
        assertInvalid(cache, before, CarrierCache.SnapshotError.FORMAT, "{\"version\":2,\"\\u１２３４\":0}");
        assertInvalid(cache, before, CarrierCache.SnapshotError.VALUE, json.replace("\"attempts\":1", "\"attempts\":-1"));
    }

    private static void assertInvalid(CarrierCache cache, byte[] before, CarrierCache.SnapshotError reason, String fixture) {
        assertEquals(reason, assertThrows(CarrierCache.InvalidSnapshotException.class,
                () -> cache.importSnapshot(fixture.getBytes(StandardCharsets.UTF_8))).reason());
        assertArrayEquals(before, cache.export());
    }
}
