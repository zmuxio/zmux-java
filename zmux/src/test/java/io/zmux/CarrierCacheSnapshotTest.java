package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.testutil.Json;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** CarrierCache snapshots: the vectors shared across implementations (src/test/resources/carrier-cache). */
@Timeout(30)
class CarrierCacheSnapshotTest {
    record Expect(String key, String kind, boolean usable, boolean failed, long backoffMs, String url) {}

    private static final Pattern AT = Pattern.compile("\"@([+-])(\\d+)\"");

    /** Substitutes "@+N" and "@-N" with the RFC 3339 time N seconds from now. */
    private static String substitute(String text, Instant now) {
        Matcher m = AT.matcher(text);
        var b = new StringBuilder();
        while (m.find()) {
            long n = Long.parseLong(m.group(2));
            Instant t = m.group(1).equals("+") ? now.plusSeconds(n) : now.minusSeconds(n);
            m.appendReplacement(b, Matcher.quoteReplacement("\"" + SnapshotJson.formatTime(t) + "\""));
        }
        return m.appendTail(b).toString();
    }

    private static List<Expect> entries(CarrierCache cache, Instant now) {
        var out = new ArrayList<Expect>();
        for (var e : cache.snapshot().entries()) {
            out.add(new Expect(e.key(), e.kind().toString(), e.goodUntil() != null && e.goodUntil().isAfter(now),
                    e.failUntil() != null && e.failUntil().isAfter(now), e.backoff().toMillis(), e.url()));
        }
        out.sort(Comparator.comparing(Expect::key).thenComparing(Expect::kind));
        return out;
    }

    @SuppressWarnings("unchecked")
    @Test void sharedVectors() throws Exception {
        var v = Json.object(Files.readString(Path.of("src", "test", "resources", "carrier-cache", "vectors.json")));
        var exact = (List<String>) v.get("exact");
        var cases = (List<Map<String, Object>>) v.get("cases");
        assertFalse(exact.isEmpty());
        assertFalse(cases.isEmpty());
        for (String text : exact) assertEquals(text, CarrierCache.Snapshot.fromJson(text).toJson());
        for (var c : cases) {
            String name = (String) c.get("name");
            Instant now = Instant.now();
            var cache = new CarrierCache();
            int step = 0;
            for (var s : (List<Map<String, Object>>) c.get("steps")) {
                String json = substitute((String) s.get("snapshot"), now);
                boolean error = Boolean.TRUE.equals(s.get("error"));
                if (error) {
                    var before = entries(cache, now);
                    assertThrows(CarrierCache.InvalidSnapshotException.class, () -> CarrierCache.Snapshot.fromJson(json), name + " step " + step + ": " + json);
                    assertEquals(before, entries(cache, now), name + " unchanged");
                } else {
                    cache.restore(CarrierCache.Snapshot.fromJson(json));
                }
                step++;
            }
            var expect = new ArrayList<Expect>();
            for (var e : (List<Map<String, Object>>) c.get("expect")) {
                expect.add(new Expect((String) e.get("key"), (String) e.get("kind"), (Boolean) e.get("usable"), (Boolean) e.get("failed"),
                        ((Number) e.get("backoff_ms")).longValue(), (String) e.get("url")));
            }
            assertEquals(expect, entries(cache, now), name);
            // The cache's own snapshot round-trips through JSON and into another cache.
            var snap = cache.snapshot();
            var again = CarrierCache.Snapshot.fromJson(snap.toJson());
            assertEquals(snap, again, name);
            var other = new CarrierCache();
            other.restore(again);
            assertEquals(expect, entries(other, now), name + " again");
        }
    }

    @Test void restoreConvertsMonotonicTimesAndKeepsTheNewer() {
        var c = new CarrierCache();
        c.record("k", CarrierKind.SPLIT, false, System.nanoTime());
        c.record("k", CarrierKind.WEBSOCKET, true, System.nanoTime());
        var snap = c.snapshot();
        assertEquals(2, snap.entries().size());
        var split = snap.entries().getFirst();
        assertEquals(CarrierKind.SPLIT, split.kind());
        assertEquals(Duration.ofMinutes(1), split.backoff());
        long ahead = Duration.between(Instant.now(), split.failUntil()).toSeconds();
        assertTrue(ahead > 50 && ahead <= 60, "fail time on the wall clock: " + ahead);
        var other = new CarrierCache(Duration.ofHours(1), Duration.ofMinutes(5), Duration.ofHours(1));
        other.restore(snap);
        assertTrue(other.backingOff("k", CarrierKind.SPLIT));
        assertTrue(other.usable("k", CarrierKind.WEBSOCKET));
        assertEquals(Duration.ofMinutes(5), other.snapshot().entries().getFirst().backoff(), "within the bounds");
        // An older import leaves the newer entry; the configuration stays.
        var old = new CarrierCache.Snapshot(List.of(new CarrierCache.Snapshot.Entry("k", CarrierKind.WEBSOCKET, null,
                Instant.now().plusSeconds(10), Duration.ofMinutes(1), "")));
        other.restore(old);
        assertTrue(other.usable("k", CarrierKind.WEBSOCKET));
        assertEquals(Duration.ofHours(1), other.successTtl());
        assertThrows(IllegalArgumentException.class, () -> new CarrierCache.Snapshot.Entry("k", CarrierKind.QUIC, null, null, Duration.ofMillis(-1), ""));
    }

    @Test void restoreBoundsTimes() {
        Instant far = Instant.now().plus(Duration.ofDays(3650));
        var c = new CarrierCache();
        c.restore(new CarrierCache.Snapshot(List.of(
                new CarrierCache.Snapshot.Entry("g", CarrierKind.SPLIT, far, null, Duration.ZERO, ""),
                new CarrierCache.Snapshot.Entry("f", CarrierKind.SPLIT, null, far, Duration.ofMinutes(1), ""))));
        Instant limit = Instant.now().plus(Duration.ofHours(6));
        for (var e : c.snapshot().entries()) {
            for (Instant t : new Instant[] {e.goodUntil(), e.failUntil()}) {
                assertTrue(t == null || !t.isAfter(limit), e + " beyond its bound");
            }
        }
        assertTrue(c.usable("g", CarrierKind.SPLIT));
        assertTrue(c.backingOff("f", CarrierKind.SPLIT));
    }
}
