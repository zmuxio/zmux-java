package io.zmux;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Remembers which carrier kinds worked per server and local network ({@link ModePolicy#key()}):
 * a usable kind for the success TTL, and a failed one is skipped for a backoff that doubles with
 * every failure in a row, between the backoff bounds. A kind that fails soon after it was chosen
 * ({@link ModePolicy#dropWindow()}) counts as failed, so that a path that lets a kind through only
 * briefly is not tried again at once. Safe for concurrent use; share one across dials and pools.
 *
 * <p>The cache can be carried across runs with {@link #snapshot()} and {@link #restore}, while
 * it is in use; zmux stores nothing itself.
 */
public final class CarrierCache {
    /** Absent time. */
    private static final long NONE = Long.MIN_VALUE;
    /** How far ahead an imported time is taken at most: further is the same for a hint. */
    private static final long HORIZON = Duration.ofDays(100 * 365).toNanos();

    private final Duration successTtl, backoffMin, backoffMax;
    private final HashMap<Key, Entry> entries = new HashMap<>();

    private record Key(String key, CarrierKind kind) {}

    /** Times in System.nanoTime, NONE when absent. */
    private static final class Entry {
        long goodUntil = NONE, failUntil = NONE;
        long backoff;
        String url = "";

        /** The later of the two times; NONE without either. */
        long latest() {
            if (goodUntil == NONE) return failUntil;
            if (failUntil == NONE) return goodUntil;
            return failUntil - goodUntil > 0 ? failUntil : goodUntil;
        }

        boolean newerThan(Entry o) {
            long a = latest(), b = o.latest();
            return a != NONE && (b == NONE || a - b > 0);
        }

        /** Both times and the fail time plus the longest backoff have passed. */
        boolean expired(long now, long backoffMax) {
            return passed(goodUntil, now) && passed(failUntil, now) && (failUntil == NONE || now - (failUntil + backoffMax) >= 0);
        }

        private static boolean passed(long t, long now) { return t == NONE || now - t >= 0; }
    }

    /** A cache with the defaults: a success TTL of 6 h and a backoff from 1 min to 6 h. */
    public CarrierCache() { this(Duration.ofHours(6), Duration.ofMinutes(1), Duration.ofHours(6)); }

    /**
     * A cache with these bounds, taken as they are: a success TTL of zero remembers no success,
     * a backoffMin of zero retries a failed kind at once (and keeps doing so). Each is zero or
     * more, backoffMax at least backoffMin, and none beyond a century.
     */
    public CarrierCache(Duration successTtl, Duration backoffMin, Duration backoffMax) {
        this.successTtl = bounded(successTtl, "successTtl");
        this.backoffMin = bounded(backoffMin, "backoffMin");
        this.backoffMax = bounded(backoffMax, "backoffMax");
        if (backoffMax.compareTo(backoffMin) < 0) throw new IllegalArgumentException("backoffMax below backoffMin");
    }

    private static Duration bounded(Duration d, String what) {
        if (Objects.requireNonNull(d, what).isNegative()) throw new IllegalArgumentException("negative " + what);
        if (d.compareTo(Duration.ofNanos(HORIZON)) > 0) throw new IllegalArgumentException(what + " beyond a century");
        return d;
    }

    public Duration successTtl() { return successTtl; }
    public Duration backoffMin() { return backoffMin; }
    public Duration backoffMax() { return backoffMax; }

    /** Whether kind was usable for key within the success TTL (and has not failed since), as of now (System.nanoTime). */
    synchronized boolean good(String key, CarrierKind kind, long now) {
        Entry e = entries.get(new Key(key, kind));
        return e != null && !failing(e, now) && e.goodUntil != NONE && now - e.goodUntil < 0;
    }

    /** When kind's backoff under key ends (System.nanoTime), or Long.MIN_VALUE when it is not backing off. */
    synchronized long failUntil(String key, CarrierKind kind, long now) {
        Entry e = entries.get(new Key(key, kind));
        return e != null && failing(e, now) ? e.failUntil : NONE;
    }

    private static boolean failing(Entry e, long now) {
        return e.failUntil != NONE && now - e.failUntil < 0;
    }

    /** Records an outcome of kind under key at now (System.nanoTime); a failure forgets the kind's final URL. */
    synchronized void record(String key, CarrierKind kind, boolean ok, long now) {
        Entry e = entries.computeIfAbsent(new Key(key, kind), k -> new Entry());
        if (ok) {
            e.goodUntil = now + successTtl.toNanos();
            e.failUntil = NONE;
            e.backoff = 0;
            return;
        }
        e.backoff = Math.clamp(2 * e.backoff, backoffMin.toNanos(), backoffMax.toNanos()); // min <= max: constructor
        e.goodUntil = NONE;
        e.failUntil = now + e.backoff;
        e.url = "";
    }

    /** Whether kind is backing off under key now. */
    public boolean backingOff(String key, CarrierKind kind) {
        return failUntil(key, kind, System.nanoTime()) != NONE;
    }

    /** Whether kind is remembered as usable under key now. */
    public boolean usable(String key, CarrierKind kind) {
        return good(key, kind, System.nanoTime());
    }

    /** The entries that have not expired, with their times on the wall clock, by key and kind. */
    public Snapshot snapshot() {
        var out = new ArrayList<Snapshot.Entry>();
        synchronized (this) {
            long now = System.nanoTime();
            Instant wall = Instant.now();
            long hi = backoffMax.toNanos();
            for (Map.Entry<Key, Entry> m : entries.entrySet()) {
                Entry e = m.getValue();
                if (e.expired(now, hi)) continue;
                out.add(new Snapshot.Entry(m.getKey().key(), m.getKey().kind(), wall(e.goodUntil, now, wall), wall(e.failUntil, now, wall),
                        Duration.ofNanos(e.backoff), e.url));
            }
        }
        out.sort(Comparator.comparing(Snapshot.Entry::key).thenComparing(e -> e.kind().toString()));
        return new Snapshot(out);
    }

    /**
     * Merges snapshot into the cache: an entry replaces the one held for its key and kind only
     * when it is newer (the later of its two times is later), a non-zero backoff is brought
     * within this cache's bounds, a good time further ahead than the success TTL or a fail time
     * further ahead than the maximum backoff is brought back to that bound, and expired entries
     * are dropped. The cache's configuration stays as it is.
     */
    public void restore(Snapshot snapshot) {
        Objects.requireNonNull(snapshot);
        long lo = backoffMin.toNanos(), hi = backoffMax.toNanos();
        synchronized (this) {
            long now = System.nanoTime();
            Instant wall = Instant.now();
            var merged = new HashMap<Key, Entry>();
            for (Snapshot.Entry s : snapshot.entries()) {
                var e = new Entry();
                e.goodUntil = bound(mono(s.goodUntil(), now, wall), now + successTtl.toNanos());
                e.failUntil = bound(mono(s.failUntil(), now, wall), now + hi);
                e.backoff = s.backoff().isZero() ? 0 : s.backoff().compareTo(backoffMax) > 0 ? hi : Math.max(lo, s.backoff().toNanos());
                e.url = s.url();
                if (!e.expired(now, hi)) merge(merged, new Key(s.key(), s.kind()), e);
            }
            merged.forEach((k, e) -> merge(entries, k, e));
        }
    }

    /** t, at most limit. */
    private static long bound(long t, long limit) { return t == NONE || t - limit <= 0 ? t : limit; }

    private static void merge(Map<Key, Entry> m, Key k, Entry e) {
        Entry o = m.get(k);
        if (o == null || e.newerThan(o)) m.put(k, e);
    }

    private static Instant wall(long t, long now, Instant wall) { return t == NONE ? null : wall.plusNanos(t - now); }

    /** The monotonic time of t, at most a century away from now. */
    private static long mono(Instant t, long now, Instant wall) {
        if (t == null) return NONE;
        Duration d = Duration.between(wall, t);
        long n = d.compareTo(Duration.ofNanos(HORIZON)) > 0 ? HORIZON : d.compareTo(Duration.ofNanos(-HORIZON)) < 0 ? -HORIZON : d.toNanos();
        return now + n;
    }

    /**
     * The exported state of a cache ({@link #snapshot()}), the input of {@link #restore}; its
     * interchange form is JSON ({@link #toJson()}, {@link #fromJson}), the same in every zmux
     * implementation.
     */
    public record Snapshot(List<Entry> entries) {
        /** The format version this implementation reads and writes. */
        public static final int VERSION = 1;

        public Snapshot { entries = List.copyOf(entries); }

        /**
         * One remembered kind.
         *
         * @param goodUntil until when the kind is held as usable; null: not
         * @param failUntil until when the kind is held as failed; null: not
         * @param backoff the current backoff; zero: no failure in a row
         * @param url the final URL redirects led the kind to; empty: the base URL
         */
        public record Entry(String key, CarrierKind kind, Instant goodUntil, Instant failUntil, Duration backoff, String url) {
            public Entry {
                Objects.requireNonNull(key, "key");
                Objects.requireNonNull(kind, "kind");
                Objects.requireNonNull(url, "url");
                if (Objects.requireNonNull(backoff, "backoff").isNegative()) throw new IllegalArgumentException("negative backoff");
            }
        }

        /**
         * The JSON form: {@code {"version":1,"entries":[...]}}, each entry's members in the order
         * key, kind, good_until, fail_until, backoff_ms, url, those that are empty left out, times
         * in RFC 3339 UTC.
         */
        public String toJson() {
            var b = new StringBuilder("{\"version\":").append(VERSION).append(",\"entries\":[");
            for (int i = 0; i < entries.size(); i++) {
                Entry e = entries.get(i);
                if (i > 0) b.append(',');
                b.append("{\"key\":");
                SnapshotJson.writeString(b, e.key());
                b.append(",\"kind\":");
                SnapshotJson.writeString(b, e.kind().toString());
                if (e.goodUntil() != null) b.append(",\"good_until\":\"").append(SnapshotJson.formatTime(e.goodUntil())).append('"');
                if (e.failUntil() != null) b.append(",\"fail_until\":\"").append(SnapshotJson.formatTime(e.failUntil())).append('"');
                long ms = e.backoff().toMillis();
                if (ms > 0) b.append(",\"backoff_ms\":").append(ms);
                if (!e.url().isEmpty()) {
                    b.append(",\"url\":");
                    SnapshotJson.writeString(b, e.url());
                }
                b.append('}');
            }
            return b.append("]}").toString();
        }

        /**
         * Parses the JSON form. Unknown members are ignored and entries of an unknown kind
         * skipped; text that is not JSON, another version, a member of the wrong type or a
         * negative backoff fail the whole snapshot.
         *
         * @throws InvalidSnapshotException when text is not a snapshot this implementation reads
         */
        public static Snapshot fromJson(String text) {
            Objects.requireNonNull(text);
            SnapshotJson.Value v;
            try {
                v = SnapshotJson.parse(text);
            } catch (IllegalArgumentException e) {
                throw new InvalidSnapshotException(e.getMessage());
            }
            if (!(v instanceof SnapshotJson.Obj top)) throw new InvalidSnapshotException("not an object");
            if (!(top.get("version") instanceof SnapshotJson.Int(long version))) throw new InvalidSnapshotException("no integer version");
            if (version != VERSION) throw new InvalidSnapshotException("version " + version);
            List<SnapshotJson.Value> list = switch (top.get("entries")) {
                case null -> List.of();
                case SnapshotJson.Null _ -> List.of();
                case SnapshotJson.Arr(List<SnapshotJson.Value> a) -> a;
                default -> throw new InvalidSnapshotException("entries not an array");
            };
            var out = new ArrayList<Entry>();
            for (SnapshotJson.Value item : list) {
                if (!(item instanceof SnapshotJson.Obj o)) throw new InvalidSnapshotException("an entry not an object");
                String key = string(o, "key"), kind = string(o, "kind"), url = string(o, "url");
                Instant good = time(o, "good_until"), fail = time(o, "fail_until");
                Duration backoff = switch (o.get("backoff_ms")) {
                    case null -> Duration.ZERO;
                    case SnapshotJson.Null _ -> Duration.ZERO;
                    case SnapshotJson.Int(long ms) when ms >= 0 -> Duration.ofMillis(ms);
                    default -> throw new InvalidSnapshotException("backoff_ms not a non-negative integer");
                };
                CarrierKind k = kind(kind);
                if (k != null) out.add(new Entry(key, k, good, fail, backoff, url));
            }
            return new Snapshot(out);
        }

        private static String string(SnapshotJson.Obj o, String name) {
            return switch (o.get(name)) {
                case null -> "";
                case SnapshotJson.Null _ -> "";
                case SnapshotJson.Str(String s) -> s;
                default -> throw new InvalidSnapshotException(name + " not a string");
            };
        }

        private static Instant time(SnapshotJson.Obj o, String name) {
            String s = string(o, name);
            if (s.isEmpty()) return null;
            Instant t = SnapshotJson.parseTime(s);
            if (t == null) throw new InvalidSnapshotException(name + " \"" + s + "\" not an RFC 3339 time");
            return t;
        }

        private static CarrierKind kind(String name) {
            for (CarrierKind k : CarrierKind.values()) if (k.toString().equals(name)) return k;
            return null;
        }
    }

    /** Text that {@link Snapshot#fromJson} does not read as a snapshot; nothing of it is taken. */
    public static final class InvalidSnapshotException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        InvalidSnapshotException(String what) { super("invalid carrier cache snapshot: " + what); }
    }
}
