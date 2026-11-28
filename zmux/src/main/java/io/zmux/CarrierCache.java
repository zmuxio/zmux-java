package io.zmux;

import io.zmux.internal.core.Time;
import io.zmux.internal.spi.RetryTiming;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Scoring history and retry state shared by carrier choices. Channels and pools use the same candidate key. */
public final class CarrierCache {
    private static final Instant UNSET = Instant.parse("0001-01-01T00:00:00Z");
    private static final Duration SHORT_WINDOW = Duration.ofMinutes(10);
    private static final double RATE_REFERENCE = 1_250_000;
    private static final int VERSION = 2;

    /** Scoring, retry and snapshot bounds for one carrier cache. */
    public record Config(Duration halfLife, Duration dropWindow, Duration performanceWindow, Duration healthReset,
                         Backoff backoff, int maxImportBytes, int maxImportEntries, boolean shortDropResilient) {
        private static final Backoff SHORT_DROP_BACKOFF = new Backoff(Duration.ofSeconds(30), Duration.ofMinutes(30),
                Backoff.DEFAULT_JITTER, Backoff.DEFAULT_JITTER);
        public static final Config DEFAULT = new Config(Duration.ofHours(6), Duration.ofSeconds(60),
                Duration.ofSeconds(60), Duration.ofMinutes(10), new Backoff(Duration.ofMinutes(1), Duration.ofHours(6)),
                16 << 20, 65536);

        public Config(Duration halfLife, Duration dropWindow, Duration performanceWindow, Duration healthReset,
                      Backoff backoff, int maxImportBytes, int maxImportEntries) {
            this(halfLife, dropWindow, performanceWindow, healthReset, backoff, maxImportBytes, maxImportEntries, false);
        }

        /** Selects the deployment profile that raises the nominal wait after repeated short drops. */
        public Config withShortDropResilience() {
            return new Config(halfLife, dropWindow, performanceWindow, healthReset, SHORT_DROP_BACKOFF,
                    maxImportBytes, maxImportEntries, true);
        }

        public Config {
            for (Duration d : List.of(halfLife, dropWindow, performanceWindow, healthReset)) {
                if (d.isZero() || d.isNegative() || d.compareTo(Duration.ofDays(36500)) > 0)
                    throw new IllegalArgumentException("carrier cache durations must be positive and at most a century");
            }
            Objects.requireNonNull(backoff);
            if (shortDropResilient && !backoff.equals(SHORT_DROP_BACKOFF))
                throw new IllegalArgumentException("cannot override the short-drop-resilient backoff");
            if (maxImportBytes <= 0 || maxImportEntries <= 0) throw new IllegalArgumentException("invalid snapshot bounds");
        }
    }

    /** The kind of evidence contributed by an observation. */
    public enum Outcome { USABLE, FAILED, REJECTED, SURVIVED, SHORT_DROP, LATE_DROP, LIFETIME, PERFORMANCE, RECOVERED }

    /** Original observation time, healthy interval, RTT and rate in bytes per second. Zero indicates absent performance evidence. */
    public record Observation(Instant at, Duration duration, Duration rtt, long rate, Duration validFor, MeasurementSource source) {
        public Observation(Instant at, Duration duration, Duration rtt, long rate, Duration validFor) {
            this(at, duration, rtt, rate, validFor, MeasurementSource.HOST_REPORT);
        }
        public Observation(Instant at, Duration duration, Duration rtt, long rate) { this(at, duration, rtt, rate, Duration.ZERO); }
        public static final Observation NOW = new Observation(null, Duration.ZERO, Duration.ZERO, 0);

        public Observation {
            if (Objects.requireNonNull(validFor).isNegative() || Objects.requireNonNull(duration).isNegative() || Objects.requireNonNull(rtt).isNegative() || rate < 0)
                throw new IllegalArgumentException("negative observation");
        }
    }

    /** The origin of a performance observation. */
    public enum MeasurementSource { PROTOCOL, HOST_REPORT }

    /** Latest observed value with its original age, effective validity and provenance where known. */
    public record Measurement(double value, Duration age, Duration validFor, Optional<MeasurementSource> source) {}

    /** The carrier's scoring evidence and remaining retry delay at lookup. */
    public record Score(double score, double reliability, long attempts, double weight, double successWeight, Duration age,
                        Optional<Duration> cooling, Duration nominalBackoff, long cooldownLevel,
                        double handshake, double stability, Optional<Measurement> rttSeconds, Optional<Measurement> rateBytesPerSecond) {}

    /** Reason a start opportunity did not invoke an opener. */
    public enum Suppression { ALREADY_IN_FLIGHT, RECOVERY_WAIT, COOLING, RETRY_AFTER, DIAL_AFTER, COVERAGE, UNAUTHORIZED, CAPACITY, DEADLINE, STALE }

    /** Why a fault committed a local cooldown. */
    public enum CooldownReason { ATTEMPT, REJECTED, SHORT_DROP, LATE_DROP }

    /** An ordered local cooldown commit. Timestamps are readings of the cache's clock domain. */
    public sealed interface CooldownEvent permits CooldownStarted, CooldownReset {
        Key key();
    }

    /** The committed local retry schedule for one fault episode. */
    public record CooldownStarted(Key key, long episode, long level, CooldownReason reason, long startedAt,
                                  Duration nominalWait, Duration sampledWait, long retryAt) implements CooldownEvent {}

    /** Continuous health cleared the sequence. A zero episode indicates imported cooldown state. */
    public record CooldownReset(Key key, long episode, long resetAt) implements CooldownEvent {}

    /** Process-local counters and current local cooldown, excluding hard restrictions. */
    public record RetryMetrics(long attemptedStarts, long faults, Duration recoveryNominal, Duration recoverySampled, Long recoveryRetryAt,
                               Map<Suppression, Long> suppressions, Optional<Duration> elapsed, Duration remaining) {
        public RetryMetrics { suppressions = Map.copyOf(suppressions); }
    }

    /** Why an imported snapshot could not be read. */
    public enum SnapshotError { VERSION, FORMAT, VALUE }

    /** A snapshot failed validation and was discarded. */
    public static final class InvalidSnapshotException extends IllegalArgumentException {
        @java.io.Serial private static final long serialVersionUID = 1L;
        private final SnapshotError reason;
        InvalidSnapshotException(SnapshotError reason, String message) { super(message); this.reason = reason; }
        public SnapshotError reason() { return reason; }
    }

    /** Opaque host key, carrier kind and variant used by the shared cache. */
    public record Key(String key, CarrierKind kind, String variant) {
        public Key {
            Objects.requireNonNull(key);
            Objects.requireNonNull(kind);
            if (variant == null) variant = "";
        }
    }

    private enum Metric {
        SUCCESS("success"), FAILURE("failure"), SURVIVED("survived"), SHORT("short_drop"),
        LATE("late_drop"), LIFETIME("late_seconds");
        final String wire;
        Metric(String wire) { this.wire = wire; }
    }
    private static final Metric[] METRICS = Metric.values();

    /** Wall time orders snapshot updates. The monotonic anchor preserves age through clock changes. */
    private record Anchor(Instant at, long nano, long initialAge) {
        static final Anchor EMPTY = new Anchor(UNSET, 0, 0);

        long age(long now) {
            return at.equals(UNSET) ? 0 : Time.add(initialAge, Math.max(0, now - nano));
        }

        boolean newerThan(Anchor other) { return at.isAfter(other.at); }

        Anchor reanchor(long now, long elapsed) {
            return at.equals(UNSET) ? EMPTY : new Anchor(at, now, Time.add(initialAge, elapsed));
        }
    }

    private static final class History {
        private Anchor anchor = Anchor.EMPTY;
        private long attempts;
        private final EnumMap<Metric, Double> values = new EnumMap<>(Metric.class);
        double get(Metric metric) { return values.getOrDefault(metric, 0.0); }
        void add(Metric metric, double value) { values.merge(metric, value, Double::sum); }

        void advance(Anchor next, double factor) {
            if (factor != 1) values.replaceAll((metric, value) -> value * factor);
            anchor = next;
        }
    }

    private static final class Cooling {
        private Anchor anchor = Anchor.EMPTY;
        private long nominal, level, episode;
        private long until, peerUntil;
        private final ArrayList<Anchor> shortDrops = new ArrayList<>(3);
    }

    private record Performance(Anchor anchor, double value, long validity, MeasurementSource source) {
        static final Performance EMPTY = new Performance(Anchor.EMPTY, 0, 0, null);

        Performance reanchor(long now, long elapsed) {
            return new Performance(anchor.reanchor(now, elapsed), value, validity, source);
        }
    }

    private record HealthySince(long time) {}

    private static final class Entry {
        private final java.util.IdentityHashMap<Object, HealthySince> healthy = new java.util.IdentityHashMap<>();
        private long healthDeadline = Time.NEVER;
        private Runnable cancelHealth;
        private History history = new History();
        private Cooling cooling = new Cooling();
        private Performance rtt = Performance.EMPTY, rate = Performance.EMPTY;
        private String url = "";
        private long attemptedStarts, faults;
        private final java.util.Set<Object> recoveryOwners = new java.util.HashSet<>();
        private long recoveryNominal, recoverySampled, recoveryAt;
        private Object recoveryDraw;

        private void resetRecovery() {
            recoveryNominal = recoverySampled = recoveryAt = 0;
            recoveryDraw = null;
        }
        private final EnumMap<Suppression, Long> suppressions = new EnumMap<>(Suppression.class);

        private void suppress(Suppression reason) { suppressions.compute(reason, (key, count) -> count == null ? 1 : increment(count)); }
    }

    private final Config config;
    private final ClockDomain domain;
    private final Supplier<Instant> wallClock;
    private final LongSupplier clock;
    private final java.util.random.RandomGenerator random;
    private final io.zmux.internal.loop.SerialCallbacks sampler = new io.zmux.internal.loop.SerialCallbacks();
    private final HashMap<Key, Entry> entries = new HashMap<>();
    private final Map<io.zmux.carrier.CarrierFaultException.Episode, java.util.Set<Key>> physicalFaults = new java.util.WeakHashMap<>();
    private final java.util.HashSet<Key> inFlight = new java.util.HashSet<>();
    private final java.util.concurrent.CopyOnWriteArrayList<Runnable> changeListeners = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final java.util.concurrent.SubmissionPublisher<CooldownEvent> cooldownEvents = new java.util.concurrent.SubmissionPublisher<>();
    private final java.util.concurrent.atomic.AtomicLong droppedCooldownEvents = new java.util.concurrent.atomic.AtomicLong();
    private Instant wallFloor = UNSET;
    private long lastEpisode;

    final class Attempt implements AutoCloseable {
        private final Key candidate;
        private final boolean recovery;
        private final LongSupplier dialAfter;
        private boolean started, closed;
        private long blockedUntil;

        private Attempt(Key candidate, boolean recovery, LongSupplier dialAfter) {
            this.candidate = candidate;
            this.recovery = recovery;
            this.dialAfter = dialAfter;
        }

        boolean start() { return startReason() == null; }

        Suppression startReason() {
            synchronized (CarrierCache.this) {
                var entry = entries.get(candidate);
                long now = clock.getAsLong();
                var cooling = entry.cooling;
                long after = dialAfter.getAsLong();
                if (closed || started) return Suppression.STALE;
                if (now < after) {
                    blockedUntil = after;
                    return Suppression.DIAL_AFTER;
                }
                if (now < cooling.peerUntil) return Suppression.RETRY_AFTER;
                if (recovery && now < entry.recoveryAt) return Suppression.RECOVERY_WAIT;
                if (!recovery && now < cooling.until) return Suppression.COOLING;
                entry.attemptedStarts = increment(entry.attemptedStarts);
                started = true;
                return null;
            }
        }

        long blockedUntil() {
            synchronized (CarrierCache.this) { return blockedUntil; }
        }

        boolean started() {
            synchronized (CarrierCache.this) {
                return started;
            }
        }

        void report(Outcome outcome, long at, boolean cooldown) {
            update(candidate, outcome, new Observation(domain.wallTime(at), Duration.ZERO, Duration.ZERO, 0),
                    null, cooldown && outcome == Outcome.FAILED, true);
        }

        void cooldownOnly(long at) {
            synchronized (CarrierCache.this) {
                var anchor = new Anchor(domain.wallTime(at), at, 0);
                cool(candidate, entries.get(candidate), anchor, Outcome.FAILED, null);
            }
            signalChange();
        }

        CompletableFuture<Void> terminal(boolean confirmed, boolean candidateFailure, long at) {
            Object draw = new Object();
            long nominal;
            synchronized (CarrierCache.this) {
                if (!started || closed) return CompletableFuture.completedFuture(null);
                Entry entry = entries.get(candidate);
                if (confirmed) entry.resetRecovery();
                if (!recovery || !candidateFailure || confirmed || entry.recoveryOwners.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                }
                nominal = Math.clamp(entry.recoveryNominal * 2, Time.SECOND, 60 * Time.SECOND);
                entry.recoveryNominal = nominal;
                entry.recoveryDraw = draw;
            }
            var settled = new CompletableFuture<Void>();
            sampler.execute(() -> {
                long sampled = sample(nominal, 1, 0);
                synchronized (CarrierCache.this) {
                    Entry entry = entries.get(candidate);
                    if (entry.recoveryDraw == draw) {
                        entry.recoverySampled = sampled;
                        entry.recoveryAt = Time.add(at, sampled);
                        entry.recoveryDraw = null;
                    }
                }
                settled.complete(null);
                signalChange();
            });
            return settled;
        }

        @Override public void close() {
            synchronized (CarrierCache.this) {
                if (closed) return;
                closed = true;
                inFlight.remove(candidate);
            }
            signalChange();
        }
    }

    Runnable observeChanges(Runnable listener) {
        changeListeners.add(listener);
        return () -> changeListeners.remove(listener);
    }

    private void signalChange() { changeListeners.forEach(Runnable::run); }

    record Reservation(Attempt attempt, Suppression blocked) {}

    synchronized Reservation acquire(String key, CarrierKind kind, String variant, boolean recovery, LongSupplier dialAfter) {
        Key candidate = key(key, kind, variant);
        var entry = entries.computeIfAbsent(candidate, ignored -> new Entry());
        long now = clock.getAsLong();
        var blocked = now < dialAfter.getAsLong() ? Suppression.DIAL_AFTER : admission(candidate, entry, now, recovery);
        if (blocked != null) return new Reservation(null, blocked);
        inFlight.add(candidate);
        return new Reservation(new Attempt(candidate, recovery, dialAfter), null);
    }

    private Suppression admission(Key key, Entry entry, long now, boolean recovery) {
        if (inFlight.contains(key)) return Suppression.ALREADY_IN_FLIGHT;
        if (entry == null) return null;
        if (now < entry.cooling.peerUntil) return Suppression.RETRY_AFTER;
        if (recovery) return now < entry.recoveryAt ? Suppression.RECOVERY_WAIT : null;
        return now < entry.cooling.until ? Suppression.COOLING : null;
    }

    synchronized Suppression admission(String key, CarrierKind kind, String variant, boolean recovery) {
        Key id = key(key, kind, variant);
        return admission(id, entries.get(id), clock.getAsLong(), recovery);
    }

    synchronized boolean recoveryDue(Key key) {
        var entry = entries.get(key);
        return entry == null || (entry.recoveryDraw == null && clock.getAsLong() >= entry.recoveryAt);
    }

    synchronized void suppress(String key, CarrierKind kind, String variant, Suppression reason) {
        entries.computeIfAbsent(key(key, kind, variant), ignored -> new Entry()).suppress(reason);
    }

    /** Observes future commits. Event delivery is asynchronous and can drop items for slow subscribers. */
    public java.util.concurrent.Flow.Publisher<CooldownEvent> cooldownEvents() { return cooldownEvents; }

    /** Number of event deliveries dropped for subscribers that fell behind their buffers. */
    public long droppedCooldownEvents() { return droppedCooldownEvents.get(); }

    /** Returns current counters and wait for the candidate's shared cache key. */
    public synchronized RetryMetrics retryMetrics(String key, CarrierKind kind, String variant) {
        var entry = entries.get(key(key, kind, variant));
        if (entry == null) return new RetryMetrics(0, 0, Duration.ZERO, Duration.ZERO, null, Map.of(), Optional.empty(), Duration.ZERO);
        var cooling = entry.cooling;
        long now = clock.getAsLong();
        return new RetryMetrics(entry.attemptedStarts, entry.faults, Duration.ofNanos(entry.recoveryNominal),
                Duration.ofNanos(entry.recoverySampled), entry.recoveryNominal == 0 ? null : entry.recoveryAt,
                entry.suppressions, cooling.nominal != 0 || cooling.level != 0
                        ? Optional.of(Duration.ofNanos(cooling.anchor.age(now))) : Optional.empty(),
                Duration.ofNanos(Math.max(0, Time.sub(cooling.until, now))));
    }

    private static long increment(long count) { return count == Long.MAX_VALUE ? count : count + 1; }

    private void publish(CooldownEvent event) {
        cooldownEvents.offer(event, (subscriber, dropped) -> {
            droppedCooldownEvents.updateAndGet(CarrierCache::increment);
            return false;
        });
    }

    synchronized long recoveryDelay(String key, CarrierKind kind, String variant) {
        Entry entry = entries.get(key(key, kind, variant));
        return entry == null ? 0 : Math.max(0, Time.sub(Math.max(entry.recoveryAt, entry.cooling.peerUntil), clock.getAsLong()));
    }

    synchronized void recoveryDemand(Key key, Object owner, boolean wanted) {
        var entry = entries.computeIfAbsent(key, ignored -> new Entry());
        if (wanted) entry.recoveryOwners.add(owner);
        else if (entry.recoveryOwners.remove(owner) && entry.recoveryOwners.isEmpty()) entry.resetRecovery();
    }

    public CarrierCache() { this(Config.DEFAULT); }

    public CarrierCache(Config config) { this(config, ClockDomain.system()); }

    public CarrierCache(Config config, ClockDomain domain) {
        this(config, domain, domain::wallTime, domain::now, ThreadLocalRandom.current());
    }

    CarrierCache(Config config, Supplier<Instant> wallClock, LongSupplier clock, java.util.random.RandomGenerator random) {
        this(config, ClockDomain.system(), wallClock, clock, random);
    }

    public CarrierCache(Config config, ClockDomain domain, java.util.random.RandomGenerator random) {
        this(config, domain, domain::wallTime, domain::now, Objects.requireNonNull(random));
    }

    private CarrierCache(Config config, ClockDomain domain, Supplier<Instant> wallClock, LongSupplier clock,
                         java.util.random.RandomGenerator random) {
        this.config = Objects.requireNonNull(config);
        this.domain = domain;
        this.wallClock = wallClock;
        this.clock = clock;
        this.random = random;
    }

    ClockDomain domain() { return domain; }

    public Config config() { return config; }

    private Instant now() {
        Instant reading = wallClock.get();
        if (reading.isAfter(wallFloor)) wallFloor = reading;
        return wallFloor;
    }

    private static Key key(String key, CarrierKind kind, String variant) {
        return new Key(key, kind, variant);
    }

    private static double seconds(Duration d) { return d.getSeconds() + d.getNano() / 1e9; }

    private static long wallAge(Instant at, Instant now) {
        return at.equals(UNSET) || at.isAfter(now) ? 0 : Time.nanos(Duration.between(at, now));
    }

    private double decay(long age) {
        return Math.pow(2, -(double) age / config.halfLife().toNanos());
    }

    private double performance(Performance sample, long now, double reference, boolean inverse) {
        long window = config.performanceWindow().toNanos();
        if (sample.validity > 0) window = Math.min(window, sample.validity);
        if (sample.value == 0 || sample.anchor.age(now) >= window) return 1;
        return Math.clamp(inverse ? reference / sample.value : sample.value / reference, .5, 2);
    }

    private Optional<Measurement> measurement(Performance sample, long now) {
        if (sample.value == 0) return Optional.empty();
        long validity = config.performanceWindow().toNanos();
        if (sample.validity > 0) validity = Math.min(validity, sample.validity);
        return Optional.of(new Measurement(sample.value, Duration.ofNanos(sample.anchor.age(now)), Duration.ofNanos(validity),
                Optional.ofNullable(sample.source)));
    }

    public Score score(String key, CarrierKind kind, String variant) {
        return scoreAt(key(key, kind, variant), clock.getAsLong());
    }

    synchronized Score scoreAt(Key key, long now) {
        var entry = entries.get(key);
        if (entry == null) return new Score(.125, .125, 0, 0, 0, Duration.ZERO, Optional.empty(), Duration.ZERO, 0, .5, .5, Optional.empty(), Optional.empty());
        History h = entry.history;
        long age = h.anchor.age(now);
        double factor = decay(age);
        double success = h.get(Metric.SUCCESS) * factor, failure = h.get(Metric.FAILURE) * factor;
        double survived = h.get(Metric.SURVIVED) * factor, shortDrop = h.get(Metric.SHORT) * factor;
        double dropWindow = seconds(config.dropWindow());
        double frequency = h.get(Metric.LATE) * factor / (dropWindow + h.get(Metric.LIFETIME) * factor);
        double stability = (1 + survived) / (2 + survived + shortDrop) / (1 + frequency * dropWindow);
        double handshake = (1 + success) / (2 + success + failure);
        double reliability = handshake * stability * stability;
        double value = reliability * Math.sqrt(performance(entry.rtt, now, .1, true) * performance(entry.rate, now, RATE_REFERENCE, false));
        long remaining = Math.max(0, Time.sub(entry.cooling.until, now));
        return new Score(value, reliability, h.attempts, success + failure, success, Duration.ofNanos(age),
                remaining > 0 ? Optional.of(Duration.ofNanos(remaining)) : Optional.empty(),
                Duration.ofNanos(entry.cooling.nominal), entry.cooling.level, handshake, stability, measurement(entry.rtt, now), measurement(entry.rate, now));
    }

    private static long remaining(Cooling cooling, long now) {
        return Math.max(0, Time.sub(Math.max(cooling.until, cooling.peerUntil), now));
    }

    public boolean backingOff(String key, CarrierKind kind) { return backingOff(key, kind, ""); }
    public boolean backingOff(String key, CarrierKind kind, String variant) { return retryDelay(key, kind, variant) != 0; }
    public boolean usable(String key, CarrierKind kind) { return usable(key, kind, ""); }
    public boolean usable(String key, CarrierKind kind, String variant) {
        Score score = score(key, kind, variant);
        return score.successWeight() > 0 && !backingOff(key, kind, variant);
    }

    synchronized long retryDelay(String key, CarrierKind kind, String variant) {
        Entry entry = entries.get(key(key, kind, variant));
        return entry == null ? 0 : remaining(entry.cooling, clock.getAsLong());
    }

    synchronized String url(String key, CarrierKind kind, String variant) {
        Entry entry = entries.get(key(key, kind, variant));
        return entry == null ? "" : entry.url;
    }

    void record(String key, CarrierKind kind, String variant, boolean ok, HttpTarget target) {
        String redirect = null;
        if (target != null) {
            if (ok) redirect = target.url().equals(target.base()) ? "" : target.url();
            else target.reset();
        }
        update(key(key, kind, variant), ok ? Outcome.USABLE : Outcome.FAILED, Observation.NOW, redirect, true);
    }

    public void report(String key, CarrierKind kind, String variant, Outcome outcome) {
        report(key, kind, variant, outcome, Observation.NOW);
    }

    public void report(String key, CarrierKind kind, String variant, Outcome outcome, Observation observation) {
        update(key(key, kind, variant), outcome, observation, null, true);
    }

    void reportFault(String key, CarrierKind kind, String variant, Outcome outcome, Instant at,
                     io.zmux.carrier.CarrierFaultException.Episode episode) {
        synchronized (this) {
            Key id = key(key, kind, variant);
            boolean sample = episode == null || physicalFaults.computeIfAbsent(episode, ignored -> new java.util.HashSet<>()).add(id);
            commit(id, outcome, new Observation(at, Duration.ZERO, Duration.ZERO, 0), null, sample, null);
        }
        signalChange();
    }

    void retryAfter(String key, CarrierKind kind, String variant, Duration retry) {
        synchronized (this) {
            var cooling = entries.computeIfAbsent(key(key, kind, variant), ignored -> new Entry()).cooling;
            cooling.peerUntil = Math.max(cooling.peerUntil, Time.add(clock.getAsLong(), Time.nanos(retry)));
        }
        signalChange();
    }

    private void update(Key key, Outcome outcome, Observation observation, String redirect, boolean startCooldown) {
        update(key, outcome, observation, redirect, startCooldown, false);
    }

    private void update(Key key, Outcome outcome, Observation observation, String redirect, boolean startCooldown,
                        boolean ownerCommit) {
        boolean needsDraw = startCooldown && switch (outcome) {
            case FAILED, REJECTED, SHORT_DROP, LATE_DROP -> true;
            default -> false;
        };
        var backoff = config.backoff();
        Double draw = null;
        if (needsDraw && !ownerCommit && (backoff.jitter() != 0 || backoff.upwardJitter() != 0)) {
            if (io.zmux.internal.loop.SerialCallbacks.onContext(sampler)) draw = 1.0;
            else {
                var sampled = new CompletableFuture<Double>();
                sampler.execute(() -> sampled.complete(fraction()));
                draw = sampled.join();
            }
        }
        synchronized (this) { commit(key, outcome, observation, redirect, startCooldown, draw); }
        signalChange();
    }

    private double fraction() {
        try {
            double value = random.nextDouble();
            return Double.isFinite(value) && value >= 0 && value <= 1 ? value : 1;
        } catch (RuntimeException | Error callbackFailure) {
            return 1;
        }
    }

    private long sample(long nominal, double down, double up) {
        return RetryTiming.sample(nominal, down, up, fraction());
    }

    private void commit(Key key, Outcome outcome, Observation observation, String redirect, boolean startCooldown, Double draw) {
        Instant wall = now();
        long nano = clock.getAsLong();
        Instant at = observation.at == null ? wall : observation.at;
        if (at.isAfter(wall)) throw new IllegalArgumentException("future observation");
        Anchor now = new Anchor(wall, nano, 0);
        long observationAge = wallAge(at, wall);
        Entry e = entries.computeIfAbsent(key, ignored -> new Entry());
        switch (outcome) {
            case USABLE, FAILED, REJECTED, SURVIVED, SHORT_DROP, LATE_DROP, LIFETIME -> e.history.advance(now, decay(e.history.anchor.age(nano)));
            case PERFORMANCE, RECOVERED -> { }
        }
        double w = decay(observationAge);
        switch (outcome) {
            case USABLE -> {
                if (e.history.attempts < Long.MAX_VALUE) e.history.attempts++;
                e.history.add(Metric.SUCCESS, w);
                if (redirect != null) e.url = redirect;
            }
            case FAILED, REJECTED -> {
                if (e.history.attempts < Long.MAX_VALUE) e.history.attempts++;
                e.history.add(Metric.FAILURE, w);
                if (startCooldown) cool(key, e, new Anchor(at, Time.sub(nano, observationAge), 0), outcome, draw);
                e.url = "";
            }
            case SURVIVED -> e.history.add(Metric.SURVIVED, w);
            case SHORT_DROP, LATE_DROP -> {
                e.history.add(outcome == Outcome.SHORT_DROP ? Metric.SHORT : Metric.LATE, w);
                if (startCooldown) cool(key, e, new Anchor(at, Time.sub(nano, observationAge), 0), outcome, draw);
            }
            case LIFETIME -> {
                double decayRate = Math.log(2) / seconds(config.halfLife());
                e.history.add(Metric.LIFETIME, w * -Math.expm1(-decayRate * seconds(observation.duration)) / decayRate);
            }
            case PERFORMANCE -> {
                Anchor sample = new Anchor(at, nano, observationAge);
                if (!observation.rtt.isZero() && sample.newerThan(e.rtt.anchor)) e.rtt = new Performance(sample, seconds(observation.rtt), Time.nanos(observation.validFor), observation.source);
                if (observation.rate > 0 && sample.newerThan(e.rate.anchor)) e.rate = new Performance(sample, observation.rate, Time.nanos(observation.validFor), observation.source);
            }
            case RECOVERED -> {
                if (observation.duration.compareTo(config.healthReset()) >= 0
                        && (e.cooling.anchor.at.equals(UNSET)
                        || Time.add(observationAge, Time.nanos(observation.duration)) <= e.cooling.anchor.age(nano))) {
                    resetCooldown(key, e, now);
                }
            }
        }
    }

    synchronized void memberHealth(Key key, Object member, Long since) {
        Entry entry = entries.computeIfAbsent(key, ignored -> new Entry());
        if (since == null) entry.healthy.remove(member);
        else entry.healthy.putIfAbsent(member, new HealthySince(since));
        armHealth(key, entry);
    }

    private void armHealth(Key key, Entry entry) {
        long next = Time.NEVER;
        var cooling = entry.cooling;
        if (cooling.nominal != 0 || cooling.level != 0) {
            long fault = Time.sub(cooling.anchor.nano, cooling.anchor.initialAge);
            for (var health : entry.healthy.values()) {
                long start = health.time();
                next = Math.min(next, Time.add(Math.max(fault, start), config.healthReset().toNanos()));
            }
        }
        if (entry.healthDeadline == next) return;
        if (entry.cancelHealth != null) entry.cancelHealth.run();
        entry.healthDeadline = next;
        long deadline = next;
        entry.cancelHealth = next == Time.NEVER ? null : domain.schedule(next, () -> {
            synchronized (CarrierCache.this) {
                if (entry.healthDeadline != deadline || clock.getAsLong() < deadline) return;
                resetCooldown(key, entry, new Anchor(now(), clock.getAsLong(), 0));
            }
            signalChange();
        });
    }

    private void resetCooldown(Key key, Entry entry, Anchor now) {
        var previous = entry.cooling;
        entry.cooling = new Cooling();
        entry.cooling.anchor = now;
        entry.cooling.peerUntil = previous.peerUntil;
        armHealth(key, entry);
        if (previous.level != 0 || previous.nominal != 0) publish(new CooldownReset(key, previous.episode, now.nano));
    }

    private void cool(Key key, Entry entry, Anchor now, Outcome outcome, Double draw) {
        var c = entry.cooling;
        Backoff backoff = config.backoff();
        long max = backoff.max().toNanos();
        long nominal = Math.max(backoff.min().toNanos(), RetryTiming.advance(c.nominal, max));
        if (outcome == Outcome.SHORT_DROP && config.shortDropResilient()) {
            c.shortDrops.removeIf(at -> at.age(now.nano) >= SHORT_WINDOW.toNanos());
            if (c.shortDrops.size() == 3) c.shortDrops.removeFirst();
            c.shortDrops.addLast(now);
            if (c.shortDrops.size() == 3) nominal = Math.max(nominal, SHORT_WINDOW.toNanos());
        }
        if (outcome == Outcome.REJECTED) nominal = max;
        lastEpisode = Math.incrementExact(lastEpisode);
        c.anchor = now;
        c.nominal = nominal;
        c.level = increment(c.level);
        c.episode = lastEpisode;
        c.until = Time.NEVER;
        entry.faults = increment(entry.faults);
        var reason = switch (outcome) {
            case SHORT_DROP -> CooldownReason.SHORT_DROP;
            case LATE_DROP -> CooldownReason.LATE_DROP;
            case REJECTED -> CooldownReason.REJECTED;
            default -> CooldownReason.ATTEMPT;
        };
        long base = nominal;
        long episode = c.episode;
        long level = c.level;
        if (draw != null || (backoff.jitter() == 0 && backoff.upwardJitter() == 0)) {
            completeCooldown(key, entry, c, now, reason, base, episode, level,
                    RetryTiming.sample(base, backoff.jitter(), backoff.upwardJitter(), draw == null ? 1.0 : draw));
        } else {
            sampler.execute(() -> {
                long sampled = sample(base, backoff.jitter(), backoff.upwardJitter());
                synchronized (this) { completeCooldown(key, entry, c, now, reason, base, episode, level, sampled); }
                signalChange();
            });
        }
    }

    private void completeCooldown(Key key, Entry entry, Cooling cooling, Anchor at, CooldownReason reason,
                                  long nominal, long episode, long level, long sampled) {
        long retry = Time.add(at.nano, sampled);
        if (entry.cooling == cooling && cooling.episode == episode) {
            cooling.until = retry;
            armHealth(key, entry);
        }
        publish(new CooldownStarted(key, episode, level, reason, at.nano, Duration.ofNanos(nominal),
                Duration.ofNanos(sampled), retry));
    }

    /** Encodes the shared version 2 snapshot. Process-local redirects are not persisted. */
    public synchronized byte[] export() {
        var out = new StringBuilder("{\"version\":2,\"exported_at\":");
        time(out, now());
        out.append(",\"entries\":[");
        long nano = clock.getAsLong();
        var ordered = entries.entrySet().stream().filter(item -> {
            var e = item.getValue();
            return !e.history.anchor.at.equals(UNSET) || !e.cooling.anchor.at.equals(UNSET)
                    || !e.rtt.anchor.at.equals(UNSET) || !e.rate.anchor.at.equals(UNSET);
        }).sorted(Comparator
                .comparing((Map.Entry<Key, Entry> e) -> e.getKey().key)
                .thenComparing(e -> e.getKey().kind.toString()).thenComparing(e -> e.getKey().variant)).toList();
        for (var item : ordered) {
            if (out.charAt(out.length() - 1) != '[') out.append(',');
            Key key = item.getKey();
            Entry e = item.getValue();
            out.append("{\"key\":"); SnapshotJson.writeString(out, key.key);
            out.append(",\"kind\":"); SnapshotJson.writeString(out, key.kind.toString());
            out.append(",\"variant\":"); SnapshotJson.writeString(out, key.variant);
            out.append(",\"history\":{"); anchor(out, e.history.anchor, nano);
            out.append(",\"attempts\":").append(e.history.attempts);
            for (Metric metric : METRICS) out.append(",\"").append(metric.wire).append("\":").append(e.history.get(metric));
            out.append("},\"cooling\":{"); anchor(out, e.cooling.anchor, nano);
            out.append(",\"level\":").append(e.cooling.level)
                    .append(",\"nominal_ms\":").append(e.cooling.nominal / Time.MILLISECOND)
                    .append(",\"remaining_ms\":").append(Math.max(0, Time.sub(e.cooling.until, nano)) / Time.MILLISECOND);
            if (!e.cooling.shortDrops.isEmpty()) {
                out.append(",\"short\":[");
                for (Anchor t : e.cooling.shortDrops) {
                    if (out.charAt(out.length() - 1) != '[') out.append(',');
                    out.append('{');
                    anchor(out, t, nano);
                    out.append('}');
                }
                out.append(']');
            }
            out.append("},\"rtt_seconds\":"); performance(out, e.rtt, nano);
            out.append(",\"rate_bytes_per_second\":"); performance(out, e.rate, nano);
            out.append('}');
        }
        return out.append("]}").toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void time(StringBuilder out, Instant time) { out.append('"').append(SnapshotJson.formatTime(time)).append('"'); }

    private static void anchor(StringBuilder out, Anchor anchor, long now) {
        out.append("\"at\":"); time(out, anchor.at);
        out.append(",\"age_ns\":").append(anchor.age(now));
    }

    private static void performance(StringBuilder out, Performance p, long now) {
        out.append('{');
        anchor(out, p.anchor, now);
        out.append(",\"value\":").append(p.value);
        if (p.validity > 0) out.append(",\"valid_for_ns\":").append(p.validity);
        out.append('}');
    }

    /** Validates the entire interchange document before updating the cache. */
    public void importSnapshot(byte[] bytes) {
        if (bytes.length > config.maxImportBytes()) throw invalid(SnapshotError.VALUE, "snapshot exceeds byte bound");
        SnapshotJson.Obj root;
        try {
            root = object(SnapshotJson.parse(StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString()));
        } catch (java.nio.charset.CharacterCodingException | IllegalArgumentException e) {
            throw invalid(SnapshotError.FORMAT, "invalid snapshot JSON");
        }
        if (integer(root.get("version")) != VERSION) throw invalid(SnapshotError.VERSION, "unsupported snapshot version");
        Instant exported = instant(root.get("exported_at"));
        if (exported.equals(UNSET)) throw invalid(SnapshotError.VALUE, "absent export time");
        var input = root.get("entries") == null ? List.<SnapshotJson.Value>of() : array(root.get("entries"));
        if (input.size() > config.maxImportEntries()) throw invalid(SnapshotError.VALUE, "snapshot exceeds entry bound");
        var decoded = new HashMap<Key, Entry>();
        for (var item : input) {
            var doc = object(item);
            String kindText = stringField(doc, "kind");
            CarrierKind kind = java.util.Arrays.stream(CarrierKind.values()).filter(k -> k.toString().equals(kindText))
                    .findFirst().orElseThrow(() -> invalid(SnapshotError.VALUE, "unknown kind"));
            var key = key(stringField(doc, "key"), kind, stringField(doc, "variant"));
            if (decoded.putIfAbsent(key, decode(doc, exported)) != null) throw invalid(SnapshotError.VALUE, "duplicate key");
        }
        merge(decoded, exported);
        signalChange();
    }

    private Entry decode(SnapshotJson.Obj doc, Instant exported) {
        var e = new Entry();
        var h = snapshotField(doc, "history");
        e.history.anchor = decodeAnchor(h, exported);
        e.history.attempts = count(h.get("attempts"));
        for (Metric metric : METRICS) e.history.add(metric, measure(h.get(metric.wire)));
        var c = snapshotField(doc, "cooling");
        e.cooling.anchor = decodeAnchor(c, exported);
        long nominal = count(c.get("nominal_ms")), wait = count(c.get("remaining_ms"));
        long max = config.backoff().max().toMillis();
        long waitMax = config.backoff().maximumWaitNanos() / Time.MILLISECOND;
        if (nominal > max || wait > waitMax) throw invalid(SnapshotError.VALUE, "invalid cooling bounds");
        e.cooling.level = count(c.get("level"));
        if (e.cooling.level != 0 && e.cooling.anchor.at.equals(UNSET))
            throw invalid(SnapshotError.VALUE, "cooldown level without timestamp");
        e.cooling.nominal = nominal * Time.MILLISECOND;
        e.cooling.until = wait * Time.MILLISECOND;
        if (c.get("short") != null) {
            var shortDrops = array(c.get("short"));
            if (shortDrops.size() > 3) throw invalid(SnapshotError.VALUE, "too many short drops");
            for (var drop : shortDrops) e.cooling.shortDrops.add(decodeAnchor(object(drop), exported));
        }
        e.rtt = decodePerformance(snapshotField(doc, "rtt_seconds"), exported);
        e.rate = decodePerformance(snapshotField(doc, "rate_bytes_per_second"), exported);
        return e;
    }

    private synchronized void merge(Map<Key, Entry> incoming, Instant exported) {
        long elapsed = wallAge(exported, now());
        long nano = clock.getAsLong();
        for (var item : incoming.entrySet()) {
            Entry s = item.getValue(), e = entries.computeIfAbsent(item.getKey(), ignored -> new Entry());
            if (s.history.anchor.newerThan(e.history.anchor)) {
                s.history.anchor = s.history.anchor.reanchor(nano, elapsed);
                e.history = s.history;
            }
            if (s.cooling.anchor.newerThan(e.cooling.anchor)) {
                s.cooling.anchor = s.cooling.anchor.reanchor(nano, elapsed);
                s.cooling.shortDrops.replaceAll(drop -> drop.reanchor(nano, elapsed));
                s.cooling.until = Time.add(nano, Math.max(0, s.cooling.until - elapsed));
                s.cooling.peerUntil = e.cooling.peerUntil;
                e.cooling = s.cooling;
                armHealth(item.getKey(), e);
            }
            if (s.rtt.anchor.newerThan(e.rtt.anchor)) e.rtt = s.rtt.reanchor(nano, elapsed);
            if (s.rate.anchor.newerThan(e.rate.anchor)) e.rate = s.rate.reanchor(nano, elapsed);
        }
    }

    private static Anchor decodeAnchor(SnapshotJson.Obj doc, Instant exported) {
        Instant at = instant(doc.get("at"));
        SnapshotJson.Value ageField = doc.get("age_ns");
        if (ageField == null) return wallAnchor(at, exported);
        long age = count(ageField);
        if (at.equals(UNSET) && age != 0) throw invalid(SnapshotError.VALUE, "age without timestamp");
        return new Anchor(at, 0, age);
    }

    private static Anchor wallAnchor(Instant at, Instant exported) {
        if (at.isAfter(exported)) throw invalid(SnapshotError.VALUE, "future timestamp without age");
        return new Anchor(at, 0, wallAge(at, exported));
    }

    private static Performance decodePerformance(SnapshotJson.Obj p, Instant exported) {
        return new Performance(decodeAnchor(p, exported), measure(p.get("value")), count(p.get("valid_for_ns")), null);
    }

    private static SnapshotJson.Obj snapshotField(SnapshotJson.Obj doc, String field) {
        var v = doc.get(field);
        return v == null ? new SnapshotJson.Obj(List.of()) : object(v);
    }

    private static SnapshotJson.Obj object(SnapshotJson.Value v) {
        if (v instanceof SnapshotJson.Obj o) return o;
        throw invalid(SnapshotError.FORMAT, "expected object");
    }

    private static List<SnapshotJson.Value> array(SnapshotJson.Value v) {
        if (v instanceof SnapshotJson.Arr(var values)) return values;
        throw invalid(SnapshotError.FORMAT, "expected array");
    }

    private static long integer(SnapshotJson.Value v) {
        if (v instanceof SnapshotJson.Int integer) return integer.value();
        throw invalid(SnapshotError.FORMAT, "expected integer");
    }

    private static String string(SnapshotJson.Value v) {
        if (v instanceof SnapshotJson.Str(String s)) return s;
        throw invalid(SnapshotError.FORMAT, "expected string");
    }

    private static String stringField(SnapshotJson.Obj doc, String field) {
        var v = doc.get(field);
        return v == null ? "" : string(v);
    }

    private static Instant instant(SnapshotJson.Value v) {
        if (v == null) return UNSET;
        Instant time = SnapshotJson.parseTime(string(v));
        if (time == null) throw invalid(SnapshotError.FORMAT, "invalid timestamp");
        return time;
    }

    private static long count(SnapshotJson.Value v) {
        long n = v == null ? 0 : integer(v);
        if (n < 0) throw invalid(SnapshotError.VALUE, "negative snapshot value");
        return n;
    }

    private static double measure(SnapshotJson.Value v) {
        double n = switch (v) {
            case SnapshotJson.Int value -> value.value();
            case SnapshotJson.Num value -> value.value();
            case null -> 0;
            default -> throw invalid(SnapshotError.FORMAT, "expected number");
        };
        if (!Double.isFinite(n) || n < 0) throw invalid(SnapshotError.VALUE, "invalid snapshot measurement");
        return n;
    }

    private static InvalidSnapshotException invalid(SnapshotError reason, String message) {
        return new InvalidSnapshotException(reason, message);
    }
}
