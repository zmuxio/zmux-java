package io.zmux;

import io.zmux.carrier.PipeIo;
import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.Time;
import io.zmux.internal.spi.AdapterPorts;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** Optional opener and target reconciler shared by the sessions in one host isolation domain. */
public final class SessionController implements AutoCloseable {
    /** A carrier registration. Identity is independent of transport kind and variant. */
    public record Candidate(String id, String cacheKey, CarrierKind kind, String variant, long tier, boolean standby,
                            boolean canStart, List<AttachmentSpec> attachments, CarrierOpener opener) {
        public Candidate {
            if (Objects.requireNonNull(id).isEmpty() || Objects.requireNonNull(cacheKey).isEmpty()
                    || tier < 0 || tier > io.zmux.internal.wire.VarInt.MAX) {
                throw new IllegalArgumentException("candidate has no identity or cache key");
            }
            Objects.requireNonNull(kind);
            Objects.requireNonNull(variant);
            Objects.requireNonNull(opener);
            attachments = List.copyOf(attachments);
        }

        List<Directions> registeredLayout() {
            return attachments.isEmpty() ? List.of(Directions.BOTH) : attachments.stream().map(AttachmentSpec::directions).toList();
        }

        MemberSpec member() { return new MemberSpec(id, attachments); }

        SessionTarget.Preferences preferences(SessionTarget.Quota quota) {
            var found = quota == null ? null : quota.preferences().get(id);
            var defaults = new SessionTarget.PathPreference(tier, standby, 1);
            return new SessionTarget.Preferences(found == null || found.send() == null ? defaults : found.send(),
                    found == null || found.receive() == null ? defaults : found.receive());
        }

        MemberSpec member(SessionTarget.Quota quota) {
            var prefs = preferences(quota);
            return new MemberSpec(id, attachments.stream().map(spec -> {
                var options = spec.options().toBuilder();
                if (spec.directions().sends()) {
                    var send = prefs.send();
                    options.sendPriority(send.tier()).sendStandby(send.standby()).weight(send.weight());
                }
                if (spec.directions().receives()) {
                    var receive = prefs.receive();
                    options.receivePriority(receive.tier()).receiveStandby(receive.standby());
                }
                return new AttachmentSpec(spec.directions(), options.build());
            }).toList());
        }
    }

    /** The rank used within each tier and standby level. */
    public enum Choice { STATIC, D160 }

    /** Bounds shared by the controller domain. A zero count selects the corresponding default. */
    public record Limits(int maximumPendingOwners, int maximumOpeners, int maximumCleanupAttachments,
                         Duration attemptTimeout) {
        /** The default domain and attempt limits. */
        private static final int DEFAULT_PENDING_OWNERS = 64;
        private static final int DEFAULT_OPENERS = 4;
        private static final int DEFAULT_CLEANUP_ATTACHMENTS = 8;
        public static final Limits DEFAULT = new Limits(DEFAULT_PENDING_OWNERS, DEFAULT_OPENERS,
                DEFAULT_CLEANUP_ATTACHMENTS, Duration.ofSeconds(10));

        public Limits {
            if (maximumPendingOwners < 0 || maximumOpeners < 0 || maximumCleanupAttachments < 0
                    || Objects.requireNonNull(attemptTimeout).isNegative() || attemptTimeout.isZero()) {
                throw new IllegalArgumentException("invalid controller limits");
            }
            if (maximumPendingOwners == 0) maximumPendingOwners = DEFAULT_PENDING_OWNERS;
            if (maximumOpeners == 0) maximumOpeners = DEFAULT_OPENERS;
            if (maximumCleanupAttachments == 0) maximumCleanupAttachments = DEFAULT_CLEANUP_ATTACHMENTS;
        }

        /** Copies these limits with a different opener concurrency bound. */
        public Limits withMaximumOpeners(int count) {
            return new Limits(maximumPendingOwners, count, maximumCleanupAttachments, attemptTimeout);
        }

        /** Copies these limits with a different attempt deadline. */
        public Limits withAttemptTimeout(Duration timeout) {
            return new Limits(maximumPendingOwners, maximumOpeners, maximumCleanupAttachments, timeout);
        }
    }

    /** Fixed registrations, execution limits and cache for one isolation domain. */
    public record Options(String domain, List<Candidate> candidates, Choice choice, Limits limits,
                          CarrierCache cache, ClockDomain clockDomain, Map<String, Long> dialAfter, double upgradeRatio) {
        public Options(String domain, List<Candidate> candidates, Choice choice, Limits limits,
                       CarrierCache cache, ClockDomain clockDomain, Map<String, Long> dialAfter) {
            this(domain, candidates, choice, limits, cache, clockDomain, dialAfter, 1.25);
        }

        public Options {
            if (!Double.isFinite(upgradeRatio) || upgradeRatio <= 1) throw new IllegalArgumentException("invalid upgrade ratio");
            if (Objects.requireNonNull(domain).isEmpty()) throw new IllegalArgumentException("empty controller domain");
            candidates = List.copyOf(candidates);
            dialAfter = dialAfter == null ? Map.of() : Map.copyOf(dialAfter);
            Objects.requireNonNull(choice);
            Objects.requireNonNull(limits);
            if (clockDomain == null) clockDomain = ClockDomain.system();
        }
    }

    /** A started session and its control binding, when multipath was negotiated. */
    public record Opened(ZmuxSession session, SessionControl control) {}


    private static final Map<String, SessionController> DOMAINS = new ConcurrentHashMap<>();
    private final Options options;
    final CarrierCache cache;
    private final Runnable removeCacheListener;
    private final LinkedHashMap<String, Candidate> candidates = new LinkedHashMap<>();
    private final Map<String, Integer> order = new HashMap<>();
    private final Object budget = new Object();
    private final LinkedHashMap<Owner, Demand> pending = new LinkedHashMap<>();
    private int openers;
    private int cleanup;
    private final Map<ZmuxSession, SessionControl> controls = new ConcurrentHashMap<>();
    private final java.util.Set<BootstrapRace> bootstraps = ConcurrentHashMap.newKeySet();
    private final CompletableFuture<Void> done = new CompletableFuture<>();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final java.util.concurrent.atomic.AtomicReference<CompletableFuture<Void>> availability =
            new java.util.concurrent.atomic.AtomicReference<>(new CompletableFuture<>());

    public SessionController(Options options) {
        this.options = Objects.requireNonNull(options);
        cache = options.cache() == null ? new CarrierCache(CarrierCache.Config.DEFAULT, clock()) : options.cache();
        if (cache.domain() != clock()) throw new IllegalArgumentException("cache clock domain differs from controller");
        for (Candidate candidate : options.candidates()) {
            if (candidate.attachments().size() > options.limits().maximumCleanupAttachments()) {
                throw new IllegalArgumentException("candidate layout exceeds domain cleanup capacity");
            }
            if (candidates.putIfAbsent(candidate.id(), candidate) != null) {
                throw new IllegalArgumentException("duplicate candidate ID");
            }
            order.put(candidate.id(), order.size());
        }
        if (!candidates.keySet().containsAll(options.dialAfter().keySet())) throw new IllegalArgumentException("unknown DialAfter candidate");
        removeCacheListener = cache.observeChanges(this::signalAvailability);
        if (DOMAINS.putIfAbsent(options.domain(), this) != null) {
            removeCacheListener.run();
            throw new IllegalStateException("controller already exists for domain");
        }
    }

    /** Starts a session using an eligible registered candidate. */
    public Opened open(ZmuxConfig config, SessionTarget target) throws IOException {
        if (io.zmux.internal.loop.SessionHost.onHostThread()) throw new IllegalStateException("bootstrap on a zmux event-loop thread");
        if (config.clockDomain != null && config.clockDomain() != clock()) {
            throw new IllegalArgumentException("session clock domain differs from controller");
        }
        var role = Zmux.core(Role.CLIENT);
        config.checkWithoutListener(role);
        BootstrapRace race;
        Owner owner = new Owner();
        synchronized (this) {
            validate(target);
            owner.enqueue();
            race = new BootstrapRace(this, owner, config, ranked(candidates.keySet()).stream()
                    .filter(c -> includes(config.sessionPolicy().topology().getOrDefault(c.id(), Directions.BOTH), Directions.BOTH))
                    .toList());
            bootstraps.add(race);
        }
        boolean transferred = false;
        try (race) {
            race.start();
            // The returned session is handed to the caller after bootstrap publishes it.
            //noinspection resource
            var opened = bootstrap(config, target, race, role);
            // The opened result carries the control to the caller.
            //noinspection resource
            transferred = opened.control() != null;
            return opened;
        } finally {
            if (!transferred) owner.withdraw();
            bootstraps.remove(race);
            finishClose();
        }
    }

    private Opened bootstrap(ZmuxConfig config, SessionTarget target, BootstrapRace race,
                             io.zmux.internal.core.CoreConfig.Role role) throws IOException {
        var tries = new ArrayList<CandidateResult>();
        for (BootstrapRace.Prepared prepared; (prepared = race.take()) != null;) {
            var opening = prepared.opening();
            var candidate = opening.candidate;
            ZmuxSession session = null;
            PipeIo handedOff = null;
            boolean published = false;
            try {
                var resources = prepared.resources();
                if (resources.failure() != null) throw Errors.failure(resources.failure());
                if (opening.expired()) throw new java.net.SocketTimeoutException("bootstrap attempt deadline expired");
                if (resources.carriers().size() != 1
                        || resources.carriers().getFirst().reader() == null || resources.carriers().getFirst().writer() == null) {
                    throw new IllegalArgumentException("bootstrap needs one duplex carrier");
                }
                handedOff = resources.carriers().getFirst().withCarrier(candidate.kind()).withVariant(candidate.variant());
                var started = Zmux.startExternal(handedOff, config, role, role, config.hooks.withClock(clock()));
                session = Zmux.started(started, config.carrierCredential().orElse(null));
                var starting = session;
                try (var _ = opening.setup.onCancel(() -> starting.abort(0, ""))) {
                    session.awaitEstablished(Long.MAX_VALUE);
                }
                if (opening.expired()) throw new java.net.SocketTimeoutException("bootstrap attempt deadline expired");
                session.bindBootstrapAttempt(opening.id);
                // The session owns the bound control, which is also published in the opened result.
                //noinspection resource
                SessionControl control = bind(session, target, candidate.id(), true, race.owner, null);
                race.close();
                if (control != null) control.start();
                if (closing.get()) throw new java.io.InterruptedIOException("controller closed during bootstrap");
                session.closed().whenComplete((value, error) -> opening.lifetime.close());
                report(candidate, CarrierCache.Outcome.USABLE);
                opening.lease.terminal(true, false);
                tries.add(new CandidateResult(candidate.kind(), CandidateOutcome.USABLE, false, null, candidate.variant()));
                session.reportCarrier(tries, candidate.kind(), candidate.variant());
                opening.lease.cleanup.close();
                published = true;
                return new Opened(session, control);
            } catch (IOException | RuntimeException error) {
                if (error instanceof NotNegotiatedException notNegotiated) throw notNegotiated;
                if (error instanceof QueueFullException full) throw full;
                if (closing.get() || Thread.currentThread().isInterrupted()) throw error;
                CandidateOutcome outcome = CandidateOutcome.outcomeOf(error);
                tries.add(new CandidateResult(candidate.kind(), outcome, false, error, candidate.variant()));
                var retry = CarrierRetryException.find(error);
                if (retry != null) retryAfter(candidate, retry.retryAfter());
                if (opening.lease.started() && !(error instanceof IllegalArgumentException)) report(candidate, CarrierCache.Outcome.FAILED);
                race.releaseStagger();
            } finally {
                race.finished(opening);
                opening.lease.finished();
                opening.endSetup(!published);
                if (handedOff == null) Thread.ofVirtual().name("zmux-bootstrap-discard").start(prepared::discard);
                else if (!published) {
                    if (session == null) {
                        AdapterPorts.released(handedOff).whenComplete((value, error) -> opening.lease.cleanup.close());
                    }
                    else {
                        new ZmuxPipe(session, 0).closed().whenComplete((value, error) -> opening.lease.cleanup.close());
                        session.abort(0, "");
                    }
                }
            }
        }
        if (tries.isEmpty()) {
            var blocked = race.dialAfter();
            if (blocked != null) throw blocked;
            throw new IOException("no eligible bootstrap candidate completed");
        }
        throw new CarrierChoiceException(tries);
    }

    /** Binds a negotiated session to one target reconciler. */
    public SessionControl manage(ZmuxSession session, SessionTarget target) throws IOException {
        return manage(session, target, "");
    }

    /**
     * Binds an established multipath session to a reconciler. A nonempty bootstrap candidate
     * ID attributes the existing member on pipe 0 before the target is reconciled, preserving
     * the member, its attachments and accounting.
     *
     * @throws NoEligibleCandidateException if the bootstrap candidate ID is unregistered
     * @throws PipeGoneException if the bootstrap pipe is gone
     * @throws IllegalArgumentException if the bootstrap carrier differs in kind or variant, or
     *         the bootstrap member is unavailable or already attributed to another candidate
     */
    public SessionControl manage(ZmuxSession session, SessionTarget target, String bootstrapCandidate) throws IOException {
        return bind(session, target, Objects.requireNonNull(bootstrapCandidate), false, new Owner(), null);
    }

    /** Replaces the exact current binding on its existing session. */
    public SessionControl takeOver(SessionControl previous, SessionTarget target) throws IOException {
        return takeOver(previous, target, "");
    }

    /** Optionally attributes an existing confirmed bootstrap member during the same commit. */
    public SessionControl takeOver(SessionControl previous, SessionTarget target, String bootstrapCandidate) throws IOException {
        return previous.replaceWith(this, target, Objects.requireNonNull(bootstrapCandidate));
    }

    /** The handle used for a release or takeover is no longer current. */
    public static final class StaleDecisionException extends IllegalStateException {
        StaleDecisionException() { super("stale quality decision"); }
    }

    /** The binding no longer owns the session. */
    public static final class StaleBindingException extends IllegalStateException {
        StaleBindingException() { super("stale control binding"); }
    }

    private void checkIdentity(io.zmux.internal.core.Session core) {
        for (var candidate : candidates.values()) {
            core.checkCandidateIdentity(candidate.id(), CapacityViews.identity(key(candidate), candidate.registeredLayout()));
        }
    }

    void checkEnvelope(SessionTarget target, CapacitySnapshot capacity) {
        long slots = 0;
        for (var group : target.groups()) for (var quota : group.quotas()) {
            int size = quota.candidates().stream().map(candidates::get).mapToInt(c -> c.attachments().size()).min().orElse(0);
            slots += (long) quota.count() * size;
            if (slots > capacity.envelope()) throw new IllegalArgumentException("target exceeds protocol envelope");
        }
    }

    SessionControl bind(ZmuxSession session, SessionTarget target, String bootstrapCandidate, boolean opened, Owner owner, SessionControl previous) throws IOException {
        session.driver.checkBlocking();
        if (session.driver.clockDomain() != clock()) throw new IllegalArgumentException("session clock domain differs from controller");
        synchronized (this) {
            validate(target);
            boolean singlePathAllowed = opened && target.groups().stream().flatMap(group -> group.quotas().stream())
                    .noneMatch(quota -> quota.count() != 0);
            if (!opened) owner.enqueue();
            boolean bound = false;
            try {
                // The owner serializes bootstrap attribution with competing bindings, retirement and close.
                var control = (SessionControl) session.run(op -> {
                    var core = session.core;
                    if (core.error() != null) return CoreException.session(core.error());
                    boolean multipath = core.features().multipath();
                    if (!multipath && !singlePathAllowed) return new NotNegotiatedException("multipath");
                    if (session.control.get() != previous) return previous == null
                            ? new IllegalStateException("session already has a controller") : new StaleBindingException();
                    var capacity = session.capacityOnOwner();
                    if (previous != null && previous.controller() == this) {
                        return new IllegalArgumentException("cannot take over own binding");
                    }
                    try { checkIdentity(core); }
                    catch (IllegalArgumentException incompatible) { return incompatible; }
                    try { checkEnvelope(target, capacity); }
                    catch (IllegalArgumentException invalid) { return invalid; }
                    for (var intent : target.replace()) for (var input : intent.evidence()) {
                        if (previous == null || input.binding() != previous.generation()
                                || input.targetRevision() != previous.revision() || !session.validQualityInput(input))
                            return new StaleDecisionException();
                    }
                    if (session.controlGeneration == Long.MAX_VALUE) return new IllegalStateException("binding generation exhausted");
                    long generation = session.controlGeneration + 1;

                    if (!bootstrapCandidate.isEmpty()) {
                        var candidate = candidates.get(bootstrapCandidate);
                        if (candidate == null) return new NoEligibleCandidateException(bootstrapCandidate);
                        try {
                            core.bindBootstrapCandidate(candidate.id(), CapacityViews.identity(key(candidate), candidate.registeredLayout()));
                        } catch (IllegalArgumentException | PipeGoneException error) {
                            return error;
                        }
                    }
                    if (!multipath) return null;
                    var binding = new SessionControl(this, owner, session, target, session.capacityOnOwner(), generation,
                            previous == null ? null : previous.generation());
                    session.controlGeneration = generation;
                    core.attend(1);
                    binding.install(previous);
                    session.control.set(binding);
                    core.targetChanged(generation, target.revision(), true, previous == null ? null : previous.generation());
                    return binding;
                });
                if (control != null) {
                    controls.put(session, control);
                    if (!opened) control.start();
                    bound = true;
                }
                return control;
            } finally {
                if (!bound) owner.withdraw();
            }
        }
    }

    void unbind(ZmuxSession session, SessionControl control) {
        controls.remove(session, control);
        session.driver.post(() -> {
            if (session.control.get() == control) {
                control.detach();
                session.control.set(null);
            }
            session.core.attend(-1);
        });
        finishClose();
    }

    void validate(SessionTarget target) {
        if (closing.get()) throw new IllegalStateException("controller closed");
        var groups = new java.util.HashSet<String>();
        var quotas = new java.util.HashSet<String>();
        for (var group : target.groups()) {
            if (!groups.add(group.id())) throw new IllegalArgumentException("duplicate group ID");
            quotas.clear();
            for (var quota : group.quotas()) {
                if (!quotas.add(quota.id())) throw new IllegalArgumentException("duplicate quota ID");
                for (String id : quota.candidates()) {
                    var candidate = candidates.get(id);
                    if (candidate == null || candidate.attachments().isEmpty()
                            || !includes(candidate.member().directions(), quota.directions())) {
                        throw new IllegalArgumentException("candidate does not satisfy quota " + quota.id());
                    }
                }
            }
        }
    }

    List<Candidate> upgrades(Candidate current, SessionTarget.Quota quota, CapacitySnapshot capacity) {
        if (options.choice() != Choice.D160) return List.of();
        long now = clock().now();
        double floor = cache.scoreAt(key(current), now).reliability() * options.upgradeRatio();
        return ranked(quota.candidates(), false, now).stream().filter(candidate -> {
            if (candidate == current || candidate.standby() || candidate.tier() > current.tier()
                    || !includes(capacity.topology().getOrDefault(candidate.id(), Directions.BOTH), candidate.member().directions())) return false;
            var score = cache.scoreAt(key(candidate), now);
            return score.attempts() >= 3 && retryDelay(candidate) == 0 && score.reliability() >= floor;
        }).toList();
    }

    static CarrierCache.Key key(Candidate candidate) {
        return new CarrierCache.Key(candidate.cacheKey(), candidate.kind(), candidate.variant());
    }

    java.util.Collection<Candidate> candidates() { return candidates.values(); }

    Candidate candidate(String id) { return candidates.get(id); }

    static boolean includes(Directions actual, Directions needed) {
        return (!needed.sends() || actual.sends()) && (!needed.receives() || actual.receives());
    }

    List<Candidate> ranked(java.util.Collection<String> ids) {
        return ranked(ids, false, clock().now());
    }

    List<Candidate> ranked(java.util.Collection<String> ids, boolean recovery, long now) {
        record Rank(Candidate candidate, int stage, long waitNanos, double scoreBucket) {}
        return ids.stream().map(candidates::get).filter(Objects::nonNull).map(candidate -> {
            var sample = cache.scoreAt(key(candidate), now);
            long wait = recovery ? sample.cooling().map(Duration::toNanos).orElse(0L) : 0;
            int stage = candidate.standby() ? 2 : wait > 0 ? 1 : 0;
            double score = recovery || options.choice() == Choice.D160 ? sample.score() : 0;
            double rounded = score < 0x1.0p23 ? Math.rint(score * 1e9) / 1e9 + 0.0 : score;
            return new Rank(candidate, stage, stage == 1 ? wait : 0, rounded);
        }).sorted(java.util.Comparator.comparingInt(Rank::stage)
                .thenComparingLong(rank -> rank.candidate().tier())
                .thenComparingLong(Rank::waitNanos)
                .thenComparing(Rank::scoreBucket, java.util.Comparator.reverseOrder())
                .thenComparingInt(rank -> order.get(rank.candidate().id())))
                .map(Rank::candidate).toList();
    }

    long retryDelay(Candidate c) { return cache.retryDelay(c.cacheKey(), c.kind(), c.variant()); }


    void report(Candidate c, CarrierCache.Outcome outcome) { cache.report(c.cacheKey(), c.kind(), c.variant(), outcome); }

    Duration timeout() { return options.limits().attemptTimeout(); }

    ClockDomain clock() { return options.clockDomain(); }

    Map<String, Long> initialDialAfter() { return options.dialAfter(); }

    CarrierCache.Suppression admission(Candidate c, boolean recovery) {
        return cache.admission(c.cacheKey(), c.kind(), c.variant(), recovery);
    }

    private void suppress(Candidate c, CarrierCache.Suppression reason) {
        cache.suppress(c.cacheKey(), c.kind(), c.variant(), reason);
    }

    long recoveryDelay(Candidate c) { return cache.recoveryDelay(c.cacheKey(), c.kind(), c.variant()); }

    void reportFault(Candidate c, CarrierCache.Outcome outcome, long at, io.zmux.carrier.CarrierFaultException.Episode episode) {
        cache.reportFault(c.cacheKey(), c.kind(), c.variant(), outcome, clock().wallTime(at), episode);
    }

    void retryAfter(Candidate c, Duration retry) {
        cache.retryAfter(c.cacheKey(), c.kind(), c.variant(), retry);
    }

    void report(Candidate c, CarrierCache.Outcome outcome, long at, long duration) {
        cache.report(c.cacheKey(), c.kind(), c.variant(), outcome,
                new CarrierCache.Observation(clock().wallTime(at), Duration.ofNanos(duration), Duration.ZERO, 0));
    }

    /** Synchronous rejection of a new pending owner in a saturated domain. */
    public static final class QueueFullException extends IllegalStateException {
        QueueFullException() { super("controller pending owner queue full"); }
    }

    /** Execution and cleanup currently charged to the binding and to its controller domain. */
    public record Occupancy(int bindingOpeners, int bindingCleanup, int domainOpeners, int domainCleanup) {}

    private record Demand(boolean opener, int cleanup) {}
    record Admission(Lease lease, SessionControl.Blocking blocked) {
        Admission { Objects.requireNonNull(blocked); }
    }
    record BlockedStart(Candidate candidate, CarrierCache.Suppression reason, Long retry) {}

    final class Owner {
        private final java.util.Set<CarrierCache.Key> suppressed = new java.util.HashSet<>();
        private int heldOpeners, heldCleanup;

        Occupancy occupancy() {
            synchronized (budget) { return new Occupancy(heldOpeners, heldCleanup, openers, cleanup); }
        }

        void inheritOpportunities(Owner previous, java.util.Set<Candidate> pending) {
            if (previous.cache() != cache) return;
            var inherited = previous.suppressedSnapshot();
            synchronized (budget) {
                suppressed.addAll(inherited);
                retainOpportunities(pending);
            }
        }

        void retainOpportunities(java.util.Set<Candidate> pending) {
            var eligible = pending.stream().map(SessionController::key).collect(java.util.stream.Collectors.toSet());
            synchronized (budget) {
                suppressed.removeIf(key -> !recovery.contains(key) && !eligible.contains(key));
            }
        }

        private CarrierCache cache() { return SessionController.this.cache; }
        private java.util.Set<CarrierCache.Key> suppressedSnapshot() {
            synchronized (budget) { return java.util.Set.copyOf(suppressed); }
        }

        private final java.util.Set<CarrierCache.Key> recovery = new java.util.HashSet<>();

        void endOpportunities() {
            synchronized (budget) { suppressed.clear(); }
        }

        void recoveryDemand(java.util.Set<Candidate> next) {
            synchronized (budget) {
                var wanted = new java.util.HashSet<CarrierCache.Key>();
                for (var candidate : next) wanted.add(key(candidate));
                for (var key : wanted) if (recovery.add(key)) cache.recoveryDemand(key, this, true);
                recovery.removeIf(key -> {
                    if (wanted.contains(key)) return false;
                    suppressed.remove(key);
                    cache.recoveryDemand(key, this, false);
                    return true;
                });
            }
        }

        void suppressDue(BlockedStart start) {
            synchronized (budget) {
                var key = key(start.candidate());
                if (recovery.contains(key) && !cache.recoveryDue(key)) return;
                if (suppressed.add(key)) suppress(start.candidate(), start.reason());
            }
        }

        private Admission deny(Candidate candidate, SessionControl.Blocking block) {
            if (candidate != null) suppressDue(new BlockedStart(candidate, CarrierCache.Suppression.CAPACITY, null));
            return new Admission(null, block);
        }

        private Admission retry(Candidate candidate, boolean recovery, java.util.function.LongSupplier dialAfter,
                                CarrierCache.Suppression reason) {
            long now = clock().now();
            long delay = recovery ? recoveryDelay(candidate) : retryDelay(candidate);
            Long next = delay > 0 ? Time.add(now, delay) : null;
            if (reason == CarrierCache.Suppression.DIAL_AFTER) next = dialAfter.getAsLong();
            blocked();
            suppressDue(new BlockedStart(candidate, reason, next));
            return new Admission(null, reason == CarrierCache.Suppression.DIAL_AFTER
                    ? SessionControl.Blocking.DIAL_AFTER : SessionControl.Blocking.NO_ELIGIBLE_CANDIDATE);
        }

        void enqueue() {
            synchronized (budget) {
                if (closing.get()) throw new IllegalStateException("controller closed");
                if (pending.containsKey(this)) return;
                if (pending.size() == options.limits().maximumPendingOwners()) throw new QueueFullException();
                pending.put(this, null);
            }
        }

        void blocked() {
            synchronized (budget) {
                if (pending.get(this) == null) return;
                pending.remove(this);
                pending.put(this, null);
            }
            signalAvailability();
        }

        void withdraw() {
            synchronized (budget) {
                if (!pending.containsKey(this)) return;
                pending.remove(this);
            }
            signalAvailability();
        }

        Admission acquire(Candidate candidate, boolean recovery, int attachments, boolean keepPending,
                          java.util.function.LongSupplier dialAfter) {
            synchronized (budget) {
                if (closing.get()) return new Admission(null, SessionControl.Blocking.OWNER_CLOSED);
                if (attachments > options.limits().maximumCleanupAttachments()) {
                    return deny(candidate, SessionControl.Blocking.CLEANUP);
                }
                try { enqueue(); }
                catch (QueueFullException full) { return deny(candidate, SessionControl.Blocking.QUEUE_FULL); }
                var demand = new Demand(candidate != null, attachments);
                if (demand.opener()) {
                    var reason = clock().now() < dialAfter.getAsLong() ? CarrierCache.Suppression.DIAL_AFTER
                            : admission(candidate, recovery);
                    if (reason != null) return retry(candidate, recovery, dialAfter, reason);
                }
                var previous = pending.put(this, demand);
                if (!demand.equals(previous)) signalAvailability();
                int freeOpeners = options.limits().maximumOpeners() - openers;
                int freeCleanup = options.limits().maximumCleanupAttachments() - cleanup;
                if (demand.opener() && freeOpeners == 0) return deny(candidate, SessionControl.Blocking.EXECUTION);
                if (attachments > freeCleanup) return deny(candidate, SessionControl.Blocking.CLEANUP);
                boolean openerPriority = false, cleanupPriority = false;
                for (var entry : pending.entrySet()) {
                    if (entry.getKey() == this) break;
                    var earlier = entry.getValue();
                    if (earlier == null || (earlier.opener() && openerPriority) || (earlier.cleanup() != 0 && cleanupPriority))
                        continue;
                    openerPriority |= earlier.opener();
                    cleanupPriority |= earlier.cleanup() != 0;
                }
                if (attachments != 0 && cleanupPriority) return deny(candidate, SessionControl.Blocking.CLEANUP);
                if (demand.opener() && openerPriority) return deny(candidate, SessionControl.Blocking.EXECUTION);
                CarrierCache.Attempt token = null;
                if (demand.opener()) {
                    var reserved = cache.acquire(candidate.cacheKey(), candidate.kind(), candidate.variant(), recovery, dialAfter);
                    if (reserved.blocked() != null) return retry(candidate, recovery, dialAfter, reserved.blocked());
                    token = reserved.attempt();
                }
                pending.remove(this);
                if (keepPending) pending.put(this, null);
                if (demand.opener()) {
                    openers++;
                    heldOpeners++;
                }
                cleanup += attachments;
                heldCleanup += attachments;
                signalAvailability();
                return new Admission(new Lease(this, token, candidate, attachments), SessionControl.Blocking.NONE);
            }
        }
    }

    final class Cleanup implements AutoCloseable {
        private final Owner owner;
        private int remaining;

        Cleanup(Owner owner, int remaining) { this.owner = owner; this.remaining = remaining; }

        void releaseOne() { release(false); }

        @Override public void close() { release(true); }

        private void release(boolean all) {
            synchronized (budget) {
                if (remaining == 0) return;
                int count = all ? remaining : 1;
                remaining -= count;
                cleanup -= count;
                owner.heldCleanup -= count;
            }
            signalAvailability();
            finishClose();
        }
    }

    final class Lease implements AutoCloseable {
        private final Owner owner;
        private final Candidate candidate;
        private final CarrierCache.Attempt token;
        final Cleanup cleanup;
        private final AtomicBoolean returned = new AtomicBoolean();
        private final AtomicBoolean finished = new AtomicBoolean();
        private final AtomicBoolean handshake = new AtomicBoolean();

        Lease(Owner owner, CarrierCache.Attempt token, Candidate candidate, int attachments) {
            this.owner = owner;
            this.candidate = candidate;
            this.token = token;
            cleanup = new Cleanup(owner, attachments);
        }

        boolean start() {
            if (closing.get()) {
                suppress(CarrierCache.Suppression.STALE);
                return false;
            }
            var blocked = token.startReason();
            if (blocked != null) {
                if (blocked != CarrierCache.Suppression.RECOVERY_WAIT) suppress(blocked);
                return false;
            }
            synchronized (budget) { owner.suppressed.remove(key()); }
            return true;
        }

        long blockedUntil() { return token.blockedUntil(); }

        CarrierCache.Key key() { return SessionController.key(candidate); }

        void suppress(CarrierCache.Suppression reason) {
            owner.suppressDue(new BlockedStart(candidate, reason, null));
        }

        boolean started() { return token.started(); }

        void openerStarted() { signalAvailability(); }

        void report(CarrierCache.Outcome outcome, long at) {
            report(outcome, at, true);
        }

        private void report(CarrierCache.Outcome outcome, long at, boolean cooldown) {
            if (handshake.compareAndSet(false, true)) token.report(outcome, at, cooldown);
        }

        void terminal(boolean confirmed, boolean candidateFailure) { token.terminal(confirmed, candidateFailure, clock().now()); }

        void settle(Attempt.Result result, boolean replied, boolean faultReported, long at) {
            if (!started()) {
                close();
                return;
            }
            var retry = CarrierRetryException.find(result.error());
            if (retry != null) retryAfter(candidate, retry.retryAfter());
            boolean confirmed = result.disposition() == Attempt.Disposition.CONFIRMED;
            boolean failed = SessionControl.candidateFailed(result);
            var settled = token.terminal(confirmed, failed, at);
            if (failed) {
                if (replied && !faultReported) token.cooldownOnly(at);
                else if (!replied) report(CarrierCache.Outcome.FAILED, at, !faultReported);
            }
            settled.whenComplete((_, error) -> finished());
        }

        void hostReturned() {
            if (token == null || !returned.compareAndSet(false, true)) return;
            synchronized (budget) { openers--; owner.heldOpeners--; }
            if (finished.get()) token.close();
            signalAvailability();
            finishClose();
        }

        void finished() {
            if (token == null || !finished.compareAndSet(false, true)) return;
            if (returned.get()) token.close();
        }

        @Override public void close() {
            finished();
            hostReturned();
            cleanup.close();
        }
    }

    void scheduleChanged() { signalAvailability(); }

    private void signalAvailability() {
        availability.getAndSet(new CompletableFuture<>()).complete(null);
    }

    CompletableFuture<Void> available() { return availability.get(); }

    /** Observes the lifetime of the controller. */
    public CompletableFuture<Void> done() { return done.copy(); }

    @Override public void close() {
        synchronized (this) {
            if (!closing.compareAndSet(false, true)) return;
            removeCacheListener.run();
            synchronized (budget) { pending.clear(); }
        }
        signalAvailability();
        controls.values().forEach(SessionControl::close);
        bootstraps.forEach(BootstrapRace::close);
        finishClose();
    }

    private void finishClose() {
        if (!closing.get()) return;
        synchronized (budget) {
            if (openers != 0 || cleanup != 0) return;
        }
        if (controls.isEmpty() && bootstraps.isEmpty()) {
            DOMAINS.remove(options.domain(), this);
            done.complete(null);
        }
    }
}
