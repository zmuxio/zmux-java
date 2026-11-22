package io.zmux.internal.loop;

import io.zmux.internal.spi.AttachmentRelease;

import io.zmux.AdmissionVerdict;
import io.zmux.Admission;
import io.zmux.internal.core.CarrierAdmission;
import io.zmux.internal.core.Action;
import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.ReadBuf;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.Time;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.VarInt;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLEngine;

/**
 * The listener side of carriers: a new carrier is classified by
 * its first record on the loop that accepted it, then starts a session there, or moves to
 * the loop of the session it joins. Sessions are found by locator. Thread-safe: the
 * admission state is guarded by this hub, the locator map is concurrent, and a carrier is
 * owned by exactly one loop at a time (a move hands it over through that loop's queue).
 */
public final class ListenerHub {
    /** What the listener does with the sessions its carriers start. */
    public interface Owner {
        /** A responder configuration for a new session. */
        CoreConfig config();

        DriverHooks hooks();

        /** Logs a warning of the listener itself; a failing logger is ignored. */
        default void log(System.Logger.Level level, String message, Throwable error) { hooks().log(level, message, error); }

        /** The configuration and hooks of a new session on this carrier; an exception closes it, counted rejected. */
        default Chosen choose(Admission.CarrierInfo info, Object admitted) { return new Chosen(config(), hooks(), null); }

        /** A session was started on its loop (on that loop, before its first round). */
        void started(SessionRuntime d, String applicationProtocol, io.zmux.CarrierCredential credential, CarrierSetup setup);
    }

    /** A new session's configuration, and the credential its dialled pipes present (or null). */
    public record Chosen(CoreConfig config, DriverHooks hooks, io.zmux.CarrierCredential credential) {}

    /** What an accepted carrier carries on: the admission's value and its label of the carrier. */
    private record Admitted(Object value, String variant) {
        static final Admitted NONE = new Admitted(null, "");
    }

    private static final long MAX_FIRST_RECORD = 65535;

    private final Owner owner;
    private final MemoryBudget budget;

    /**
     * The listener's outcome counts: each pipe input handed to the listener (a carrier, a lane,
     * or the two halves of a split pipe once paired) counts once, when its outcome is decided, in sessions, joins,
     * rejected, refused or timedOut; setupFailures counts connections that never became a pipe input.
     */
    public static final class Counts {
        private final java.util.concurrent.atomic.AtomicLong sessions = new java.util.concurrent.atomic.AtomicLong(),
                joins = new java.util.concurrent.atomic.AtomicLong(), setupFailures = new java.util.concurrent.atomic.AtomicLong(),
                rejected = new java.util.concurrent.atomic.AtomicLong(), refused = new java.util.concurrent.atomic.AtomicLong(),
                timedOut = new java.util.concurrent.atomic.AtomicLong();
        /** The admission's outcomes: each carrier past its source slot once. */
        final java.util.concurrent.atomic.AtomicLong accepted = new java.util.concurrent.atomic.AtomicLong(),
                fallback = new java.util.concurrent.atomic.AtomicLong(), takeover = new java.util.concurrent.atomic.AtomicLong(),
                stalled = new java.util.concurrent.atomic.AtomicLong(),
                closed = new java.util.concurrent.atomic.AtomicLong(), stallOverflow = new java.util.concurrent.atomic.AtomicLong(),
                tooLarge = new java.util.concurrent.atomic.AtomicLong(),
                peekTimedOut = new java.util.concurrent.atomic.AtomicLong(), handshakeFailed = new java.util.concurrent.atomic.AtomicLong();

        private static final class VariantCounts {
            long accepted, fallback, takeover, stalled, closed;

            Admission.VariantStat snapshot(String name, boolean overflow) {
                return new Admission.VariantStat(name, overflow, accepted, fallback, takeover, stalled, closed);
            }
        }

        private final java.util.Map<String, VariantCounts> variants = new java.util.TreeMap<>();
        private VariantCounts variantOverflow;

        private synchronized void variant(String name, AdmissionVerdict verdict) {
            if (name.isEmpty()) return;
            VariantCounts c = variants.get(name);
            if (c == null) {
                if (variants.size() < Admission.MAX_VARIANTS) {
                    c = new VariantCounts();
                    variants.put(name, c);
                } else {
                    if (variantOverflow == null) variantOverflow = new VariantCounts();
                    c = variantOverflow;
                }
            }
            switch (verdict) {
                case ACCEPT -> c.accepted++;
                case FALLBACK -> c.fallback++;
                case TAKEOVER -> c.takeover++;
                case STALL -> c.stalled++;
                case CLOSE -> c.closed++;
                case MORE, DEFER -> throw new AssertionError(verdict);
            }
        }

        void decision(AdmissionVerdict verdict, String variant, boolean overflow) {
            java.util.concurrent.atomic.AtomicLong counter = switch (verdict) {
                case ACCEPT -> accepted;
                case FALLBACK -> fallback;
                case TAKEOVER -> takeover;
                case STALL -> overflow ? stallOverflow : stalled;
                case CLOSE -> closed;
                case MORE, DEFER -> throw new AssertionError(verdict);
            };
            counter.incrementAndGet();
            variant(variant, verdict);
        }

        /** A copy sorted by name, a bucket of further labels last. */
        public synchronized java.util.List<Admission.VariantStat> variantStats() {
            var out = new java.util.ArrayList<Admission.VariantStat>(variants.size() + 1);
            variants.forEach((n, c) -> out.add(c.snapshot(n, false)));
            if (variantOverflow != null) out.add(variantOverflow.snapshot("", true));
            return out;
        }

        void session() { sessions.incrementAndGet(); }
        void joined() { joins.incrementAndGet(); }
        /** A connection the listener could not make a pipe input of: a failed TLS handshake, HTTP or WebSocket upgrade. */
        public void setupFailed() { setupFailures.incrementAndGet(); }
        public void rejected() { rejected.incrementAndGet(); }
        public void refused() { refused.incrementAndGet(); }
        void timedOut() { timedOut.incrementAndGet(); }
        void count(Outcome o) {
            switch (o) {
                case REJECTED -> rejected();
                case REFUSED -> refused();
                case TIMED_OUT -> timedOut();
            }
        }

        public long sessions() { return sessions.get(); }
        public long joins() { return joins.get(); }
        public long setupFailures() { return setupFailures.get(); }
        public long rejectedCount() { return rejected.get(); }
        public long refusedCount() { return refused.get(); }
        public long timedOutCount() { return timedOut.get(); }
        public io.zmux.AdmissionStats admission() {
            return new io.zmux.AdmissionStats(accepted.get(), fallback.get(), takeover.get(), stalled.get(), closed.get(), stallOverflow.get(),
                    tooLarge.get(), peekTimedOut.get(), handshakeFailed.get());
        }
    }

    private final Counts counts = new Counts();

    /** How an input that started no session and joined nothing ended. */
    enum Outcome { REJECTED, REFUSED, TIMED_OUT }

    /** The listener's outcome counts; any thread. */
    public Counts counts() { return counts; }
    private final byte[] instanceId;
    private final AdmissionSlots admission;
    private final CarrierAdmission.SplitPairing<PairHalf> pairing = new CarrierAdmission.SplitPairing<>();
    /** Changed by {@link #limits}; read by carrier loops. */
    private volatile long firstRecordTimeout;
    private final ConcurrentHashMap<String, SessionRuntime> sessions = new ConcurrentHashMap<>();
    private final java.util.HashSet<Registration> registrations = new java.util.HashSet<>();
    private final SecureRandom random = new SecureRandom();
    private volatile boolean closed;
    /** Shut down: no new session; the registered ones still take their joins until they end. */
    private volatile String drainReason;
    private boolean shuttingDown;
    private Runnable drained;
    /** Changed by {@link #limits}; read by the management context and session loops. */
    private volatile int maxSuspended;
    private final ConcurrentHashMap<SessionRuntime, SessionRuntime.Resources> resources = new ConcurrentHashMap<>();
    private final SerialCallbacks management = new SerialCallbacks();
    private final java.util.concurrent.atomic.AtomicBoolean managementQueued = new java.util.concurrent.atomic.AtomicBoolean();
    private boolean evictionPending;

    /** Schedules resource policy work, coalesced; no periodic scan or application callback on a loop. */
    public void memoryChanged() { reconcile(); }

    private void reconcile() {
        if (!closed && managementQueued.compareAndSet(false, true)) management.execute(() -> {
            managementQueued.set(false);
            manage();
        });
    }

    private synchronized void manage() {
        if (closed || evictionPending) return;
        var suspended = resources.entrySet().stream()
                .filter(e -> e.getValue().state() == io.zmux.internal.core.SessionState.SUSPENDED).toList();
        boolean count = suspended.size() > maxSuspended;
        if (!count && (budget == null || !budget.pressure())) return;
        var chosen = count ? suspended.stream().min(java.util.Comparator.comparingLong(e -> e.getValue().suspendedAt()))
                : suspended.stream().max(java.util.Comparator.comparingLong(e -> e.getValue().bytes()));
        if (chosen.isEmpty()) return;
        SessionRuntime driver = chosen.get().getKey();
        evictionPending = true;
        boolean queued = driver.loop().tryExecute(eviction(driver));
        if (!queued) {
            resources.remove(driver); evictionPending = false; reconcile();
        }
    }

    /** The selected candidate may resume before its loop runs this decision. */
    Runnable eviction(SessionRuntime driver) {
        return () -> {
            // The hub's lock only for its own table: the session's core is called outside it.
            try {
                long n;
                synchronized (ListenerHub.this) {
                    n = resources.values().stream().filter(v -> v.state() == io.zmux.internal.core.SessionState.SUSPENDED).count();
                }
                if (driver.core().state() == io.zmux.internal.core.SessionState.SUSPENDED
                        && (n > maxSuspended || (budget != null && budget.pressure()))) {
                    driver.core().closeResumeTimeout();
                    driver.markDirty();
                }
                driver.core().memory().charge();
                driver.publishResources();
                driver.markDirty();
            } finally {
                // A core failure must not stop eviction for good.
                synchronized (ListenerHub.this) { evictionPending = false; }
            }
            reconcile();
        };
    }


    /**
     * @param budget the sessions' shared memory budget, or null
     * @param instanceId the leading bytes of every locator (validated by the caller, at most 8)
     * @param maxPendingPerSource 0: 8
     * @param maxPending the listener-wide bound of carriers before a session; 0: 1024, negative: unlimited
     * @param firstRecordTimeout 0: 2 s
     * @param sourcePrefixV4 the IPv4 source prefix, 1 to 32; 0: 32
     * @param sourcePrefixV6 the IPv6 source prefix, 1 to 128; 0: 64
     */
    public ListenerHub(Owner owner, MemoryBudget budget, byte[] instanceId, int maxPendingPerSource, int maxPending,
                       long firstRecordTimeout, int maxSuspended, int sourcePrefixV4, int sourcePrefixV6) {
        this.maxSuspended = maxSuspended;
        this.instanceId = instanceId.clone();
        this.owner = owner;
        this.budget = budget;
        this.admission = new AdmissionSlots(new CarrierAdmission(maxPendingPerSource, maxPending, sourcePrefixV4, sourcePrefixV6));
        this.firstRecordTimeout = firstRecordTimeout > 0 ? firstRecordTimeout : 2 * Time.SECOND;
    }

    /** Prepares all sessions owned by this endpoint before their transport parameters can be emitted. */
    public synchronized CoreConfig prepare(CoreConfig cfg) {
        if (closed || shuttingDown) throw new IllegalStateException("listener closed");
        cfg = cfg.toBuilder().budget(budget).epochs(cfg.epochs().withLocatorPrefix(instanceId)).build();
        if (cfg.local().has(TransportParams.MULTIPATH)) {
            byte[] loc = new byte[16]; random.nextBytes(loc);
            System.arraycopy(instanceId, 0, loc, 0, instanceId.length);
            cfg.local().locator = loc;
        }
        return cfg;
    }

    /** Registers routing and resource policies on the session owner, before bootstrap I/O starts. */
    public Runnable register(SessionRuntime driver) {
        var core = driver.core();
        String locator = core.localParams().has(TransportParams.MULTIPATH) ? HexFormat.of().formatHex(core.locator()) : null;
        Registration registration = new Registration(driver, locator);
        synchronized (this) {
            if (closed || shuttingDown) throw new IllegalStateException("listener closed");
            if (locator != null && sessions.containsKey(locator)) throw new IllegalStateException("duplicate session routing key");
            if (locator != null) sessions.put(locator, driver);
            resources.put(driver, driver.resources()); registrations.add(registration);
            driver.processLaneAdmission(this::observe);
        }
        driver.resourceListener(view -> {
            synchronized (ListenerHub.this) {
                if (closed || registration.closed.get()) { resources.remove(driver); return; }
                if (view.state() == io.zmux.internal.core.SessionState.CLOSED) resources.remove(driver);
                else resources.put(driver, view);
            }
            reconcile();
        });
        driver.locatorListener((loc, added) -> registration.locator(HexFormat.of().formatHex(loc), added));
        Runnable remove = driver.watchClosed(error -> registration.close());
        if (!registration.observer.compareAndSet(null, remove)) remove.run();
        return registration::close;
    }

    private final class Registration {
        final SessionRuntime driver; final String locator;
        final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
        final java.util.concurrent.atomic.AtomicReference<Runnable> observer = new java.util.concurrent.atomic.AtomicReference<>();
        Registration(SessionRuntime driver, String locator) { this.driver = driver; this.locator = locator; }
        /** The Locators of later epochs: joins find the session by every accepted one. */
        final java.util.Set<String> more = new java.util.HashSet<>();

        void locator(String loc, boolean added) {
            synchronized (ListenerHub.this) {
                if (closed.get()) return;
                if (added) {
                    if (sessions.putIfAbsent(loc, driver) == null) more.add(loc);
                } else if (more.remove(loc) || loc.equals(locator)) {
                    sessions.remove(loc, driver);
                }
            }
        }

        void close() {
            if (!closed.compareAndSet(false, true)) return;
            synchronized (ListenerHub.this) {
                for (String l : more) sessions.remove(l, driver);
                more.clear();
                if (locator != null) sessions.remove(locator, driver);
                resources.remove(driver); registrations.remove(this);
            }
            driver.processLaneAdmission(CarrierSetup::stop);
            Runnable remove = observer.getAndSet(() -> {}); if (remove != null) remove.run();
            drainedIfEmpty();
        }
    }

    /** The sessions registered now (the ones the listener started or dialed that have not ended). */
    public java.util.List<SessionRuntime> registered() {
        synchronized (this) {
            java.util.List<SessionRuntime> l = new java.util.ArrayList<>(registrations.size());
            for (Registration r : registrations) l.add(r.driver);
            return l;
        }
    }

    /** Per-classification drain policy. Only shutdown makes it permanent. */
    public synchronized void drain(String reason) {
        if (!shuttingDown && !closed) drainReason = java.util.Objects.requireNonNull(reason);
    }

    public synchronized void undrain() {
        if (!shuttingDown && !closed) drainReason = null;
    }

    public synchronized void beginShutdown(String reason) {
        if (!shuttingDown) drainReason = reason;
        shuttingDown = true;
    }

    public void onDrained(Runnable onDrained) {
        synchronized (this) {
            if (closed) return;
            assert shuttingDown;
            drained = onDrained;
        }
        drainedIfEmpty();
    }

    private void drainedIfEmpty() {
        Runnable r;
        synchronized (this) {
            if (!shuttingDown || closed || !registrations.isEmpty() || drained == null) return;
            r = drained;
            drained = null;
        }
        Thread.ofVirtual().name("zmux-listener-drained").start(r);
    }

    /** Stops taking carriers; sessions already started are the owner's. */
    public void close() {
        java.util.List<Registration> registered;
        java.util.List<CarrierSetup> arriving;
        synchronized (this) {
            closed = true; drained = null;
            registered = java.util.List.copyOf(registrations);
            arriving = java.util.List.copyOf(setups);
        }
        for (CarrierSetup setup : arriving) setup.stop();
        for (Registration registration : registered) registration.close();
        management.close();
        resources.clear();
        sessions.clear();
    }

    /**
     * Takes an external carrier whose source slot was reserved ({@link #reserve}): the
     * admission, if configured and the carrier was not admitted with its connection, then
     * its classification: by hint's pairing, or by its first record.
     * verdict (or null) receives the admission's final verdict code, off the loop; entry says
     * what the carrier is decided on and where a carrier the admission gives back goes. Returns
     * the carrier, for its {@link Transport#whenTaken} and {@link Transport#whenClosed}.
     */
    public Transport external(SessionHost loop, io.zmux.carrier.PipeIo io, io.zmux.JoinHint hint, boolean admitted,
                              Object admittedValue, Entry entry, CompletableFuture<Decided> verdict, CarrierSetup setup) {
        boolean pair = !hint.pairing().isEmpty();
        Transport carrier = setup.own(() -> pair ? new ExternalTransport(loop, io) : carrier(loop, io));
        if (carrier == null) {
            admissionAborted(admitted);
            report(verdict, Admit.CLOSE, null);
            return null;
        }
        var info = info(carrier, io, hint).at(entry.head() ? Admission.Stage.HEAD : Admission.Stage.CARRIER, io.framing(), entry.upgrade() != null);
        InetSocketAddress source = hint.source();
        Next next;
        if (pair) {
            byte[] key = decodePairToken(hint.pairing());
            boolean shaped = carrier.directions() == (hint.uplink() ? Action.DIR_IN : Action.DIR_OUT);
            next = (c, value) -> {
                // A waiting half keeps its slots until the pair is complete.
                if (key == null || !shaped) { c.reject(null); counts.rejected(); return false; }
                var half = (ExternalTransport) c;
                return new PairHalf(half, key, hint, source, value, info).begin(half);
            };
        } else {
            String alpn = io.capabilities().applicationProtocol();
            next = transformed(hint, info, (c, value) -> gate(c, source, alpn, value, info) != null);
        }
        if (!carrier.loop().tryExecute(() -> admit(carrier, info, admitted, new Admitted(admittedValue, ""), entry, io.carrierConn(), verdict, false, next))) {
            admissionAborted(admitted);
            carrier.reject(null);
            report(verdict, Admit.CLOSE, null);
        }
        return carrier;
    }

    /** The transport of an external carrier: its own on its host, or one on loop. */
    private static Transport carrier(SessionHost loop, io.zmux.carrier.PipeIo io) {
        Transport t = Transport.claim(io);
        return t != null ? t : new ExternalTransport(loop, io);
    }

    /** The admission metadata of an external carrier: its exporter is the carrier's, when its pipe input offers one. */
    private static Admission.CarrierInfo info(Transport carrier, io.zmux.carrier.PipeIo io, io.zmux.JoinHint hint) {
        var caps = io.capabilities();
        return new Admission.CarrierInfo(hint.source(), hint.peer(), caps.applicationProtocol(), caps.serverName(),
                caps.exporter() == null ? null : carrier::export, hint.request(), io.isTerminatedTls(),
                Admission.Stage.CARRIER, io.framing(), false);
    }

    private static byte[] decodePairToken(String value) {
        if (value == null || value.length() != 22) return null;
        try {
            byte[] bytes = java.util.Base64.getUrlDecoder().decode(value);
            return bytes.length == 16 && java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(bytes).equals(value) ? bytes : null;
        } catch (IllegalArgumentException invalid) { return null; }
    }

    private final class PairHalf implements Inbound {
        final CarrierSetup setup;
        boolean timersStopped;
        final byte[] token;
        final boolean uplink;
        final io.zmux.JoinHint hint;
        final InetSocketAddress source;
        /** The half's admission value. */
        final Admitted admitted;
        final Admission.CarrierInfo info;
        private final AttachmentRelease registration = new AttachmentRelease();
        final Runnable shutdown = this::shutdownCarrier;
        final TimerHeap.Timer first = new TimerHeap.Timer() { @Override void fire(long now) { fail(Outcome.TIMED_OUT); } };
        final TimerHeap.Timer expiry = new TimerHeap.Timer() { @Override void fire(long now) { fail(Outcome.TIMED_OUT); } };
        PairHalf other;
        CarrierSetup pair;
        boolean firstReady;
        boolean transferRequested;
        boolean done;

        PairHalf(ExternalTransport carrier, byte[] token, io.zmux.JoinHint hint, InetSocketAddress source, Admitted admitted, Admission.CarrierInfo info) {
            this.info = info; this.hint = hint;
            setup = carrier.setup();
            setup.track(registration.completion());
            this.token = token; this.uplink = hint.uplink(); this.source = source; this.admitted = admitted;
        }
        /** False when the half was refused (its carrier is closed). */
        boolean begin(ExternalTransport arriving) {
            var loop = setup.ownerLoop();
            setup.phase(loop, this::withdraw);
            Outcome refused = null;
            synchronized (ListenerHub.this) {
                if (closed || setup.finished() || setup.interrupted()) refused = Outcome.REFUSED; // a split bootstrap starts a session
                else {
                    var result = pairing.offer(token, uplink, this, waiting -> {
                        PairHalf up = uplink ? this : waiting, down = uplink ? waiting : this;
                        pair = CarrierSetup.pair(up.setup, down.setup, up.setup.ownerLoop());
                        return pair != null;
                    });
                    if (result == null) refused = Outcome.REJECTED;
                    else if (result.partner() == null) setup.reserveToken(result.reservation(), ListenerHub.this);
                    else {
                        other = result.partner(); other.other = this;
                        PairHalf up = uplink ? this : other, down = uplink ? other : this;
                        other.pair = pair;
                        observe(pair);
                        pair.track(up.registration.completion());
                        pair.track(down.registration.completion());
                        pair.phase(up.setup.ownerLoop(), () -> up.fail(Outcome.REFUSED));
                    }
                }
            }
            if (refused != null) { registration.close(); setup.close(null); counts.count(refused); return false; }
            loop.onShutdown(shutdown);
            loop.schedule(expiry, Time.add(Clock.now(), owner.config().handshakeTimeout()));
            // A transform runs on the pair and reads the first bytes itself, an Establish among them.
            if (uplink && transform != null) {
                synchronized (ListenerHub.this) { firstReady = true; }
            } else if (uplink) {
                arriving.boundPendingInput(MAX_FIRST_RECORD + 8);
                arriving.start(this);
            }
            tryPair();
            return true;
        }
        private void withdraw() {
            fail(Outcome.REFUSED, true);
        }
        private void shutdownCarrier() { try { fail(Outcome.REFUSED); } finally { stopTimers(); } }
        private void stopTimers() {
            var loop = setup.ownerLoop();
            Runnable stop = () -> {
                if (timersStopped) return;
                timersStopped = true;
                loop.removeShutdownHook(shutdown);
                loop.schedule(first, Time.NEVER); loop.schedule(expiry, Time.NEVER);
                registration.close();
            };
            loop.release(stop);
        }
        /** Ends the half and, once paired, its partner: one input then, counted once as how says. */
        void fail(Outcome how) { fail(how, false); }

        void fail(Outcome how, boolean withdrawing) {
            PairHalf partner;
            synchronized (ListenerHub.this) {
                if (done || (withdrawing && other != null && !other.setup.interrupted())) return;
                done = true; setup.sealToken();
                partner = other;
                if (partner != null) partner.done = true;
            }
            if (partner != null) pair.refused();
            counts.count(how);
            closeHalf();
            if (partner != null) partner.closeHalf();
        }
        private void closeHalf() {
            setup.ownerLoop().release(() -> {
                stopTimers();
                setup.close(null);
            });
        }

        void tryPair() {
            PairHalf up;
            boolean ready;
            synchronized (ListenerHub.this) {
                if (done || other == null) return;
                up = uplink ? this : other;
                ready = up.firstReady;
            }
            // The first record is timed from the pairing; a half alone ends at the handshake timeout.
            if (!up.setup.ownerLoop().tryExecute(ready ? up::combine : up::armFirst)) up.fail(Outcome.REFUSED);
        }
        void armFirst() {
            synchronized (ListenerHub.this) { if (done || firstReady) return; }
            setup.ownerLoop().schedule(first, Time.add(Clock.now(), firstRecordTimeout));
        }
        void combine() {
            PairHalf down;
            synchronized (ListenerHub.this) {
                if (done || transferRequested || other == null || !firstReady) return;
                transferRequested = true; down = other;
            }
            // Detach on the downlink's owner before handing it to this loop. Its shutdown
            // hook cannot race a check-and-transfer on another loop.
            var loop = setup.ownerLoop();
            if (!down.setup.ownerLoop().tryExecute(() -> {
                synchronized (ListenerHub.this) { if (done || down.done) return; }
                down.stopTimers();
                ExternalTransport moved = (ExternalTransport) down.setup.move(loop);
                // moveTo returns null for a carrier that cannot move (closed, writing, a session on it); the analyzer does not follow that.
                //noinspection ConstantValue
                if (moved == null) { fail(Outcome.REJECTED); return; }
                if (!loop.tryExecute(() -> finishPair(down))) { moved.reject(null); fail(Outcome.REFUSED); }
            })) fail(Outcome.REFUSED);
        }
        void finishPair(PairHalf down) {
            boolean refused;
            synchronized (ListenerHub.this) {
                refused = done || down.done || closed;
                if (!refused) {
                    done = down.done = true;
                    // The pair is one carrier now: it goes on with the uplink's slots.
                    down.setup.releaseSlots();
                }
            }
            if (refused) { down.setup.close(null); fail(Outcome.REFUSED); return; }
            stopTimers();
            ExternalTransport combined = pair.combine();
            if (combined == null) { counts.rejected(); return; }
            combined.unboundPendingInput();
            // The session's admission value is the uplink's, the half that carries the client's parameters.
            PairHalf up = uplink ? this : down;
            String alpn = combined.info().applicationProtocol();
            Admission.CarrierInfo paired = new Admission.CarrierInfo(up.info.source(), up.info.peer(), alpn, up.info.serverName(),
                    combined.exporterPresent() ? combined::export : null, up.info.request(), info.terminatedTls() && down.info.terminatedTls(),
                    Admission.Stage.CARRIER, io.zmux.carrier.Framing.BYTE_STREAM, false);
            Next session = (c, value) -> gate(c, source, alpn, value, paired) != null;
            if (transform == null) session.start(combined, up.admitted);
            else new Transformed(up.hint, paired, session).start(combined, up.admitted);
        }
        @Override public int bytes(Transport c, ReadBuf buf, ByteBuffer bytes, int pos, int lim) {
            int hello = helloOffset(c, bytes, pos, lim);
            if (hello == HELLO_MORE) return 0;
            if (hello < 0) {
                var record = CarrierAdmission.classify(bytes, pos, lim, MAX_FIRST_RECORD);
                if (record.kind() == CarrierAdmission.Kind.MORE || (record.kind() == CarrierAdmission.Kind.SESSION && record.firstRecordLength() == 0)) return 0;
                if (record.kind() != CarrierAdmission.Kind.SESSION) { fail(Outcome.REJECTED); return Inbound.GONE; }
            }
            setup.ownerLoop().schedule(first, Time.NEVER); c.pauseInput();
            synchronized (ListenerHub.this) { firstReady = true; }
            tryPair(); return 0;
        }
        @Override public void ended(Transport c, boolean graceful, boolean partial) { fail(Outcome.REJECTED); }
    }

    // ---- the source slot, the admission of new carriers ----

    /** Serializes the admission counters and their eviction index. A lease owns both slots. */
    final class AdmissionSlots {
        private final CarrierAdmission limits;
        private final java.util.LinkedHashMap<Lease, Stalled> stalled = new java.util.LinkedHashMap<>();

        AdmissionSlots(CarrierAdmission limits) { this.limits = limits; }

        final class Lease implements AutoCloseable {
            final InetSocketAddress source;
            private boolean held = true;
            Lease(InetSocketAddress source) { this.source = source; }
            @Override public void close() {
                synchronized (AdmissionSlots.this) {
                    if (!held) return;
                    held = false;
                    if (stalled.remove(this) == null) limits.release(source);
                    else limits.unstall(source);
                }
            }
        }

        synchronized int reserve(CarrierSetup setup, InetSocketAddress source) {
            if (closed) return CarrierAdmission.SOURCE_FULL;
            int result = limits.take(source);
            if (result == CarrierAdmission.FULL && !stalled.isEmpty()) {
                var oldest = stalled.firstEntry();
                oldest.getKey().close();
                var evicted = oldest.getValue();
                evicted.loop.tryExecute(() -> evicted.finish(false));
                result = limits.take(source);
            }
            if (result == CarrierAdmission.TAKEN) setup.reserveSlots(new Lease(source));
            return result;
        }

        private synchronized boolean park(Stalled entry, InetSocketAddress source) {
            var setup = entry.setup;
            if (closed || !setup.canStall()) return false;
            var lease = setup.slotKey();
            if (lease == null && limits.take(source) == CarrierAdmission.TAKEN) {
                setup.reserveSlots(new Lease(source));
                lease = setup.slotKey();
            }
            if (lease == null || !lease.held || !limits.stall(lease.source)) return false;
            stalled.put(lease, entry);
            return true;
        }

        synchronized void bounds(int source, int total) { limits.bounds(source, total); }
        String sourceKey(InetSocketAddress source) { return limits.sourceKey(source); }
    }

    /** Counts a carrier refused its slots: a source without one is refused, a full listener rejects. */
    private void notTaken(int r) {
        if (r == CarrierAdmission.FULL) counts.rejected(); else counts.refused();
    }

    /**
     * Takes the slots of a carrier an entry accepted, before its outer handshake; false (counted
     * refused, or rejected when the listener is full) when the source has its share, every slot
     * of the listener is taken and none is stalled, or the listener takes no carrier.
     */
    public CarrierSetup reserve(InetSocketAddress source) {
        var setup = CarrierSetup.reservation();
        int taken = admission.reserve(setup, source);
        if (taken != CarrierAdmission.TAKEN) { notTaken(taken); return null; }
        observeReservation(setup);
        return setup;
    }

    private final java.util.Set<CarrierSetup> setups = ConcurrentHashMap.newKeySet();

    private void observeReservation(CarrierSetup setup) {
        setup.onCancel(setup::refused); // Until a carrier consumes the outer-handshake reservation.
        observe(setup);
    }

    private void observe(CarrierSetup setup) {
        setups.add(setup);
        setup.onSettled(() -> setups.remove(setup));
        if (closed) setup.stop();
    }

    public void handshakeFailed(CarrierSetup setup) {
        setup.refused();
        if (decider != null) counts.handshakeFailed.incrementAndGet(); else counts.setupFailed();
    }

    /** The verdict codes: a skip of at least 0 accepts. */
    public static final class Admit {
        public static final int FALLBACK = -1, STALL = -2, CLOSE = -3, TAKEOVER = -4;
        private Admit() {}
    }

    /** Carrier shapes select the HTTP action that must complete before handing bytes to a host. */
    public sealed interface Entry {
        /** A byte or message stream already open for admission. */
        final class Stream implements Entry {
            public static final Stream CARRIER = new Stream();
            private Stream() {}
        }
        /** A request whose acceptance response body becomes the carrier's output. */
        record Request(boolean head, Admission.Fallback requestFallback, io.zmux.adapter.Upgrade writeHead) implements Entry {}
        /** A request whose accepted upgrade opens the carrier. */
        record Upgrade(io.zmux.adapter.Upgrade upgrade, Admission.Fallback requestFallback) implements Entry {
            @Override public boolean head() { return true; }
        }
        default boolean head() { return false; }
        default io.zmux.adapter.Upgrade upgrade() { return null; }
    }

    /** What an accepted carrier continues with, on its loop; false when the carrier was closed. */
    @FunctionalInterface
    private interface Next {
        boolean start(Transport c, Admitted admitted);
    }

    /**
     * The outcome of a carrier's admission: a verdict code, an accept's value, and the {@link io.zmux.Takeover} of a
     * carrier the listener's fallback took over (null otherwise), whose {@link Handback#detached()} the adapter reads
     * when the carrier closed.
     */
    public record Decided(int code, Object value, Handback takeover) {
        public Decided(int code, Object value) { this(code, value, null); }
    }

    private Admission.Decider decider;
    /** The listener's fallback: the target of every carrier given back (an HTTP request's excepted); null: none. */
    private Admission.Fallback listenerFallback;
    private int peek, maxPeek, maxMessage;
    private long peekTimeout, stallMin, stallMax;
    private boolean fallbackOnPeekTimeout;
    private final byte[] stallKey = new byte[32];
    /**
     * Configures the admission of every new carrier, before the first one; times in
     * nanoseconds. fallback (or null) is the listener's, the target of every carrier given back.
     */
    public void admission(Admission.Decider d, int peek, int maxPeek, int maxMessage, long peekTimeout, boolean fallbackOnPeekTimeout,
                          long stallMin, long stallMax, Admission.Fallback fallback) {
        this.fallbackOnPeekTimeout = fallbackOnPeekTimeout;
        this.maxPeek = maxPeek;
        this.maxMessage = maxMessage;
        decider = d;
        listenerFallback = fallback;
        this.peek = peek;
        this.peekTimeout = peekTimeout;
        this.stallMin = stallMin;
        this.stallMax = stallMax;
        random.nextBytes(stallKey);
    }

    /**
     * For a prefix that begins a Join Hello, whether its Locator names a current session. A prefix
     * that is not a Join, or is malformed, gives (false, 0); one that ends before the head
     * of the Hello (through R) gives (false, total length needed).
     */
    public io.zmux.adapter.JoinLookup knownJoin(ByteBuffer prefix) {
        int n = prefix.remaining();
        if (n == 0 || (prefix.get(prefix.position()) & 0xff) != io.zmux.internal.join.Hello.PROFILE_JOIN) return new io.zmux.adapter.JoinLookup(false, 0);
        byte[] b = new byte[n];
        prefix.get(prefix.position(), b);
        try {
            var h = io.zmux.internal.join.Hello.parseHead(b, 0, n, io.zmux.internal.join.Framing.STREAM);
            return new io.zmux.adapter.JoinLookup(sessions.containsKey(HexFormat.of().formatHex(h.locator())), 0);
        } catch (io.zmux.seal.SecureCarrierException e) {
            return new io.zmux.adapter.JoinLookup(false, e.kind() == io.zmux.seal.SecureCarrierException.Kind.INCOMPLETE ? e.need() : 0);
        }
    }

    /** The duration of a stall of a carrier from source: a keyed function of the source's client alone. */
    public long stallDuration(InetSocketAddress source) {
        if (stallMax <= stallMin) return stallMax;
        try {
            javax.crypto.Mac m = javax.crypto.Mac.getInstance("HmacSHA256");
            m.init(new javax.crypto.spec.SecretKeySpec(stallKey, "HmacSHA256"));
            long v = ByteBuffer.wrap(m.doFinal(admission.sourceKey(source).getBytes(java.nio.charset.StandardCharsets.UTF_8))).getLong();
            return stallMin + Long.remainderUnsigned(v, stallMax - stallMin + 1);
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Takes the slots of peer for an accepted socket; false when there are none. A source over its pending limit
     * (the first check of a join) goes to the reject policy as pending_limit; a stall needs a slot of this
     * same source, so while it has none the carrier is closed at once. A full listener closes it at once.
     */
    private CarrierSetup socketArrival(EventLoop loop, SocketChannel ch, InetSocketAddress peer) {
        var setup = CarrierSetup.reservation();
        setup.receive(ch);
        int taken = admission.reserve(setup, peer);
        observeReservation(setup);
        if (taken == CarrierAdmission.TAKEN && setup.arrive()) return setup;
        if (taken == CarrierAdmission.SOURCE_FULL && !closed && setup.arrive() && loop.tryExecute(() -> {
            Transport c = setup.own(() -> new SocketTransport(loop, ch));
            if (c == null) { counts.refused(); return; }
            rejectFirst(c, 0, null, "pending_limit", peer, establishStall());
        })) return null;
        setup.refused();
        if (taken == CarrierAdmission.FULL) counts.rejected(); else counts.refused();
        return null;
    }

    public void socket(EventLoop loop, SocketChannel ch, InetSocketAddress peer) {
        var setup = socketArrival(loop, ch, peer);
        if (setup == null) return;
        var info = new Admission.CarrierInfo(peer, peer, "", "", null, io.zmux.RequestHead.NONE);
        Next next = transformed(io.zmux.JoinHint.inBand(peer), info, (c, value) -> gate(c, peer, "", value, info) != null);
        if (!loop.tryExecute(() -> {
            var c = setup.own(() -> new SocketTransport(loop, ch));
            if (c == null) admissionAborted(false);
            else admit(c, info, false, Admitted.NONE, Entry.Stream.CARRIER, null, null, false, next);
        })) {
            admissionAborted(false);
            setup.refused();
        }
    }

    public void tls(EventLoop loop, SocketChannel ch, java.util.function.Supplier<SSLEngine> factory, long handshakeTimeout, boolean secure, InetSocketAddress peer) {
        var setup = socketArrival(loop, ch, peer);
        if (setup == null) return;
        // Install cancellation before starting TLS, including a stop racing synchronous completion.
        var cancel = new CompletableFuture<Runnable>();
        setup.onCancel(() -> cancel.thenAccept(Runnable::run));
        TlsTransport transport;
        try { transport = setup.own(() -> new TlsTransport(loop, ch, setup.host(() -> java.util.Objects.requireNonNull(factory.get())))); }
        catch (RuntimeException failed) {
            handshakeFailed(setup);
            cancel.complete(() -> {});
            return;
        }
        if (transport == null) { handshakeFailed(setup); cancel.complete(() -> {}); return; }
        cancel.complete(transport.handshake(handshakeTimeout, secure, (c, err) -> {
            if (err != null) { handshakeFailed(setup); return; }
            String alpn = c.info().applicationProtocol();
            var info = new Admission.CarrierInfo(peer, peer, alpn, c.serverName(), c::export, io.zmux.RequestHead.NONE, true,
                    Admission.Stage.CARRIER, io.zmux.carrier.Framing.BYTE_STREAM, false);
            admit(c, info, false, Admitted.NONE, Entry.Stream.CARRIER, null, null, false,
                    transformed(io.zmux.JoinHint.inBand(peer), info, (x, value) -> gate(x, peer, alpn, value, info) != null));
        }));
    }

    /** The admission verdict code: 0 for any accept. */
    private static void report(CompletableFuture<Decided> verdict, int v, Object value) { report(verdict, v, value, null); }

    private static void report(CompletableFuture<Decided> verdict, int v, Object value, Handback takeover) {
        if (verdict != null) Callbacks.EXECUTOR.execute(() -> verdict.complete(new Decided(Math.min(v, 0), v >= 0 ? value : null, takeover)));
    }

    /**
     * On the carrier's loop, its slot held: the admission, if configured, then next. A
     * deferred next moves the carrier, so it runs only once the skipped bytes are consumed.
     * A carrier decided on its request head is decided at once, without a prefix. conn is the
     * identity of the carrier's multi-stream connection, which a takeover keeps (null: none).
     */
    private void admit(Transport c, Admission.CarrierInfo info, boolean admitted, Admitted admittedValue, Entry entry, Object conn,
                       CompletableFuture<Decided> verdict, boolean deferred, Next next) {
        if (c.setup().interrupted()) {
            admissionAborted(admitted);
            c.reject(null);
            report(verdict, Admit.CLOSE, null);
            return;
        }
        if (decider == null || admitted) {
            Admitted value = decider == null ? Admitted.NONE : admittedValue;
            report(verdict, 0, value.value());
            if (entry.upgrade() != null && !c.setup().acceptedUpgrade(c, entry.upgrade(), owner)) { return; }
            next.start(c, value);
            return;
        }
        boolean refused;
        synchronized (this) { refused = closed; }
        if (refused) {
            admissionAborted(false);
            c.reject(null);
            report(verdict, Admit.CLOSE, null);
            return;
        }
        Admitting a = new Admitting(c, info, entry, conn, verdict, deferred, next, Time.add(Clock.now(), peekTimeout));
        c.setup().phase(c.loop(), a.shutdown);
        c.loop().onShutdown(a.shutdown);
        c.loop().schedule(a, a.due);
        if (info.stage() == Admission.Stage.HEAD || peek == 0 || (c.directions() & Action.DIR_IN) == 0) a.decide();
        else a.read();
    }

    private void admissionAborted(boolean alreadyAdmitted) {
        if (decider != null && !alreadyAdmitted) counts.closed.incrementAndGet();
        else counts.refused();
    }

    /** Holds a carrier's input until a deferred next has run; closes it when the input ends first. */
    private static final Inbound HOLD = new Inbound() {
        @Override public int bytes(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim) { return 0; }
        @Override public void ended(Transport c, boolean graceful, boolean partial) { c.reject(null); }
    };

    /**
     * A carrier under admission: it reads the prefix, keeping every byte in the read buffer,
     * then pauses its input while the decider decides, and acts on the verdict on its loop.
     * Its timer is the peek timeout, which bounds the reading and the decision, and, after a
     * {@link AdmissionVerdict#DEFER} on an upgrade's head, the upgrade and the decision on the
     * carrier's first message.
     */
    private final class Admitting extends TimerHeap.Timer implements Inbound {
        final CarrierSetup setup;
        final SessionHost loop;
        /** The metadata of the decision under way: a deferral moves it to the carrier's stage. */
        private Admission.CarrierInfo info;
        final Entry entry;
        final Object conn;
        final InetSocketAddress source;
        final boolean deferred;
        final CompletableFuture<Decided> verdict;
        final Next next;
        /** The accept's value and label. */
        private Admitted value = Admitted.NONE;
        final long due;
        /** A copy of every byte seen so far, still unconsumed in the read buffer until the input ends. */
        private byte[] seen = new byte[0];
        /** The prefix shown to the decider: its offset in seen (past a message's length header), and its length. */
        private int prefixAt, prefixLength;
        /** The prefix length asked for; on a byte stream the peek, then each {@link AdmissionVerdict#MORE}'s need. */
        private int want = peek;
        final boolean messages;
        final boolean resumablePrefix;
        /** The length of an oversized first message, or -1 when none was seen. */
        private long oversizedMessage = -1;
        /** The first message is empty: the carrier closes without a decision. */
        private boolean empty;
        /** Label of the MORE whose requested bytes are outstanding. */
        private String waitingVariant = "";
        private final int prefixMessageLimit = Math.max(peek, maxPeek);
        private final int messageLimit = peek == 0 ? maxMessage : Math.min(maxMessage, prefixMessageLimit);
        private boolean decidingNow, done, paused, ended, graceful = true;
        /** The acceptance response is outstanding. Its write shares the admission deadline. */
        private boolean openingTakeover;
        /** The head selected DEFER, so first input shares the upgrade deadline and outcome rules. */
        private boolean deferredAdmission;
        private CompletableFuture<Admission.Decision> pending;
        /** The upgrade a deferral waits for, until it completes. */
        private CarrierSetup.Opening upgrading;
        final Runnable shutdown;

        Admitting(Transport c, Admission.CarrierInfo info, Entry entry, Object conn,
                  CompletableFuture<Decided> verdict, boolean deferred, Next next, long due) {
            setup = c.setup(); loop = c.loop();
            this.info = info;
            this.source = info.source();
            this.entry = entry;
            this.conn = conn;
            this.verdict = verdict;
            this.deferred = deferred;
            this.next = next;
            this.due = due;
            shutdown = () -> abandon(counts.closed);
            messages = c.info().carrier().framing() == io.zmux.internal.core.Framing.RECORDS;
            resumablePrefix = fallbackOnPeekTimeout && !messages && entry instanceof Entry.Stream
                    && c.resumesAfterPeekTimeout();
        }

        /** Starts reading the prefix: the first message of a message-framed carrier, bounded. */
        void read() {
            // Registered while it reads, so that the listener's stop ends it at once.

            if (closed) { abandon(counts.closed); return; }
            if (messages) setup.borrow().limitFirstMessage(messageLimit);
            setup.borrow().start(this);
        }

        /** Whether what was seen is enough to decide: a whole first message, or the prefix asked for. */
        private boolean enough() {
            if (!messages) {
                prefixAt = 0;
                return seen.length >= want;
            }
            if (seen.length == 0) return false;
            int sn = VarInt.lengthOf(seen[0]);
            if (seen.length < sn) return false;
            long len = VarInt.get(ByteBuffer.wrap(seen), 0, seen.length);
            prefixAt = sn;
            if (len > messageLimit) {
                oversizedMessage = len;
                return true;
            }
            // An empty first message is no prefix: a takeover could not replay it.
            if (len == 0) empty = true;
            return seen.length >= sn + len;
        }

        @Override
        public int bytes(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim) {
            if (done) return 0;
            if (deferredAdmission && Clock.now() >= due) { abandon(counts.peekTimedOut); return 0; }
            seen = new byte[lim - pos];
            data.get(pos, seen);
            if (!decidingNow && enough()) {
                c.pauseInput();
                paused = true;
                if (oversizedMessage >= 0) tooLarge(oversizedMessage);
                else if (empty) abandon(counts.closed);
                else decide();
            }
            return 0;
        }

        @Override
        void fire(long now) {
            if (done) return;
            if (openingTakeover) { headTimedOut(); return; }
            if (resumablePrefix && !decidingNow && !ended && seen.length < want) {
                decided(new Admission.Decision(AdmissionVerdict.FALLBACK, 0, null, 0, waitingVariant));
            } else abandon(counts.peekTimedOut);
        }

        @Override
        public void ended(Transport c, boolean graceful, boolean partial) {
            if (done) return;
            ended = true;
            this.graceful = graceful;
            if (decidingNow) return;
            if (deferredAdmission && Clock.now() >= due) { abandon(counts.peekTimedOut); return; }
            if (!messages) {
                if (deferredAdmission && !graceful) abandon(counts.handshakeFailed);
                else decide();
                return;
            }
            // A message-framed carrier is decided on a whole binary first message.
            if (c.inputError() instanceof io.zmux.carrier.MessageTooLargeException large) tooLarge(large.observedBytes().orElse(-1));
            else if (!enough()) abandon(deferredAdmission ? counts.handshakeFailed : counts.closed);
            else if (oversizedMessage >= 0) tooLarge(oversizedMessage);
            else if (empty) abandon(counts.closed);
            else decide();
        }

        /** Closes a carrier whose first message is longer than the bound, without a decision. */
        private void tooLarge(long size) {
            // A bounded reader can report exceeding the effective limit before the full size is known.
            boolean exceedsOnlyAdapterLimit = maxMessage < prefixMessageLimit && (size < 0 || size <= prefixMessageLimit);
            abandon(deferredAdmission || exceedsOnlyAdapterLimit ? counts.tooLarge : counts.closed);
        }

        /** Asks the decider, outside every lock; its stage completes on any thread. */
        void decide() {
            if (Clock.now() >= due) { abandon(counts.peekTimedOut); return; }
            decidingNow = true;
            prefixLength = messages ? messagePrefix() : Math.min(want, seen.length);
            ByteBuffer prefix = ByteBuffer.wrap(seen, prefixAt, prefixLength).slice().asReadOnlyBuffer();
            var deadline = new Admission.Deadline(System.nanoTime() + Math.max(0, due - Clock.now()));
            var f = setup.stage(() -> decider.decide(info, prefix, deadline));
            pending = f;
            if (Clock.now() >= due) { abandon(counts.peekTimedOut); return; }

            // Stop may have raced the host decision.
            if (closed) { abandon(counts.closed); return; }
            f.whenComplete((d, e) -> {
                boolean expired = Clock.now() >= due;
                loop.tryExecute(() -> {
                    if (done) return;
                    if (expired) abandon(counts.peekTimedOut);
                    else if (e == null) decided(d);
                    else abandon(counts.closed, e);
                });
            });
        }

        /** The first message's length as far as it was read; a part of one is shown too, when the carrier ended or the time passed. */
        private int messagePrefix() {
            if (seen.length == 0 || seen.length < VarInt.lengthOf(seen[0])) {
                prefixAt = 0;
                return 0;
            }
            prefixAt = VarInt.lengthOf(seen[0]);
            return (int) Math.min(seen.length - prefixAt, VarInt.get(ByteBuffer.wrap(seen), 0, seen.length));
        }

        /** Asks the decider again for a longer prefix of a byte stream; false when the verdict stands as closing. */
        private boolean more(Admission.Decision d) {
            if (messages || info.stage() == Admission.Stage.HEAD || ended || d.need() <= prefixLength || d.need() > maxPeek) return false;

            pending = null;
            decidingNow = false;
            want = d.need();
            waitingVariant = d.variant();
            if (enough()) {
                decide();
            } else if (paused) {
                paused = false;
                setup.borrow().resumeInput();
            } else {
                setup.borrow().redeliver();
            }
            return true;
        }

        /**
         * The head of an upgrade deferred its decision to the carrier: the upgrade runs, then the
         * carrier's first message (or its first peek bytes) is read and decided on, all by the
         * head's deadline; the carrier stays registered, so that the listener's stop ends the wait.
         */
        private void defer() {
            pending = null;
            decidingNow = false;
            deferredAdmission = true;
            info = info.at(Admission.Stage.CARRIER, info.framing(), true);
            var f = setup.openUpgrade(entry.upgrade(), owner);
            upgrading = f;
            f.ready.whenComplete((v, e) -> loop.tryExecute(() -> upgraded(f, e)));
        }

        /** The deferral's upgrade completed (failed with e): the first message is read, or the carrier closed. */
        private void upgraded(CarrierSetup.Opening f, Throwable e) {
            if (done || upgrading != f) return;
            upgrading = null;
            if (setup.interrupted()) { abandon(counts.closed); return; }
            if (Clock.now() >= due) { abandon(counts.peekTimedOut); return; }
            if (e instanceof java.util.concurrent.CancellationException) { abandon(counts.closed); return; }
            if (e != null) { abandon(counts.handshakeFailed); return; }
            if ((setup.borrow().directions() & Action.DIR_IN) == 0 || (!messages && peek == 0)) decide();
            else read();
        }

        /** Ends the admission undecided: cancelled, timed out, the listener stopped, or no first message. */
        void abandon(java.util.concurrent.atomic.AtomicLong count) { abandon(count, null); }

        private void abandon(java.util.concurrent.atomic.AtomicLong count, Throwable cause) {
            if (done) return;
            finish();
            count.incrementAndGet();
            if (upgrading != null) upgrading.cancel(cause);
            setup.cancelStage(pending, cause);
            if (cause == null) setup.close(null); else setup.fail(cause);
            report(verdict, Admit.CLOSE, null);
        }

        private void headTimedOut() {
            abandon(counts.peekTimedOut, new java.net.SocketTimeoutException("carrier response head timed out"));
        }

        private void finish() {
            done = true;
            loop.schedule(this, Time.NEVER);
            loop.removeShutdownHook(shutdown);
        }

        /** Resumes the paused input: the bytes kept in the read buffer are delivered again. */
        private void resume(Transport input) {
            if (paused) { paused = false; input.resumeInput(); }
            else input.redeliver();
        }

        /** Normal HTTP fallback remains a request until upgrade. Takeover always serves carrier I/O. */
        private Admission.Fallback target(boolean takeover) {
            if (takeover) return listenerFallback;
            return switch (entry) {
                case Entry.Request request -> request.requestFallback();
                case Entry.Upgrade upgrade when info.stage() == Admission.Stage.HEAD -> upgrade.requestFallback();
                case Entry.Upgrade ignored -> listenerFallback;
                case Entry.Stream ignored -> listenerFallback;
            };
        }

        void decided(Admission.Decision d) {
            if (done) return;
            if (setup.interrupted()) { abandon(counts.closed); return; }
            if (d != null && d.verdict() == AdmissionVerdict.MORE && more(d)) return;
            boolean head = info.stage() == Admission.Stage.HEAD;
            if (d != null && d.verdict() == AdmissionVerdict.DEFER && head && entry.upgrade() != null) {
                defer();
                return;
            }
            boolean takeover = d != null && d.verdict() == AdmissionVerdict.TAKEOVER;
            Admission.Fallback target = target(takeover);
            if (takeover && target != null) {
                switch (entry) {
                    case Entry.Request request -> { prepareTakeover(d, request.writeHead()); return; }
                    case Entry.Upgrade upgrade when head -> { prepareTakeover(d, upgrade.upgrade()); return; }
                    case Entry.Upgrade ignored -> { /* DEFER already opened the carrier. */ }
                    case Entry.Stream ignored -> { /* Already a byte stream. */ }
                }
            }
            applyDecision(d, target);
        }

        private void prepareTakeover(Admission.Decision decision, io.zmux.adapter.Upgrade writeHead) {
            openingTakeover = true;
            setup.retainSlotsUntilClosed();
            upgrading = setup.openUpgrade(writeHead, owner);
            upgrading.ready.whenComplete((ignored, error) -> loop.tryExecute(() -> {
                if (done) return;
                upgrading = null;
                if (setup.interrupted()) abandon(counts.closed);
                else if (Clock.now() >= due) headTimedOut();
                else if (error != null) abandon(counts.handshakeFailed, error);
                else applyDecision(decision, listenerFallback);
            }));
        }

        private void applyDecision(Admission.Decision d, Admission.Fallback target) {
            boolean head = info.stage() == Admission.Stage.HEAD;
            finish();
            String variant = d == null ? "" : d.variant();
            if (d != null && d.verdict() == AdmissionVerdict.ACCEPT) value = new Admitted(d.value(), variant);
            int v = d == null ? Admit.CLOSE : switch (d.verdict()) {
                case ACCEPT -> skip(d.skip());
                case TAKEOVER -> target == null ? Admit.STALL : Admit.TAKEOVER;
                case FALLBACK -> target == null ? Admit.STALL : Admit.FALLBACK;
                case STALL -> Admit.STALL;
                case CLOSE, MORE, DEFER -> Admit.CLOSE;
            };
            if (v == Admit.CLOSE) {
                counts.decision(AdmissionVerdict.CLOSE, variant, false);
                setup.close(null);
                report(verdict, v, null);
                return;
            }
            if (v == Admit.STALL) {
                if (ended) {
                    counts.decision(AdmissionVerdict.STALL, variant, false);
                    setup.borrow().shutdownOutput();
                    setup.close(null);
                    report(verdict, v, null);
                    return;
                }
                Transport stalled = setup.borrow();
                Stalled s = reserveStall(stalled, source, stallDuration(source));
                if (s == null) {
                    counts.decision(AdmissionVerdict.STALL, variant, true);
                    setup.close(null);
                    report(verdict, Admit.CLOSE, null);
                    return;
                }
                counts.decision(AdmissionVerdict.STALL, variant, false);
                report(verdict, v, value.value());
                stalled.start(s);
                resume(stalled);
                return;
            }
            if (v == Admit.FALLBACK || v == Admit.TAKEOVER) {
                Handback h = new Handback(setup.borrow(), info, conn, ended ? seen : null, ended, graceful, messages, d.value());
                setup.fallback(h, target, started -> {
                    if (started) {
                        boolean notFound = target instanceof io.zmux.internal.spi.AdapterPorts.RequestFallback request && request.notFound();
                        counts.decision(notFound ? AdmissionVerdict.STALL : d.verdict(), variant, false);
                        report(verdict, v, null, target == listenerFallback ? h : null);
                    } else {
                        counts.decision(AdmissionVerdict.CLOSE, variant, false);
                        report(verdict, Admit.CLOSE, null);
                    }
                }, error -> owner.log(System.Logger.Level.WARNING, "listener fallback failed", error));
                return;
            }
            counts.decision(d.verdict(), variant, false);
            report(verdict, v, value.value());
            if (ended) {
                acceptEnded(v, graceful);
                return;
            }
            Transport c = setup.borrow();
            if (v > 0) {
                c.rebind(new Skip(v));
                resume(c);
                return;
            }
            if (head && entry.upgrade() != null && !c.setup().acceptedUpgrade(c, entry.upgrade(), owner)) { return; }
            if (!next.start(c, value)) return;
            if (!c.isClosed() && c.inbound() != this && !parked(c)) resume(c);
        }

        /** After the skipped bytes of a live delivery: next; the count consumed of [pos, lim), or GONE. */
        int proceed(ReadBuf buf, ByteBuffer data, int pos, int lim) {
            Transport c = setup.borrow();
            if (deferred) return later();
            if (!next.start(c, value)) return Inbound.GONE;
            if (c.inbound() == this || c.isClosed() || pos == lim) return 0;
            return c.inbound().bytes(c, buf, data, pos, lim);
        }

        /**
         * Holds the carrier's input and starts next in a later task, once the delivery that
         * consumed the skipped bytes has returned: a next that moves the carrier then takes
         * only the bytes after them. Input resumes when the carrier stayed.
         */
        int later() {
            Transport c = setup.borrow();
            c.rebind(HOLD);
            c.pauseInput();
            if (!c.loop().tryExecute(() -> {
                if (c.inbound() != HOLD || c.isClosed()) return;
                if (next.start(c, value) && !c.isClosed() && c.inbound() != HOLD && !parked(c)) c.resumeInput();
            })) {
                c.reject(null);
                return Inbound.GONE;
            }
            return 0;
        }

        /** An accept after the input ended: next takes the prefix after the skip, then the end. */
        private void acceptEnded(int skip, boolean graceful) {
            Transport c = setup.borrow();
            if (next instanceof Transformed t) {
                // The transform takes the prefix after the skip, then the end.
                t.begin(c, value, java.util.Arrays.copyOfRange(seen, skip, seen.length), true, graceful);
                return;
            }
            if (deferred) {
                c.reject(null);
                return;
            }
            if (!next.start(c, value)) return;
            if (c.inbound() == this || c.isClosed()) {
                c.reject(null);
                return;
            }
            int len = seen.length - skip, used = len;
            if (len > 0) {
                ReadBuf rb = new ReadBuf(ByteBuffer.wrap(seen, skip, len).slice(), null);
                used = c.inbound().bytes(c, rb, rb.data(), 0, len);
                if (used == Inbound.GONE) return;
            }
            c.inbound().ended(c, graceful, used < len);
        }

        /** The raw bytes an accept skips, or {@link Admit#CLOSE}: a message goes whole or not at all. */
        private int skip(int n) {
            if (n < 0 || n > prefixLength) return Admit.CLOSE;
            if (!messages) return n;
            if (n == 0) return 0;
            return n == prefixLength ? prefixAt + n : Admit.CLOSE;
        }

        /** Drops the first n input bytes, then proceeds. */
        private final class Skip implements Inbound {
            private int n;

            Skip(int n) { this.n = n; }

            @Override
            public int bytes(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim) {
                int k = Math.min(n, lim - pos);
                n -= k;
                if (n > 0) return k;
                // Bytes after the skipped ones go to next only once the skip is consumed: a next
                // that moves the carrier inside this delivery would carry the skipped bytes along.
                int r = pos + k < lim ? later() : proceed(buf, data, lim, lim);
                return r == Inbound.GONE ? r : k + r;
            }

            @Override public void ended(Transport c, boolean graceful, boolean partial) { c.reject(null); }
        }
    }

    /**
     * A stalled carrier, its slots held while it is in the admission eviction index: what arrives is discarded;
     * it ends when the peer ends it, its duration passes, the listener closes, or a new carrier
     * takes its slot of a full listener.
     */
    private static final class Stalled extends TimerHeap.Timer implements Inbound {
        final CarrierSetup setup;
        final SessionHost loop;
        private boolean done;
        final Runnable shutdown = () -> finish(false);

        Stalled(CarrierSetup setup, SessionHost loop) { this.setup = setup; this.loop = loop; }

        boolean start() {
            setup.phase(loop, () -> finish(true));
            return setup.stall();
        }

        private void finish(boolean graceful) {
            if (done) return;
            done = true;
            loop.schedule(this, Time.NEVER);
            loop.removeShutdownHook(shutdown);
            setup.endStall(graceful);
        }

        @Override public int bytes(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim) { return lim - pos; }
        @Override public void ended(Transport c, boolean graceful, boolean partial) { finish(true); }
        @Override void fire(long now) { finish(true); }
    }

    /**
     * On the carrier's loop, its slots held: the first-record gate; null when the carrier was
     * closed.
     */
    private Pending gate(Transport c, InetSocketAddress source, String alpn, Admitted admitted, Admission.CarrierInfo info) {
        if (closed) {
            c.reject(null);
            counts.refused();
            return null;
        }
        Pending p = new Pending(c, source, alpn, admitted, info);

        // close writes closed before it reads pendings: one of the two refuses the carrier.
        if (closed) {
            c.reject(null);
            counts.refused();
            return null;
        }
        c.setup().phase(c.loop(), p.shutdown);
        c.loop().onShutdown(p.shutdown);
        c.loop().schedule(p, Time.add(Clock.now(), firstRecordTimeout));
        c.start(p);
        return p;
    }

    /** A carrier waiting for its first record: its admission slot and first-record timer. */
    private final class Pending extends TimerHeap.Timer implements Inbound {
        final CarrierSetup setup;
        final SessionHost loop;
        final InetSocketAddress source;
        final String alpn;
        final Admitted admitted;
        final Admission.CarrierInfo info;
        boolean done;
        final Runnable shutdown = () -> ended(null, false, false);

        Pending(Transport carrier, InetSocketAddress source, String alpn, Admitted admitted, Admission.CarrierInfo info) {
            this.info = info;
            setup = carrier.setup();
            loop = carrier.loop();
            this.source = source;
            this.alpn = alpn;
            this.admitted = admitted;
        }

        private void classified() {
            done = true;
            loop.schedule(this, Time.NEVER);
            loop.removeShutdownHook(shutdown);
        }

        @Override
        void fire(long now) {
            if (done) return;
            classified();
            setup.close(null);
            counts.timedOut();
        }

        @Override
        public void ended(Transport c, boolean graceful, boolean partial) {
            if (done) return;
            classified();
            setup.close(null);
            // The loop's shutdown (no carrier given) refuses it; a carrier that ended first was not zmux.
            if (c == null) counts.refused(); else counts.rejected();
        }

        @Override
        public int bytes(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim) {
            int helloAt = helloOffset(c, data, pos, lim);
            if (helloAt == HELLO_MORE) return 0;
            if (helloAt >= 0) {
                io.zmux.internal.join.Framing f = helloAt > pos ? io.zmux.internal.join.Framing.MESSAGE : io.zmux.internal.join.Framing.STREAM;
                byte[] b = new byte[lim - helloAt];
                data.get(helloAt, b);
                io.zmux.internal.join.Hello h = null;
                String bad = null;
                try {
                    h = io.zmux.internal.join.Hello.parseExact(b, 0, b.length, f);
                } catch (io.zmux.seal.SecureCarrierException e) {
                    if (e.kind() == io.zmux.seal.SecureCarrierException.Kind.INCOMPLETE) return 0;
                    bad = "malformed"; // whatever the class: the local reasons of a Join name no parse failure finer
                }
                classified(); // The join keeps the slot until it has its result.
                if (bad != null) {

                    rejectJoin(c, null, bad, source);
                } else {
                    join(c, h, lim - pos, admitted, source);
                }
                return Inbound.GONE;
            }
            CarrierAdmission.Classified cl = CarrierAdmission.classify(data, pos, lim, MAX_FIRST_RECORD);
            switch (cl.kind()) {
                case MORE -> {
                    return 0;
                }
                case SESSION, JOIN, LANE -> {
                    if (cl.firstRecordLength() == 0) return 0;
                }
                default -> {
                    classified();
                    c.reject(null);
                    counts.rejected();
                    return Inbound.GONE;
                }
            }
            if (cl.kind() == CarrierAdmission.Kind.SESSION) {
                if (c.directions() != Action.DIR_BIDI) {
                    classified();
                    c.reject(null);
                    counts.rejected();
                    return Inbound.GONE;
                }
                classified(); // The new session keeps the slots until its transport parameters are exchanged.
                return startSession(c, buf, data, pos, lim, alpn, admitted, info);
            }
            int sn = VarInt.lengthOf(data.get(pos));
            byte[] body = new byte[cl.firstRecordLength() - sn];
            data.get(pos + sn, body);
            classified();
            var frame = CarrierAdmission.parseLane(body);
            SessionRuntime target = frame == null ? null : LaneRoutes.of(CarrierAdmission.laneKey(frame));
            if (target == null || !resources.containsKey(target)) { c.reject(null); counts.rejected(); } else transferLane(c, body, target);
            return Inbound.GONE;
        }
    }

    /** Starts a session on c, which holds the slots of source: the session gives them back once established, or ended. */
    private int startSession(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim, String alpn,
                             Admitted admitted, Admission.CarrierInfo info) {
        if (closed) {
            c.reject(null);
            counts.refused();
            return Inbound.GONE;
        }
        // Refused by policy: the session starts, unregistered, and ends with CONNECTION_REFUSED
        // after its own parameters.
        String policy = drainReason;
        String refusal = policy != null ? policy : budget != null && budget.pressure() ? "memory budget" : null;
        Chosen chosen;
        CoreConfig cfg;
        try {
            chosen = c.setup().host(() -> owner.choose(info, admitted.value()));
        } catch (RuntimeException failed) {
            owner.log(System.Logger.Level.WARNING, "the listener's sessionConfigs failed; the carrier is closed", failed);
            c.reject(null);
            counts.rejected();
            return Inbound.GONE;
        }
        try {
            synchronized (this) {
                if (shuttingDown) refusal = drainReason;
                cfg = refusal != null ? chosen.config().toBuilder().budget(budget).build() : prepare(chosen.config());
            }
        } catch (IllegalStateException ended) { c.reject(null); counts.refused(); return Inbound.GONE; }
        try {
            cfg = Bootstrap.withExporter(cfg, c);
        } catch (IllegalArgumentException noExporter) {
            counts.rejected();
            return Inbound.GONE;
        }
        SessionRuntime d = new SessionRuntime(c.loop(), new Session(cfg, Clock.now()), chosen.hooks());
        // An Establish's peer is the session's host value, in place of the admission's.
        d.admitted(c.carrierPeer() != null ? c.carrierPeer() : admitted.value());
        d.bootstrapVariant(admitted.variant());

        if (refusal == null) {
            try { register(d); }
            catch (IllegalStateException ended) {
                refusal = drainReason;
                if (closed || refusal == null) { c.reject(null); d.failInternalNow(ended); counts.refused(); return Inbound.GONE; }
            }
        }
        if (refusal != null) {
            String reason = refusal;
            counts.refused();
            int r = d.attachBootstrap(c).bytes(c, buf, data, pos, lim);
            c.setup().refused();
            d.post(() -> d.core().refuse(reason));
            return r;
        }
        counts.session();
        c.setup().phase(d.loop(), () -> d.failInternalNow(new java.io.InterruptedIOException()));
        CarrierSetup setup = c.setup();
        owner.started(d, alpn, chosen.credential(), setup);
        return d.attachBootstrap(c).bytes(c, buf, data, pos, lim);
    }

    /** Hands a lane to its session's loop: joins when its pipe accepts it, rejected when it names none. */
    private void transferLane(Transport carrier, byte[] body, SessionRuntime driver) {
        CarrierSetup setup = carrier.setup();
        Transport moved = setup.move(driver.loop());
        if (moved == null) { counts.rejected(); return; }
        if (!driver.loop().tryExecute(() -> {
            if (driver.acceptLane(moved, body) != null) counts.joined();
            else counts.rejected();
        })) { moved.reject(null); counts.refused(); }
    }

    /**
     * The longest a lane carrier may take to deliver its lane header, its first record, before it is closed: the
     * default first-record timeout (the session it names is not known before the header is read).
     */
    private static final long LANE_HEADER_NANOS = io.zmux.ListenerConfig.DEFAULT_FIRST_RECORD_TIMEOUT.toNanos();

    /**
     * The process-wide lane entry: reads a lane carrier's header on the carrier's own host, routes it by its Lane Key
     * over the sessions of the process ({@link LaneRoutes}), and closes one that names no session or pipe.
     */
    public static void laneInput(io.zmux.carrier.PipeIo io) { laneInput(EventLoopGroup.nextShared(), io); }

    /** {@link #laneInput(io.zmux.carrier.PipeIo)}, on fallback when the carrier has no host of its own. */
    static void laneInput(SessionHost fallback, io.zmux.carrier.PipeIo io) {
        var setup = new CarrierSetup();
        Transport carrier = setup.lane(io, fallback, true);
        if (carrier == null) return;
        SessionHost host = carrier.loop();
        if (!host.tryExecute(() -> {
            class LaneGate extends TimerHeap.Timer implements Inbound {
                final Runnable shutdown = () -> ended(carrier, false, false);
                private void stop() {
                    host.schedule(this, Time.NEVER);
                    host.removeShutdownHook(shutdown);
                }
                @Override void fire(long now) { ended(carrier, false, false); }
                @Override public int bytes(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim) {
                    var cl = CarrierAdmission.classify(data, pos, lim, MAX_FIRST_RECORD);
                    if (cl.kind() == CarrierAdmission.Kind.MORE || (cl.kind() == CarrierAdmission.Kind.LANE && cl.firstRecordLength() == 0)) return 0;
                    stop();
                    if (cl.kind() != CarrierAdmission.Kind.LANE) { c.reject(null); return Inbound.GONE; }
                    int size = VarInt.lengthOf(data.get(pos));
                    byte[] body = new byte[cl.firstRecordLength() - size]; data.get(pos + size, body);
                    var frame = CarrierAdmission.parseLane(body);
                    SessionRuntime driver = frame == null ? null : LaneRoutes.of(CarrierAdmission.laneKey(frame));
                    if (driver == null) { c.reject(null); return Inbound.GONE; }
                    driver.admitProcessLane(setup);
                    if (driver.loop() == host) {
                        Inbound in = driver.acceptLane(c, body);
                        return in == null ? Inbound.GONE : in.bytes(c, buf, data, pos, lim);
                    }
                    Transport moved = setup.move(driver.loop());
                    if (moved != null && !driver.loop().tryExecute(() -> driver.acceptLane(moved, body))) moved.reject(null);
                    return Inbound.GONE;
                }
                @Override public void ended(Transport c, boolean graceful, boolean partial) { stop(); carrier.reject(null); }
            }
            LaneGate gate = new LaneGate();
            host.onShutdown(gate.shutdown);
            host.schedule(gate, Time.add(Clock.now(), LANE_HEADER_NANOS));
            carrier.start(gate);
        })) carrier.reject(null);
    }

    private static final int HELLO_MORE = -2;

    /** Stalls a rejected carrier, silent, for duration (nanoseconds); false when its source has no room. */
    private boolean stallRejected(Transport c, InetSocketAddress source, long duration) {
        Stalled stall = reserveStall(c, source, duration);
        if (stall == null) return false;
        c.taken();
        c.start(stall);
        c.resumeInput();
        return true;
    }

    private Stalled reserveStall(Transport carrier, InetSocketAddress source, long duration) {
        CarrierSetup setup = carrier.setup();
        SessionHost loop = carrier.loop();
        var stall = new Stalled(setup, loop);
        if (!admission.park(stall, source)) return null;
        if (!stall.start()) return null;
        loop.onShutdown(stall.shutdown);
        loop.schedule(stall, Time.add(Clock.now(), duration));
        return stall;
    }

    /**
     * Where the Hello a carrier starts with begins: its first byte on a byte stream, past the Size
     * ExternalTransport prepends before admission on a message carrier; -1 when it is no Hello,
     * HELLO_MORE when the input does not tell yet or the message is not complete.
     */
    private static int helloOffset(Transport c, ByteBuffer data, int pos, int lim) {
        if (lim <= pos) return HELLO_MORE;
        if (c.info().carrier().framing() != io.zmux.internal.core.Framing.RECORDS) {
            return (data.get(pos) & 0xff) == io.zmux.internal.join.Hello.PROFILE_JOIN ? pos : -1;
        }
        long size = VarInt.get(data, pos, lim);
        if (size < 0) return HELLO_MORE;
        int n = VarInt.lengthOf(data.get(pos));
        if (lim - pos - n < 1) return HELLO_MORE;
        if ((data.get(pos + n) & 0xff) != io.zmux.internal.join.Hello.PROFILE_JOIN) return -1;
        return lim - pos - n < size ? HELLO_MORE : pos + n;
    }

    /**
     * What the configuration does with a rejected Join, nothing written either way: a stall ends
     * one join timeout of the listener's session configuration after now, whether or not the
     * Locator named a session (and whatever that session's own join timeout).
     */
    private void rejectJoin(Transport c, io.zmux.RecordMode mode, String reason, InetSocketAddress source) {
        rejectFirst(c, io.zmux.internal.join.Hello.PROFILE_JOIN, mode, reason, source, owner.config().joinDeadline());
    }

    /**
     * What the configuration does with a rejected carrier, nothing written either way. Without
     * {@link #onReject} an Establish refused before authentication stalls and a Join closes. A
     * stall lasts stall nanoseconds; one whose source already holds its maximum of stalled
     * carriers, or while the stalled carriers hold half of the listener's slots, is closed at
     * once. It was already counted as a rejected pipe input.
     */
    private void rejectFirst(Transport c, int profile, io.zmux.RecordMode mode, String reason, InetSocketAddress source, long stall) {
        rejectFirst(c, profile, mode, reason, source, stall, null);
    }

    private void rejectFirst(Transport c, int profile, io.zmux.RecordMode mode, String reason, InetSocketAddress source,
                             long stall, io.zmux.CarrierRejectedException error) {
        counts.rejected();
        var f = onReject;
        io.zmux.RejectInfo.Action act = profile == io.zmux.internal.join.Hello.PROFILE_JOIN
                ? io.zmux.RejectInfo.Action.CLOSE : io.zmux.RejectInfo.Action.STALL;
        if (f != null) {
            try {
                act = c.setup().host(() -> f.apply(new io.zmux.RejectInfo(profile, mode, reason, source, error)));
            } catch (RuntimeException failed) {
                act = io.zmux.RejectInfo.Action.CLOSE;
                owner.log(System.Logger.Level.WARNING, "the listener's onReject failed; the carrier is closed", failed);
            }
        }
        if (act != io.zmux.RejectInfo.Action.STALL) { c.reject(null); return; }
        if (!stallRejected(c, source, stall)) c.reject(null);
    }

    /** The longest stall of a refused Establish (nanoseconds): the configured or drawn maximum stall duration. */
    private volatile long rejectStallMax = 30 * Time.SECOND;

    /** Sets the bound of an Establish's stall: the admission's maximum stall, or one drawn when none is configured. */
    public void rejectStallMax(long nanos) { rejectStallMax = nanos; }

    /** A refused Establish stalls until the handshake timeout, never beyond the maximum stall duration. */
    private long establishStall() {
        return Math.min(owner.config().handshakeTimeout(), rejectStallMax);
    }

    private java.util.function.Function<io.zmux.RejectInfo, io.zmux.RejectInfo.Action> onReject;

    private volatile int maxPendingPerSession = 4;

    /**
     * Replaces the bounds {@link io.zmux.ListenerLimits} holds, validated by the caller: the
     * slot bounds apply to the next take and stall (held carriers keep their slots), the join
     * bound to the next join, the first-record timeout to carriers that arrive after the call.
     * Suspended sessions beyond a lowered maxSuspended are chosen under this monitor from the
     * suspension table ({@link #resources}), the longest-suspended first, and their close is
     * queued on each one's loop before this returns.
     *
     * <p>Why that covers every suspended session: a session enters the table as suspended
     * under this monitor at the end of the loop pass in which its core suspended (its
     * resource listener), and then reconciles against the volatile bound. A session in the
     * table when the bound is written is trimmed here; one that enters later reads the
     * written bound in its own {@link #manage}. The core is the owner thread's alone, so the
     * end of that pass is the earliest point the listener can know of the suspension.
     *
     * @param maxPending negative: unlimited
     */
    public void limits(int maxPending, int maxPendingPerSource, int maxPendingPerSession, int maxSuspended, long firstRecordTimeout) {
        this.maxPendingPerSession = maxPendingPerSession;
        this.firstRecordTimeout = firstRecordTimeout;
        java.util.List<SessionRuntime> over;
        synchronized (this) {
            admission.bounds(maxPendingPerSource, maxPending);
            this.maxSuspended = maxSuspended;
            var suspended = resources.entrySet().stream()
                    .filter(e -> e.getValue().state() == io.zmux.internal.core.SessionState.SUSPENDED)
                    .sorted(java.util.Comparator.comparingLong(e -> e.getValue().suspendedAt())).map(java.util.Map.Entry::getKey).toList();
            over = suspended.subList(0, Math.max(0, suspended.size() - maxSuspended));
        }
        for (SessionRuntime d : over) {
            d.loop().tryExecute(() -> {
                if (d.core().state() != io.zmux.internal.core.SessionState.SUSPENDED) return;
                d.core().closeResumeTimeout();
                d.markDirty();
            });
        }
        reconcile();
    }

    /** Configures the listener's answer to a rejected Hello, before the first carrier. */
    public void onReject(java.util.function.Function<io.zmux.RejectInfo, io.zmux.RejectInfo.Action> f) { onReject = f; }

    /** Hands a joining carrier to the session its Hello names. */
    private void join(Transport c, io.zmux.internal.join.Hello h, int skip, Admitted admitted, InetSocketAddress source) {
        CarrierSetup setup = c.setup();
        long slotAt = Clock.now();
        if (closed) { c.reject(null); counts.refused(); return; }
        io.zmux.RecordMode mode = io.zmux.internal.join.Mode.record(h.mode());
        SessionRuntime d = sessions.get(HexFormat.of().formatHex(h.locator()));
        if (d == null) {
            rejectJoin(c, mode, "unknown_locator", source);
            return;
        }
        // A carrier without the contract has no Binding: the session refuses the Hello as auth, whatever the joiner mixed.
        byte[] binding;
        try {
            binding = h.mode() == io.zmux.internal.join.Mode.BOUND && c.bindable()
                    ? setup.host(() -> c.export(io.zmux.internal.join.Kdf.EXPORTER_JOIN, null, io.zmux.internal.join.Kdf.BINDING_LEN)) : null;
        } catch (RuntimeException failed) {
            counts.rejected(); // The host boundary already failed and reclaimed this arrival.
            return;
        }
        Transport moved = setup.move(d.loop());
        if (moved == null) { counts.rejected(); return; }
        // Closing the listener ends the handshake.
        if (setup.interrupted()) { moved.reject(null); counts.refused(); return; }
        offered.incrementAndGet();
        setup.onFinished(offered::decrementAndGet);
        boolean ok = d.loop().tryExecute(() -> {
            moved.setup().phase(d.loop(), () -> d.endOffered(setup));
            // Under pressure only a join that lets a suspended session resume.
            if (budget != null && budget.pressure() && d.core().state() != io.zmux.internal.core.SessionState.SUSPENDED) {
                rejectJoin(moved, mode, "memory_pressure", source);
            } else {
                d.acceptHello(new SessionRuntime.OfferedHello(h, skip, binding, admitted.value(), admitted.variant(), counts, setup,
                        (t, reason) -> rejectJoin(t, mode, reason, source), slotAt, maxPendingPerSession));
            }
        });
        if (!ok) { moved.reject(null); counts.refused(); }
    }

    private final java.util.concurrent.atomic.AtomicInteger offered = new java.util.concurrent.atomic.AtomicInteger();
    public int offeredJoins() { return offered.get(); }

    // ---- the inbound transform ----

    private io.zmux.InboundTransform transform;

    /** Configures the inbound transform (or null), before the first carrier. */
    public void inbound(io.zmux.InboundTransform t) {
        transform = t;
    }

    private Next transformed(io.zmux.JoinHint hint, Admission.CarrierInfo info, Next next) {
        return transform == null ? next : new Transformed(hint, info, next);
    }

    /** Whether c waits in a transform's view: its input resumes when the transform reads. */
    private static boolean parked(Transport c) { return c.inbound() instanceof Forwarded; }

    /**
     * The transform of one accepted carrier, its slot held: the carrier is given to the
     * transform as a pipe input of its framing, then the result goes on to next. The timer is
     * the handshake timeout.
     */
    private final class Transformed extends TimerHeap.Timer implements Next {
        final io.zmux.JoinHint hint;
        final Admission.CarrierInfo info;
        final Next next;
        private CarrierSetup setup;
        private SessionHost loop;
        private Forwarded view;
        private io.zmux.carrier.PipeIo given;
        private Admitted value;
        private CompletableFuture<io.zmux.carrier.PipeIo> pending;
        private boolean done;
        private AttachmentRelease processing;
        private final Runnable shutdown = () -> fail(new java.io.IOException("event loop closed"), null);

        Transformed(io.zmux.JoinHint hint, Admission.CarrierInfo info, Next next) {
            this.hint = hint; this.info = info; this.next = next;
        }

        @Override public boolean start(Transport c, Admitted v) {
            begin(c, v, null, false, true);
            return true;
        }

        /** On the carrier's loop: replay (or null) is what the admission read once the input ended. */
        void begin(Transport c, Admitted v, byte[] replay, boolean ended, boolean graceful) {
            setup = c.setup(); loop = c.loop();
            value = v;
            c.taken(); // the layer may read and write on it
            var ci = c.info();
            boolean messages = ci.carrier().framing() == io.zmux.internal.core.Framing.RECORDS;
            c.setup().phase(c.loop(), shutdown);
            view = new Forwarded(c, replay, ended, graceful, messages);
            given = PipeIoAccess.get().withCarrierPeer(view.pipe(info, ci.connection()), c.carrierPeer());
            if (!v.variant().isEmpty()) given = given.withVariant(v.variant());
            c.loop().onShutdown(shutdown);
            long due = Time.add(Clock.now(), owner.config().handshakeTimeout());
            c.loop().schedule(this, due);

            if (closed) { fail(new java.io.IOException("listener closed"), null); return; }
            var deadline = new Admission.Deadline(System.nanoTime() + Math.max(0, due - Clock.now()));
            processing = c.setup().cleanup();
            if (!processing.active()) { fail(new java.io.IOException("carrier setup ended"), null); return; }
            var context = new io.zmux.InboundTransform.Context(hint, java.util.Optional.ofNullable(v.value()), deadline);
            var f = c.setup().stage(() -> transform.transform(given, context));
            pending = f;
            f.whenComplete((r, e) -> {
                if (!c.loop().tryExecute(() -> {
                    try { finish(e == null ? r : null, e); }
                    finally { processing.close(); }
                })) {
                    try { if (r != null && !same(r)) closeIo(r, new java.io.IOException("event loop closed")); }
                    finally { processing.close(); }
                }
            });
        }

        private void closeIo(io.zmux.carrier.PipeIo result, Throwable cause) {
            var cleanup = processing.fork();
            Callbacks.EXECUTOR.execute(() -> {
                Bootstrap.discard(result, cause);
                cleanup.until(io.zmux.internal.spi.AdapterPorts.released(result));
            });
        }

        private boolean same(io.zmux.carrier.PipeIo r) { return r.reader() == given.reader() && r.writer() == given.writer(); }

        @Override void fire(long now) { fail(new java.util.concurrent.TimeoutException("inbound transform timed out"), null); }

        private boolean end() {
            if (done) return false;
            done = true;
            loop.schedule(this, Time.NEVER);
            loop.removeShutdownHook(shutdown);
            return true;
        }

        /** Closes the carrier (and result r, unless it is the carrier) and counts a setup failure. */
        private void fail(Throwable cause, io.zmux.carrier.PipeIo r) {
            if (!end()) { if (r != null && !same(r)) closeIo(r, cause); return; }
            setup.cancelStage(pending, cause);
            if (r != null && !same(r)) closeIo(r, cause);
            view.abort(cause);
            counts.setupFailed();
        }

        private void finish(io.zmux.carrier.PipeIo r, Throwable error) {
            if (done) { if (r != null && !same(r)) closeIo(r, new java.io.IOException("inbound transform ended")); return; }
            if ((error instanceof java.util.concurrent.CompletionException w ? w.getCause() : error) instanceof io.zmux.CarrierRejectedException x) {
                // Not closed by the transform: the carrier goes to the reject policy, nothing written.
                end();
                view.release();
                rejectFirst(setup.borrow(), x.profile(), x.recordMode(), x.reason(), hint.source(), establishStall(), x);
                return;
            }
            if (error != null || r == null) {
                fail(error != null ? error : new NullPointerException("inbound transform returned null"), null);
                return;
            }
            if ((r.reader() == null) != (given.reader() == null) || (r.writer() == null) != (given.writer() == null)
                    || r.framing() != given.framing()) {
                fail(new IllegalArgumentException("inbound transform changed the carrier's halves or framing"), r);
                return;
            }
            end();
            if (!r.variant().isEmpty()) value = new Admitted(value.value(), r.variant());
            Transport c = setup.borrow();
            if (same(r) && view.untouched()) {
                // The carrier itself: it goes on as it is, its input where the admission left it.
                view.release();
                if (next.start(c, value) && !c.isClosed() && !parked(c)) c.resumeInput();
                return;
            }
            var ci = c.info();
            io.zmux.carrier.PipeIo io = r.withCarrier(ci.carrier().kind()).withLabel(ci.carrier().label())
                    .withFateGroup(ci.fateGroup());
            Transport t;
            try {
                t = Transport.of(c.loop(), io, processing);
            } catch (IllegalArgumentException belowFloor) { // The transform's carrier is closed: refused, the listener goes on.
                owner.log(System.Logger.Level.DEBUG, "the transformed carrier takes records below the floor; refused", belowFloor);
                // What fail() does once end() ran: the result carrier is closed by Transport.of, the original aborts.
                setup.cancelStage(pending, belowFloor);
                view.abort(belowFloor);
                counts.setupFailed();
                return;
            }
            if (t == null) { view.abort(new java.io.IOException("transformed carrier closed")); counts.setupFailed(); return; }
            if (!c.setup().adoptTracked(t)) { t.reject(null); counts.setupFailed(); return; }
            next.start(t, value);
        }
    }
}
