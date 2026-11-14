package io.zmux.internal.loop;

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
        void started(SessionRuntime d, String applicationProtocol, io.zmux.CarrierCredential credential);
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
                fallback = new java.util.concurrent.atomic.AtomicLong(), stalled = new java.util.concurrent.atomic.AtomicLong(),
                closed = new java.util.concurrent.atomic.AtomicLong(), stallOverflow = new java.util.concurrent.atomic.AtomicLong(),
                peekTimedOut = new java.util.concurrent.atomic.AtomicLong(), handshakeFailed = new java.util.concurrent.atomic.AtomicLong();

        /** Per variant: accepted, fallback, stalled, closed. */
        static final int ACCEPTED = 0, FALLBACK = 1, STALLED = 2, CLOSED = 3;
        private final java.util.Map<String, long[]> variants = new java.util.TreeMap<>();
        private long[] variantOverflow;

        synchronized void variant(String name, int which) {
            if (name.isEmpty()) return;
            long[] c = variants.get(name);
            if (c == null) {
                if (variants.size() < Admission.MAX_VARIANTS) {
                    c = new long[4];
                    variants.put(name, c);
                } else {
                    if (variantOverflow == null) variantOverflow = new long[4];
                    c = variantOverflow;
                }
            }
            c[which]++;
        }

        /** A copy sorted by name, a bucket of further labels last. */
        public synchronized java.util.List<Admission.VariantStat> variantStats() {
            var out = new java.util.ArrayList<Admission.VariantStat>(variants.size() + 1);
            variants.forEach((n, c) -> out.add(new Admission.VariantStat(n, false, c[0], c[1], c[2], c[3])));
            if (variantOverflow != null) {
                long[] c = variantOverflow;
                out.add(new Admission.VariantStat("", true, c[0], c[1], c[2], c[3]));
            }
            return out;
        }

        void session() { sessions.incrementAndGet(); }
        void joined() { joins.incrementAndGet(); }
        /** A connection the listener could not make a pipe input of: a failed TLS handshake, HTTP or WebSocket upgrade. */
        public void setupFailed() { setupFailures.incrementAndGet(); }
        public void rejected() { rejected.incrementAndGet(); }
        public void refused() { refused.incrementAndGet(); }
        /** A started session the backlog had no room for: refused instead of a session. */
        public void sessionRefused() { refused.incrementAndGet(); sessions.decrementAndGet(); }
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
            return new io.zmux.AdmissionStats(accepted.get(), fallback.get(), stalled.get(), closed.get(), stallOverflow.get(),
                    peekTimedOut.get(), handshakeFailed.get());
        }
    }

    private final Counts counts = new Counts();

    /** How an input that started no session and joined nothing ended. */
    enum Outcome { REJECTED, REFUSED, TIMED_OUT }

    /** The listener's outcome counts; any thread. */
    public Counts counts() { return counts; }
    private final byte[] instanceId;
    private final CarrierAdmission admission;
    private final CarrierAdmission.SplitPairing pairing = new CarrierAdmission.SplitPairing();
    private final java.util.HashSet<PairHalf> pendingPairs = new java.util.HashSet<>();
    private final long firstRecordTimeout;
    private final ConcurrentHashMap<String, SessionRuntime> sessions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SessionRuntime> laneSessions = new ConcurrentHashMap<>();
    private final java.util.HashSet<Registration> registrations = new java.util.HashSet<>();
    private final SecureRandom random = new SecureRandom();
    private volatile boolean closed;
    /** Shut down: no new session; the registered ones still take their joins until they end. */
    private volatile boolean draining;
    private Runnable drained;
    private final int maxSuspended;
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
        boolean queued = driver.loop().tryExecute(() -> {
            synchronized (ListenerHub.this) {
                long n = resources.values().stream().filter(v -> v.state() == io.zmux.internal.core.SessionState.SUSPENDED).count();
                if (driver.core().state() == io.zmux.internal.core.SessionState.SUSPENDED
                        && (n > maxSuspended || (budget != null && budget.pressure()))) {
                    driver.core().closeResumeTimeout();
                    driver.markDirty();
                }
                driver.core().memory().charge();
                driver.publishResources();
                driver.markDirty();
                evictionPending = false;
            }
            reconcile();
        });
        if (!queued) {
            resources.remove(driver); evictionPending = false; reconcile();
        }
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
        this.admission = new CarrierAdmission(maxPendingPerSource, maxPending, firstRecordTimeout, sourcePrefixV4, sourcePrefixV6);
        this.firstRecordTimeout = firstRecordTimeout > 0 ? firstRecordTimeout : 2 * Time.SECOND;
    }

    /** Prepares all sessions owned by this endpoint before their transport parameters can be emitted. */
    public synchronized CoreConfig prepare(CoreConfig cfg) {
        if (closed || draining) throw new IllegalStateException("listener closed");
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
        byte[] lane = core.lanes().key(); String laneId = lane == null ? null : HexFormat.of().formatHex(lane);
        Registration registration = new Registration(driver, locator, laneId);
        synchronized (this) {
            if (closed || draining) throw new IllegalStateException("listener closed");
            if ((locator != null && sessions.containsKey(locator)) || (laneId != null && laneSessions.containsKey(laneId)))
                throw new IllegalStateException("duplicate session routing key");
            if (locator != null) sessions.put(locator, driver);
            if (laneId != null) laneSessions.put(laneId, driver);
            resources.put(driver, driver.resources()); registrations.add(registration);
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
        final SessionRuntime driver; final String locator, lane;
        final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
        final java.util.concurrent.atomic.AtomicReference<Runnable> observer = new java.util.concurrent.atomic.AtomicReference<>();
        Registration(SessionRuntime driver, String locator, String lane) { this.driver = driver; this.locator = locator; this.lane = lane; }
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
                if (lane != null) laneSessions.remove(lane, driver);
                resources.remove(driver); registrations.remove(this);
            }
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

    /**
     * Takes no new session from now on (a new session's first record and a split bootstrap are
     * refused); the registered sessions take their joins and resumptions until they end.
     * onDrained runs once, off the loops, when none is left (at once when there is none).
     */
    public void drain(Runnable onDrained) {
        synchronized (this) {
            if (closed) return;
            draining = true;
            drained = onDrained;
        }
        drainedIfEmpty();
    }

    private void drainedIfEmpty() {
        Runnable r;
        synchronized (this) {
            if (!draining || closed || !registrations.isEmpty() || drained == null) return;
            r = drained;
            drained = null;
        }
        Thread.ofVirtual().name("zmux-listener-drained").start(r);
    }

    /** Stops taking carriers; sessions already started are the owner's. */
    public void close() {
        java.util.List<PairHalf> halves;
        java.util.List<Registration> registered;
        java.util.List<JoinSlot> offered;
        synchronized (this) {
            closed = true; drained = null;
            halves = java.util.List.copyOf(pendingPairs); registered = java.util.List.copyOf(registrations);
            offered = java.util.List.copyOf(offeredSlots);
        }
        for (Registration registration : registered) registration.close();
        for (JoinSlot slot : offered) slot.listenerClosing();
        for (PairHalf half : halves) half.fail(Outcome.REFUSED);
        // The decisions under way are cancelled: the listener takes no carrier any more.
        for (Transformed t : java.util.List.copyOf(transforming)) t.abandon();
        for (Admitting a : java.util.List.copyOf(deciding)) {
            if (!a.carrier.loop().tryExecute(() -> a.abandon(counts.closed))) a.carrier.close(null);
        }
        java.util.List<Stalled> stalled;
        synchronized (this) { stalled = java.util.List.copyOf(stalls); }
        for (Stalled st : stalled) {
            if (!st.carrier.loop().tryExecute(() -> st.finish(true))) st.carrier.close(null);
        }
        management.close();
        resources.clear();
        sessions.clear();
        laneSessions.clear();
    }

    /**
     * Takes an external carrier whose source slot was reserved ({@link #reserve}): the
     * admission, if configured and the carrier was not admitted with its connection, then
     * its classification: by hint's pairing, or by its first record.
     * verdict (or null) receives the admission's verdict code, off the loop; fallback (or
     * null) takes a carrier the admission does not accept. Returns the carrier, for its
     * {@link Transport#whenTaken} and {@link Transport#whenClosed}.
     */
    public Transport external(SessionHost loop, io.zmux.carrier.PipeIo io, io.zmux.JoinHint hint, boolean admitted,
                              Object admittedValue, Forward fallback, CompletableFuture<Decided> verdict) {
        boolean pair = !hint.pairing().isEmpty();
        Transport carrier = pair ? new ExternalTransport(loop, io) : carrier(loop, io);
        var info = info(carrier, io, hint);
        InetSocketAddress source = hint.source();
        Next next;
        if (pair) {
            byte[] key = decodePairToken(hint.pairing());
            boolean shaped = carrier.directions() == (hint.uplink() ? Action.DIR_IN : Action.DIR_OUT);
            next = (c, value) -> {
                // A waiting half keeps its slots until the pair is complete.
                if (key == null || !shaped) { release(source); c.close(null); counts.rejected(); return false; }
                return new PairHalf((ExternalTransport) c, key, hint, source, value, info).begin();
            };
        } else {
            String alpn = io.capabilities().applicationProtocol();
            next = transformed(hint, info, (c, value) -> gate(c, source, alpn, value, info, ignored -> {}) != null);
        }
        if (!carrier.loop().tryExecute(() -> admit(carrier, info, admitted, new Admitted(admittedValue, ""), fallback, verdict, false, next))) {
            release(source);
            carrier.close(null);
            report(verdict, Admit.CLOSE, null);
            counts.refused();
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
                caps.exporter() == null ? null : carrier::export, hint.request(), io.isTerminatedTls());
    }

    private static byte[] decodePairToken(String value) {
        if (value == null || value.length() != 22) return null;
        try {
            byte[] bytes = java.util.Base64.getUrlDecoder().decode(value);
            return bytes.length == 16 && java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(bytes).equals(value) ? bytes : null;
        } catch (IllegalArgumentException invalid) { return null; }
    }

    private final class PairHalf implements Inbound {
        final ExternalTransport carrier;
        final byte[] token;
        final boolean uplink;
        final io.zmux.JoinHint hint;
        final InetSocketAddress source;
        /** The half's admission value. */
        final Admitted admitted;
        final Admission.CarrierInfo info;
        final Runnable shutdown = this::shutdownCarrier;
        final TimerHeap.Timer first = new TimerHeap.Timer() { @Override void fire(long now) { fail(Outcome.TIMED_OUT); } };
        final TimerHeap.Timer expiry = new TimerHeap.Timer() { @Override void fire(long now) { fail(Outcome.TIMED_OUT); } };
        PairHalf other;
        boolean firstReady;
        boolean transferRequested;
        boolean done;

        PairHalf(ExternalTransport carrier, byte[] token, io.zmux.JoinHint hint, InetSocketAddress source, Admitted admitted, Admission.CarrierInfo info) {
            this.info = info; this.hint = hint;
            this.carrier = carrier; this.token = token; this.uplink = hint.uplink(); this.source = source; this.admitted = admitted;
        }
        /** False when the half was refused (its carrier is closed). */
        boolean begin() {
            Outcome refused = null;
            synchronized (ListenerHub.this) {
                if (closed || draining) refused = Outcome.REFUSED; // a split bootstrap starts a session
                else {
                    var result = pairing.offer(token, uplink, this);
                    if (result == CarrierAdmission.SplitPairing.Result.REJECT) refused = Outcome.REJECTED;
                    else pendingPairs.add(this);
                    if (result == CarrierAdmission.SplitPairing.Result.DONE) {
                        other = (PairHalf) pairing.partner(); other.other = this;
                    }
                }
            }
            if (refused != null) { release(source); carrier.close(null); counts.count(refused); return false; }
            carrier.loop().onShutdown(shutdown);
            carrier.loop().schedule(expiry, Time.add(Clock.now(), owner.config().handshakeTimeout()));
            // A transform runs on the pair and reads the first bytes itself, an Establish among them.
            if (uplink && transform != null) {
                synchronized (ListenerHub.this) { firstReady = true; }
            } else if (uplink) {
                carrier.boundPendingInput(MAX_FIRST_RECORD + 8);
                carrier.start(this);
            }
            tryPair();
            return true;
        }
        private void shutdownCarrier() { fail(Outcome.REFUSED); carrier.close(null); }
        private void stopTimers() {
            Runnable stop = () -> {
                carrier.loop().removeShutdownHook(shutdown);
                carrier.loop().schedule(first, Time.NEVER); carrier.loop().schedule(expiry, Time.NEVER);
            };
            if (carrier.loop().inLoop()) stop.run(); else carrier.loop().tryExecute(stop);
        }
        /** Ends the half and, once paired, its partner: one input then, counted once as how says. */
        void fail(Outcome how) {
            PairHalf partner;
            synchronized (ListenerHub.this) {
                if (done) return;
                done = true; pendingPairs.remove(this); pairing.withdraw(token, this);
                partner = other;
                if (partner != null) { partner.done = true; pendingPairs.remove(partner); pairing.forget(token); }
                admission.release(source);
                if (partner != null) admission.release(partner.source);
            }
            counts.count(how);
            stopTimers();
            if (carrier.loop().inLoop()) carrier.close(null); else carrier.loop().tryExecute(() -> carrier.close(null));
            if (partner != null) {
                partner.stopTimers();
                if (partner.carrier.loop().inLoop()) partner.carrier.close(null);
                else partner.carrier.loop().tryExecute(() -> partner.carrier.close(null));
            }
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
            if (!up.carrier.loop().tryExecute(ready ? up::combine : up::armFirst)) up.fail(Outcome.REFUSED);
        }
        void armFirst() {
            synchronized (ListenerHub.this) { if (done || firstReady) return; }
            carrier.loop().schedule(first, Time.add(Clock.now(), firstRecordTimeout));
        }
        void combine() {
            PairHalf down;
            synchronized (ListenerHub.this) {
                if (done || transferRequested || other == null || !firstReady) return;
                transferRequested = true; down = other;
            }
            // Detach on the downlink's owner before handing it to this loop. Its shutdown
            // hook cannot race a check-and-transfer on another loop.
            if (!down.carrier.loop().tryExecute(() -> {
                synchronized (ListenerHub.this) { if (done || down.done) return; }
                down.stopTimers();
                ExternalTransport moved = (ExternalTransport) down.carrier.moveTo(carrier.loop);
                if (moved == null) { fail(Outcome.REJECTED); return; }
                if (!carrier.loop().tryExecute(() -> finishPair(down, moved))) { moved.close(null); fail(Outcome.REFUSED); }
            })) fail(Outcome.REFUSED);
        }
        void finishPair(PairHalf down, ExternalTransport moved) {
            boolean refused;
            synchronized (ListenerHub.this) {
                refused = done || down.done || closed;
                if (!refused) {
                    done = down.done = true; pendingPairs.remove(this); pendingPairs.remove(down);
                    // The pair is one carrier now: it goes on with the uplink's slots.
                    admission.release(down.source);
                }
            }
            if (refused) { moved.close(null); fail(Outcome.REFUSED); return; }
            stopTimers();
            ExternalTransport combined;
            try { combined = carrier.pairWith(moved); }
            catch (ArithmeticException invalid) { combined = null; }
            if (combined == null) {
                synchronized (ListenerHub.this) { pairing.forget(token); admission.release(source); }
                carrier.close(null); moved.close(null); counts.rejected(); return;
            }
            combined.unboundPendingInput();
            var adopted = new java.util.concurrent.atomic.AtomicBoolean();
            Runnable forget = () -> { synchronized (ListenerHub.this) { pairing.forget(token); } };
            combined.whenClosed().whenComplete((value, error) -> { if (!adopted.get()) forget.run(); });
            // The session's admission value is the uplink's, the half that carries the client's parameters.
            PairHalf up = uplink ? this : down;
            String alpn = combined.info().applicationProtocol();
            Admission.CarrierInfo paired = new Admission.CarrierInfo(up.info.source(), up.info.peer(), alpn, up.info.serverName(),
                    combined.exporterPresent() ? combined::export : null, up.info.request(), info.terminatedTls() && down.info.terminatedTls());
            Next session = (c, value) -> gate(c, source, alpn, value, paired,
                    driver -> { adopted.set(true); driver.closedFuture().whenComplete((v, error) -> forget.run()); }) != null;
            if (transform == null) session.start(combined, up.admitted);
            else new Transformed(up.hint, paired, session).start(combined, up.admitted);
        }
        @Override public int bytes(Transport c, ReadBuf buf, ByteBuffer bytes, int pos, int lim) {
            var record = CarrierAdmission.classify(bytes, pos, lim, MAX_FIRST_RECORD);
            if (record.kind() == CarrierAdmission.Kind.MORE || (record.kind() == CarrierAdmission.Kind.SESSION && record.firstRecordLength() == 0)) return 0;
            if (record.kind() != CarrierAdmission.Kind.SESSION && record.kind() != CarrierAdmission.Kind.JOIN) { fail(Outcome.REJECTED); return Inbound.GONE; }
            carrier.loop().schedule(first, Time.NEVER); carrier.pauseInput();
            synchronized (ListenerHub.this) { firstReady = true; }
            tryPair(); return 0;
        }
        @Override public void ended(Transport c, boolean graceful, boolean partial) { fail(Outcome.REJECTED); }
    }

    // ---- the source slot, the admission of new carriers ----

    /**
     * Takes the slots of a new carrier from source, its source's and one of the listener's,
     * under this lock: {@link CarrierAdmission#TAKEN}, {@link CarrierAdmission#SOURCE_FULL}, or
     * {@link CarrierAdmission#FULL} when every slot of the listener is taken and none is
     * stalled. A full listener closes its oldest stalled carrier to make room, not counted.
     */
    private int take(InetSocketAddress source) {
        int r = admission.take(source);
        if (r != CarrierAdmission.FULL || stalls.isEmpty()) return r;
        var first = stalls.iterator();
        Stalled oldest = first.next();
        first.remove();
        admission.unstall(oldest.source);
        if (!oldest.carrier.loop().tryExecute(() -> oldest.finish(false))) oldest.carrier.close(null);
        return admission.take(source);
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
    public boolean reserve(InetSocketAddress source) {
        int r;
        synchronized (this) { r = closed ? CarrierAdmission.SOURCE_FULL : take(source); }
        if (r != CarrierAdmission.TAKEN) notTaken(r);
        return r == CarrierAdmission.TAKEN;
    }

    /** Gives back the slots a carrier took. */
    public synchronized void release(InetSocketAddress source) { admission.release(source); }

    /** A reserved carrier whose outer handshake failed or timed out: its slot goes, and it counts. */
    public void handshakeFailed(InetSocketAddress source) {
        release(source);
        if (decider != null) counts.handshakeFailed.incrementAndGet(); else counts.setupFailed();
    }

    /** The verdict codes: a skip of at least 0 accepts. */
    public static final class Admit {
        public static final int FALLBACK = -1, STALL = -2, CLOSE = -3;
        private Admit() {}
    }

    /** A fallback target: takes a carrier that was not accepted, every byte read replayed; called off the loop. */
    @FunctionalInterface
    public interface Forward {
        void serve(io.zmux.carrier.PipeIo carrier, Admission.CarrierInfo info);
    }

    /** What an accepted carrier continues with, on its loop; false when the carrier was closed. */
    @FunctionalInterface
    private interface Next {
        boolean start(Transport c, Admitted admitted);
    }

    /** The outcome of a carrier's admission: a verdict code and an accept's value. */
    public record Decided(int code, Object value) {}

    private Admission.Decider decider;
    private Forward socketFallback;
    private int peek, maxPeek;
    private long peekTimeout, stallMin, stallMax;
    private boolean fallbackOnPeekTimeout;
    private final byte[] stallKey = new byte[32];
    /** The admissions waiting for their decision, cancelled when the listener stops taking carriers. */
    private final java.util.Set<Admitting> deciding = ConcurrentHashMap.newKeySet();
    /** The stalled carriers that hold their slots, in the order their stalls began; guarded by this. */
    private final java.util.LinkedHashSet<Stalled> stalls = new java.util.LinkedHashSet<>();

    /**
     * Configures the admission of every new carrier, before the first one; times in
     * nanoseconds. fallback (or null) is the target of socket and TLS carriers.
     */
    public void admission(Admission.Decider d, int peek, int maxPeek, long peekTimeout, boolean fallbackOnPeekTimeout, long stallMin, long stallMax, Forward fallback) {
        this.fallbackOnPeekTimeout = fallbackOnPeekTimeout;
        this.maxPeek = maxPeek;
        decider = d;
        socketFallback = fallback;
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
            return new io.zmux.adapter.JoinLookup(false, e.code() == io.zmux.seal.SecureCarrierException.Code.INCOMPLETE ? e.need() : 0);
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

    private static void closeQuietly(SocketChannel ch) {
        try { ch.close(); } catch (java.io.IOException ignored) { /* Gone. */ }
    }

    /**
     * Takes the slots of peer for an accepted socket; false when there are none. A source over its pending limit
     * (the first check of a join) goes to the reject policy as pending_limit; a stall needs a slot of this
     * same source, so while it has none the carrier is closed at once. A full listener closes it at once.
     */
    private boolean slotOrReject(EventLoop loop, SocketChannel ch, InetSocketAddress peer) {
        int r;
        boolean open;
        synchronized (this) { open = !closed; r = open ? take(peer) : CarrierAdmission.SOURCE_FULL; }
        if (r == CarrierAdmission.TAKEN) return true;
        if (r == CarrierAdmission.FULL) {
            closeQuietly(ch);
            counts.rejected();
        } else if (!open || !loop.tryExecute(() -> rejectFirst(new SocketTransport(loop, ch), 0, null, "pending_limit", peer, establishStall()))) {
            closeQuietly(ch);
            counts.refused();
        }
        return false;
    }

    /** Takes a TCP carrier from peer: its slot, the admission, the first record. */
    public void socket(EventLoop loop, SocketChannel ch, InetSocketAddress peer) {
        if (!slotOrReject(loop, ch, peer)) return;
        var info = new Admission.CarrierInfo(peer, peer, "", "", null, io.zmux.RequestHead.NONE);
        Next next = transformed(io.zmux.JoinHint.inBand(peer), info, (c, value) -> gate(c, peer, "", value, info, ignored -> {}) != null);
        if (!loop.tryExecute(() -> admit(new SocketTransport(loop, ch), info, false, Admitted.NONE, socketFallback, null, false, next))) {
            release(peer);
            closeQuietly(ch);
            counts.refused();
        }
    }

    /** Takes a TLS carrier from peer: its slot, the handshake within handshakeTimeout, the admission, the first record. */
    public void tls(EventLoop loop, SocketChannel ch, SSLEngine engine, long handshakeTimeout, boolean secure, InetSocketAddress peer) {
        if (!slotOrReject(loop, ch, peer)) return;
        TlsTransport.handshake(loop, ch, engine, handshakeTimeout, secure, (c, err) -> {
            if (err != null) { handshakeFailed(peer); return; }
            String alpn = c.info().applicationProtocol();
            var info = new Admission.CarrierInfo(peer, peer, alpn, c.serverName(), c::export, io.zmux.RequestHead.NONE, true);
            admit(c, info, false, Admitted.NONE, socketFallback, null, false,
                    transformed(io.zmux.JoinHint.inBand(peer), info, (x, value) -> gate(x, peer, alpn, value, info, ignored -> {}) != null));
        });
    }

    /** The admission verdict code: 0 for any accept. */
    private static void report(CompletableFuture<Decided> verdict, int v, Object value) {
        if (verdict != null) Callbacks.EXECUTOR.execute(() -> verdict.complete(new Decided(Math.min(v, 0), v >= 0 ? value : null)));
    }

    /**
     * On the carrier's loop, its slot held: the admission, if configured, then next. A
     * deferred next moves the carrier, so it runs only once the skipped bytes are consumed.
     * A carrier decided on its request head is decided at once, without a prefix.
     */
    private void admit(Transport c, Admission.CarrierInfo info, boolean admitted, Admitted admittedValue, Forward fallback,
                       CompletableFuture<Decided> verdict, boolean deferred, Next next) {
        if (decider == null || admitted) {
            Admitted value = decider == null ? Admitted.NONE : admittedValue;
            report(verdict, 0, value.value());
            next.start(c, value);
            return;
        }
        boolean refused;
        synchronized (this) { refused = closed; }
        if (refused) {
            release(info.source());
            c.close(null);
            report(verdict, Admit.CLOSE, null);
            counts.refused();
            return;
        }
        Admitting a = new Admitting(c, info, fallback, verdict, deferred, next, Time.add(Clock.now(), peekTimeout));
        c.loop().onShutdown(a.shutdown);
        c.loop().schedule(a, a.due);
        if (peek == 0 || info.head() || (c.directions() & Action.DIR_IN) == 0) a.decide();
        else {
            if (a.messages) c.limitFirstMessage(Math.max(peek, maxPeek));
            c.start(a);
        }
    }

    /** Holds a carrier's input until a deferred next has run; closes it when the input ends first. */
    private static final Inbound HOLD = new Inbound() {
        @Override public int bytes(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim) { return 0; }
        @Override public void ended(Transport c, boolean graceful, boolean partial) { c.close(null); }
    };

    /**
     * A carrier under admission: it reads the prefix, keeping every byte in the read buffer,
     * then pauses its input while the decider decides, and acts on the verdict on its loop.
     * Its timer is the peek timeout, which bounds the reading and the decision.
     */
    private final class Admitting extends TimerHeap.Timer implements Inbound {
        final Transport carrier;
        final Admission.CarrierInfo info;
        final InetSocketAddress source;
        final boolean deferred;
        final Forward fallback;
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
        /** The first message is longer than the limit: its decision is closing. */
        private boolean oversized;
        private boolean timedOut;
        private boolean decidingNow, done, paused, ended, graceful = true;
        private CompletableFuture<Admission.Decision> pending;
        final Runnable shutdown;

        Admitting(Transport c, Admission.CarrierInfo info, Forward fallback,
                  CompletableFuture<Decided> verdict, boolean deferred, Next next, long due) {
            carrier = c;
            this.info = info;
            this.source = info.source();
            this.fallback = fallback;
            this.verdict = verdict;
            this.deferred = deferred;
            this.next = next;
            this.due = due;
            shutdown = () -> abandon(counts.closed);
            messages = c.info().carrier().framing() == io.zmux.internal.core.Framing.RECORDS;
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
            if (len > Math.max(peek, maxPeek)) {
                oversized = true;
                return true;
            }
            return seen.length >= sn + len;
        }

        @Override
        public int bytes(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim) {
            if (done) return 0;
            seen = new byte[lim - pos];
            data.get(pos, seen);
            if (!decidingNow && enough()) {
                c.pauseInput();
                paused = true;
                decide();
            }
            return 0;
        }

        @Override
        @SuppressWarnings("ReferenceEquality") // RequestHead.NONE is a sentinel
        void fire(long now) {
            if (done) return;
            if (!decidingNow) { // the deadline passed
                // A byte stream whose prefix is late goes to the fallback with the bytes read.
                if (fallbackOnPeekTimeout && !messages && info.request() == io.zmux.RequestHead.NONE && !ended && seen.length < want) {
                    counts.peekTimedOut.incrementAndGet();
                    decided(Admission.Decision.FALLBACK);
                    return;
                }
                // A prefix the decider said is too short gets no second decision.
                if (want > peek) { abandon(counts.peekTimedOut); return; }
                timedOut = true; // what was read is the prefix
                decide();
            } else abandon(counts.peekTimedOut);
        }

        @Override
        public void ended(Transport c, boolean graceful, boolean partial) {
            if (done) return;
            ended = true;
            this.graceful = graceful;
            if (!decidingNow) decide();
        }

        /** Asks the decider, outside every lock; its stage completes on any thread. */
        void decide() {
            decidingNow = true;
            if (oversized) {
                decided(Admission.Decision.CLOSE);
                return;
            }
            prefixLength = messages ? messagePrefix() : Math.min(want, seen.length);
            ByteBuffer prefix = ByteBuffer.wrap(seen, prefixAt, prefixLength).slice().asReadOnlyBuffer();
            var deadline = new Admission.Deadline(System.nanoTime() + Math.max(0, due - Clock.now()));
            CompletableFuture<Admission.Decision> f;
            try {
                var stage = decider.decide(info, prefix, deadline);
                f = stage == null ? CompletableFuture.completedFuture(null) : stage.toCompletableFuture();
            } catch (RuntimeException failed) {
                f = CompletableFuture.failedFuture(failed);
            }
            pending = f;
            if (!f.isDone() && Clock.now() >= due) { abandon(counts.peekTimedOut); return; }
            deciding.add(this);
            // A close that ran while the decider decided saw no registration: closed is written
            // before close reads deciding, and read here after the add, so one of the two acts.
            if (closed) { abandon(counts.closed); return; }
            f.whenComplete((d, e) -> {
                if (!carrier.loop().tryExecute(() -> decided(e == null ? d : null))) carrier.close(null);
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

        /** Asks the decider again for a longer prefix; false when the verdict stands as closing. */
        private boolean more(Admission.Decision d) {
            if (messages || ended || timedOut || d.need() <= prefixLength || d.need() > maxPeek) return false;
            deciding.remove(this);
            pending = null;
            decidingNow = false;
            want = d.need();
            if (enough()) {
                decide();
            } else if (paused) {
                paused = false;
                carrier.resumeInput();
            } else {
                carrier.redeliver();
            }
            return true;
        }

        /** Ends the admission undecided: cancelled, timed out, or the listener stopped. */
        void abandon(java.util.concurrent.atomic.AtomicLong count) {
            if (done) return;
            finish();
            count.incrementAndGet();
            if (pending != null) pending.cancel(false);
            release(source);
            carrier.close(null);
            report(verdict, Admit.CLOSE, null);
        }

        private void finish() {
            done = true;
            deciding.remove(this);
            carrier.loop().schedule(this, Time.NEVER);
            carrier.loop().removeShutdownHook(shutdown);
        }

        /** Resumes the paused input: the bytes kept in the read buffer are delivered again. */
        private void resume() {
            if (paused) { paused = false; carrier.resumeInput(); }
            else carrier.redeliver();
        }

        /** Acts on the decision (null: none, a failure), on the carrier's loop. */
        void decided(Admission.Decision d) {
            if (done) return;
            if (d != null && d.verdict() == AdmissionVerdict.MORE && more(d)) return;
            finish();
            String variant = d == null ? "" : d.variant();
            if (d != null && d.verdict() == AdmissionVerdict.ACCEPT) value = new Admitted(d.value(), variant);
            int v = d == null ? Admit.CLOSE : switch (d.verdict()) {
                case ACCEPT -> skip(d.skip());
                case FALLBACK -> fallback == null ? Admit.STALL : Admit.FALLBACK;
                case STALL -> Admit.STALL;
                case CLOSE, MORE -> Admit.CLOSE;
            };
            if (v == Admit.CLOSE) {
                counts.closed.incrementAndGet();
                counts.variant(variant, Counts.CLOSED);
                release(source);
                carrier.close(null);
                report(verdict, v, null);
                return;
            }
            if (v == Admit.STALL) {
                // A fallback without a target stalls but counts as a fallback.
                boolean asFallback = d.verdict() == AdmissionVerdict.FALLBACK;
                var counter = asFallback ? counts.fallback : counts.stalled;
                if (ended) {
                    // The peer already ended it: the stall is over at once.
                    counter.incrementAndGet();
                    counts.variant(variant, asFallback ? Counts.FALLBACK : Counts.STALLED);
                    release(source);
                    carrier.shutdownOutput();
                    carrier.close(null);
                    report(verdict, v, null);
                    return;
                }
                boolean room;
                Stalled s = new Stalled(carrier, source);
                synchronized (ListenerHub.this) {
                    // Registered with the room taken, under the lock close() takes: a close after it ends the stall.
                    room = !closed && admission.stall(source);
                    if (room) stalls.add(s);
                }
                if (!room) {
                    counts.stallOverflow.incrementAndGet();
                    counts.variant(variant, Counts.STALLED);
                    release(source);
                    carrier.close(null);
                    report(verdict, Admit.CLOSE, null);
                    return;
                }
                counter.incrementAndGet();
                counts.variant(variant, asFallback ? Counts.FALLBACK : Counts.STALLED);
                report(verdict, v, value.value());
                carrier.loop().onShutdown(s.shutdown);
                carrier.loop().schedule(s, Time.add(Clock.now(), stallDuration(source)));
                carrier.start(s);
                resume();
                return;
            }
            (v == Admit.FALLBACK ? counts.fallback : counts.accepted).incrementAndGet();
            counts.variant(variant, v == Admit.FALLBACK ? Counts.FALLBACK : Counts.ACCEPTED);
            report(verdict, v, value.value());
            if (v == Admit.FALLBACK) {
                release(source);
                // The bytes read stay in the read buffer (or, once the input ended, in seen).
                forward(carrier, info, fallback, ended ? seen : null, ended, graceful);
                return;
            }
            if (ended) {
                acceptEnded(v, graceful);
                return;
            }
            if (v > 0) {
                carrier.rebind(new Skip(v));
                resume();
                return;
            }
            Transport c = carrier;
            if (!next.start(c, value)) return;
            if (!c.isClosed() && c.inbound() != this && !parked(c)) resume();
        }

        /** After the skipped bytes of a live delivery: next; the count consumed of [pos, lim), or GONE. */
        int proceed(ReadBuf buf, ByteBuffer data, int pos, int lim) {
            Transport c = carrier;
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
            Transport c = carrier;
            c.rebind(HOLD);
            c.pauseInput();
            if (!c.loop().tryExecute(() -> {
                if (c.inbound() != HOLD || c.isClosed()) return;
                if (next.start(c, value) && !c.isClosed() && c.inbound() != HOLD && !parked(c)) c.resumeInput();
            })) {
                c.close(null);
                return Inbound.GONE;
            }
            return 0;
        }

        /** An accept after the input ended: next takes the prefix after the skip, then the end. */
        private void acceptEnded(int skip, boolean graceful) {
            Transport c = carrier;
            if (next instanceof Transformed t) {
                // The transform takes the prefix after the skip, then the end.
                t.begin(c, value, java.util.Arrays.copyOfRange(seen, skip, seen.length), true, graceful);
                return;
            }
            if (deferred) {
                release(source);
                c.close(null);
                return;
            }
            if (!next.start(c, value)) return;
            if (c.inbound() == this || c.isClosed()) {
                c.close(null);
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

            @Override public void ended(Transport c, boolean graceful, boolean partial) { release(source); c.close(null); }
        }
    }

    /** Hands a carrier that was not accepted to target, with replay (or what its read buffer holds) in front. */
    private static void forward(Transport c, Admission.CarrierInfo info, Forward target, byte[] replay, boolean ended, boolean graceful) {
        Forwarded f = new Forwarded(c, replay, ended, graceful);
        io.zmux.carrier.PipeIo io = f.pipe(info.applicationProtocol());
        Callbacks.EXECUTOR.execute(() -> {
            try {
                target.serve(io, info);
            } catch (RuntimeException e) {
                f.abort(e);
            }
        });
    }

    /**
     * A stalled carrier, its slots held while it is in {@link #stalls}: what arrives is discarded;
     * it ends when the peer ends it, its duration passes, the listener closes, or a new carrier
     * takes its slot of a full listener.
     */
    private final class Stalled extends TimerHeap.Timer implements Inbound {
        final Transport carrier;
        final InetSocketAddress source;
        private boolean done;
        final Runnable shutdown = () -> finish(false);

        Stalled(Transport c, InetSocketAddress source) { carrier = c; this.source = source; }

        private void finish(boolean graceful) {
            if (done) return;
            done = true;
            carrier.loop().schedule(this, Time.NEVER);
            carrier.loop().removeShutdownHook(shutdown);
            synchronized (ListenerHub.this) {
                if (stalls.remove(this)) admission.unstall(source);
            }
            if (graceful) carrier.shutdownOutput();
            carrier.close(null);
        }

        @Override public int bytes(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim) { return lim - pos; }
        @Override public void ended(Transport c, boolean graceful, boolean partial) { finish(true); }
        @Override void fire(long now) { finish(true); }
    }

    /**
     * On the carrier's loop, its slots held: the first-record gate; null when the carrier was
     * closed. started observes a session the carrier starts.
     */
    private Pending gate(Transport c, InetSocketAddress source, String alpn, Admitted admitted, Admission.CarrierInfo info,
                         java.util.function.Consumer<SessionRuntime> started) {
        if (closed) {
            release(source);
            c.close(null);
            counts.refused();
            return null;
        }
        Pending p = new Pending(c, source, alpn, admitted, info, started);
        c.loop().onShutdown(p.shutdown);
        c.loop().schedule(p, Time.add(Clock.now(), firstRecordTimeout));
        c.start(p);
        return p;
    }

    /** A carrier waiting for its first record: its admission slot and first-record timer. */
    private final class Pending extends TimerHeap.Timer implements Inbound {
        final Transport carrier;
        final InetSocketAddress source;
        final String alpn;
        final Admitted admitted;
        final Admission.CarrierInfo info;
        final java.util.function.Consumer<SessionRuntime> started;
        boolean done;
        final Runnable shutdown = () -> ended(null, false, false);

        Pending(Transport carrier, InetSocketAddress source, String alpn, Admitted admitted, Admission.CarrierInfo info, java.util.function.Consumer<SessionRuntime> started) {
            this.info = info;
            this.carrier = carrier;
            this.source = source;
            this.alpn = alpn;
            this.admitted = admitted;
            this.started = started;
        }

        /** Ends the pending phase: the timer goes. */
        private void classified() {
            done = true;
            carrier.loop().schedule(this, Time.NEVER);
            carrier.loop().removeShutdownHook(shutdown);
        }

        /** Ends the pending phase of a carrier that holds no join: the slot goes too. */
        private void decided() {
            classified();
            release(source);
        }

        @Override
        void fire(long now) {
            if (done) return;
            decided();
            carrier.close(null);
            counts.timedOut();
        }

        @Override
        public void ended(Transport c, boolean graceful, boolean partial) {
            if (done) return;
            decided();
            carrier.close(null);
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
                    if (e.code() == io.zmux.seal.SecureCarrierException.Code.INCOMPLETE) return 0;
                    bad = "malformed"; // whatever the class: the local reasons of a Join name no parse failure finer
                }
                classified(); // The join keeps the slot until it has its result.
                if (bad != null) {
                    release(source);
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
                    decided();
                    c.close(null);
                    counts.rejected();
                    return Inbound.GONE;
                }
            }
            if (cl.kind() == CarrierAdmission.Kind.SESSION) {
                classified(); // The new session keeps the slots until its transport parameters are exchanged.
                return startSession(c, buf, data, pos, lim, source, alpn, admitted, info, started);
            }
            int sn = VarInt.lengthOf(data.get(pos));
            byte[] body = new byte[cl.firstRecordLength() - sn];
            data.get(pos + sn, body);
            decided();
            var frame = CarrierAdmission.parseLane(body);
            SessionRuntime target = frame == null ? null : laneSessions.get(HexFormat.of().formatHex(CarrierAdmission.laneKey(frame)));
            if (target == null) { c.close(null); counts.rejected(); } else transferLane(c, body, target);
            return Inbound.GONE;
        }
    }

    /** Starts a session on c, which holds the slots of source: the session gives them back once established, or ended. */
    private int startSession(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim, InetSocketAddress source, String alpn,
                             Admitted admitted, Admission.CarrierInfo info, java.util.function.Consumer<SessionRuntime> started) {
        if (closed) {
            release(source);
            c.close(null);
            counts.refused();
            return Inbound.GONE;
        }
        // Refused by policy: the session starts, unregistered, and ends with CONNECTION_REFUSED
        // after its own parameters.
        String refusal = draining ? "draining" : budget != null && budget.pressure() ? "memory budget" : null;
        Chosen chosen;
        CoreConfig cfg;
        try {
            chosen = owner.choose(info, admitted.value());
        } catch (RuntimeException failed) {
            owner.log(System.Logger.Level.WARNING, "the listener's sessionConfigs failed; the carrier is closed", failed);
            release(source);
            c.close(null);
            counts.rejected();
            return Inbound.GONE;
        }
        try {
            cfg = refusal != null ? chosen.config().toBuilder().budget(budget).build() : prepare(chosen.config());
        } catch (IllegalStateException ended) { release(source); c.close(null); counts.refused(); return Inbound.GONE; }
        try {
            cfg = Bootstrap.withExporter(cfg, c);
        } catch (IllegalArgumentException noExporter) {
            release(source);
            counts.rejected();
            return Inbound.GONE;
        }
        SessionRuntime d = new SessionRuntime(c.loop(), new Session(cfg, Clock.now()), chosen.hooks());
        // An Establish's peer is the session's host value, in place of the admission's.
        d.admitted(c.carrierPeer() != null ? c.carrierPeer() : admitted.value());
        d.bootstrapVariant(admitted.variant());
        d.holdSlots(() -> release(source));
        if (refusal != null) {
            counts.refused();
            int r = d.attachBootstrap(c).bytes(c, buf, data, pos, lim);
            d.post(() -> d.core().refuse(refusal));
            return r;
        }
        try { register(d); }
        catch (IllegalStateException ended) { c.close(null); d.failInternalNow(ended); counts.refused(); return Inbound.GONE; }
        counts.session();
        owner.started(d, alpn, chosen.credential());
        started.accept(d);
        return d.attachBootstrap(c).bytes(c, buf, data, pos, lim);
    }

    /** Hands a lane to its session's loop: joins when its pipe accepts it, rejected when it names none. */
    private void transferLane(Transport carrier, byte[] body, SessionRuntime driver) {
        Transport moved = carrier.moveTo(driver.loop());
        if (moved == null) { counts.rejected(); return; }
        if (!driver.loop().tryExecute(() -> {
            if (driver.acceptLane(moved, body) != null) counts.joined(); else counts.rejected();
        })) { moved.close(null); counts.refused(); }
    }

    /** An inbound lane of a known session, bounded by its carrier's stream credit. */
    static void laneInput(SessionRuntime driver, io.zmux.carrier.PipeIo io) {
        Transport carrier = Transport.of(driver.loop(), io);
        if (carrier == null) return;
        if (!driver.loop().tryExecute(() -> {
            if (driver.finished()) { carrier.close(null); return; }
            class LaneGate extends TimerHeap.Timer implements Inbound {
                final Runnable shutdown = () -> ended(carrier, false, false);
                private void stop() {
                    driver.loop().schedule(this, Time.NEVER);
                    driver.loop().removeShutdownHook(shutdown);
                }
                @Override void fire(long now) { ended(carrier, false, false); }
                @Override public int bytes(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim) {
                    var cl = CarrierAdmission.classify(data, pos, lim, MAX_FIRST_RECORD);
                    if (cl.kind() == CarrierAdmission.Kind.MORE || (cl.kind() == CarrierAdmission.Kind.LANE && cl.firstRecordLength() == 0)) return 0;
                    stop();
                    if (cl.kind() != CarrierAdmission.Kind.LANE) { c.close(null); return Inbound.GONE; }
                    int size = VarInt.lengthOf(data.get(pos));
                    byte[] body = new byte[cl.firstRecordLength() - size]; data.get(pos + size, body);
                    Inbound in = driver.acceptLane(c, body);
                    return in == null ? Inbound.GONE : in.bytes(c, buf, data, pos, lim);
                }
                @Override public void ended(Transport c, boolean graceful, boolean partial) { stop(); carrier.close(null); }
            }
            LaneGate gate = new LaneGate();
            driver.loop().onShutdown(gate.shutdown);
            driver.loop().schedule(gate, Time.add(Clock.now(), driver.core().joinDeadline()));
            carrier.start(gate);
        })) carrier.close(null);
    }

    private static final int HELLO_MORE = -2;

    /** Stalls a rejected carrier, silent, for duration (nanoseconds); false when its source has no room. */
    private boolean stallRejected(Transport c, InetSocketAddress source, long duration) {
        Stalled s = new Stalled(c, source);
        boolean room;
        synchronized (this) {
            // The join's slot went with its result: a stall holds one of its own, never by closing another stall.
            room = !closed && admission.take(source) == CarrierAdmission.TAKEN;
            if (room && !admission.stall(source)) {
                admission.release(source);
                room = false;
            }
            if (room) stalls.add(s);
        }
        if (!room) return false;
        c.taken();
        c.loop().onShutdown(s.shutdown);
        c.loop().schedule(s, Time.add(Clock.now(), duration));
        c.start(s);
        c.resumeInput();
        return true;
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
     * once as stall_overflow, whatever the reason.
     */
    private void rejectFirst(Transport c, int profile, io.zmux.RecordMode mode, String reason, InetSocketAddress source, long stall) {
        counts.rejected();
        var f = onReject;
        io.zmux.RejectInfo.Action act = profile == io.zmux.internal.join.Hello.PROFILE_JOIN
                ? io.zmux.RejectInfo.Action.CLOSE : io.zmux.RejectInfo.Action.STALL;
        if (f != null) {
            try {
                act = f.apply(new io.zmux.RejectInfo(profile, mode, reason, source));
            } catch (RuntimeException failed) {
                act = io.zmux.RejectInfo.Action.CLOSE;
                owner.log(System.Logger.Level.WARNING, "the listener's onReject failed; the carrier is closed", failed);
            }
        }
        if (act != io.zmux.RejectInfo.Action.STALL) { c.close(null); return; }
        if (!stallRejected(c, source, stall)) {
            counts.stallOverflow.incrementAndGet();
            c.close(null);
        }
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

    /** Sets the Unconfirmed join carriers one session keeps, before the first carrier. */
    public void maxPendingPerSession(int n) { maxPendingPerSession = n; }

    /** Configures the listener's answer to a rejected Hello, before the first carrier. */
    public void onReject(java.util.function.Function<io.zmux.RejectInfo, io.zmux.RejectInfo.Action> f) { onReject = f; }

    /** Hands a joining carrier to the session its Hello names. */
    private void join(Transport c, io.zmux.internal.join.Hello h, int skip, Admitted admitted, InetSocketAddress source) {
        JoinSlot slot = new JoinSlot(source);
        long slotAt = Clock.now();
        if (closed) { slot.settled(); c.close(null); counts.refused(); return; }
        io.zmux.RecordMode mode = io.zmux.internal.join.Mode.record(h.mode());
        SessionRuntime d = sessions.get(HexFormat.of().formatHex(h.locator()));
        if (d == null) {
            slot.settled();
            rejectJoin(c, mode, "unknown_locator", source);
            return;
        }
        // A carrier without the contract has no Binding: the session refuses the Hello as auth, whatever the joiner mixed.
        byte[] binding = h.mode() == io.zmux.internal.join.Mode.BOUND && c.bindable() ? c.export(io.zmux.internal.join.Kdf.EXPORTER_JOIN, null, io.zmux.internal.join.Kdf.BINDING_LEN) : null;
        Transport moved = c.moveTo(d.loop());
        if (moved == null) { slot.settled(); counts.rejected(); return; }
        // Closing the listener ends the handshake.
        if (!slot.offered(d)) { slot.settled(); moved.close(null); counts.refused(); return; }
        boolean ok = d.loop().tryExecute(() -> {
            // Under pressure only a join that lets a suspended session resume.
            if (budget != null && budget.pressure() && d.core().state() != io.zmux.internal.core.SessionState.SUSPENDED) {
                slot.settled();
                rejectJoin(moved, mode, "memory_pressure", source);
            } else {
                d.acceptHello(new SessionRuntime.OfferedHello(moved, h, skip, binding, admitted.value(), admitted.variant(), counts, slot,
                        (t, reason) -> rejectJoin(t, mode, reason, source), slotAt, maxPendingPerSession));
            }
        });
        if (!ok) { slot.settled(); moved.close(null); counts.refused(); }
    }

    /**
     * What a join holds of the listener until it has its result: its slots, its source's and one
     * of the listener's. Any thread; idempotent.
     */
    final class JoinSlot {
        private final InetSocketAddress source;
        private boolean settled;

        /** The session handling the join, whose handshake the listener's close ends. */
        private SessionRuntime session;

        JoinSlot(InetSocketAddress source) { this.source = source; }

        /** The join goes to session; false when the listener closed. */
        boolean offered(SessionRuntime session) {
            synchronized (ListenerHub.this) {
                if (closed || settled) return false;
                this.session = session;
                offeredSlots.add(this);
                return true;
            }
        }

        boolean listenerClosed() { return closed; }

        void listenerClosing() {
            if (!session.loop().tryExecute(() -> session.endOffered(this))) settled();
        }

        /** The join has its result: accepted, refused or closed. */
        void settled() {
            synchronized (ListenerHub.this) {
                if (settled) return;
                settled = true;
                offeredSlots.remove(this);
                admission.release(source);
            }
        }
    }

    /** The joins in a session's handshake; guarded by this. */
    private final java.util.Set<JoinSlot> offeredSlots = new java.util.HashSet<>();

    /** The joins routed to a session and still in its handshake. */
    public synchronized int offeredJoins() { return offeredSlots.size(); }

    // ---- the inbound transform ----

    private io.zmux.InboundTransform transform;
    /** The transforms under way, cancelled when the listener stops taking carriers. */
    private final java.util.Set<Transformed> transforming = ConcurrentHashMap.newKeySet();

    /** Configures the inbound transform (or null), before the first carrier. */
    public void inbound(io.zmux.InboundTransform t) {
        transform = t;
    }

    private Next transformed(io.zmux.JoinHint hint, Admission.CarrierInfo info, Next next) {
        return transform == null ? next : new Transformed(hint, info, next);
    }

    /** Whether c waits in a transform's view: its input resumes when the transform reads. */
    private static boolean parked(Transport c) { return c.inbound() instanceof Forwarded; }

    private static void closeIo(io.zmux.carrier.PipeIo io, Throwable cause) {
        if (io == null) return;
        // The host's code: never on the loop.
        Callbacks.EXECUTOR.execute(() -> {
            try { if (io.reader() != null) io.reader().close(); }
            catch (RuntimeException ignored) { /* Closing anyway. */ }
            finally {
                try { if (io.writer() != null) io.writer().abort(cause); } catch (RuntimeException ignored) { /* Closing anyway. */ }
            }
        });
    }

    /**
     * The transform of one accepted carrier, its slot held: the carrier is given to the
     * transform as a pipe input of its framing, then the result goes on to next. The timer is
     * the handshake timeout.
     */
    private final class Transformed extends TimerHeap.Timer implements Next {
        final io.zmux.JoinHint hint;
        final Admission.CarrierInfo info;
        final Next next;
        private Transport carrier;
        private Forwarded view;
        private io.zmux.carrier.PipeIo given;
        private Admitted value;
        private CompletableFuture<io.zmux.carrier.PipeIo> pending;
        private boolean done;
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
            carrier = c;
            value = v;
            c.taken(); // the layer may read and write on it
            var ci = c.info();
            boolean messages = ci.carrier().framing() == io.zmux.internal.core.Framing.RECORDS;
            view = new Forwarded(c, replay, ended, graceful, messages);
            var caps = io.zmux.carrier.PipeIo.Capabilities.builder().exporter(info.exporter()).receiveMemory(Forwarded.AHEAD)
                    .applicationProtocol(info.applicationProtocol()).serverName(info.serverName()).fateGroup(ci.fateGroup())
                    .writeOverhead(ci.carrier().writeOverhead()).terminatedTls(info.terminatedTls()).secure(c.isSecure())
                    .handshakePadding(c.handshakePadding()).build();
            given = PipeIoAccess.get().withCarrierPeer(view.pipe(info.applicationProtocol()).withCapabilities(caps).withLabel(ci.carrier().label())
                    .withCarrierConn(ci.connection()).withCarrier(ci.carrier().kind()).withVariant(v.variant()), c.carrierPeer());
            c.loop().onShutdown(shutdown);
            long due = Time.add(Clock.now(), owner.config().handshakeTimeout());
            c.loop().schedule(this, due);
            transforming.add(this);
            if (closed) { fail(new java.io.IOException("listener closed"), null); return; }
            var deadline = new Admission.Deadline(System.nanoTime() + Math.max(0, due - Clock.now()));
            CompletableFuture<io.zmux.carrier.PipeIo> f;
            try {
                var stage = transform.transform(given, new io.zmux.InboundTransform.Context(hint, java.util.Optional.ofNullable(v.value()), deadline));
                f = stage == null ? CompletableFuture.completedFuture(null) : stage.toCompletableFuture();
            } catch (RuntimeException e) {
                f = CompletableFuture.failedFuture(e);
            }
            pending = f;
            f.whenComplete((r, e) -> {
                if (!c.loop().tryExecute(() -> finish(e == null ? r : null, e))) {
                    if (r != null && !same(r)) closeIo(r, new java.io.IOException("event loop closed"));
                }
            });
        }

        private boolean same(io.zmux.carrier.PipeIo r) { return r.reader() == given.reader() && r.writer() == given.writer(); }

        @Override void fire(long now) { fail(new java.util.concurrent.TimeoutException("inbound transform timed out"), null); }

        /** The listener stopped taking carriers; from any thread. */
        void abandon() {
            if (!carrier.loop().tryExecute(() -> fail(new java.io.IOException("listener closed"), null))) carrier.close(null);
        }

        private boolean end() {
            if (done) return false;
            done = true;
            transforming.remove(this);
            carrier.loop().schedule(this, Time.NEVER);
            carrier.loop().removeShutdownHook(shutdown);
            return true;
        }

        /** Closes the carrier (and result r, unless it is the carrier) and counts a setup failure. */
        private void fail(Throwable cause, io.zmux.carrier.PipeIo r) {
            if (!end()) { if (r != null && !same(r)) closeIo(r, cause); return; }
            if (pending != null) pending.cancel(false);
            if (r != null && !same(r)) closeIo(r, cause);
            release(hint.source());
            view.abort(cause);
            counts.setupFailed();
        }

        private void finish(io.zmux.carrier.PipeIo r, Throwable error) {
            if (done) { if (r != null && !same(r)) closeIo(r, new java.io.IOException("inbound transform ended")); return; }
            if ((error instanceof java.util.concurrent.CompletionException w ? w.getCause() : error) instanceof io.zmux.CarrierRejectedException x) {
                // Not closed by the transform: the carrier goes to the reject policy, nothing written.
                end();
                view.release();
                release(hint.source());
                rejectFirst(carrier, x.profile(), x.recordMode(), x.reason(), hint.source(), establishStall());
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
            Transport c = carrier;
            if (same(r) && view.untouched()) {
                // The carrier itself: it goes on as it is, its input where the admission left it.
                view.release();
                if (next.start(c, value) && !c.isClosed() && !parked(c)) c.resumeInput();
                return;
            }
            var ci = c.info();
            io.zmux.carrier.PipeIo io = r.withCarrier(ci.carrier().kind()).withLabel(ci.carrier().label())
                    .withFateGroup(ci.fateGroup());
            next.start(Transport.of(c.loop(), io), value);
        }
    }
}
