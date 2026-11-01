package io.zmux.internal.loop;

import io.zmux.Admission;
import io.zmux.internal.core.CarrierAdmission;
import io.zmux.internal.core.Action;
import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.ReadBuf;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.Time;
import io.zmux.internal.wire.JoinCrypto;
import io.zmux.internal.wire.TransportParams;
import io.zmux.carrier.VarInt;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLEngine;

/**
 * The listener side of carriers (MULTIPATH.md 5.4 to 5.6): a new carrier is classified by
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

        /** A session was started on its loop (on that loop, before its first round). */
        void started(SessionRuntime d, String applicationProtocol);
    }

    private static final long MAX_FIRST_RECORD = 65535;

    private final Owner owner;
    private final MemoryBudget budget;

    /**
     * The listener's outcome counts (API.md Section 7.2): each pipe input handed to the listener (a carrier, a lane,
     * or the two halves of a split pipe once paired) counts once, when its outcome is decided, in sessions, joins,
     * rejected, refused or timedOut; setupFailures counts connections that never became a pipe input.
     */
    public static final class Counts {
        private final java.util.concurrent.atomic.AtomicLong sessions = new java.util.concurrent.atomic.AtomicLong(),
                joins = new java.util.concurrent.atomic.AtomicLong(), setupFailures = new java.util.concurrent.atomic.AtomicLong(),
                rejected = new java.util.concurrent.atomic.AtomicLong(), refused = new java.util.concurrent.atomic.AtomicLong(),
                timedOut = new java.util.concurrent.atomic.AtomicLong();
        /** The admission's outcomes (API.md Section 7.2): each carrier past its source slot once. */
        final java.util.concurrent.atomic.AtomicLong accepted = new java.util.concurrent.atomic.AtomicLong(),
                fallback = new java.util.concurrent.atomic.AtomicLong(), stalled = new java.util.concurrent.atomic.AtomicLong(),
                closed = new java.util.concurrent.atomic.AtomicLong(), stallOverflow = new java.util.concurrent.atomic.AtomicLong(),
                peekTimedOut = new java.util.concurrent.atomic.AtomicLong(), handshakeFailed = new java.util.concurrent.atomic.AtomicLong();

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
            return new io.zmux.AdmissionStats(accepted.get(), fallback.get(), stalled.get(), closed.get(), stallOverflow.get(),
                    peekTimedOut.get(), handshakeFailed.get());
        }
    }

    private final Counts counts = new Counts();

    /** How an input that started no session and joined nothing ended. */
    private enum Outcome { REJECTED, REFUSED, TIMED_OUT }

    /** The listener's outcome counts; any thread. */
    public Counts counts() { return counts; }
    private final byte[] instanceId;
    private final CarrierAdmission admission;
    private final CarrierAdmission.SplitPairing pairing = new CarrierAdmission.SplitPairing(0, 0);
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
                        && (n > maxSuspended || budget != null && budget.pressure())) {
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
     * @param firstRecordTimeout 0: 2 s
     */
    public ListenerHub(Owner owner, MemoryBudget budget, byte[] instanceId, int maxPendingPerSource,
                       long firstRecordTimeout, int maxSuspended) {
        this.maxSuspended = maxSuspended;
        this.instanceId = instanceId.clone();
        this.owner = owner;
        this.budget = budget;
        this.admission = new CarrierAdmission(maxPendingPerSource, firstRecordTimeout);
        this.firstRecordTimeout = firstRecordTimeout > 0 ? firstRecordTimeout : 2 * Time.SECOND;
    }

    /** Prepares all sessions owned by this endpoint before their transport parameters can be emitted. */
    public synchronized CoreConfig prepare(CoreConfig cfg) {
        if (closed || draining) throw new IllegalStateException("listener closed");
        cfg = cfg.toBuilder().budget(budget).build();
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
            if (locator != null && sessions.containsKey(locator) || laneId != null && laneSessions.containsKey(laneId))
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
        Runnable remove = driver.watchClosed(error -> registration.close());
        if (!registration.observer.compareAndSet(null, remove)) remove.run();
        return registration::close;
    }

    private final class Registration {
        final SessionRuntime driver; final String locator, lane;
        final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
        final java.util.concurrent.atomic.AtomicReference<Runnable> observer = new java.util.concurrent.atomic.AtomicReference<>();
        Registration(SessionRuntime driver, String locator, String lane) { this.driver = driver; this.locator = locator; this.lane = lane; }
        void close() {
            if (!closed.compareAndSet(false, true)) return;
            synchronized (ListenerHub.this) {
                if (locator != null) sessions.remove(locator, driver);
                if (lane != null) laneSessions.remove(lane, driver);
                resources.remove(driver); registrations.remove(this);
            }
            Runnable remove = observer.getAndSet(() -> {}); if (remove != null) remove.run();
            drainedIfEmpty();
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
        synchronized (this) {
            closed = true; drained = null;
            halves = java.util.List.copyOf(pendingPairs); registered = java.util.List.copyOf(registrations);
        }
        for (Registration registration : registered) registration.close();
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
     * its classification: by hint's out-of-band join or pairing, or by its first record.
     * verdict (or null) receives the admission's verdict code, off the loop; fallback (or
     * null) takes a carrier the admission does not accept. Returns the carrier, for its
     * {@link Transport#whenTaken} and {@link Transport#whenClosed}.
     */
    public Transport external(SessionHost loop, io.zmux.carrier.PipeIo io, io.zmux.JoinHint hint, boolean admitted,
                              Object admittedValue, Forward fallback, CompletableFuture<Decided> verdict) {
        boolean pair = !hint.pairing().isEmpty(), oob = !hint.join().isEmpty();
        Transport carrier = pair ? new ExternalTransport(loop, io) : carrier(loop, io);
        var info = info(carrier, io, hint);
        InetSocketAddress source = hint.source();
        Next next;
        if (pair) {
            byte[] key = decodePairToken(hint.pairing());
            boolean shaped = carrier.directions() == (hint.uplink() ? Action.DIR_IN : Action.DIR_OUT);
            next = transformed(hint, info, (c, value) -> {
                release(source); // the pairing bounds a waiting half
                if (key == null || !shaped) { c.close(null); counts.rejected(); return false; }
                return new PairHalf((ExternalTransport) c, key, hint.uplink(), source, value).begin();
            });
        } else if (oob) {
            byte[] body = CarrierAdmission.decodeJoinHeader(hint.join());
            next = transformed(hint, info, (c, value) -> {
                release(source);
                if (body == null) { c.close(null); counts.rejected(); return false; }
                join(c, body, false, value);
                return true;
            });
        } else {
            String alpn = io.capabilities().applicationProtocol();
            next = transformed(hint, info, (c, value) -> gate(c, source, alpn, value, ignored -> {}, true) != null);
        }
        if (!carrier.loop().tryExecute(() -> admit(carrier, info, admitted, admittedValue, fallback, verdict, oob, next))) {
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
                caps.exporter() == null ? null : carrier::export, hint.request());
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
        final InetSocketAddress source;
        /** The half's admission value. */
        final Object admitted;
        final Runnable shutdown = this::shutdownCarrier;
        final TimerHeap.Timer first = new TimerHeap.Timer() { @Override void fire(long now) { fail(Outcome.TIMED_OUT); } };
        final TimerHeap.Timer expiry = new TimerHeap.Timer() { @Override void fire(long now) { fail(Outcome.TIMED_OUT); } };
        PairHalf other;
        boolean firstReady;
        boolean transferRequested;
        boolean done;

        PairHalf(ExternalTransport carrier, byte[] token, boolean uplink, InetSocketAddress source, Object admitted) {
            this.carrier = carrier; this.token = token; this.uplink = uplink; this.source = source; this.admitted = admitted;
        }
        /** False when the half was refused (its carrier is closed). */
        boolean begin() {
            Outcome refused = null;
            synchronized (ListenerHub.this) {
                if (closed || draining) refused = Outcome.REFUSED; // a split bootstrap starts a session
                else {
                    var result = pairing.offer(token, uplink, source, this, Time.add(Clock.now(), owner.config().handshakeTimeout()));
                    if (result == CarrierAdmission.SplitPairing.Result.REJECT) refused = Outcome.REJECTED;
                    else if (result == CarrierAdmission.SplitPairing.Result.REFUSE) refused = Outcome.REFUSED;
                    else pendingPairs.add(this);
                    if (result == CarrierAdmission.SplitPairing.Result.DONE) {
                        other = (PairHalf) pairing.partner(); other.other = this;
                    }
                }
            }
            if (refused != null) { carrier.close(null); counts.count(refused); return false; }
            carrier.loop().onShutdown(shutdown);
            carrier.loop().schedule(expiry, Time.add(Clock.now(), owner.config().handshakeTimeout()));
            if (uplink) {
                carrier.boundPendingInput(MAX_FIRST_RECORD + 8);
                carrier.loop().schedule(first, Time.add(Clock.now(), firstRecordTimeout));
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
            synchronized (ListenerHub.this) {
                if (done || other == null) return;
                up = uplink ? this : other;
                if (!up.firstReady) return;
            }
            if (!up.carrier.loop().tryExecute(up::combine)) up.fail(Outcome.REFUSED);
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
                if (!refused) { done = down.done = true; pendingPairs.remove(this); pendingPairs.remove(down); }
            }
            if (refused) { moved.close(null); fail(Outcome.REFUSED); return; }
            stopTimers();
            ExternalTransport combined;
            try { combined = carrier.pairWith(moved); }
            catch (ArithmeticException invalid) { combined = null; }
            if (combined == null) {
                synchronized (ListenerHub.this) { pairing.forget(token); }
                carrier.close(null); moved.close(null); counts.rejected(); return;
            }
            combined.unboundPendingInput();
            var adopted = new java.util.concurrent.atomic.AtomicBoolean();
            Runnable forget = () -> { synchronized (ListenerHub.this) { pairing.forget(token); } };
            combined.whenClosed().whenComplete((value, error) -> { if (!adopted.get()) forget.run(); });
            // The session's admission value is the uplink's, the half that carries the client's parameters.
            gate(combined, source, combined.info().applicationProtocol(), admitted,
                    driver -> { adopted.set(true); driver.closedFuture().whenComplete((value, error) -> forget.run()); }, false);
        }
        @Override public int bytes(Transport c, ReadBuf buf, ByteBuffer bytes, int pos, int lim) {
            var record = CarrierAdmission.classify(bytes, pos, lim, MAX_FIRST_RECORD);
            if (record.kind() == CarrierAdmission.Kind.MORE || record.kind() == CarrierAdmission.Kind.SESSION && record.firstRecordLength() == 0) return 0;
            if (record.kind() != CarrierAdmission.Kind.SESSION) { fail(Outcome.REJECTED); return Inbound.GONE; }
            carrier.loop().schedule(first, Time.NEVER); carrier.pauseInput();
            synchronized (ListenerHub.this) { firstReady = true; }
            tryPair(); return 0;
        }
        @Override public void ended(Transport c, boolean graceful, boolean partial) { fail(Outcome.REJECTED); }
    }

    // ---- the source slot, the admission of new carriers ----

    /**
     * Takes a slot of source for a carrier an entry accepted, before its outer handshake;
     * false (counted refused) when the source has its share or the listener takes no carrier.
     */
    public boolean reserve(InetSocketAddress source) {
        boolean ok;
        synchronized (this) { ok = !closed && admission.admit(source); }
        if (!ok) counts.refused();
        return ok;
    }

    /** Gives back a reserved slot. */
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
        /** admitted: the carrier's admission value, or null. */
        boolean start(Transport c, Object admitted);
    }

    /** The outcome of a carrier's admission: a verdict code and an accept's value. */
    public record Decided(int code, Object value) {}

    private Admission.Decider decider;
    private Forward socketFallback;
    private int peek;
    private long peekTimeout, stallMin, stallMax;
    private final byte[] stallKey = new byte[32];
    /** The admissions waiting for their decision, cancelled when the listener stops taking carriers. */
    private final java.util.Set<Admitting> deciding = ConcurrentHashMap.newKeySet();
    /** The stalled carriers, guarded by this. */
    private final java.util.Set<Stalled> stalls = new java.util.HashSet<>();

    /**
     * Configures the admission of every new carrier, before the first one; times in
     * nanoseconds. fallback (or null) is the target of socket and TLS carriers.
     */
    public void admission(Admission.Decider d, int peek, long peekTimeout, long stallMin, long stallMax, Forward fallback) {
        decider = d;
        socketFallback = fallback;
        this.peek = peek;
        this.peekTimeout = peekTimeout;
        this.stallMin = stallMin;
        this.stallMax = stallMax;
        random.nextBytes(stallKey);
    }

    /** The duration of a stall of a carrier from source: a keyed function of the source's client alone. */
    public long stallDuration(InetSocketAddress source) {
        if (stallMax <= stallMin) return stallMax;
        try {
            javax.crypto.Mac m = javax.crypto.Mac.getInstance("HmacSHA256");
            m.init(new javax.crypto.spec.SecretKeySpec(stallKey, "HmacSHA256"));
            long v = ByteBuffer.wrap(m.doFinal(CarrierAdmission.sourceKey(source).getBytes(java.nio.charset.StandardCharsets.UTF_8))).getLong();
            return stallMin + Long.remainderUnsigned(v, stallMax - stallMin + 1);
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void closeQuietly(SocketChannel ch) {
        try { ch.close(); } catch (java.io.IOException ignored) { /* Gone. */ }
    }

    /** Takes a TCP carrier from peer: its slot, the admission, the first record. */
    public void socket(EventLoop loop, SocketChannel ch, InetSocketAddress peer) {
        if (!reserve(peer)) { closeQuietly(ch); return; }
        var info = new Admission.CarrierInfo(peer, peer, "", "", null, io.zmux.RequestHead.NONE);
        Next next = transformed(io.zmux.JoinHint.inBand(peer), info, (c, value) -> gate(c, peer, "", value, ignored -> {}, true) != null);
        if (!loop.tryExecute(() -> admit(new SocketTransport(loop, ch), info, false, null, socketFallback, null, false, next))) {
            release(peer);
            closeQuietly(ch);
            counts.refused();
        }
    }

    /** Takes a TLS carrier from peer: its slot, the handshake within handshakeTimeout, the admission, the first record. */
    public void tls(EventLoop loop, SocketChannel ch, SSLEngine engine, long handshakeTimeout, InetSocketAddress peer) {
        if (!reserve(peer)) { closeQuietly(ch); return; }
        TlsTransport.handshake(loop, ch, engine, handshakeTimeout, (c, err) -> {
            if (err != null) { handshakeFailed(peer); return; }
            String alpn = c.info().applicationProtocol();
            var info = new Admission.CarrierInfo(peer, peer, alpn, c.serverName(), c::export, io.zmux.RequestHead.NONE);
            admit(c, info, false, null, socketFallback, null, false,
                    transformed(io.zmux.JoinHint.inBand(peer), info, (x, value) -> gate(x, peer, alpn, value, ignored -> {}, true) != null));
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
    private void admit(Transport c, Admission.CarrierInfo info, boolean admitted, Object admittedValue, Forward fallback,
                       CompletableFuture<Decided> verdict, boolean deferred, Next next) {
        if (decider == null || admitted) {
            Object value = decider == null ? null : admittedValue;
            report(verdict, 0, value);
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
        else c.start(a);
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
        /** The accept's value. */
        private Object value;
        final long due;
        /** A copy of every byte seen so far, still unconsumed in the read buffer until the input ends. */
        private byte[] seen = new byte[0];
        private int prefixLength;
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
        }

        @Override
        public int bytes(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim) {
            if (done) return 0;
            seen = new byte[lim - pos];
            data.get(pos, seen);
            if (!decidingNow && seen.length >= peek) {
                c.pauseInput();
                paused = true;
                decide();
            }
            return 0;
        }

        @Override
        void fire(long now) {
            if (done) return;
            if (!decidingNow) decide(); // what was read is the prefix; the deadline passed
            else abandon(counts.peekTimedOut);
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
            prefixLength = Math.min(peek, seen.length);
            ByteBuffer prefix = ByteBuffer.wrap(seen, 0, prefixLength).slice().asReadOnlyBuffer();
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
            finish();
            if (d != null && d.verdict() == Admission.Verdict.ACCEPT) value = d.value();
            int v = d == null ? Admit.CLOSE : switch (d.verdict()) {
                case ACCEPT -> d.skip() < 0 || d.skip() > prefixLength ? Admit.CLOSE : d.skip();
                case FALLBACK -> fallback == null ? Admit.STALL : Admit.FALLBACK;
                case STALL -> Admit.STALL;
                case CLOSE -> Admit.CLOSE;
            };
            if (v == Admit.CLOSE) {
                counts.closed.incrementAndGet();
                release(source);
                carrier.close(null);
                report(verdict, v, value);
                return;
            }
            if (v == Admit.STALL) {
                // A fallback without a target stalls but counts as a fallback.
                var counter = d.verdict() == Admission.Verdict.FALLBACK ? counts.fallback : counts.stalled;
                if (ended) {
                    // The peer already ended it: the stall is over at once.
                    counter.incrementAndGet();
                    release(source);
                    carrier.shutdownOutput();
                    carrier.close(null);
                    report(verdict, v, value);
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
                    release(source);
                    carrier.close(null);
                    report(verdict, Admit.CLOSE, null);
                    return;
                }
                counter.incrementAndGet();
                report(verdict, v, value);
                carrier.loop().onShutdown(s.shutdown);
                carrier.loop().schedule(s, Time.add(Clock.now(), stallDuration(source)));
                carrier.start(s);
                resume();
                return;
            }
            (v == Admit.FALLBACK ? counts.fallback : counts.accepted).incrementAndGet();
            report(verdict, v, value);
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
            if (!c.closed() && c.inbound() != this && !parked(c)) resume();
        }

        /** After the skipped bytes of a live delivery: next; the count consumed of [pos, lim), or GONE. */
        int proceed(ReadBuf buf, ByteBuffer data, int pos, int lim) {
            Transport c = carrier;
            if (deferred) return later();
            if (!next.start(c, value)) return Inbound.GONE;
            if (c.inbound() == this || c.closed() || pos == lim) return 0;
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
                if (c.inbound() != HOLD || c.closed()) return;
                if (next.start(c, value) && !c.closed() && c.inbound() != HOLD && !parked(c)) c.resumeInput();
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
            if (c.inbound() == this || c.closed()) {
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
     * A stalled carrier, its slot held: what arrives is discarded; it ends when the peer ends it,
     * its duration passes or the listener closes.
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
                admission.unstall(source);
                stalls.remove(this);
            }
            if (graceful) carrier.shutdownOutput();
            carrier.close(null);
        }

        @Override public int bytes(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim) { return lim - pos; }
        @Override public void ended(Transport c, boolean graceful, boolean partial) { finish(true); }
        @Override void fire(long now) { finish(true); }
    }

    /**
     * On the carrier's loop: the first-record gate; null when the carrier was closed.
     * reserved: the carrier holds its source's slot already; otherwise it takes one now.
     * started observes a session the carrier starts.
     */
    private Pending gate(Transport c, InetSocketAddress source, String alpn, Object admitted,
                         java.util.function.Consumer<SessionRuntime> started, boolean reserved) {
        boolean ok;
        synchronized (this) {
            if (!reserved) ok = !closed && admission.admit(source);
            else if (!(ok = !closed)) admission.release(source);
        }
        if (!ok) {
            c.close(null);
            counts.refused();
            return null;
        }
        Pending p = new Pending(c, source, alpn, admitted, started);
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
        final Object admitted;
        final java.util.function.Consumer<SessionRuntime> started;
        boolean done;
        final Runnable shutdown = () -> ended(null, false, false);

        Pending(Transport carrier, InetSocketAddress source, String alpn, Object admitted, java.util.function.Consumer<SessionRuntime> started) {
            this.carrier = carrier;
            this.source = source;
            this.alpn = alpn;
            this.admitted = admitted;
            this.started = started;
        }

        /** Ends the pending phase: the slot and the timer go. */
        private void decided() {
            done = true;
            carrier.loop().schedule(this, Time.NEVER);
            carrier.loop().removeShutdownHook(shutdown);
            synchronized (ListenerHub.this) {
                admission.release(source);
            }
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
            decided();
            if (cl.kind() == CarrierAdmission.Kind.SESSION) return startSession(c, buf, data, pos, lim, alpn, admitted, started);
            int sn = VarInt.lengthOf(data.get(pos));
            byte[] body = new byte[cl.firstRecordLength() - sn];
            data.get(pos + sn, body);
            if (cl.kind() == CarrierAdmission.Kind.LANE) {
                var frame = CarrierAdmission.parseLane(body);
                SessionRuntime target = frame == null ? null : laneSessions.get(HexFormat.of().formatHex(CarrierAdmission.laneKey(frame)));
                if (target == null) { c.close(null); counts.rejected(); } else transferLane(c, body, target);
            } else if (join(c, body, true, admitted)) return lim - pos; // answered: what follows is discarded
            return Inbound.GONE;
        }
    }

    private int startSession(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim, String alpn, Object admitted,
                             java.util.function.Consumer<SessionRuntime> started) {
        if (closed || draining || budget != null && budget.pressure()) {
            c.close(null);
            counts.refused();
            return Inbound.GONE;
        }
        CoreConfig cfg;
        try { cfg = prepare(owner.config()); }
        catch (IllegalStateException ended) { c.close(null); counts.refused(); return Inbound.GONE; }
        if ((cfg.local().secretFlags & TransportParams.SECRET_EXPORTER) != 0)
            cfg = cfg.toBuilder().exporter(c.export(JoinCrypto.EXPORTER_SESSION, null, c.info().exporterLength())).build();
        SessionRuntime d = new SessionRuntime(c.loop(), new Session(cfg, Clock.now()), owner.hooks());
        d.admitted(admitted);
        try { register(d); }
        catch (IllegalStateException ended) { c.close(null); d.failInternal(ended); counts.refused(); return Inbound.GONE; }
        counts.session();
        owner.started(d, alpn);
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
                    if (cl.kind() == CarrierAdmission.Kind.MORE || cl.kind() == CarrierAdmission.Kind.LANE && cl.firstRecordLength() == 0) return 0;
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
            driver.loop().schedule(gate, Time.add(Clock.now(), driver.core().handshakeTimeout()));
            carrier.start(gate);
        })) carrier.close(null);
    }

    /** Hands a joining carrier on; true when the carrier stays here, answered as an unknown join. */
    private boolean join(Transport c, byte[] body, boolean inband, Object admitted) {
        if (closed) { c.close(null); counts.refused(); return false; }
        io.zmux.internal.wire.Frame f = CarrierAdmission.parseJoin(body);
        byte[] locator = f == null ? null : CarrierAdmission.joinLocator(f);
        SessionRuntime d = f == null ? null : sessions.get(HexFormat.of().formatHex(locator));
        if (d == null) {
            counts.rejected();
            if (f != null && answerUnknownJoins && startsWith(locator, instanceId)) return new Answering(c).begin();
            c.close(null);
            return false;
        }
        byte[] binding = null;
        if (f.join.carrierBound) {
            binding = c.export(JoinCrypto.EXPORTER_PIPE, null, 32);
            if (binding == null) {
                c.close(null);
                counts.rejected();
                return false;
            }
        }
        Transport moved = c.moveTo(d.loop());
        if (moved == null) { counts.rejected(); return false; }
        byte[] b = binding;
        boolean ok = d.loop().tryExecute(() -> {
            // Under pressure only a join that lets a suspended session resume.
            if (budget != null && budget.pressure() && d.core().state() != io.zmux.internal.core.SessionState.SUSPENDED) {
                moved.close(null);
                counts.refused();
            } else {
                d.acceptJoin(moved, body, b, inband, admitted, counts);
            }
        });
        if (!ok) { moved.close(null); counts.refused(); }
        return false;
    }

    // ---- the inbound transform (API.md 4.4) and the unknown-join answer (MULTIPATH.md 5.4) ----

    private io.zmux.InboundTransform transform;
    private boolean answerUnknownJoins;
    /** The transforms under way, cancelled when the listener stops taking carriers. */
    private final java.util.Set<Transformed> transforming = ConcurrentHashMap.newKeySet();

    /** Configures the inbound transform (or null) and the unknown-join answer, before the first carrier. */
    public void inbound(io.zmux.InboundTransform t, boolean answerUnknown) {
        transform = t;
        answerUnknownJoins = answerUnknown;
    }

    private Next transformed(io.zmux.JoinHint hint, Admission.CarrierInfo info, Next next) {
        return transform == null ? next : new Transformed(hint, info, next);
    }

    /** Whether c waits in a transform's view: its input resumes when the transform reads. */
    private static boolean parked(Transport c) { return c.inbound() instanceof Forwarded; }

    private static boolean startsWith(byte[] a, byte[] prefix) {
        return a.length >= prefix.length && java.util.Arrays.equals(a, 0, prefix.length, prefix, 0, prefix.length);
    }

    private static void closeIo(io.zmux.carrier.PipeIo io, Throwable cause) {
        if (io == null) return;
        try { if (io.reader() != null) io.reader().close(); }
        catch (RuntimeException ignored) { /* Closing anyway. */ }
        finally {
            try { if (io.writer() != null) io.writer().abort(cause); } catch (RuntimeException ignored) { /* Closing anyway. */ }
        }
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
        private Object value;
        private CompletableFuture<io.zmux.carrier.PipeIo> pending;
        private boolean done;
        private final Runnable shutdown = () -> fail(new java.io.IOException("event loop closed"), null);

        Transformed(io.zmux.JoinHint hint, Admission.CarrierInfo info, Next next) {
            this.hint = hint; this.info = info; this.next = next;
        }

        @Override public boolean start(Transport c, Object v) {
            begin(c, v, null, false, true);
            return true;
        }

        /** On the carrier's loop: replay (or null) is what the admission read once the input ended. */
        void begin(Transport c, Object v, byte[] replay, boolean ended, boolean graceful) {
            carrier = c;
            value = v;
            c.taken(); // the layer may read and write on it
            var ci = c.info();
            boolean messages = ci.carrier().framing() == io.zmux.internal.core.Framing.RECORDS;
            view = new Forwarded(c, replay, ended, graceful, messages);
            var caps = new io.zmux.carrier.PipeIo.Capabilities(null, info.exporter(), Forwarded.AHEAD, info.applicationProtocol(),
                    info.serverName(), ci.fateGroup(), ci.carrier().writeOverhead());
            given = view.pipe(info.applicationProtocol()).withCapabilities(caps).withLabel(ci.carrier().label())
                    .withCarrierConn(ci.connection()).withCarrier(ci.carrier().kind());
            c.loop().onShutdown(shutdown);
            long due = Time.add(Clock.now(), owner.config().handshakeTimeout());
            c.loop().schedule(this, due);
            transforming.add(this);
            if (closed) { fail(new java.io.IOException("listener closed"), null); return; }
            var deadline = new Admission.Deadline(System.nanoTime() + Math.max(0, due - Clock.now()));
            CompletableFuture<io.zmux.carrier.PipeIo> f;
            try {
                var stage = transform.transform(given, new io.zmux.InboundTransform.Context(hint, java.util.Optional.ofNullable(v), deadline));
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
                if (next.start(c, value) && !c.closed() && !parked(c)) c.resumeInput();
                return;
            }
            var ci = c.info();
            io.zmux.carrier.PipeIo io = r.withCarrier(ci.carrier().kind()).withLabel(ci.carrier().label())
                    .withFateGroup(ci.fateGroup());
            next.start(new ExternalTransport(c.loop(), io), value);
        }
    }

    /** The answer to an unknown join: CONNECTION_CLOSE, ZMUX_RESUME_FAILED, frame type 0, no reason, as a record. */
    private static final byte[] UNKNOWN_JOIN_ANSWER = {0x07, 0x1c, (byte) 0x8f, 0x05, (byte) 0xf7, 0x4e, 0x00, 0x00};

    /**
     * Writes the unknown-join answer on a carrier and ends it: the sending half gracefully,
     * then what the joiner sends is discarded until it ends the carrier or the first-record
     * timeout passes, so that the answer is not lost to a reset over unread data.
     */
    private final class Answering extends TimerHeap.Timer implements Inbound {
        final Transport carrier;
        final io.zmux.internal.core.Batch batch = new io.zmux.internal.core.Batch();
        final Runnable shutdown = this::finish;
        private boolean done;

        Answering(Transport c) { carrier = c; }

        /** True: the carrier stays bound here. */
        boolean begin() {
            if ((carrier.directions() & Action.DIR_OUT) == 0) { carrier.close(null); return false; }
            carrier.taken(); // the listener writes on it
            carrier.loop().onShutdown(shutdown);
            carrier.loop().schedule(this, Time.add(Clock.now(), firstRecordTimeout));
            carrier.start(this);
            boolean messages = carrier.info().carrier().framing() == io.zmux.internal.core.Framing.RECORDS;
            ByteBuffer answer = messages ? ByteBuffer.wrap(UNKNOWN_JOIN_ANSWER, 1, 7).slice() : ByteBuffer.wrap(UNKNOWN_JOIN_ANSWER);
            batch.wrap(io.zmux.carrier.OutboundBatch.of(answer));
            carrier.write(batch);
            return !done;
        }

        @Override public int bytes(Transport c, ReadBuf buf, ByteBuffer data, int pos, int lim) { return done ? Inbound.GONE : lim - pos; }
        @Override public void ended(Transport c, boolean graceful, boolean partial) { finish(); }
        @Override public void written(Transport c, io.zmux.internal.core.Batch b, Throwable cause) {
            b.discard();
            if (cause != null) { finish(); return; }
            carrier.shutdownOutput();
            if ((carrier.directions() & Action.DIR_IN) == 0) finish();
        }
        @Override void fire(long now) { finish(); }

        private void finish() {
            if (done) return;
            done = true;
            carrier.loop().schedule(this, Time.NEVER);
            carrier.loop().removeShutdownHook(shutdown);
            carrier.close(null);
        }
    }

}
