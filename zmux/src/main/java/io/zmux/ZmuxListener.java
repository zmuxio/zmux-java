package io.zmux;

import io.zmux.adapter.ListenerAdapter;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.Event;
import io.zmux.internal.loop.MemoryBudget;
import io.zmux.internal.loop.Bootstrap;
import io.zmux.internal.loop.ListenerHub;
import io.zmux.internal.loop.SessionRuntime;
import io.zmux.internal.spi.AdapterPorts;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import javax.net.ssl.SSLEngine;

/**
 * Accepts sessions, and the pipes that join them, on a TCP or TLS port. A new carrier is classified by its first record: it starts a session, which
 * {@link #accept()} offers once established (at most {@code backlog} wait, later ones are
 * refused), or it joins the session its Hello names. The listener's sessions share one
 * memory budget. A listener without a socket ({@link Zmux#listener(ListenerConfig)}) runs nothing of its own
 * until its first carrier; one with a socket runs its acceptor from creation. Either must be
 * {@link #close() closed}, which stops its tasks and ends its sessions.
 */
public final class ZmuxListener implements AutoCloseable {
    private static final java.security.SecureRandom RANDOM = new java.security.SecureRandom();
    private final ServerSocketChannel server;
    private final ZmuxConfig cfg;
    private final Supplier<SSLEngine> tls;
    private final boolean tlsSecure;
    private final InetSocketAddress localAddress;
    private final int backlog;
    /**
     * Established sessions not yet accepted. The loop offers without locks: a count reserves
     * a backlog slot, a permit per queued session wakes one acceptor.
     */
    private final ConcurrentLinkedQueue<ZmuxSession> ready = new ConcurrentLinkedQueue<>();
    final AtomicInteger queued = new AtomicInteger();
    private final Semaphore available = new Semaphore(0);
    private final MemoryBudget budget;
    final ListenerHub hub;
    private final Admission admission;
    private final java.time.Duration firstRecordTimeout;
    private final Thread acceptor;
    /** Accept is closed: by {@link #shutdown} or {@link #close}. */
    private volatile boolean closed;
    /** Closed: the listener takes no carrier at all. */
    private final AtomicBoolean stopped = new AtomicBoolean();
    /** The running shutdown's bound, run at most once whichever caller's future triggers it; null: none started. */
    private final java.util.concurrent.atomic.AtomicReference<Runnable> shutdownBound = new java.util.concurrent.atomic.AtomicReference<>();
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();
    private final io.zmux.internal.loop.SerialCallbacks pressureCallbacks = new io.zmux.internal.loop.SerialCallbacks();
    private final BiConsumer<Long, Long> pressureListener;

    private void memoryChanged() {
        ListenerHub h = hub;
        if (h != null) h.memoryChanged();
        if (pressureListener != null) {
            long used = budget.used(), limit = budget.limit();
            pressureCallbacks.execute(() -> pressureListener.accept(used, limit));
        }
    }


    private ZmuxListener(ServerSocketChannel server, ListenerConfig o) {
        this.server = server;
        this.cfg = o.session();
        this.firstRecordTimeout = o.firstRecordTimeout();
        // The whole configuration is checked here, never first on a carrier: what does not depend on the carrier.
        cfg.toCore(CoreConfig.Role.RESPONDER, CoreConfig.Role.RESPONDER);
        InetSocketAddress local = null;
        if (server != null) {
            try { local = (InetSocketAddress) server.getLocalAddress(); } catch (IOException e) { throw new IllegalStateException("bound socket without a local address", e); }
        }
        this.localAddress = local;
        TlsOptions t = o.tls();
        String[] protos = ZmuxConfig.withAliases(cfg.alpn()).toArray(String[]::new);
        this.tls = t == null ? null : () -> t.serverEngine(protos);
        tlsSecure = t != null && t.secure();
        this.backlog = o.backlog();
        this.pressureListener = o.memoryPressureListener();
        this.budget = new MemoryBudget(o.memoryBudget().orElse(io.zmux.internal.core.CoreConfig.UNLIMITED), this::memoryChanged);
        this.hub = new ListenerHub(new ListenerHub.Owner() {
            @Override
            public io.zmux.internal.loop.DriverHooks hooks() { return cfg.hooks; }

            @Override
            public void log(System.Logger.Level level, String message, Throwable error) {
                if (o.logger() != null) io.zmux.internal.loop.DriverHooks.log(o.logger(), level, message, error);
                else cfg.hooks.log(level, message, error);
            }

            @Override
            public CoreConfig config() {
                return cfg.toCore(CoreConfig.Role.RESPONDER, CoreConfig.Role.RESPONDER);
            }

            @Override
            public ListenerHub.Chosen choose(Admission.CarrierInfo info, Object admitted) {
                ZmuxConfig c = o.sessionConfigs() == null ? null : o.sessionConfigs().choose(info, admitted);
                if (c == null) c = cfg;
                return new ListenerHub.Chosen(c.toCore(CoreConfig.Role.RESPONDER, CoreConfig.Role.RESPONDER), c.hooks, c.carrierCredential);
            }

            @Override
            public void started(SessionRuntime d, String alpn, CarrierCredential credential) {
                offerWhenEstablished(d, new ZmuxSession(d, d.core(), alpn, credential));
            }
        }, budget, o.instanceId(), o.maxPendingPerSource(), o.maxPending().orElse(-1), ZmuxConfig.nanos(o.firstRecordTimeout()), o.maxSuspended(),
                o.sourcePrefixV4(), o.sourcePrefixV6());
        Admission adm = o.admission();
        admission = adm;
        // A refused Establish stalls at most this long, with or without an admission.
        long stallMax = adm == null ? 0 : ZmuxConfig.nanos(adm.stallMax()), stallMin = adm == null ? 0 : ZmuxConfig.nanos(adm.stallMin());
        if (stallMax == 0) {
            stallMax = draw(30_000_000_000L, 120_000_000_000L);
            stallMin = draw(stallMax / 4, stallMax * 3 / 4);
        }
        hub.rejectStallMax(stallMax);
        if (adm != null) {
            long peekTimeout = adm.peekTimeout().isZero() ? draw(5_000_000_000L, 30_000_000_000L) : ZmuxConfig.nanos(adm.peekTimeout());
            hub.admission(adm.decider(), adm.peek(), adm.maxPeek(), adm.maxMessage(), peekTimeout, adm.isFallbackOnPeekTimeout(), stallMin, stallMax, o.fallback());
        }
        hub.inbound(o.inboundTransform());
        hub.onReject(o.onReject());
        hub.maxPendingPerSession(o.maxPendingPerSession());
        this.acceptor = server == null ? null : Thread.ofVirtual().name("zmux-acceptor").start(this::acceptLoop);
    }

    /** Uniform in [lo, hi] from a secure random source. */
    private static long draw(long lo, long hi) {
        return lo + Long.remainderUnsigned(RANDOM.nextLong(), hi - lo + 1);
    }

    /** A listener without a socket ({@link Zmux#listener}). */
    static ZmuxListener unbound(ListenerConfig cfg) {
        return new ZmuxListener(null, cfg);
    }

    /** The listener's side of its {@link #adapter()} view. */
    private final AdapterPorts.Listener port = new AdapterPorts.Listener() {
        @Override public long stallNanos(InetSocketAddress source) { return hub.stallDuration(source); }
        @Override public io.zmux.adapter.JoinLookup knownJoin(java.nio.ByteBuffer prefix) { return hub.knownJoin(prefix); }
        @Override public boolean reserve(InetSocketAddress source) { return hub.reserve(source); }
        @Override public void release(InetSocketAddress source) { hub.release(source); }
        @Override public void handshakeFailed(InetSocketAddress source) { hub.handshakeFailed(source); }
        @Override public AdapterPorts.Carrier transfer(PipeIo io, JoinHint hint, boolean admitted, Object value, ListenerHub.Entry entry) {
            return ZmuxListener.this.transfer(io, hint, admitted, value, entry);
        }
        @Override public AdapterPorts.Carrier refused(PipeIo io) { return ZmuxListener.refused(io, admission); }
        @Override public void countSetupFailure() { hub.counts().setupFailed(); }
        @Override public Duration firstRecordTimeout() { return firstRecordTimeout; }
        @Override public long memoryUsed() { return budget.used(); }
    };

    private volatile ListenerAdapter adapterView;

    /**
     * The operations only a carrier adapter or a host's own accept loop needs: arrival slots,
     * handing carriers over, join lookups, setup-failure counting and the listener's own bounds.
     */
    public ListenerAdapter adapter() {
        ListenerAdapter a = adapterView;
        if (a == null) adapterView = a = AdapterPorts.listener(port);
        return a;
    }

    private static AdapterPorts.Carrier refused(PipeIo io, Admission admission) {
        // Closed as the listener closes a carrier it refuses: both halves end, nothing written.
        try { if (io.reader() != null) io.reader().close(); }
        finally { if (io.writer() != null) io.writer().close(); }
        // Without an admission every carrier counts as accepted; refused, it was not taken and nothing was written.
        var v = admission == null ? AdmissionVerdict.ACCEPT : AdmissionVerdict.CLOSE;
        var c = new AdapterPorts.Carrier();
        c.decide(v, null);
        c.taken(false);
        c.ended(null);
        return c;
    }

    /** Transfers a carrier whose slot is held. */
    private AdapterPorts.Carrier transfer(PipeIo io, JoinHint hint, boolean admitted, Object value, ListenerHub.Entry entry) {
        Objects.requireNonNull(io); Objects.requireNonNull(hint);
        CompletableFuture<ListenerHub.Decided> code = new CompletableFuture<>();
        var h = new AdapterPorts.Carrier();
        // The value is recorded before the verdict is reported (the hub completes code off the loop).
        code.thenAccept(d -> h.decide(switch (d.code()) {
            case ListenerHub.Admit.FALLBACK -> AdmissionVerdict.FALLBACK;
            case ListenerHub.Admit.STALL -> AdmissionVerdict.STALL;
            case ListenerHub.Admit.CLOSE -> AdmissionVerdict.CLOSE;
            default -> AdmissionVerdict.ACCEPT;
        }, d.value()));
        try {
            String protocol = io.capabilities().applicationProtocol();
            if (!acceptsProtocol(protocol)) {
                hub.release(hint.source());
                hub.counts().rejected();
                returnCarrier(io, new javax.net.ssl.SSLHandshakeException("carrier refused"));
                code.complete(new ListenerHub.Decided(ListenerHub.Admit.CLOSE, null));
                h.taken(false);
                h.ended(null);
                return h;
            }
            var loop = io.carrierConn() instanceof SessionRuntime parent ? parent.loop() : cfg.nextLoop();
            io.zmux.internal.loop.Transport carrier = hub.external(loop, io, hint, admitted, value, entry, code);
            carrier.whenTaken().whenComplete((t, e) -> h.taken(Boolean.TRUE.equals(t)));
            carrier.whenClosed().whenComplete((t, e) -> h.ended(null));
            return h;
        } catch (RuntimeException error) {
            hub.release(hint.source());
            hub.counts().refused();
            returnCarrier(io, error);
            code.complete(new ListenerHub.Decided(ListenerHub.Admit.CLOSE, null));
            h.taken(false);
            h.ended(error);
            return h;
        }
    }

    /** Both untransferred halves must be returned, even when one adapter close fails. */
    private static void returnCarrier(PipeIo io, Throwable cause) {
        try { if (io.reader() != null) io.reader().close(); }
        catch (RuntimeException error) { if (error != cause) cause.addSuppressed(error); }
        finally {
            try { if (io.writer() != null) io.writer().abort(cause); }
            catch (RuntimeException error) { if (error != cause) cause.addSuppressed(error); }
        }
    }

    private boolean acceptsProtocol(String protocol) {
        return protocol.isEmpty() || ZmuxConfig.withAliases(cfg.alpn()).contains(protocol);
    }

    private CoreConfig outgoing(CoreConfig.Role role) throws IOException {
        if (io.zmux.internal.loop.SessionHost.onHostThread()) throw new IllegalStateException("connect on a zmux event-loop thread");
        if (closed) throw new ListenerClosedException();
        try { return hub.prepare(cfg.toCore(role, role)); }
        catch (IllegalStateException ended) { throw new ListenerClosedException(); }
    }

    /**
     * The admission of this listener's carriers ({@link ListenerConfig#admission()}), read-only;
     * null: every carrier is accepted. Adapters that write a response before the listener's
     * first byte (an HTTP carrier's header) ask it whether a verdict is to wait for.
     */
    public Admission admission() { return admission; }

    /** The configuration of this listener's sessions ({@link ListenerConfig#session()}), which its dialers also use. */
    public ZmuxConfig config() { return cfg; }

    /** Dials a session whose peer can join new carriers through this listener; presents {@link ZmuxConfig#carrierCredential()}. */
    public ZmuxSession connect(InetSocketAddress address) throws IOException {
        CoreConfig core = outgoing(CoreConfig.Role.INITIATOR);
        var loop = cfg.nextLoop();
        SocketChannel channel = Zmux.dial(address, cfg.handshakeNanos());
        Zmux.present(channel, cfg.carrierCredential, address);
        return Zmux.established(Bootstrap.socket(loop, core, channel, cfg.hooks, hub::register), cfg);
    }

    /** Dials a TLS session and registers its inbound join route before sending its parameters; presents {@link ZmuxConfig#carrierCredential()}. */
    public ZmuxSession connectTls(InetSocketAddress address, TlsOptions tls) throws IOException {
        CoreConfig core = outgoing(CoreConfig.Role.INITIATOR);
        SSLEngine engine = tls.clientEngine(address.getPort(), cfg.alpn());
        var loop = cfg.nextLoop();
        SocketChannel channel = Zmux.dial(address, cfg.handshakeNanos());
        return Zmux.established(Bootstrap.tls(loop, core, channel, engine, tls.secure(), cfg.hooks, hub::register, Zmux.presenter(cfg.carrierCredential, address)), cfg);
    }

    /**
     * Starts a session on an external carrier, with joins routed by this listener: as
     * {@link Role#CLIENT}, or {@link Role#AUTO} (designate its bootstrap dialer before
     * duplicate resolution). Responder carriers go through the {@link #adapter()}.
     */
    public ZmuxSession open(PipeIo io, Role role) throws IOException {
        if (role == Role.SERVER) throw new IllegalArgumentException("responder carriers go through handle");
        return external(io, Zmux.core(role));
    }

    private ZmuxSession external(PipeIo io, CoreConfig.Role role) throws IOException {
        Objects.requireNonNull(io);
        CompletableFuture<Bootstrap.Started> started;
        try {
            CoreConfig core = outgoing(role);
            if (!acceptsProtocol(io.capabilities().applicationProtocol())) throw new javax.net.ssl.SSLHandshakeException("carrier ALPN refused");
            started = Bootstrap.external(() -> io.carrierConn() instanceof SessionRuntime parent
                    ? parent.loop() : cfg.nextLoop(), core, io, cfg.hooks, hub::register);
        } catch (IOException | RuntimeException error) {
            returnCarrier(io, error);
            throw error;
        }
        return Zmux.established(started, cfg);
    }

    static ZmuxListener bind(InetSocketAddress address, ListenerConfig cfg) throws IOException {
        ServerSocketChannel server = open(address);
        try {
            return new ZmuxListener(server, cfg);
        } catch (RuntimeException e) {
            server.close();
            throw e;
        }
    }

    private static ServerSocketChannel open(InetSocketAddress address) throws IOException {
        ServerSocketChannel s = ServerSocketChannel.open();
        try {
            s.bind(address);
        } catch (IOException e) {
            s.close();
            throw e;
        }
        return s;
    }

    /**
     * Whether the listener is closed ({@link #close}, or the end of a {@link #shutdown}): it
     * takes no carrier at all, so an adapter that creates carrier state per request (the HTTP
     * exchange carrier) refuses new carriers.
     */
    public boolean isClosed() { return stopped.get(); }

    /** The bound address. */
    public InetSocketAddress localAddress() {
        if (localAddress == null) throw new IllegalStateException("external listener has no socket address");
        return localAddress;
    }

    /** First and largest pause after a failed accept (descriptor exhaustion and the like). */
    private static final long ACCEPT_BACKOFF_MIN = TimeUnit.MILLISECONDS.toNanos(5);
    /** Hands s to the accept queue once established; the queue (or a backlog close) then owns it. */
    private void offerWhenEstablished(SessionRuntime d, ZmuxSession s) {
        boolean[] offered = {false};
        d.eventSink(e -> {
            if (e.kind() == Event.Kind.ESTABLISHED && !offered[0]) {
                offered[0] = true;
                if (offer(s)) return;
                if (closed) s.abort(0x02, "listener closed");
                else {
                    hub.counts().sessionRefused();
                    d.post(() -> d.core().refuse("backlog"));
                }
            }
        });
    }

    private static final long ACCEPT_BACKOFF_MAX = TimeUnit.SECONDS.toNanos(1);

    private void acceptLoop() {
        long hto = cfg.handshakeNanos();
        long backoff = 0;
        while (!stopped.get()) {
            SocketChannel ch;
            InetSocketAddress peer;
            try {
                ch = server.accept();
                ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
                peer = ch.getRemoteAddress() instanceof InetSocketAddress a ? a : null;
                backoff = 0;
            } catch (IOException e) {
                if (stopped.get()) return;
                // A persistent failure (EMFILE, ENOBUFS) must not spin: back off exponentially.
                backoff = backoff == 0 ? ACCEPT_BACKOFF_MIN : Math.min(backoff * 2, ACCEPT_BACKOFF_MAX);
                LockSupport.parkNanos(backoff);
                continue;
            }
            var loop = cfg.nextLoop();
            if (tls == null) hub.socket(loop, ch, peer);
            else hub.tls(loop, ch, tls.get(), hto, tlsSecure, peer);
        }
    }

    /** Queues an established session unless the listener is closed or the backlog is full; never locks. */
    private boolean offer(ZmuxSession s) {
        if (closed) return false;
        for (int n = queued.get();; n = queued.get()) {
            if (n >= backlog) return false;
            if (queued.compareAndSet(n, n + 1)) break;
        }
        ready.add(s);
        if (closed && ready.remove(s)) { queued.decrementAndGet(); return false; } // Raced close's drain.
        available.release();
        return true;
    }

    /** The listener's outcome counts; any thread, no lock. */
    public java.util.List<Admission.VariantStat> variantStats() {
        return hub.counts().variantStats();
    }

    /** The listener's counts; any thread. */
    public ListenerStats stats() {
        var c = hub.counts();
        return new ListenerStats(c.sessions(), c.joins(), c.setupFailures(), c.rejectedCount(), c.refusedCount(), c.timedOutCount(),
                c.admission());
    }

    /** Waits for the next established session; {@link ListenerClosedException} once the listener closes. */
    public ZmuxSession accept() throws IOException {
        return take(false, 0);
    }

    /**
     * {@link #accept()} on a virtual thread; cancelling the future, or completing it from outside
     * ({@link ZmuxFuture#orTimeout}), interrupts the wait.
     */
    public ZmuxFuture<ZmuxSession> acceptAsync() {
        return ZmuxFuture.onThread("zmux-accept", this::accept);
    }

    /**
     * Waits at most timeout for the next established session: {@link SocketTimeoutException}
     * after it, {@link ListenerClosedException} once the listener closes.
     */
    public ZmuxSession accept(Duration timeout) throws IOException {
        return take(true, ZmuxConfig.nanos(Objects.requireNonNull(timeout, "timeout")));
    }

    private ZmuxSession take(boolean timed, long nanos) throws IOException {
        if (io.zmux.internal.loop.SessionHost.onHostThread()) throw new IllegalStateException("accept on a zmux event-loop thread");
        if (closed) throw new ListenerClosedException();
        try {
            if (!timed) available.acquire();
            else if (!available.tryAcquire(Math.max(0, nanos), TimeUnit.NANOSECONDS)) throw new SocketTimeoutException("no session within the accept timeout");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted");
        }
        ZmuxSession s = ready.poll();
        if (s == null) { available.release(); throw new ListenerClosedException(); } // close drained it: wake the next taker.
        queued.decrementAndGet();
        if (closed) available.release(); // The permit may have been close's wake-up: pass it on.
        return s;
    }

    /**
     * Shuts the listener down gracefully: {@link #accept()} fails from now on, the sessions not
     * yet accepted are aborted, and no new session is taken (a new session's first record or
     * split bootstrap is refused); every session the listener started or dialed is shut down
     * ({@link ZmuxSession#shutdown(long, String)}) with code and reason, and until they have all
     * ended the listener still takes their joins and resumptions, which pass the source slot, the
     * TLS handshake and the admission as before. Then the listener {@link #close closes}. Returns
     * when it is closed.
     *
     * @param code an application error code, from 0 to 2<sup>62</sup> - 1
     * @param reason the reason; beyond 1024 bytes of UTF-8 it is cut at a character boundary
     */
    public void shutdown(long code, String reason) throws IOException {
        await(shutdownAsync(code, reason), null);
    }

    /**
     * {@link #shutdown(long, String)} bounded by timeout: once it expires, the sessions still
     * running are closed at once with code and reason ({@link ZmuxSession#close(long, String)})
     * and the listener closes; the call returns then.
     */
    public void shutdown(long code, String reason, Duration timeout) throws IOException {
        await(shutdownAsync(code, reason), Objects.requireNonNull(timeout, "timeout"));
    }

    /**
     * {@link #shutdown(long, String)} without blocking: completes once the listener closed.
     * Completing the future from outside (cancel, {@link ZmuxFuture#orTimeout},
     * {@link ZmuxFuture#completeOnTimeout}) is the shutdown's bound: the sessions still running
     * are closed at once with code and reason, and the listener closes.
     * A call while a shutdown runs joins it: the first call's code and reason apply, and the bound of
     * any caller's future (or timeout) closes the listener.
     */
    public ZmuxFuture<Void> shutdownAsync(long code, String reason) {
        if (code < 0 || code > (1L << 62) - 1) throw new IllegalArgumentException("application error code outside 0 to 2^62 - 1");
        byte[] r = io.zmux.internal.wire.Utf8.truncate(Objects.requireNonNull(reason, "reason"), ZmuxSession.MAX_REASON_BYTES);
        if (stopped.get()) return ZmuxFuture.of(terminated, null);
        Runnable bound = boundOf(code, r);
        Runnable running = shutdownBound.compareAndExchange(null, bound);
        if (running != null) return ZmuxFuture.of(terminated, running); // a later call: the running shutdown, bounded by it too
        closeAccept();
        for (SessionRuntime d : hub.registered()) d.post(() -> d.core().shutdown(code, r));
        hub.drain(this::close);
        return ZmuxFuture.of(terminated, bound);
    }

    /**
     * The bound of a shutdown, run at most once: the sessions close at once, the listener with them.
     * Not on the thread that completed the future (the JDK's delay thread for orTimeout): close() blocks.
     */
    private Runnable boundOf(long code, byte[] r) {
        AtomicBoolean once = new AtomicBoolean();
        return () -> {
            if (once.compareAndSet(false, true)) Thread.ofVirtual().name("zmux-shutdown").start(() -> {
                for (SessionRuntime d : hub.registered()) d.post(() -> d.core().closeBounded(code, r, io.zmux.internal.core.Session.closeLimit,
                        io.zmux.internal.loop.Clock.now()));
                close();
            });
        };
    }

    private void await(ZmuxFuture<Void> f, Duration timeout) throws IOException {
        try {
            if (timeout == null) f.get();
            else f.get(Math.max(1, timeout.toNanos()), TimeUnit.NANOSECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            f.cancel(false);
            // The bound's close runs on its own thread: returns when the listener is closed, as documented.
            boolean interrupted = false;
            long end = System.nanoTime() + io.zmux.internal.core.Session.closeLimit + TimeUnit.SECONDS.toNanos(5);
            while (!terminated.isDone() && System.nanoTime() - end < 0) {
                try {
                    terminated.get(Math.max(1, end - System.nanoTime()), TimeUnit.NANOSECONDS);
                } catch (InterruptedException i) {
                    interrupted = true;
                } catch (Exception ignored) {
                    break;
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
        } catch (InterruptedException e) {
            f.cancel(false);
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted");
        } catch (java.util.concurrent.ExecutionException e) {
            throw Errors.failure(e.getCause());
        }
    }

    /**
     * Ends {@link #accept()} and aborts the sessions not yet accepted: nobody took them, so
     * nothing of theirs waits to be drained, and the caller never waits for their peers.
     */
    private void closeAccept() {
        closed = true;
        //noinspection resource: each polled session is aborted, which releases it
        for (ZmuxSession s; (s = ready.poll()) != null; ) { queued.decrementAndGet(); s.abort(0, "listener closed"); }
        available.release(); // Wakes one waiting acceptor; each passes the wake-up on.
    }

    /**
     * Closes the listener at once: it takes no carrier any more (joins and resumptions
     * included), cancels the admission decisions under way, and aborts the sessions not yet
     * accepted; accepted sessions stay, but their pipes can no longer join.
     */
    @Override
    public void close() {
        if (!stopped.compareAndSet(false, true)) return;
        try {
            closeAccept();
            hub.close();
            pressureCallbacks.close();
            if (server != null) {
                try { server.close(); } catch (IOException ignored) { /* The listener is closed all the same. */ }
            }
            if (acceptor != null) acceptor.interrupt(); // Ends a backoff pause at once.
            try {
                if (acceptor != null && acceptor != Thread.currentThread()) acceptor.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } finally {
            terminated.complete(null);
        }
    }
}
