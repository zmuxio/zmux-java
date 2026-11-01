package io.zmux;

import io.zmux.carrier.PipeIo;
import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.Event;
import io.zmux.internal.loop.MemoryBudget;
import io.zmux.internal.loop.Bootstrap;
import io.zmux.internal.loop.ListenerHub;
import io.zmux.internal.loop.SessionRuntime;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
 * Accepts sessions, and the pipes that join them, on a TCP or TLS port (MULTIPATH.md 5.4
 * to 5.6). A new carrier is classified by its first record: it starts a session, which
 * {@link #accept()} offers once established (at most {@code backlog} wait, later ones are
 * refused), or it joins the session its PIPE_JOIN names. The listener's sessions share one
 * memory budget.
 */
public final class ZmuxListener implements AutoCloseable {
    private static final java.security.SecureRandom RANDOM = new java.security.SecureRandom();
    private final ServerSocketChannel server;
    private final ZmuxConfig cfg;
    private final Supplier<SSLEngine> tls;
    private final InetSocketAddress localAddress;
    private final int backlog;
    /**
     * Established sessions not yet accepted. The loop offers without locks: a count reserves
     * a backlog slot, a permit per queued session wakes one acceptor.
     */
    private final ConcurrentLinkedQueue<ZmuxSession> ready = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queued = new AtomicInteger();
    private final Semaphore available = new Semaphore(0);
    private final MemoryBudget budget;
    private final ListenerHub hub;
    private final Admission admission;
    private final Thread acceptor;
    /** Accept is closed: by {@link #shutdown} or {@link #close}. */
    private volatile boolean closed;
    /** Closed: the listener takes no carrier at all. */
    private final AtomicBoolean stopped = new AtomicBoolean();
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
        InetSocketAddress local = null;
        if (server != null) {
            try { local = (InetSocketAddress) server.getLocalAddress(); } catch (IOException e) { throw new java.io.UncheckedIOException(e); }
        }
        this.localAddress = local;
        TlsOptions t = o.tls();
        String[] protos = ZmuxConfig.withAliases(cfg.alpn()).toArray(String[]::new);
        this.tls = t == null ? null : () -> t.serverEngine(protos);
        this.backlog = o.backlog();
        this.pressureListener = o.memoryPressureListener();
        this.budget = new MemoryBudget(o.memoryBudget().orElse(io.zmux.internal.core.CoreConfig.UNLIMITED), this::memoryChanged);
        this.hub = new ListenerHub(new ListenerHub.Owner() {
            @Override
            public io.zmux.internal.loop.DriverHooks hooks() { return cfg.hooks; }

            @Override
            public CoreConfig config() {
                return cfg.toCore(CoreConfig.Role.RESPONDER, CoreConfig.Role.RESPONDER);
            }

            @Override
            public void started(SessionRuntime d, String alpn) {
                offerWhenEstablished(d, new ZmuxSession(d, d.core(), alpn, cfg.credential()));
            }
        }, budget, o.instanceId(), o.maxPendingPerSource(), ZmuxConfig.nanos(o.firstRecordTimeout()), o.maxSuspended());
        Admission adm = o.admission();
        admission = adm;
        if (adm != null) {
            long stallMax = ZmuxConfig.nanos(adm.stallMax()), stallMin = ZmuxConfig.nanos(adm.stallMin());
            if (stallMax == 0) {
                stallMax = draw(30_000_000_000L, 120_000_000_000L);
                stallMin = draw(stallMax / 4, stallMax * 3 / 4);
            }
            long peekTimeout = adm.peekTimeout().isZero() ? draw(5_000_000_000L, 30_000_000_000L) : ZmuxConfig.nanos(adm.peekTimeout());
            hub.admission(adm.decider(), adm.peek(), peekTimeout, stallMin, stallMax, forward(o.fallback()));
        }
        hub.inbound(o.inboundTransform(), o.answerUnknownJoins());
        this.acceptor = server == null ? null : Thread.ofVirtual().name("zmux-acceptor").start(this::acceptLoop);
    }

    private static ListenerHub.Forward forward(Admission.Fallback f) {
        return f == null ? null : f::serve;
    }

    /** Uniform in [lo, hi] from a secure random source. */
    private static long draw(long lo, long hi) {
        return lo + Long.remainderUnsigned(RANDOM.nextLong(), hi - lo + 1);
    }

    /**
     * The duration of a {@link Admission.Verdict#STALL} for a carrier from source: within the
     * admission's bounds and the same for every carrier of one client (an IPv6 address
     * aggregated to its /64, the port ignored) for the listener's lifetime; 0 without an admission.
     */
    public java.time.Duration stallDuration(InetSocketAddress source) {
        return java.time.Duration.ofNanos(hub.stallDuration(source));
    }

    static ZmuxListener external(ListenerConfig cfg) {
        return new ZmuxListener(null, cfg);
    }

    /**
     * A carrier handed to the listener ({@link #handle}). verdict is the carrier's admission
     * verdict ({@link Admission.Verdict#ACCEPT} without an admission; a {@link
     * Admission.Verdict#FALLBACK} without a target is reported as {@link Admission.Verdict#STALL},
     * a stall beyond its source's share as {@link Admission.Verdict#CLOSE}); closed ends with the
     * carrier's lifetime, which keeps the handler of an HTTP carrier alive. taken completes with
     * true once a session took the carrier, or with false when the carrier ended before one did:
     * the listener (or the admission) refused it, and wrote nothing on it.
     */
    public final class Handled {
        private final CompletionStage<Admission.Verdict> verdict;
        private final CompletionStage<Boolean> taken;
        private final CompletionStage<Void> closed;
        /** The accept's admission value, which the siblings of an accepted carrier share. */
        private volatile Object value;

        private Handled(CompletionStage<Admission.Verdict> verdict, CompletionStage<Boolean> taken, CompletionStage<Void> closed) {
            this.verdict = verdict; this.taken = taken; this.closed = closed;
        }

        public CompletionStage<Admission.Verdict> verdict() { return verdict; }
        public CompletionStage<Boolean> taken() { return taken; }
        public CompletionStage<Void> closed() { return closed; }

        /**
         * Hands over a further carrier of the connection this carrier's admission accepted (a
         * further stream of a QUIC connection, admitted by its first): it takes its own source
         * slot and is classified like this one, without being decided again. Only after an
         * {@link Admission.Verdict#ACCEPT}; a carrier refused at its slot is closed.
         */
        public Handled sibling(PipeIo io, JoinHint hint) {
            var v = verdict.toCompletableFuture();
            if (!v.isDone() || v.isCompletedExceptionally() || v.join() != Admission.Verdict.ACCEPT)
                throw new IllegalStateException("the carrier was not accepted");
            Objects.requireNonNull(io); Objects.requireNonNull(hint);
            if (!hub.reserve(hint.source())) return refused(io);
            return transfer(io, hint, true, value, null);
        }
    }

    /**
     * A slot of a carrier's source ({@link ListenerConfig#maxPendingPerSource()}), taken by
     * {@link #reserve} when an entry accepts a connection, before its outer handshake; it is
     * used once, by {@link #handle(PipeIo, JoinHint, Admission.Fallback)}, {@link #handshakeFailed()} or {@link #release()}.
     */
    public final class Slot {
        private final InetSocketAddress peer, source;
        private final AtomicBoolean used = new AtomicBoolean();

        private Slot(InetSocketAddress peer, InetSocketAddress source) { this.peer = peer; this.source = source; }

        /** The address the carrier arrived from directly. */
        public InetSocketAddress peer() { return peer; }
        /** The carrier's source: the one given, else the peer. */
        public InetSocketAddress source() { return source; }

        /**
         * Hands the carrier over, as {@link ZmuxListener#handle(PipeIo, JoinHint, Admission.Fallback)}
         * does after taking a slot; hint's addresses are this slot's.
         */
        public Handled handle(PipeIo io, JoinHint hint, Admission.Fallback fallback) {
            take();
            return transfer(io, new JoinHint(hint.join(), hint.pairing(), hint.uplink(), source, peer, hint.request()), false, null, fallback);
        }

        /** The entry gave the carrier up before handing it over (its connection ended unused): the slot goes, counted nowhere. */
        public void release() {
            take();
            hub.release(source);
        }

        /** The carrier's outer handshake (TLS, QUIC) failed or timed out: the slot goes, counted as such. */
        public void handshakeFailed() {
            take();
            hub.handshakeFailed(source);
        }

        private void take() {
            if (!used.compareAndSet(false, true)) throw new IllegalStateException("slot used");
        }
    }

    /**
     * Takes a slot of source (null: peer) for a carrier an entry just accepted, before its
     * outer handshake; null when the source has its share of pending carriers, or the listener
     * takes no carrier any more: the entry then closes the connection at once, without a
     * handshake, a read or a decision (counted in {@link ListenerStats#refused()}).
     */
    public Slot reserve(InetSocketAddress peer, InetSocketAddress source) {
        InetSocketAddress s = source == null ? peer : source;
        return hub.reserve(s) ? new Slot(peer, s) : null;
    }

    /**
     * Hands a carrier to the listener, the one entry for carriers its own sockets do not
     * accept (HTTP and WebSocket requests, QUIC connections, a host's own pipes): it takes a
     * slot of hint's source, then the listener's {@link ListenerConfig#admission() admission},
     * when configured, decides on it with hint's addresses and {@link JoinHint#request()
     * request} (a carrier decided on its request head is decided at once, without a prefix) and
     * the TLS metadata of io's capabilities; an accepted carrier is classified by hint's join or
     * pairing or by its in-band first record. fallback (null: none) takes a carrier the
     * admission gives back, with every byte read replayed. A carrier refused at its slot is closed.
     */
    public Handled handle(PipeIo io, JoinHint hint, Admission.Fallback fallback) {
        Objects.requireNonNull(io); Objects.requireNonNull(hint);
        if (!hub.reserve(hint.source())) return refused(io);
        return transfer(io, hint, false, null, fallback);
    }

    /** {@link #handle(PipeIo, JoinHint, Admission.Fallback)} without a fallback target. */
    public Handled handle(PipeIo io, JoinHint hint) {
        return handle(io, hint, null);
    }

    private Handled refused(PipeIo io) {
        // Closed as the listener closes a carrier it refuses: both halves end, nothing written.
        try { if (io.reader() != null) io.reader().close(); }
        finally { if (io.writer() != null) io.writer().close(); }
        // Without an admission every carrier counts as accepted; refused, it was not taken and nothing was written.
        var v = admission == null ? Admission.Verdict.ACCEPT : Admission.Verdict.CLOSE;
        return new Handled(CompletableFuture.completedFuture(v), CompletableFuture.completedFuture(false),
                CompletableFuture.completedFuture(null));
    }

    /** Transfers a carrier whose slot is held. */
    private Handled transfer(PipeIo io, JoinHint hint, boolean admitted, Object value, Admission.Fallback fallback) {
        Objects.requireNonNull(io); Objects.requireNonNull(hint);
        CompletableFuture<ListenerHub.Decided> code = new CompletableFuture<>();
        Handled[] self = new Handled[1];
        CompletionStage<Admission.Verdict> verdict = code.thenApply(d -> { if (self[0] != null) self[0].value = d.value(); return d.code(); })
                .thenApply(v -> switch (v) {
            case ListenerHub.Admit.FALLBACK -> Admission.Verdict.FALLBACK;
            case ListenerHub.Admit.STALL -> Admission.Verdict.STALL;
            case ListenerHub.Admit.CLOSE -> Admission.Verdict.CLOSE;
            default -> Admission.Verdict.ACCEPT;
        });
        try {
            String protocol = io.capabilities().applicationProtocol();
            if (!acceptsProtocol(protocol) || !hint.pairing().isEmpty() && !hint.join().isEmpty()) {
                hub.release(hint.source());
                hub.counts().rejected();
                returnCarrier(io, new javax.net.ssl.SSLHandshakeException("carrier refused"));
                code.complete(new ListenerHub.Decided(ListenerHub.Admit.CLOSE, null));
                return new Handled(verdict, CompletableFuture.completedFuture(false), CompletableFuture.completedFuture(null));
            }
            var loop = io.carrierConn() instanceof SessionRuntime parent ? parent.loop() : cfg.nextLoop();
            // The value is recorded before the verdict is reported (the hub completes code off the loop).
            Handled h = new Handled(verdict, new CompletableFuture<>(), new CompletableFuture<>());
            self[0] = h;
            io.zmux.internal.loop.Transport carrier = hub.external(loop, io, hint, admitted, value, forward(fallback), code);
            carrier.whenTaken().whenComplete((t, e) -> ((CompletableFuture<Boolean>) h.taken).complete(Boolean.TRUE.equals(t)));
            carrier.whenClosed().whenComplete((t, e) -> ((CompletableFuture<Void>) h.closed).complete(null));
            return h;
        } catch (RuntimeException error) {
            hub.release(hint.source());
            hub.counts().refused();
            returnCarrier(io, error);
            code.complete(new ListenerHub.Decided(ListenerHub.Admit.CLOSE, null));
            return new Handled(verdict, CompletableFuture.completedFuture(false), CompletableFuture.failedFuture(error));
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
        if (closed) throw new ClosedChannelException();
        try { return hub.prepare(cfg.toCore(role, role)); }
        catch (IllegalStateException ended) { throw new ClosedChannelException(); }
    }

    /**
     * The admission of this listener's carriers ({@link ListenerConfig#admission()}), read-only;
     * null: every carrier is accepted. Adapters that write a response before the listener's
     * first byte (an HTTP carrier's header) ask it whether a verdict is to wait for.
     */
    public Admission admission() { return admission; }

    /** The configuration of this listener's sessions ({@link ListenerConfig#session()}), which its dialers also use. */
    public ZmuxConfig config() { return cfg; }

    /** Dials a session whose peer can join new carriers through this listener; presents {@link ZmuxConfig#credential()}. */
    public ZmuxSession connect(InetSocketAddress address) throws IOException {
        CoreConfig core = outgoing(CoreConfig.Role.INITIATOR);
        var loop = cfg.nextLoop();
        SocketChannel channel = Zmux.dial(address, cfg.handshakeNanos());
        Zmux.present(channel, cfg.credential(), address);
        return Zmux.established(Bootstrap.socket(loop, core, channel, cfg.hooks, hub::register), cfg);
    }

    /** Dials a TLS session and registers its inbound join route before sending its parameters; presents {@link ZmuxConfig#credential()}. */
    public ZmuxSession connectTls(InetSocketAddress address, TlsOptions tls) throws IOException {
        CoreConfig core = outgoing(CoreConfig.Role.INITIATOR);
        SSLEngine engine = tls.clientEngine(address.getPort(), cfg.alpn());
        var loop = cfg.nextLoop();
        SocketChannel channel = Zmux.dial(address, cfg.handshakeNanos());
        return Zmux.established(Bootstrap.tls(loop, core, channel, engine, cfg.hooks, hub::register, Zmux.presenter(cfg.credential(), address)), cfg);
    }

    /**
     * Starts a session on an external carrier, with joins routed by this listener: as
     * {@link Role#CLIENT}, or {@link Role#PEER} (designate its bootstrap dialer before
     * duplicate resolution). Responder carriers go through {@link #handle(PipeIo, JoinHint, Admission.Fallback)}.
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

    /** The bytes the listener's sessions hold against its memory budget. */
    public long memoryUsed() {
        return budget.used();
    }

    /** First and largest pause after a failed accept (descriptor exhaustion and the like). */
    private static final long ACCEPT_BACKOFF_MIN = TimeUnit.MILLISECONDS.toNanos(5);
    /** Hands s to the accept queue once established; the queue (or a backlog close) then owns it. */
    private void offerWhenEstablished(SessionRuntime d, ZmuxSession s) {
        boolean[] offered = {false};
        d.eventSink(e -> {
            if (e.kind() == Event.Kind.ESTABLISHED && !offered[0]) {
                offered[0] = true;
                if (!offer(s)) s.close(new ApplicationError(0x02, "listener backlog"));
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
            else hub.tls(loop, ch, tls.get(), hto, peer);
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

    /**
     * Counts a connection that an adapter running its own accept loop could not make a pipe input of (a failed TLS
     * handshake or its timeout, a failed HTTP or WebSocket upgrade) in {@link ListenerStats#setupFailures()}; any
     * thread.
     */
    public void countSetupFailure() { hub.counts().setupFailed(); }

    /** The listener's outcome counts (API.md Section 7.2); any thread, no lock. */
    public ListenerStats stats() {
        var c = hub.counts();
        return new ListenerStats(c.sessions(), c.joins(), c.setupFailures(), c.rejectedCount(), c.refusedCount(), c.timedOutCount(),
                c.admission());
    }

    /** Waits for the next established session; throws ClosedChannelException once the listener closes. */
    public ZmuxSession accept() throws IOException {
        return take(false, 0);
    }

    /** {@link #accept()} on a virtual thread. */
    public CompletableFuture<ZmuxSession> acceptAsync() {
        CompletableFuture<ZmuxSession> f = new CompletableFuture<>();
        Thread.ofVirtual().name("zmux-accept").start(() -> {
            try { f.complete(accept()); } catch (Throwable e) { f.completeExceptionally(e); }
        });
        return f;
    }

    /**
     * Waits at most timeout for the next established session; null on timeout, and
     * ClosedChannelException once the listener closes.
     */
    public ZmuxSession accept(Duration timeout) throws IOException {
        return take(true, ZmuxConfig.nanos(timeout));
    }

    private ZmuxSession take(boolean timed, long nanos) throws IOException {
        if (io.zmux.internal.loop.SessionHost.onHostThread()) throw new IllegalStateException("accept on a zmux event-loop thread");
        if (closed) throw new ClosedChannelException();
        try {
            if (!timed) available.acquire();
            else if (!available.tryAcquire(Math.max(0, nanos), TimeUnit.NANOSECONDS)) return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted");
        }
        ZmuxSession s = ready.poll();
        if (s == null) { available.release(); throw new ClosedChannelException(); } // close drained it: wake the next taker.
        queued.decrementAndGet();
        if (closed) available.release(); // The permit may have been close's wake-up: pass it on.
        return s;
    }

    /**
     * Shuts the listener down gracefully: {@link #accept()} fails from now on, the sessions not
     * yet accepted are closed, and no new session is taken (a new session's first record or
     * split bootstrap is refused); the listener's live and suspended sessions still take their
     * joins and resumptions, which pass the source slot, the TLS handshake and the admission as
     * before. Once none of its sessions is left, or when timeout passes first, the listener
     * {@link #close closes}; the stage completes then. Sessions are not closed by it.
     */
    public CompletionStage<Void> shutdown(Duration timeout) {
        Objects.requireNonNull(timeout);
        if (!stopped.get()) {
            closeAccept();
            hub.drain(this::closeQuietly);
            CompletableFuture.delayedExecutor(Math.max(0, ZmuxConfig.nanos(timeout)), TimeUnit.NANOSECONDS)
                    .execute(this::closeQuietly);
        }
        return terminated.copy();
    }

    /** Ends {@link #accept()} and closes the sessions not yet accepted. */
    private void closeAccept() {
        closed = true;
        for (ZmuxSession s; (s = ready.poll()) != null; ) { queued.decrementAndGet(); s.close(); }
        available.release(); // Wakes one waiting acceptor; each passes the wake-up on.
    }

    private void closeQuietly() {
        try { close(); } catch (IOException ignored) { /* The socket close failed: the listener is closed all the same. */ }
    }

    /**
     * Closes the listener at once: it takes no carrier any more (joins and resumptions
     * included), cancels the admission decisions under way, and closes the sessions not yet
     * accepted; accepted sessions stay, but their pipes can no longer join.
     */
    @Override
    public void close() throws IOException {
        if (!stopped.compareAndSet(false, true)) return;
        try {
            closeAccept();
            hub.close();
            pressureCallbacks.close();
            if (server != null) server.close();
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
