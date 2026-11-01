package io.zmux.quic;

import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.zmux.PoolPolicy;
import io.zmux.carrier.PipeIo;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** A route's QUIC connection pool. The client endpoint remains application-owned. */
public final class QuicPool implements AutoCloseable {
    /**
     * Limits of each connection. maxPipesPerConnectionLifetime 0 and maxConnectionAge zero are no limit:
     * a connection that may carry no pipe, or live no time, could not serve the pool.
     */
    public record Limits(int maxPipesPerConnection, long maxPipesPerConnectionLifetime, Duration maxConnectionAge) {
        private static final Duration MAX_AGE = Duration.ofNanos(Long.MAX_VALUE);
        public Limits {
            Objects.requireNonNull(maxConnectionAge);
            if (maxPipesPerConnection < 1 || maxPipesPerConnectionLifetime < 0 || maxConnectionAge.isNegative())
                throw new IllegalArgumentException("invalid connection pool limits");
            if (maxConnectionAge.compareTo(MAX_AGE) > 0) throw new IllegalArgumentException("age exceeds monotonic clock range");
        }
        public static Limits defaults() { return new Limits(8, 0, Duration.ZERO); }
    }
    /** Active includes streams reserved for creation; total counts created streams and failed native opening attempts, including cancelled creations. */
    public record ConnectionInfo(QuicChannel connection, long fateGroup, int active, long total, boolean retiring, Duration age) {}

    private static final AtomicLong NEXT_FATE = new AtomicLong(ThreadLocalRandom.current().nextLong(1, 1L << 61));
    private final QuicClient client;
    private final InetSocketAddress address;
    private final String serverName;
    private final Limits limits;
    private final io.zmux.Credential credential;
    private final PoolPolicy policy;
    private boolean dialed;
    private long lastDial;
    private int failures;
    /** Connections the policy retired that no added connection has replaced yet. */
    private int replacements;
    private final ArrayList<Entry> entries = new ArrayList<>();
    private final HashSet<Request> requests = new HashSet<>();
    private Opening opening;
    private static final class Opening {
        final CompletableFuture<Entry> ready = new CompletableFuture<>();
        CompletableFuture<ConnectionCarrier> nativeDial;
        boolean cancelled;
        /** Opened for the policy without a request; ending requests do not cancel it. */
        boolean background;
    }
    private boolean closed;
    /** Asks the policy again at the latest decision's recheck; null: none pending. */
    private io.netty.util.concurrent.ScheduledFuture<?> recheck;
    /** Counts the rechecks scheduled or dropped; a firing recheck is the pending one only when it matches. */
    private long recheckGeneration;

    /**
     * A pool of connections to address; each connection's first stream starts with
     * credential's prefix (null: none), for the server's admission of the connection.
     *
     * <p>policy (null: none) is asked when a pipe is taken or released and after a connection is
     * added, with a member per connection that takes new pipes (ID: its fate group; idle: 0 while
     * it carries a pipe, else since its last pipe ended, or since it was added if it never carried
     * one; pipes: its active pipes; bytes 0). A count is the connections to
     * spread pipes over (unset: none beyond those the pipes need): while fewer are open and dialAfter has passed since the last dial
     * started, the pool adds one. Retired connections take no new pipe, a replacement is
     * dialled before they close, and they close once their last pipe ends. The policy is asked
     * again no later than a decision's recheck, without pipe events. A null decision or an
     * exception from the policy keeps the pool's own behaviour.
     */
    public QuicPool(QuicClient client, InetSocketAddress address, String serverName, Limits limits, io.zmux.Credential credential,
                    PoolPolicy policy) {
        this.client = Objects.requireNonNull(client);
        if (!client.listenerBacked()) throw new IllegalArgumentException("pooled connections require a listener-backed client for shared incoming routing");
        this.address = Objects.requireNonNull(address);
        this.serverName = Objects.requireNonNull(serverName); this.limits = Objects.requireNonNull(limits);
        this.credential = credential;
        this.policy = policy;
    }
    public QuicPool(QuicClient client, InetSocketAddress address, String serverName, Limits limits, io.zmux.Credential credential) {
        this(client, address, serverName, limits, credential, null);
    }
    public QuicPool(QuicClient client, InetSocketAddress address, String serverName, Limits limits) { this(client, address, serverName, limits, null); }
    public QuicPool(QuicClient client, InetSocketAddress address, String serverName) { this(client, address, serverName, Limits.defaults()); }

    private static final class Entry {
        final ConnectionCarrier carrier;
        final long fate, born = System.nanoTime();
        long idleSince = born;
        final AtomicBoolean disposed = new AtomicBoolean();
        int active, pending;
        long total;
        boolean retiring;
        volatile io.netty.util.concurrent.ScheduledFuture<?> expiry;
        Entry(ConnectionCarrier carrier, long fate) { this.carrier = carrier; this.fate = fate; }
    }
    private final class Request {
        final CompletableFuture<PipeIo> result = new CompletableFuture<>();
        volatile CompletableFuture<PipeIo> stream;
        boolean stopped;
        Request() {
            result.whenComplete((value,error) -> {
                CompletableFuture<ConnectionCarrier> unused = null;
                synchronized (QuicPool.this) {
                    if (error != null) stopped = true;
                    requests.remove(this);
                    if (requests.isEmpty() && opening != null && !opening.background) { opening.cancelled = true; unused = opening.nativeDial; }
                }
                if (unused != null) unused.cancel(false);
                if (error != null) { var pending = stream; if (pending != null) pending.cancel(false); }
            });
        }
    }
    private final class Lease implements ConnectionCarrier.StreamObserver {
        final Entry entry;
        boolean settled;
        final AtomicBoolean released = new AtomicBoolean();
        Lease(Entry entry) { this.entry = entry; }
        @Override public void created(QuicStreamChannel stream) {
            synchronized (QuicPool.this) {
                if (!settled) { settled = true; entry.pending--; if (entry.total < Long.MAX_VALUE) entry.total++; }
                if (limits.maxPipesPerConnectionLifetime() > 0 && entry.total >= limits.maxPipesPerConnectionLifetime()) entry.retiring = true;
            }
            stream.closeFuture().addListener(ignored -> release());
        }
        @Override public void failed(Throwable cause) {
            boolean unobserved;
            synchronized (QuicPool.this) {
                unobserved = !settled;
                if (unobserved) { settled = true; entry.pending--; if (entry.total < Long.MAX_VALUE) entry.total++; }
                entry.retiring = true;
            }
            // Once observed, the native channel's closeFuture owns release even if registration failed.
            if (unobserved) release(); else retire(entry);
        }
        void cancelReservation() {
            synchronized (QuicPool.this) { if (!settled) { settled = true; entry.pending--; } }
            release();
        }
        void release() {
            if (!released.compareAndSet(false,true)) return;
            boolean dispose;
            synchronized (QuicPool.this) {
                entry.active--; expire(entry);
                if (entry.active == 0) entry.idleSince = System.nanoTime();
                dispose = entry.retiring && entry.active == 0 && entries.remove(entry);
            }
            if (dispose) dispose(entry);
            maintain();
        }
    }

    /** Opens one pooled carrier. Cancellation reclaims its stream; joined carriers outlive this call and pool close. */
    public CompletionStage<PipeIo> openPipe() {
        Request request = new Request(); boolean rejected;
        synchronized (this) { rejected = closed; if (!rejected) requests.add(request); }
        var notification = ConnectionCarrier.offLoop(request.result, client.completions(), QuicPool::discard);
        if (rejected) request.result.completeExceptionally(new IOException("QUIC pool closed")); else acquire(request, true);
        return notification;
    }
    private static void discard(PipeIo pipe) {
        try { if (pipe.reader() != null) pipe.reader().close(); }
        finally { if (pipe.writer() != null) pipe.writer().abort(new IOException("pooled operation cancelled")); }
    }
    private void expire(Entry entry) {
        if (!entry.carrier.channel().isActive() || !limits.maxConnectionAge().isZero()
                && System.nanoTime() - entry.born >= limits.maxConnectionAge().toNanos()) entry.retiring = true;
    }
    private long room(Entry entry) {
        long remaining = limits.maxPipesPerConnection() - entry.active;
        if (limits.maxPipesPerConnectionLifetime() > 0)
            remaining = Math.min(remaining, limits.maxPipesPerConnectionLifetime() - Math.min(entry.total,limits.maxPipesPerConnectionLifetime()) - entry.pending);
        return remaining;
    }
    /** policyDial: the policy may send the request to a new connection although one has room. */
    private void acquire(Request request, boolean policyDial) {
        ArrayList<Entry> idle = new ArrayList<>(); Lease lease = null;
        Opening waiting = null; boolean dial = false, reject = false, diverted = false;
        synchronized (this) {
            if (request.result.isDone()) return;
            if (closed) reject = true;
            else {
                boolean due = consultLocked() && policyDial;
                Entry best = null;
                for (int i=0;i<entries.size();) {
                    Entry entry = entries.get(i); expire(entry);
                    if (entry.retiring && entry.active == 0) { entries.remove(i); idle.add(entry); continue; }
                    if (!entry.retiring && room(entry) > 0 && (best == null || room(entry) > room(best))) best = entry;
                    i++;
                }
                if (best != null && !due) { best.active++; best.pending++; lease = new Lease(best); }
                else {
                    if (opening == null) { opening = new Opening(); dial = true; }
                    waiting = opening; diverted = best != null;
                }
            }
        }
        idle.forEach(QuicPool::dispose);
        if (reject) { request.result.completeExceptionally(new IOException("QUIC pool closed")); return; }
        if (lease != null) { open(request,lease); return; }
        var shared = waiting; boolean fallback = diverted;
        shared.ready.whenComplete((entry,error) -> {
            if (error instanceof CancellationException) acquire(request, policyDial);
            else if (error != null && fallback) acquire(request, false); // a connection had room
            else if (error != null) request.result.completeExceptionally(error);
            else acquire(request, true);
        });
        if (dial) dial(shared);
    }
    private void dial(Opening shared) {
        boolean abort;
        synchronized (this) {
            abort = shared.cancelled || closed || opening != shared;
            if (abort && opening == shared) opening = null;
        }
        if (abort) { shared.ready.completeExceptionally(new CancellationException()); return; }
        synchronized (this) { dialed = true; lastDial = System.nanoTime(); }
        // Keep the Opening published until this factory returns a handle and cancellation is propagated.
        CompletableFuture<ConnectionCarrier> dialing;
        try { dialing = client.connectCarrier(address,serverName,credential).toCompletableFuture(); }
        catch (RuntimeException error) {
            synchronized (this) { if (opening == shared) opening = null; failures++; }
            shared.ready.completeExceptionally(error); return;
        }
        boolean cancel;
        synchronized (this) { shared.nativeDial = dialing; cancel = shared.cancelled || closed; }
        dialing.whenComplete((carrier,error) -> {
            if (error != null) {
                boolean cancelled;
                synchronized (this) { if (opening == shared) opening = null; cancelled = shared.cancelled; if (!cancelled) failures++; }
                shared.ready.completeExceptionally(cancelled ? new CancellationException() : error); return;
            }
            long fate = NEXT_FATE.getAndIncrement();
            if (fate <= 0 || fate > ((1L << 62)-1)) {
                carrier.dispose(); synchronized (this) { if (opening == shared) opening = null; }
                shared.ready.completeExceptionally(new IOException("fate group namespace exhausted")); return;
            }
            Entry entry = new Entry(carrier,fate); boolean adopted;
            synchronized (this) {
                adopted = !closed && !shared.cancelled && opening == shared;
                if (opening == shared) opening = null;
                if (adopted) { entries.add(entry); failures = 0; if (replacements > 0) replacements--; }
            }
            if (!adopted) { carrier.dispose(); shared.ready.completeExceptionally(new CancellationException()); return; }
            carrier.channel().closeFuture().addListener(ignored -> retire(entry));
            if (!limits.maxConnectionAge().isZero()) {
                try {
                    entry.expiry = ChannelLoops.schedule(carrier.channel(), () -> retire(entry), limits.maxConnectionAge().toNanos(), TimeUnit.NANOSECONDS);
                    if (entry.disposed.get()) entry.expiry.cancel(false);
                } catch (RejectedExecutionException ended) { retire(entry); }
            }
            shared.ready.complete(entry);
            maintain();
        });
        if (cancel) dialing.cancel(false);
    }
    private void open(Request request, Lease lease) {
        if (request.result.isDone()) { lease.cancelReservation(); return; }
        var stream = lease.entry.carrier.openManagedPipe(lease).toCompletableFuture(); request.stream = stream;
        stream.whenComplete((pipe,error) -> {
            if (error != null) request.result.completeExceptionally(error);
            else {
                boolean accepted;
                synchronized (this) {
                    accepted = !closed && !request.stopped && !request.result.isDone();
                    if (accepted) requests.remove(request);
                }
                if (!accepted || !request.result.complete(pipe.withFateGroup(lease.entry.fate))) discard(pipe);
            }
        });
        if (request.result.isDone()) stream.cancel(false);
    }
    /**
     * Asks the policy, if any, and marks the connections it retires; reports whether a new
     * connection is due. Retired connections without pipes are left to the caller's sweep.
     */
    private boolean consultLocked() {
        if (policy == null || closed) return false;
        long now = System.nanoTime();
        var view = new ArrayList<PoolPolicy.Member>(entries.size());
        for (Entry entry : entries) {
            expire(entry);
            if (entry.retiring) continue;
            view.add(new PoolPolicy.Member(entry.fate, Duration.ofNanos(Math.max(0, now - entry.born)),
                    entry.active > 0 ? Duration.ZERO : Duration.ofNanos(Math.max(0, now - entry.idleSince)),
                    entry.active, 0, 0, true, false));
        }
        PoolPolicy.Decision decision;
        try {
            decision = policy.decide(new PoolPolicy.View(view, failures, dialed ? Duration.ofNanos(now - lastDial) : null));
            if (decision == null) decision = PoolPolicy.Decision.KEEP;
        } catch (RuntimeException error) { decision = PoolPolicy.Decision.KEEP; }
        recheckLocked(decision.recheckNanos());
        int open = view.size();
        for (Entry entry : entries)
            if (!entry.retiring && decision.retire().contains(entry.fate)) { entry.retiring = true; open--; replacements++; }
        long after;
        try { after = decision.dialAfter().map(Duration::toNanos).orElse(0L); } catch (ArithmeticException tooLong) { after = Long.MAX_VALUE; }
        return (open < decision.count().orElse(0) || replacements > 0) && (!dialed || now - lastDial >= after);
    }
    /** Replaces the pending recheck by one after n nanoseconds (0: none). */
    private void recheckLocked(long n) {
        if (recheck != null) recheck.cancel(false);
        recheck = null;
        long generation = ++recheckGeneration;
        if (n <= 0) return;
        try { recheck = client.nativeLoop().schedule(() -> rechecked(generation), n, TimeUnit.NANOSECONDS); }
        catch (RejectedExecutionException ended) { /* The client is closed. */ }
    }
    /** A recheck fired; one replaced (or dropped) after it started to fire leaves the pending one alone. */
    private void rechecked(long generation) {
        synchronized (this) {
            if (generation != recheckGeneration) return;
            recheck = null;
        }
        maintain();
    }
    /** Applies the policy outside a request: closes idle retired connections and dials a due connection. */
    private void maintain() {
        if (policy == null) return;
        ArrayList<Entry> idle = new ArrayList<>(); Opening start = null;
        synchronized (this) {
            if (closed) return;
            boolean due = consultLocked();
            for (int i=0;i<entries.size();) {
                Entry entry = entries.get(i);
                if (entry.retiring && entry.active == 0) { entries.remove(i); idle.add(entry); } else i++;
            }
            if (due && opening == null) { start = opening = new Opening(); start.background = true; }
        }
        idle.forEach(QuicPool::dispose);
        if (start != null) dial(start);
    }
    private void retire(Entry entry) {
        boolean done;
        synchronized (this) { entry.retiring = true; done = entry.active == 0 && entries.remove(entry); }
        if (done) dispose(entry);
    }
    private static void dispose(Entry entry) {
        if (!entry.disposed.compareAndSet(false,true)) return;
        var expiry = entry.expiry; if (expiry != null) expiry.cancel(false);
        entry.carrier.dispose();
    }
    /** Current owned connections, including retiring connections held by live streams. */
    public synchronized List<ConnectionInfo> connections() {
        long now = System.nanoTime();
        return entries.stream().map(entry -> new ConnectionInfo(entry.carrier.channel(),entry.fate,entry.active,entry.total,
                entry.retiring,Duration.ofNanos(Math.max(0,now-entry.born)))).toList();
    }
    /** Stops creation, cancels uncommitted requests and drains connections without interrupting handed-out streams. */
    @Override public void close() {
        List<Entry> idle = new ArrayList<>(); List<Request> pending; CompletableFuture<ConnectionCarrier> cancel;
        synchronized (this) {
            if (closed) return; closed = true;
            recheckLocked(0);
            if (opening != null) opening.cancelled = true;
            cancel = opening == null ? null : opening.nativeDial;
            pending = List.copyOf(requests);
            for (Request request : pending) request.stopped = true;
            for (int i=0;i<entries.size();) {
                Entry entry = entries.get(i); entry.retiring = true;
                if (entry.active == 0) { idle.add(entry); entries.remove(i); } else i++;
            }
        }
        for (Request request : pending) request.result.completeExceptionally(new IOException("QUIC pool closed"));
        if (cancel != null) cancel.cancel(false);
        idle.forEach(QuicPool::dispose);
    }
}
