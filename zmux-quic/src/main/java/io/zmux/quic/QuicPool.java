package io.zmux.quic;

import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.zmux.PoolDecision;
import io.zmux.PoolMember;
import io.zmux.PoolPolicy;
import io.zmux.PoolView;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.spi.PolicyCall;
import io.zmux.internal.spi.PoolShrink;
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

/**
 * A route's QUIC connection pool. The client endpoint remains application-owned.
 *
 * <p>A pool built with a {@linkplain Builder#policy policy} must be closed: it looks at least every 100 ms until
 * {@link #close()}, on a timer of the client's loop, so one left open keeps asking its policy for as long as the
 * client lives. A pool without a policy holds no timer.
 */
public final class QuicPool implements AutoCloseable {
    /**
     * Limits of each connection.
     *
     * @param maxPipesPerConnection pipes open on one connection at once, at least 1 (default 8)
     * @param maxPipesPerConnectionLifetime pipes one connection carries in its life, at least 1; empty: no limit (the default)
     * @param maxConnectionAge how long one connection takes new pipes, positive; empty: no limit (the default)
     */
    public record Limits(int maxPipesPerConnection, java.util.OptionalLong maxPipesPerConnectionLifetime, java.util.Optional<Duration> maxConnectionAge) {
        private static final Duration MAX_AGE = Duration.ofNanos(Long.MAX_VALUE);
        public Limits {
            Objects.requireNonNull(maxPipesPerConnectionLifetime);
            Objects.requireNonNull(maxConnectionAge);
            if (maxPipesPerConnection < 1 || (maxPipesPerConnectionLifetime.isPresent() && maxPipesPerConnectionLifetime.getAsLong() < 1)
                    || (maxConnectionAge.isPresent() && !maxConnectionAge.get().isPositive()))
                throw new IllegalArgumentException("invalid connection pool limits");
            if (maxConnectionAge.isPresent() && maxConnectionAge.get().compareTo(MAX_AGE) > 0) throw new IllegalArgumentException("age exceeds monotonic clock range");
        }
        public static Limits defaults() { return new Limits(8, java.util.OptionalLong.empty(), java.util.Optional.empty()); }
        public Limits withMaxPipesPerConnection(int n) { return new Limits(n, maxPipesPerConnectionLifetime, maxConnectionAge); }
        public Limits withMaxPipesPerConnectionLifetime(long n) { return new Limits(maxPipesPerConnection, java.util.OptionalLong.of(n), maxConnectionAge); }
        public Limits withMaxConnectionAge(Duration d) { return new Limits(maxPipesPerConnection, maxPipesPerConnectionLifetime, java.util.Optional.of(d)); }
    }
    /** Active includes streams reserved for creation; total counts created streams and failed native opening attempts, including cancelled creations. */
    public record PooledConnection(QuicChannel connection, long fateGroup, int active, long total, boolean retiring, Duration age) {}

    private static final AtomicLong NEXT_FATE = new AtomicLong(ThreadLocalRandom.current().nextLong(1, 1L << 61));
    private final QuicClient client;
    private final InetSocketAddress address;
    private final String serverName;
    private final Limits limits;
    private final io.zmux.CarrierCredential credential;
    private final PoolPolicy policy;
    private final java.util.function.Consumer<Throwable> decisionRejected;
    private final System.Logger logger;
    /** A decision rejected, reported outside the lock. */
    private Throwable rejected;
    /** The latest decision's count (0: none, no growth) and dial spacing in nanoseconds. */
    private int target;
    private long dialAfter;
    /** A look is running on a thread of its own, and another was asked for meanwhile; guarded by this. */
    private boolean looking, lookAgain;
    /** The looks whose decision was applied; guarded by this. */
    private long looks;
    /** The maintenance interval: with a policy, the pool looks at least this often. */
    private static final long INTERVAL = TimeUnit.MILLISECONDS.toNanos(100);
    /** The limits in nanoseconds and pipes; 0: none. */
    private final long lifetime, maxAge;
    private boolean dialed;
    private long lastDial;
    private int failures;
    private final ArrayList<Entry> entries = new ArrayList<>();
    private final HashSet<Request> requests = new HashSet<>();
    private Opening opening;
    private static final class Opening {
        final CompletableFuture<Entry> ready = new CompletableFuture<>();
        CompletableFuture<ConnectionCarrier> nativeDial;
        boolean cancelled;
        /** Opened for the policy without a request; ending requests do not cancel it. */
        boolean background;
        /** The requests waiting for it; guarded by the pool. */
        final HashSet<Request> waiters = new HashSet<>();
        /** The waiters that a connection with room was passed over for, to reach the policy's count; guarded by the pool. */
        final HashSet<Request> diverted = new HashSet<>();
    }
    /**
     * A growth dial: one the pool started to reach the policy's count and no request needs. A request that a
     * connection with room was passed over for, to reach the count, waits for it without needing it (a shrink cancels
     * the dial and the request asks again); a dial a request with no room waits for is an on-demand dial: not in the
     * view's dialing, not in the size compared with the count, never cancelled by a shrink. Guarded by this.
     */
    private boolean growthDial() {
        return opening != null && !opening.cancelled && opening.background && opening.waiters.size() == opening.diverted.size();
    }
    private boolean closed;
    /** Asks the policy again at the latest decision's recheck; null: none pending. */
    private java.util.concurrent.ScheduledFuture<?> recheck;
    /** Counts the rechecks scheduled or dropped; a firing recheck is the pending one only when it matches. */
    private long recheckGeneration;

    private QuicPool(Builder b) {
        client = b.client; address = b.address; serverName = b.serverName; limits = b.limits;
        lifetime = limits.maxPipesPerConnectionLifetime().orElse(0); maxAge = limits.maxConnectionAge().map(Duration::toNanos).orElse(0L);
        credential = b.credential; policy = b.policy; decisionRejected = b.decisionRejected; logger = b.logger;
    }

    /**
     * A pool of connections to address, named serverName in their TLS; client must be listener-backed, so that the
     * streams the peer opens on a pooled connection are routed.
     */
    public static Builder builder(QuicClient client, InetSocketAddress address, String serverName) {
        Objects.requireNonNull(client, "client");
        if (!client.listenerBacked()) throw new IllegalArgumentException("pooled connections require a listener-backed client for shared incoming routing");
        return new Builder(client, Objects.requireNonNull(address, "address"), Objects.requireNonNull(serverName, "serverName"));
    }

    /** Builds a {@link QuicPool}; every setting not called keeps its default. */
    public static final class Builder {
        private final QuicClient client;
        private final InetSocketAddress address;
        private final String serverName;
        private Limits limits = Limits.defaults();
        private io.zmux.CarrierCredential credential;
        private PoolPolicy policy;
        private java.util.function.Consumer<Throwable> decisionRejected;
        private System.Logger logger;

        private Builder(QuicClient client, InetSocketAddress address, String serverName) {
            this.client = client; this.address = address; this.serverName = serverName;
        }

        /** The limits of each connection; default {@link Limits#defaults()}. */
        public Builder limits(Limits l) { limits = Objects.requireNonNull(l, "l"); return this; }

        /** The credential whose prefix starts each connection's first stream, for the server's admission; default none. */
        public Builder credential(io.zmux.CarrierCredential c) { credential = Objects.requireNonNull(c, "c"); return this; }

        /**
         * The pool's policy, asked when a pipe is taken or released, after each dial ends, no
         * later than a decision's recheck and every 100 ms, with a member per connection (its fate group; idle 0 while
         * it carries a pipe, else since its last pipe ended, or since it was added if it never carried one; pipes:
         * its active pipes; bytes 0; usable while it is not retired and has room). A count is the connections to
         * spread pipes over: while fewer are open and dialAfter has passed since the last dial started, the pool adds
         * one. A dial a pipe request waits for is not a growth dial: it is not in the view's dialing, not counted
         * toward the size, and never cancelled by a shrink. A count below the pool's size, its connections not retired
         * and its growth dial together, cancels that dial first, then retires connections: those retireConnections names, then the longest idle,
         * ties broken by the fewest pipes, the least age, then the highest fate group. A retired connection takes no
         * new pipe at once, no longer counts, is reported unusable and closes once its last pipe ends; the pool
         * dials a new connection when a pipe needs room. The policy is asked on a thread of the pool's own, never on
         * an event loop or under the pool's lock, with no timeout; a pipe request uses the latest decision and does
         * not wait for the policy. Default: no policy.
         */
        public Builder policy(PoolPolicy p) { policy = Objects.requireNonNull(p, "p"); return this; }

        /**
         * Takes the error of a look whose decision the policy failed to give (it threw anything but a VirtualMachineError, or returned null), outside the
         * pool's lock; the pool keeps its own behaviour for that look. Default: the error goes to the {@link #logger}.
         */
        public Builder decisionRejected(java.util.function.Consumer<Throwable> c) { decisionRejected = Objects.requireNonNull(c, "c"); return this; }

        /** Where the pool logs a rejected decision without a {@link #decisionRejected} callback; default: nothing is logged. */
        public Builder logger(System.Logger l) { logger = Objects.requireNonNull(l, "l"); return this; }

        public QuicPool build() { return new QuicPool(this); }
    }

    private static final class Entry {
        final ConnectionCarrier carrier;
        final long fate, born = System.nanoTime();
        long idleSince = born;
        final AtomicBoolean disposed = new AtomicBoolean();
        int active, pending;
        long total;
        boolean retiring;
        volatile java.util.concurrent.ScheduledFuture<?> expiry;
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
                    if (opening != null) { opening.waiters.remove(this); opening.diverted.remove(this); }
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
                if (lifetime > 0 && entry.total >= lifetime) entry.retiring = true;
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
    public io.zmux.ZmuxFuture<PipeIo> openPipeAsync() {
        Request request = new Request(); boolean rejected;
        synchronized (this) { rejected = closed; if (!rejected) requests.add(request); }
        var notification = ConnectionCarrier.offLoop(request.result, client.completions(), QuicPool::discard);
        if (rejected) request.result.completeExceptionally(new io.zmux.CarrierFailedException("QUIC pool closed")); else acquire(request, true);
        return QuicClient.future(notification);
    }

    /** {@link #openPipeAsync()}, waiting for the pipe input. */
    public PipeIo openPipe() throws IOException {
        return QuicClient.await(openPipeAsync());
    }
    private static void discard(PipeIo pipe) {
        try { if (pipe.reader() != null) pipe.reader().close(); }
        finally { if (pipe.writer() != null) pipe.writer().abort(new IOException("pooled operation cancelled")); }
    }
    private void expire(Entry entry) {
        if (!entry.carrier.channel().isActive() || (maxAge > 0 && System.nanoTime() - entry.born >= maxAge)) entry.retiring = true;
    }
    private long room(Entry entry) {
        long remaining = (long) limits.maxPipesPerConnection() - entry.active;
        if (lifetime > 0)
            remaining = Math.min(remaining, lifetime - Math.min(entry.total, lifetime) - entry.pending);
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
                boolean due = policyDial && dueLocked(System.nanoTime());
                Entry best = null;
                for (int i=0;i<entries.size();) {
                    Entry entry = entries.get(i); expire(entry);
                    if (entry.retiring && entry.active == 0) { entries.remove(i); idle.add(entry); continue; }
                    if (!entry.retiring && room(entry) > 0 && (best == null || room(entry) > room(best))) best = entry;
                    i++;
                }
                if (best != null && !due) { best.active++; best.pending++; lease = new Lease(best); }
                else {
                    if (opening == null) { opening = new Opening(); opening.background = best != null; dial = true; }
                    opening.waiters.add(request);
                    waiting = opening; diverted = best != null;
                    if (diverted && opening.background) opening.diverted.add(request);
                }
            }
        }
        idle.forEach(QuicPool::dispose);
        reportRejected();
        if (policyDial) look(); // a pipe is taken: the policy is asked, off this thread, and its decision serves the next request
        if (reject) { request.result.completeExceptionally(new io.zmux.CarrierFailedException("QUIC pool closed")); return; }
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
        try { dialing = client.connectCarrier(address,serverName,credential,ConnectionCarrier.POOLED).toCompletableFuture(); }
        catch (RuntimeException error) {
            synchronized (this) { if (opening == shared) opening = null; failures++; }
            shared.ready.completeExceptionally(error); maintain(); return;
        }
        boolean cancel;
        synchronized (this) { shared.nativeDial = dialing; cancel = shared.cancelled || closed; }
        dialing.whenComplete((carrier,error) -> {
            if (error != null) {
                boolean cancelled;
                synchronized (this) { if (opening == shared) opening = null; cancelled = shared.cancelled; if (!cancelled) failures++; }
                shared.ready.completeExceptionally(cancelled ? new CancellationException() : error);
                if (!cancelled) maintain(); // a dial attempt ended
                return;
            }
            long fate = NEXT_FATE.getAndIncrement();
            if (fate <= 0 || fate > ((1L << 62)-1)) {
                carrier.dispose(); synchronized (this) { if (opening == shared) opening = null; }
                shared.ready.completeExceptionally(new io.zmux.CarrierFailedException("fate group namespace exhausted")); return;
            }
            Entry entry = new Entry(carrier,fate); boolean adopted;
            synchronized (this) {
                adopted = !closed && !shared.cancelled && opening == shared;
                if (opening == shared) opening = null;
                if (adopted) { entries.add(entry); failures = 0; }
            }
            if (!adopted) { carrier.dispose(); shared.ready.completeExceptionally(new CancellationException()); return; }
            carrier.channel().closeFuture().addListener(ignored -> retire(entry));
            if (maxAge > 0) {
                // The close future retires the entry too, so the timer is never its only end.
                entry.expiry = io.zmux.internal.spi.Loops.schedule(carrier.channel().eventLoop(), () -> retire(entry), maxAge, TimeUnit.NANOSECONDS, () -> retire(entry));
                if (entry.disposed.get()) io.zmux.internal.spi.Loops.cancel(entry.expiry);
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
    /** The connections that are not retired. Guarded by this. */
    private int openLocked() {
        int open = 0;
        for (Entry entry : entries) if (!entry.retiring) open++;
        return open;
    }

    /**
     * Whether a new connection is due: the policy's count is not reached by the connections not retired and the growth
     * dial, and the dial spacing has passed. On-demand dials are not counted. Guarded by this.
     */
    private boolean dueLocked(long now) {
        return openLocked() + (growthDial() ? 1 : 0) < target && (!dialed || now - lastDial >= dialAfter);
    }

    /**
     * Asks for a look: the policy is asked on a thread of the pool's own, never on the caller's (which may be an
     * event loop) and never under the pool's lock, one look at a time, as often as it is asked for meanwhile. The
     * policy has no timeout: one that does not return holds this pool's looks and nothing else.
     */
    private void look() {
        if (policy == null) return;
        synchronized (this) {
            if (closed) return;
            if (looking) { lookAgain = true; return; }
            looking = true;
        }
        try { Thread.ofVirtual().name("zmux-quic-pool-policy").start(this::looks); }
        catch (RuntimeException | Error error) { synchronized (this) { looking = false; } throw error; }
    }

    private void looks() {
        boolean owner = true;
        try {
            while (true) {
                PoolView view;
                synchronized (this) {
                    if (closed) return;
                    lookAgain = false;
                    view = viewLocked(System.nanoTime());
                }
                var look = PolicyCall.decide(policy, view);
                apply(look.decision(), look.rejected());
                if (endLook()) { owner = false; return; }
            }
        } finally {
            if (owner) synchronized (this) { looking = false; }
        }
    }

    /**
     * Ends the look loop, or goes on when another look was asked for meanwhile: the check of lookAgain and the clearing
     * of looking are one step under the lock, so a request that finds looking still set is never lost.
     */
    synchronized boolean endLook() {
        if (lookAgain) return false;
        looking = false;
        return true;
    }

    /** The members and counts the policy is shown; the view is a snapshot. Guarded by this. */
    private PoolView viewLocked(long now) {
        var view = new ArrayList<PoolMember>(entries.size());
        for (Entry entry : entries) {
            expire(entry);
            // A retired connection is reported, not usable, until it closes.
            view.add(new PoolMember(null, entry.fate, Duration.ofNanos(Math.max(0, now - entry.born)),
                    Duration.ofNanos(idle(entry, now)), entry.active, 0, 0, !entry.retiring && room(entry) > 0, false));
        }
        return new PoolView(List.copyOf(view), failures,
                dialed ? java.util.Optional.of(Duration.ofNanos(now - lastDial)) : java.util.Optional.empty(), growthDial() ? 1 : 0);
    }

    /**
     * Applies a decision (the default one for a rejected look): marks the connections it retires, shrinks to its
     * count, remembers its count and dial spacing for the requests between looks, then sweeps the retired idle
     * connections and starts a due growth dial, outside the lock.
     */
    private void apply(PoolDecision decision, Throwable error) {
        ArrayList<Entry> idle = new ArrayList<>(); Opening start = null;
        synchronized (this) {
            if (closed) return;
            if (error != null && rejected == null) rejected = error;
            long now = System.nanoTime();
            recheckLocked(recheckDelay(decision));
            for (Entry entry : entries) if (decision.retireConnections().contains(entry.fate)) entry.retiring = true;
            target = decision.count().orElse(0);
            dialAfter = decision.dialAfter().map(QuicPool::nanos).orElse(0L);
            if (decision.count().isPresent()) shrinkLocked(target);
            for (int i=0;i<entries.size();) {
                Entry entry = entries.get(i);
                if (entry.retiring && entry.active == 0) { entries.remove(i); idle.add(entry); } else i++;
            }
            looks++;
            if (dueLocked(now) && opening == null) { start = opening = new Opening(); start.background = true; }
        }
        idle.forEach(QuicPool::dispose);
        reportRejected();
        if (start != null) dial(start);
    }

    /** The looks whose decision has been applied. */
    synchronized long appliedLooks() { return looks; }

    /**
     * Shrinks the pool to count, in this order: the growth dial is cancelled, then connections
     * retire, the longest idle first, ties broken by the fewest pipes, the least age, the highest fate group. A dial a
     * pipe request waits for is not counted and is left to the request. Guarded by this.
     */
    private void shrinkLocked(int count) {
        long now = System.nanoTime();
        var candidates = new ArrayList<PoolShrink.Candidate<Entry>>();
        for (Entry entry : entries) {
            if (!entry.retiring) candidates.add(new PoolShrink.Candidate<>(entry, entry.fate, false, idle(entry, now), entry.active, Math.max(0, now - entry.born)));
        }
        var plan = PoolShrink.plan(candidates, growthDial() ? 1 : 0, count);
        if (plan.cancelDials() > 0) {
            opening.cancelled = true;
            var nativeDial = opening.nativeDial;
            if (nativeDial != null) nativeDial.cancel(false); // its completion clears opening
        }
        for (Entry entry : plan.close()) entry.retiring = true;
    }

    /** How long entry has carried no pipe: 0 while it carries one. */
    private static long idle(Entry entry, long now) {
        return entry.active > 0 ? 0 : Math.max(0, now - entry.idleSince);
    }
    /** Reports a decision rejected under the lock; called without the lock. */
    private void reportRejected() {
        Throwable error;
        synchronized (this) { error = rejected; rejected = null; }
        if (error == null) return;
        if (decisionRejected == null) {
            if (logger != null) logger.log(System.Logger.Level.WARNING, "zmux pool policy decision rejected; keeping the pool", error);
            return;
        }
        PolicyCall.report(decisionRejected, error);
    }
    /** The delay to the next look a decision asks for: its recheck floored at MIN_RECHECK, at most the maintenance interval. */
    static long recheckDelay(PoolDecision decision) {
        long again = decision.recheck().map(d -> Math.max(nanos(d), nanos(PoolDecision.MIN_RECHECK))).orElse(INTERVAL);
        return Math.min(again, INTERVAL);
    }

    /** d in nanoseconds, saturated. */
    private static long nanos(Duration d) {
        try { return d.toNanos(); } catch (ArithmeticException tooLong) { return Long.MAX_VALUE; }
    }

    /** Replaces the pending recheck by one after n nanoseconds (0: none). */
    private void recheckLocked(long n) {
        if (recheck != null) recheck.cancel(false);
        recheck = null;
        long generation = ++recheckGeneration;
        if (n <= 0) return;
        recheck = io.zmux.internal.spi.Loops.schedule(client.nativeLoop(), () -> rechecked(generation), n, TimeUnit.NANOSECONDS, () -> {}); // refused: the client is closed
    }
    /** A recheck fired; one replaced (or dropped) after it started to fire leaves the pending one alone. */
    private void rechecked(long generation) {
        synchronized (this) {
            if (generation != recheckGeneration) return;
            recheck = null;
        }
        maintain();
    }
    /** Asks the policy outside a request: a connection gained or lost a pipe, a dial ended, or a recheck fell due. */
    private void maintain() { look(); }
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
    public synchronized List<PooledConnection> connections() {
        long now = System.nanoTime();
        return entries.stream().map(entry -> new PooledConnection(entry.carrier.channel(),entry.fate,entry.active,entry.total,
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
        for (Request request : pending) request.result.completeExceptionally(new io.zmux.CarrierFailedException("QUIC pool closed"));
        if (cancel != null) cancel.cancel(false);
        idle.forEach(QuicPool::dispose);
    }
}
