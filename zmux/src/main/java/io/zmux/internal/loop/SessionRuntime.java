package io.zmux.internal.loop;

import io.zmux.internal.core.Datagrams;
import io.zmux.internal.core.Action;
import io.zmux.internal.core.CarrierAdmission;
import io.zmux.internal.core.Joins;
import io.zmux.internal.wire.JoinCrypto;
import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.Event;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.SessionError;
import io.zmux.internal.core.SessionState;
import io.zmux.internal.core.Waiter;
import io.zmux.internal.wire.TransportError;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Drives one core session on its loop: feeds carrier input, writes batches while the core
 * wants to write, fires wake-ups, applies carrier actions, delivers events and keeps the
 * session's one timer. Application threads reach it only through its {@link #mailbox}
 * and {@link #post}.
 */
public final class SessionRuntime implements SessionHost.RoundEnd {
    final SessionHost loop;
    final Session core;
    private final CarrierSet carriers = new CarrierSet(this);
    private final ArrayList<Waiter> wakes = new ArrayList<>();
    private final ArrayList<Action> actions = new ArrayList<>();
    /** Operations registered with the core (loop only), settled when the session ends. */
    private Operation<?> live;
    private final Mailbox mailbox;
    private final EventQueue events;
    private Consumer<Event> eventSink;
    private final DriverHooks hooks;
    private final SerialCallbacks callbacks = new SerialCallbacks(this);
    /**
     * Future completions of this session, in the order the loop settled them. Never closed:
     * operations failed by the session's end still complete. Not a hook context, so blocking
     * calls stay allowed there (IMPLEMENTATION.md Section 2.5.3).
     */
    private final SerialCallbacks completions = new SerialCallbacks();

    SerialCallbacks completions() { return completions; }
    private final java.util.concurrent.atomic.AtomicInteger tracePending = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.atomic.AtomicLong traceDropped = new java.util.concurrent.atomic.AtomicLong();
    private boolean receiveHookBusy;
    private boolean traceClosed;

    /** Listener snapshots are published on the owner loop, never by reading a foreign core. */
    public record Resources(SessionState state, long bytes, long suspendedAt) {}
    private volatile Resources resources = new Resources(SessionState.ESTABLISHING, 0, 0);
    private Consumer<Resources> resourceListener;
    public Resources resources() { return resources; }
    public void resourceListener(Consumer<Resources> listener) { resourceListener = listener; }
    void publishResources() {
        if (resourceListener == null) return;
        SessionState state = core.state();
        long at = state == SessionState.SUSPENDED ? resources.state() == state ? resources.suspendedAt() : Clock.now() : 0;
        long bytes = core.memory().usage();
        if (state != resources.state() || bytes != resources.bytes() || at != resources.suspendedAt()) {
            resources = new Resources(state, bytes, at);
            resourceListener.accept(resources);
        }
    }

    /** The outbound datagram limiter, called only by application threads. */
    public io.zmux.helper.Limiter datagramSendLimiter() { return hooks.datagramSend(); }

    private void trace(Event event) {
        if (hooks.tracer() == null || traceClosed) return;
        if (event.kind() == Event.Kind.CLOSED) traceClosed = true;
        if (!terminal(event) && tracePending.get() >= hooks.tracerQueue()) {
            traceDropped.incrementAndGet(); return;
        }
        tracePending.incrementAndGet();
        callbacks.execute(() -> {
            try {
                long dropped = traceDropped.getAndSet(0);
                if (dropped != 0) {
                    try { hooks.dropped().accept(dropped); }
                    catch (RuntimeException | AssertionError ignored) { /* Still deliver the actual event. */ }
                }
                hooks.tracer().accept(event);
            } finally { tracePending.decrementAndGet(); }
        });
    }

    /** The events a queue limit never discards: the session's end and a pipe direction's terminal state. */
    private static boolean terminal(Event e) {
        if (e.kind() == Event.Kind.CLOSED) return true;
        if (e.kind() != Event.Kind.PIPE_STATE) return false;
        Object st = e.state();
        return st == io.zmux.internal.core.PipeInfo.Out.FAILED || st == io.zmux.internal.core.PipeInfo.Out.CLOSED
                || st == io.zmux.internal.core.PipeInfo.Out.RETIRED || st == io.zmux.internal.core.PipeInfo.In.ENDED
                || st == io.zmux.internal.core.PipeInfo.In.RETIRED;
    }

    private void runHooks() {
        if (core.state() == SessionState.CLOSING || core.state() == SessionState.CLOSED) return;
        if (hooks.allowWindowGrowth() != null) {
            long proposal = core.tuner().proposal();
            if (proposal >= 0) {
                long delta = core.tuner().increase(proposal);
                callbacks.execute(() -> {
                    boolean allowed;
                    try { allowed = hooks.allowWindowGrowth().test(delta); }
                    catch (RuntimeException | AssertionError e) { allowed = false; }
                    boolean answer = allowed;
                    post(() -> core.tuner().applyProposal(proposal, answer));
                });
            }
        }
        if (!receiveHookBusy && hooks.datagramReceive() != null) {
            Datagrams.Limited data = core.datagrams().pollLimited();
            if (data != null) {
                receiveHookBusy = true;
                callbacks.execute(() -> {
                    boolean allow = false;
                    if (data.active()) {
                        try {
                            allow = hooks.datagramReceive().tryReady(data.bytes()) >= data.bytes();
                        } catch (RuntimeException | AssertionError ignored) { /* a failing hook refuses */ }
                    }
                    boolean accepted = allow;
                    post(() -> {
                        receiveHookBusy = false;
                        if (core.deliverDatagram(data, !accepted)) {
                            callbacks.execute(() -> hooks.datagramReceive().charge(data.bytes()));
                        }
                    });
                });
            }
        }
    }

    /** Operations waiting for a pipe state change (joins). */
    private final ArrayList<Waiter> pipeWaiters = new ArrayList<>();
    /** Peer joins the core holds until establishment. */
    private final ArrayList<HeldJoin> heldJoins = new ArrayList<>();
    private final CompletableFuture<SessionError> closed = new CompletableFuture<>();
    private final Runnable shutdownHook = () -> failInternal(new IllegalStateException("event loop closed"));
    private boolean finished;
    private final TimerHeap.Timer timer = new TimerHeap.Timer() {
        @Override
        void fire(long now) {
            try {
                core.handleTimeout(now);
            } catch (RuntimeException e) {
                internalError(e);
                return;
            }
            markDirty();
        }
    };

    /** A driver for core; every method runs on loop. eventSink receives events on the loop (may be null). */
    SessionRuntime(SessionHost loop, Session core, DriverHooks hooks) {
        this.loop = loop;
        this.core = core;
        this.hooks = hooks;
        publishSnapshots();
        pickers = new PickerBridge(core, hooks.picker());
        mailbox = new Mailbox(loop, this);
        events = new EventQueue();
    }

    /** The session's command queue. */
    public Mailbox mailbox() { return mailbox; }

    public SessionHost loop() { return loop; }

    public Session core() { return core; }

    /** Completed with the terminal error once the session is CLOSED (on the callback executor). */
    public CompletableFuture<SessionError> closedFuture() { return closed; }

    /** Internal, removable terminal observation; the callback performs no application work. */
    public Runnable watchClosed(java.util.function.Consumer<SessionError> observer) {
        var active = new java.util.concurrent.atomic.AtomicBoolean(true);
        Runnable[] shutdown = new Runnable[1];
        var signal = new io.zmux.internal.core.Waiter() {
            @Override public void wake() {
                if (!active.compareAndSet(true, false)) return;
                core.cancelClosedWait(this); loop.removeShutdownHook(shutdown[0]);
                observer.accept(terminal());
            }
        };
        shutdown[0] = signal::wake;
        if (!loop.tryExecute(() -> {
            if (!active.get()) return;
            loop.onShutdown(shutdown[0]);
            if (core.waitClosed(signal)) signal.wake();
        })) closed.whenComplete((error, cause) -> {
            if (active.compareAndSet(true, false)) observer.accept(error == null ? terminal() : error);
        });
        return () -> {
            active.set(false);
            loop.tryExecute(() -> { core.cancelClosedWait(signal); loop.removeShutdownHook(shutdown[0]); });
        };
    }

    /** The session's transports (tests). */
    CarrierSet carriers() { return carriers; }

    /** Attaches the bootstrap transport and starts it (unless a listener already did): its inbound; on the loop. */
    Inbound attachBootstrap(Transport c) {
        Runnable edge = Nesting.tryAttach(this, c.info().connection());
        if (edge == null) throw new IllegalArgumentException("nested carrier would form a cycle");
        loop.onShutdown(shutdownHook);
        core.attachBootstrap(c.info().carrier(), c.info().carrier(), null);
        core.pipes().setCarrierConn(0, c.info().connection());
        Inbound in = carriers.attach(c, 0, 0, edge);
        markDirty();
        return in;
    }

    /** Sets the event sink; on the loop, before the first round. */
    public void eventSink(Consumer<Event> sink) {
        eventSink = sink;
    }

    /** The custom scheduling policies; run on the loop. */
    public final PickerBridge pickers;


    // ---- stream lanes ----

    /** The session's stream lanes; their loop side. */
    public final LaneTable lanes = new LaneTable(this);

    // ---- pipes (MULTIPATH.md 5) ----

    private final java.util.HashMap<Long, Integer> reservations = new java.util.HashMap<>();

    /** Protocol reservation only: no writer is installed or polled until attach. On the owner loop. */
    public Joins.Reservation reservePipe(Joins.JoinOptions options) throws CoreException {
        if (finished) throw CoreException.session(terminal());
        if (options.carrierBound()) throw CoreException.of(CoreException.Reason.INVALID);
        var info = new io.zmux.internal.core.CarrierInfo(io.zmux.internal.core.Framing.BYTES, false, false, false, false);
        options = options.toBuilder().in(info).out(info).build();
        Joins.Reservation reserved = core.reservePipe(options);
        long id = reserved.id() < 0 ? -reserved.id() - 1 : reserved.id();
        reservations.put(id, options.dir());
        return new Joins.Reservation(id, reserved.frame());
    }

    /** Binds actual halves and properties once. No fake carrier or duplicate reservation state machine. */
    public CompletableFuture<Long> attachReserved(long id, io.zmux.carrier.PipeIo io) {
        CompletableFuture<Long> result = new CompletableFuture<>();
        Transport carrier = Transport.of(loop, io);
        if (carrier == null) { result.completeExceptionally(new java.io.IOException("carrier closed")); return result; }
        if (!loop.tryExecute(() -> {
            boolean readerOwned = false, writerOwned = false;
            for (int i = 0; i < carriers.size(); i++) {
                Transport attached = carriers.get(i).t;
                readerOwned |= attached.ownsHalf(io.reader()); writerOwned |= attached.ownsHalf(io.writer());
            }
            if (readerOwned || writerOwned) {
                // Preserve already transferred halves, but return any new half from a partially overlapping input.
                try { if (!readerOwned && io.reader() != null) io.reader().close(); }
                catch (RuntimeException ignored) { /* A failed close cannot prevent the other half being returned. */ }
                try { if (!writerOwned && io.writer() != null) io.writer().abort(ABORTED); }
                catch (RuntimeException ignored) { /* The operation still reports its failure. */ }
                completions.execute(() -> result.completeExceptionally(CoreException.of(CoreException.Reason.INVALID)));
                return;
            }
            boolean claimed = false;
            Runnable edge = null;
            try {
                Integer direction = reservations.remove(id);
                if (finished) throw CoreException.session(terminal());
                if (direction == null) throw CoreException.of(CoreException.Reason.INVALID);
                claimed = true;
                if (carrier.info().fateGroup() != 0 && core.pipes().fateGroup(id) == 0) throw CoreException.of(CoreException.Reason.INVALID);
                int halves = direction == 0 ? Action.DIR_OUT : direction == 1 ? Action.DIR_IN : Action.DIR_BIDI;
                carrier.restrict(halves);
                edge = Nesting.tryAttach(this, carrier.info().connection());
                if (edge == null) throw CoreException.of(CoreException.Reason.PIPE_REFUSED);
                var info = carrier.info().carrier();
                core.pipes().attachReserved(id, (halves & Action.DIR_IN) != 0 ? info : null,
                        (halves & Action.DIR_OUT) != 0 ? info : null);
                core.pipes().setCarrierConn(id, carrier.info().connection());
                carriers.attach(carrier, id, 0, edge); edge = null;
                core.pipes().wakeWriters(id); markDirty();
                completions.execute(() -> result.complete(id));
            } catch (CoreException | RuntimeException error) {
                carrier.close(error); if (edge != null) edge.run();
                if (claimed) try { core.removePipe(id, false); } catch (CoreException ignored) { /* Already retired. */ }
                markDirty();
                completions.execute(() -> result.completeExceptionally(error));
            }
        })) { carrier.close(null); result.completeExceptionally(CoreException.session(terminal())); }
        return result;
    }

    /** Gives up only a reservation that has not transferred to an attachment. */
    public void cancelReservation(long id) {
        post(() -> {
            if (reservations.remove(id) != null) {
                try { core.removePipe(id, false); } catch (CoreException ignored) { /* Already gone. */ }
            }
        });
    }

    /** A local in-band join over c, which this loop owns and nobody started: the pipe ID. */
    long addPipe(Transport c, Joins.JoinOptions o) throws CoreException {
        if (finished) {
            c.close(null);
            throw CoreException.session(terminal());
        }
        if (o.dir() == 1) { c.close(null); throw CoreException.of(CoreException.Reason.INVALID); }
        try { c.restrict(o.dir() == 0 ? Action.DIR_OUT : Action.DIR_BIDI); }
        catch (RuntimeException invalid) { c.close(invalid); throw invalid; }
        Joins.JoinOptions.Builder ob = o.toBuilder().in(c.info().carrier()).out(c.info().carrier());
        if (o.fateGroup() == 0) ob.fateGroup(c.info().fateGroup());
        if (o.carrierBound()) {
            byte[] binding = c.export(JoinCrypto.EXPORTER_PIPE, null, 32);
            if (binding == null) {
                c.close(null);
                throw CoreException.of(CoreException.Reason.INVALID);
            }
            ob.binding(binding);
        }
        o = ob.build();
        long r;
        Runnable edge;
        try {
            edge = Nesting.tryAttach(this, c.info().connection());
            if (edge == null) throw CoreException.of(CoreException.Reason.PIPE_REFUSED);
            try { r = core.addPipe(o); }
            catch (CoreException | RuntimeException refused) { edge.run(); throw refused; }
        } catch (CoreException e) {
            c.close(null);
            throw e;
        }
        long id = r < 0 ? -r - 1 : r;
        core.pipes().setCarrierConn(id, c.info().connection());
        carriers.attach(c, id, 0, edge);
        core.pipes().wakeWriters(id);
        markDirty();
        return id;
    }

    /** Joins a TCP carrier to the session (in band); completes with the pipe ID, on the callback executor. */
    public CompletableFuture<Long> addPipe(java.nio.channels.SocketChannel ch, Joins.JoinOptions o) {
        if (loop instanceof EventLoop nio) return submitPipe(new SocketTransport(nio, ch), o);
        // A session on another kind of host reads the socket through a NIO loop.
        EventLoop nio = EventLoopGroup.nextShared();
        CompletableFuture<Long> f = new CompletableFuture<>();
        Transport t = new SocketTransport(nio, ch);
        if (!nio.tryExecute(() -> adopt(f, t, o))) {
            t.close(null);
            f.completeExceptionally(new java.io.IOException("event loop closed"));
        }
        return f;
    }

    /** On t's own loop: moves t to this session's host and joins it there. */
    private void adopt(CompletableFuture<Long> f, Transport t, Joins.JoinOptions o) {
        Transport moved = t.loop() == loop ? t : t.moveTo(loop);
        if (moved == null) {
            completions.execute(() -> f.completeExceptionally(new java.io.IOException("carrier closed")));
            return;
        }
        if (moved.loop().inLoop()) { complete(f, moved, o); return; }
        if (!loop.tryExecute(() -> complete(f, moved, o))) {
            moved.close(null);
            completions.execute(() -> f.completeExceptionally(CoreException.session(terminal())));
        }
    }

    /** Joins a pipe input to the session (in band). */
    public CompletableFuture<Long> addPipe(io.zmux.carrier.PipeIo io, Joins.JoinOptions o) {
        Transport t = Transport.of(loop, io);
        return t == null ? CompletableFuture.failedFuture(new java.io.IOException("carrier closed")) : submitPipe(t, o);
    }

    private static final class TlsJoin extends CompletableFuture<Long> { Runnable cancelHandshake = () -> {}; }

    public void cancelJoinSetup(CompletableFuture<Long> future) {
        if (future instanceof TlsJoin tls) tls.cancelHandshake.run();
    }

    /** Runs the TLS handshake of engine over ch, then joins it. */
    public CompletableFuture<Long> addPipe(java.nio.channels.SocketChannel ch, javax.net.ssl.SSLEngine engine,
                                           long handshakeTimeout, Joins.JoinOptions o, Bootstrap.Presenter presenter) {
        var f = new TlsJoin();
        EventLoop nio = loop instanceof EventLoop l ? l : EventLoopGroup.nextShared();
        f.cancelHandshake = TlsTransport.handshake(nio, ch, engine, handshakeTimeout, (c, err) -> {
            if (err != null) completions.execute(() -> f.completeExceptionally(err));
            else if (presenter == null) adopt(f, c, o);
            else Bootstrap.present(c, presenter, () -> adopt(f, c, o), e -> {
                c.close(e);
                completions.execute(() -> f.completeExceptionally(e));
            });
        });
        return f;
    }

    private CompletableFuture<Long> submitPipe(Transport c, Joins.JoinOptions o) {
        CompletableFuture<Long> f = new CompletableFuture<>();
        if (!loop.tryExecute(() -> complete(f, c, o))) {
            c.close(null);
            f.completeExceptionally(CoreException.session(terminal()));
        }
        return f;
    }

    private void complete(CompletableFuture<Long> f, Transport c, Joins.JoinOptions o) {
        try {
            long id = addPipe(c, o);
            completions.execute(() -> f.complete(id));
        } catch (CoreException | RuntimeException e) {
            completions.execute(() -> f.completeExceptionally(e));
        }
    }

    /** The admission value of the session's bootstrap carrier; set before the session starts. */
    private volatile Object admitted;

    void admitted(Object v) { admitted = v; }

    /** The admission value of the session's bootstrap carrier, null without one. */
    public Object admitted() { return admitted; }

    void acceptJoin(Transport c, byte[] join, byte[] binding, boolean inband, Object admitted, ListenerHub.Counts counts) {
        if (finished) {
            c.close(null);
            counts.rejected();
            return;
        }
        io.zmux.internal.wire.Frame f = CarrierAdmission.parseJoin(join);
        if (f == null) { // not exactly one PIPE_JOIN: refused like any other invalid join
            c.close(null);
            counts.rejected();
            return;
        }
        Joins.JoinResult res;
        // The edge is recorded before admission, so concurrent joins see it; a join that is
        // not accepted returns it at once.
        Runnable edge = Nesting.tryAttach(this, c.info().connection());
        try {
            res = core.acceptJoin(f, CarrierAdmission.joinMac(f), binding,
                    new Joins.JoinCarrier((c.directions() & Action.DIR_IN) != 0, (c.directions() & Action.DIR_OUT) != 0,
                            c.info().carrier(), c.info().carrier(), null, edge == null));
        } catch (RuntimeException failure) { if (edge != null) edge.run(); throw failure; }
        if (res != Joins.JoinResult.ACCEPTED && edge != null) edge.run();
        switch (res) {
            case ACCEPTED -> {
                c.restrict(f.join.direction == 0 ? Action.DIR_IN : f.join.direction == 1 ? Action.DIR_OUT : Action.DIR_BIDI);
                long pipe = f.join.pipeId;
                core.pipes().setCarrierConn(pipe, c.info().connection());
                core.pipes().setAdmitted(pipe, admitted);
                if (inband) core.pipes().skipJoinFrame(pipe);
                carriers.attach(c, pipe, 0, edge);
                core.pipes().wakeWriters(pipe);
            }
            case HOLD -> heldJoins.add(new HeldJoin(c, join, binding, inband, admitted, counts));
            case CLOSE, REFUSED -> c.close(null);
        }
        switch (res) {
            case ACCEPTED -> counts.joined();
            case CLOSE -> counts.rejected();
            case REFUSED -> counts.refused();
            case HOLD -> { }
        }
        markDirty();
    }

    /** Routes an authenticated-connection lane through the same core admission checks. */
    public void handleLane(io.zmux.carrier.PipeIo io) { ListenerHub.laneInput(this, io); }

    /** An inbound lane of this session over carrier: its inbound, or null when refused (the carrier is closed). */
    Inbound acceptLane(Transport carrier, byte[] body) {
        var frame = CarrierAdmission.parseLane(body);
        if (finished || frame == null || !core.lanes().keyMatches(CarrierAdmission.laneKey(frame))) {
            carrier.close(null); return null;
        }
        boolean accepted = core.lanes().accept(carrier.info().connection(), frame.pipeId, frame.lane, carrier.info().carrier());
        markDirty(); // Admission may have queued a fatal protocol close even when refused.
        if (!accepted) { carrier.close(null); return null; }
        carrier.restrict(io.zmux.internal.core.Action.DIR_IN);
        Inbound in = carriers.attach(carrier, frame.pipeId, frame.lane, null);
        markDirty(); return in;
    }

    private void openLane(CarrierSet.Attached parent) {
        java.util.concurrent.CompletionStage<io.zmux.carrier.PipeIo> opening;
        try { opening = parent.t.openLane(); }
        catch (RuntimeException error) { core.lanes().openFailed(parent.pipe); return; }
        if (opening == null) { core.lanes().openFailed(parent.pipe); return; }
        opening.whenComplete((io, error) -> {
            if (!loop.tryExecute(() -> {
                if (error != null || io == null || finished) {
                    if (io != null) discard(io);
                    core.lanes().openFailed(parent.pipe); markDirty(); return;
                }
                Transport lane = Transport.of(loop, io);
                if (lane == null) { core.lanes().openFailed(parent.pipe); markDirty(); return; }
                if (parent.t.info().connection() == null || parent.t.info().connection() != lane.info().connection()) {
                    lane.close(ABORTED); core.lanes().openFailed(parent.pipe); markDirty(); return;
                }
                long id = core.lanes().opened(parent.pipe, lane.info().carrier(), null);
                if (id < 0) { lane.close(ABORTED); return; }
                carriers.attach(lane, parent.pipe, id, null); markDirty();
            }) && io != null) discard(io);
        });
    }

    private static void discard(io.zmux.carrier.PipeIo io) {
        if (io.reader() != null) io.reader().close();
        if (io.writer() != null) io.writer().abort(ABORTED);
    }

    private record HeldJoin(Transport c, byte[] join, byte[] binding, boolean inband, Object admitted, ListenerHub.Counts counts) {}

    /** Registers op to be attempted again at the next pipe state change; on the loop. */
    public void waitPipe(Waiter op) {
        if (!pipeWaiters.contains(op)) pipeWaiters.add(op);
    }

    /** The operations waiting for a pipe state change (tests). */
    public int pipeWaitersForTest() { return pipeWaiters.size(); }

    public void cancelPipeWait(Waiter op) {
        pipeWaiters.remove(op);
    }

    void markDirty() {
        loop.markDirty(this);
    }

    /** Lists op as registered with the core; on the loop. */
    void track(Operation<?> op) {
        if (op.live) return;
        op.live = true;
        op.prevLive = null;
        op.nextLive = live;
        if (live != null) live.prevLive = op;
        live = op;
    }

    void untrack(Operation<?> op) {
        if (!op.live) return;
        op.live = false;
        if (op.prevLive == null) live = op.nextLive; else op.prevLive.nextLive = op.nextLive;
        if (op.nextLive != null) op.nextLive.prevLive = op.prevLive;
        op.prevLive = op.nextLive = null;
    }

    // ---- application side ----

    /** Queues a nonblocking command for the loop; false when the session is gone. */
    public boolean post(Runnable r) {
        return mailbox.offer(r::run);
    }

    /** Carrier cleanup/completions may still run after the session ended, on the same owner loop. */
    private long nestedLanes;

    /** Owner-loop-only lease, shared by all nested carriers on this outer session. */
    public boolean acquireNestedLane() {
        if (finished || nestedLanes >= core.nestedLaneAllowance()) return false;
        nestedLanes++;
        return true;
    }

    public void releaseNestedLane() {
        if (nestedLanes <= 0) throw new IllegalStateException("unbalanced nested lane lease");
        nestedLanes--;
    }

    public boolean executeCarrier(Runnable task) {
        return loop.tryExecute(() -> {
            task.run();
            if (!finished) markDirty();
        });
    }

    /** Checks before an application lock or carrier operation can block. */
    public void checkBlocking() {
        if (SessionHost.onHostThread() || SerialCallbacks.onContext(this)) {
            throw new IllegalStateException("blocking zmux call on its event-loop or callback thread");
        }
    }

    // ---- snapshots the loop publishes for other threads ----

    /** What the session negotiated, as of the loop's last round. */
    public record Info(boolean established, boolean initiator, boolean peerIsZmux, boolean goAway, boolean preamble,
                       boolean priority, boolean datagramFlows, boolean pipeAck, boolean multipath, boolean lanesToPeer,
                       int maxDatagramPayload) {}

    private volatile io.zmux.internal.core.SessionStats.Snapshot stats;
    private volatile Info info;
    // Loop only: what the published snapshots were made from.
    private long statsVersion = -1, statsBudget = -1;
    private boolean infoEstablished, infoGoAway;

    /** Set by the first read: the loop then also republishes when only a gauge changed. */
    private volatile boolean statsWanted;

    /**
     * The session's statistics as of the loop's last round; any thread. The first call starts
     * following the gauges and waits for one loop turn; an ended session returns its last snapshot.
     */
    public io.zmux.internal.core.SessionStats.Snapshot stats() {
        if (statsWanted) return stats;
        statsWanted = true;
        if (loop.inLoop()) { stats = core.statsSnapshot(); return stats; }
        var done = new java.util.concurrent.CompletableFuture<Void>();
        if (executeCarrier(() -> { stats = core.statsSnapshot(); done.complete(null); })) done.join();
        return stats;
    }

    /** The negotiated features as of the loop's last round; any thread, no hand-off. */
    public Info info() { return info; }

    /** Published once requested, then refreshed with the counters: per-pipe information. */
    private volatile java.util.List<io.zmux.PipeStats> pipes;
    private volatile boolean pipesWanted;
    // Loop only.
    private long pipesVersion = -1;
    private int pipesCount = -1;

    /**
     * The pipes now, taken on the loop so that their durations are of the current time; any
     * thread. An ended session returns its last snapshot.
     */
    public java.util.List<io.zmux.PipeStats> pipeSnapshot() {
        pipesWanted = true;
        if (loop.inLoop()) { publishPipes(); return pipes; }
        var done = new java.util.concurrent.CompletableFuture<Void>();
        if (!executeCarrier(() -> { publishPipes(); done.complete(null); })) {
            var last = pipes;
            return last != null ? last : java.util.List.of();
        }
        done.join();
        return pipes;
    }

    /** Republishes the pipe snapshot now if it is published; on the loop. */
    public void refreshPipes() {
        if (pipesWanted) publishPipes();
    }

    private void publishPipes() {
        var ids = new java.util.ArrayList<Long>();
        core.pipes().ids(ids);
        long now = Clock.now(), nano = System.nanoTime();
        var out = new java.util.ArrayList<io.zmux.PipeStats>(ids.size());
        for (long id : ids) {
            var i = core.pipeStats(id, now, nano);
            if (i != null) out.add(i);
        }
        pipesVersion = core.stats().version();
        pipesCount = ids.size();
        pipes = java.util.List.copyOf(out);
    }

    private volatile io.zmux.shaping.ShapingStats shaping;
    private long shapingVersion = -1;

    /** The shaping counters as of the loop's last round; null when no shaper was set. Any thread. */
    public io.zmux.shaping.ShapingStats shapingStats() { return shaping; }

    /** Publishes changed counters and features; on the loop (or before the session starts). */
    private void publishSnapshots() {
        long v = core.stats().version(), b = core.keepAlive().budgetTokens();
        long sig = core.shapingSignature();
        if (sig != shapingVersion) {
            shapingVersion = sig;
            shaping = core.shapingStats();
        }
        if (pipesWanted && (v != pipesVersion || core.pipes().count() != pipesCount)) publishPipes();
        if (v != statsVersion || b != statsBudget || statsWanted) {
            var next = core.statsSnapshot();
            if (v != statsVersion || b != statsBudget || stats == null || !next.gauges().equals(stats.gauges())) stats = next;
            statsVersion = v;
            statsBudget = b;
        }
        publishInfo();
    }

    /**
     * Republishes the negotiated features if they changed; on the loop. Run before an
     * operation completes too, so that whatever the loop observed when deciding it (an
     * established session) is in {@link #info()} by the time its caller wakes.
     */
    void publishInfo() {
        var f = core.features();
        boolean est = core.established(), away = f.goAway();
        if (info == null || est != infoEstablished || away != infoGoAway) {
            infoEstablished = est;
            infoGoAway = away;
            var p = core.peerParams();
            info = new Info(est, core.initiator(), p != null && p.isZmux(), away, f.preamble(), f.priority(), f.flows(),
                    f.pipeAck(), f.multipath(), f.lanesOut(), core.datagrams().maxPayload(f.flows() ? 1 : 0));
        }
    }

    /** The session's error as last published by the loop: read by other threads instead of the core. */
    private volatile SessionError published;

    /** The session's error once it is closing or closed; any thread. */
    public SessionError error() {
        return loop.inLoop() ? core.error() : published;
    }

    private void publishError() {
        if (published == null) published = core.error();
    }

    SessionError terminal() {
        SessionError e = error();
        return e != null ? e : SessionError.transport(TransportError.INTERNAL_ERROR, 0, "event loop closed", "", false);
    }

    // ---- carrier side ----

    /** The outcome of a write of b by one of the session's transports. */
    void written(Transport t, io.zmux.internal.core.Batch b, Throwable cause) {
        core.unsentReported(b.pipe(), b.lane(), t.unsent());
        long now = Clock.now();
        // The carrier's retransmissions, on lane 0 (lanes share their carrier).
        if (b.lane() == 0 && core.retransmittedDue(b.pipe(), now)) core.retransmittedReported(b.pipe(), t.retransmitted(), now);
        core.writeDone(b, cause, now);
        if (!writingOut) markDirty(); // Inside writeOut the same pass goes on writing.
    }

    // ---- the round ----

    @Override
    public void roundEnd() {
        if (finished) return;
        try {
            drive();
        } catch (RuntimeException e) {
            internalError(e);
        }
    }

    /**
     * One pass: write while carriers take batches, run the custom policy, apply carrier
     * actions, fire wake-ups. Work those produce (a woken operation queued data, an action
     * freed a carrier) marks the session for the next round instead of looping here.
     */
    private void drive() {
        writeOut();
        boolean picked = pickers.pick();
        core.outbox().takeActions(actions);
        for (Action a : actions) apply(a);
        actions.clear();
        publishSnapshots(); // Before anyone woken (an accepted or established session) reads them.
        core.outbox().takeWakes(wakes);
        for (Waiter w : wakes) w.wake();
        wakes.clear();
        core.settle();
        boolean pipeChanged = false;
        for (Event e; (e = core.outbox().pollEvent()) != null; ) {
            pipeChanged |= e.kind() == Event.Kind.PIPE_STATE || e.kind() == Event.Kind.ESTABLISHED;
            deliver(e);
        }
        if (pipeChanged) {
            refreshPipes();
            if (!heldJoins.isEmpty() && core.established()) {
                List<HeldJoin> h = new ArrayList<>(heldJoins);
                heldJoins.clear();
                for (HeldJoin j : h) acceptJoin(j.c, j.join, j.binding, j.inband, j.admitted, j.counts);
            }
            if (!pipeWaiters.isEmpty()) {
                List<Waiter> w = new ArrayList<>(pipeWaiters);
                pipeWaiters.clear();
                for (Waiter op : w) op.wake();
            }
        }
        // Run again next round only for work this pass produced: queued output, new wake-ups
        // or actions, or a policy answer that changes what can be sent.
        lanes.reclaimLeases();
        publishError();
        runHooks();
        core.memory().charge();
        publishResources();
        publishSnapshots();
        if (picked || core.outbox().pending() || wantsOutput()) markDirty();
        if (core.state() == SessionState.CLOSED) {
            finish();
            return;
        }
        loop.schedule(timer, core.nextTimeout());
    }

    /** Set while drive writes: a write that completes at once needs no further round. */
    private boolean writingOut;

    private void writeOut() {
        writingOut = true;
        try {
            writeAll();
        } finally {
            writingOut = false;
        }
    }

    /** Channel datagrams polled from the core, sent at once; loop only. */
    private final ArrayList<byte[]> channelOut = new ArrayList<>();
    private final ArrayList<io.zmux.internal.core.Datagrams.ChannelSent> channelSent = new ArrayList<>();

    /** A channel datagram the carrier refused: dropped instead of sent. */
    private void channelRefused(io.zmux.internal.core.Datagrams.ChannelSent c) {
        if (finished) return;
        core.channelSendFailed(c);
        markDirty();
    }

    private void writeAll() {
        for (int k = 0; k < carriers.size(); k++) {
            CarrierSet.Attached c = carriers.get(k);
            CarrierChannel ch = c.lane == 0 ? c.t.channel() : null;
            if (ch != null) {
                // The pipe's datagram channel, independent of the batch write in flight (EXTENSIONS.md 6.8).
                core.pollChannel(c.pipe, channelOut, channelSent, Clock.now());
                for (int i = 0; i < channelOut.size(); i++) {
                    var sent = channelSent.get(i);
                    ch.send(channelOut.get(i), () -> channelRefused(sent));
                }
                channelOut.clear();
                channelSent.clear();
            }
            while (c.t.writable() && core.transmitter().wantsWrite(c.pipe, c.lane)) {
                // Q_p at the hand-over includes the carrier's unsent bytes as of now.
                core.unsentReported(c.pipe, c.lane, c.t.unsent());
                if (!core.pollTransmit(c.pipe, c.lane, c.batch, Session.DEFAULT_BATCH, Clock.now())) break;
                c.t.write(c.batch);
            }
        }
    }

    /** Queries the unsent counts of pipe's outbound lanes (for its statistics); on the loop. */
    public void refreshUnsent(long pipe) {
        for (int k = 0; k < carriers.size(); k++) {
            CarrierSet.Attached c = carriers.get(k);
            if (c.pipe == pipe && (c.t.directions() & io.zmux.internal.core.Action.DIR_OUT) != 0) core.unsentReported(c.pipe, c.lane, c.t.unsent());
        }
    }

    private boolean wantsOutput() {
        for (int k = 0; k < carriers.size(); k++) {
            CarrierSet.Attached c = carriers.get(k);
            if (c.t.writable() && core.transmitter().wantsWrite(c.pipe, c.lane)) return true;
        }
        return false;
    }

    void apply(Action a) {
        if (a.kind() == Action.Kind.OPEN_LANE) {
            for (int i = 0; i < carriers.size(); i++) {
                CarrierSet.Attached c = carriers.get(i);
                if (c.pipe == a.pipe() && c.lane == 0) { openLane(c); break; }
            }
            return;
        }
        for (int i = carriers.size() - 1; i >= 0; i--) {
            CarrierSet.Attached c = carriers.get(i);
            if (c.pipe != a.pipe() || (c.t.directions() & a.dir()) == 0) continue;
            if ((a.kind() == Action.Kind.SHUTDOWN || a.kind() == Action.Kind.RESET_LANE) && c.lane != a.lane()) continue;
            switch (a.kind()) {
                case SHUTDOWN -> c.t.shutdownOutput();
                case CLOSE -> {
                    if ((c.t.directions() & ~a.dir()) == 0) {
                        carriers.remove(c, null);
                    } else {
                        if ((a.dir() & Action.DIR_IN) != 0) c.t.shutdownInput();
                        if ((a.dir() & Action.DIR_OUT) != 0) c.t.shutdownOutput();
                    }
                }
                case ABORT, RESET_LANE -> carriers.remove(c, ABORTED);
                default -> { }
            }
        }
    }

    private static final Throwable ABORTED = new java.io.IOException("pipe aborted");

    private void deliver(Event e) {
        trace(e);
        if (e.kind() == Event.Kind.CLOSED) return;
        if (eventSink != null) eventSink.accept(e);
        events.add(e);
    }

    /**
     * The next session event (a core Event or the RetiredPipes summary), or null with
     * waiter registered for the next one; on the loop.
     */
    public Object pollEvent(Waiter waiter) {
        return events.poll(waiter);
    }

    /**
     * The retirement summary (API.md Section 4.5): pipes retired since it was last read, of
     * those the ones whose failure or end was unread, the unread non-terminal events dropped,
     * and the last few retired IDs, oldest first.
     */
    public record RetiredPipes(long retired, long failed, long closed, long dropped, List<Long> last) {}

    /** Queues an event of the API above the core (EventQueue#addApplication); on the loop. */
    public void addApplicationEvent(Object key, Object event) {
        events.addApplication(key, event);
    }

    /** Withdraws a waiter registered by {@link #pollEvent}; on the loop. */
    public void cancelEventWait(Waiter waiter) {
        events.cancel(waiter);
    }

    /** A failure inside the endpoint: the session closes with INTERNAL_ERROR (I 2.5.6). */
    void internalError(Throwable t) {
        java.util.logging.Logger.getLogger(SessionRuntime.class.getName()).log(java.util.logging.Level.WARNING,
                "zmux session failed", t);
        failInternal(t);
    }

    void failInternal(Throwable t) {
        if (finished) return;
        try {
            if (core.state() != SessionState.CLOSED) core.forceClose(t);
            drive();
        } catch (RuntimeException e2) {
            finish();
        }
        if (!finished) finish();
    }

    /** Releases everything once the session is CLOSED. */
    private void finish() {
        if (finished) return;
        finished = true;
        publishSnapshots();
        loop.schedule(timer, io.zmux.internal.core.Time.NEVER);
        loop.removeShutdownHook(shutdownHook);
        carriers.closeAll();
        for (HeldJoin j : heldJoins) { j.c.close(null); j.counts.rejected(); }
        heldJoins.clear();
        reservations.clear();
        pipeWaiters.clear();
        pickers.clear();
        callbacks.close();
        publishResources();
        publishError();
        SessionError e = terminal();
        CoreException ended = CoreException.session(e);
        mailbox.close(ended);
        lanes.sessionEnded(ended);
        while (live != null) { Operation<?> op = live; untrack(op); op.settle(ended); }
        events.wakeAll();
        completions.execute(() -> closed.complete(e));
    }

    boolean finished() { return finished; }

}
