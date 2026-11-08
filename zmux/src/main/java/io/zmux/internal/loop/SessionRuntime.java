package io.zmux.internal.loop;

import io.zmux.internal.core.Datagrams;
import io.zmux.internal.core.Action;
import io.zmux.internal.core.CarrierAdmission;
import io.zmux.internal.core.Joins;
import io.zmux.internal.core.Batch;
import io.zmux.internal.core.ReadBuf;
import java.nio.ByteBuffer;
import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.Event;
import io.zmux.internal.join.Kdf;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.SessionError;
import io.zmux.internal.core.SessionState;
import io.zmux.internal.core.Time;
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
     * calls stay allowed there.
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
        // PIPE_STATE is internal: the tracer sees the application's pipe kinds.
        if (hooks.tracer() == null || traceClosed || event.kind() == Event.Kind.PIPE_STATE) return;
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
        return e.kind() == Event.Kind.CLOSED || e.pipeTerminal();
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
    private final Runnable shutdownHook = () -> failInternalNow(new IllegalStateException("event loop closed"));
    private boolean finished;
    private final TimerHeap.Timer timer = new TimerHeap.Timer() {
        @Override
        void fire(long now) {
            try {
                core.handleTimeout(now);
            } catch (RuntimeException | AssertionError | StackOverflowError e) {
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
        boot = c;
        bootTls = c.tlsSession();
        Runnable edge = Nesting.tryAttach(this, c.info().connection());
        if (edge == null) throw new IllegalArgumentException("nested carrier would form a cycle");
        loop.onShutdown(shutdownHook);
        var carrier = c.info().carrier();
        if (carrier.variant().isEmpty()) carrier = carrier.withVariant(bootstrapVariant);
        core.attachBootstrap(carrier, carrier, null);
        core.spendHandshake(c.handshakePadding());
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

    // ---- pipes ----

    /** How often a join the cap or the backoff holds asks again. */
    private static final long JOIN_HOLD_POLL = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(50);

    private static io.zmux.internal.join.Framing joinFraming(Transport c) {
        return c.info().carrier().framing() == io.zmux.internal.core.Framing.RECORDS
                ? io.zmux.internal.join.Framing.MESSAGE : io.zmux.internal.join.Framing.STREAM;
    }

    private static byte[] unit(ByteBuffer data, int pos, int lim) {
        byte[] b = new byte[lim - pos];
        data.get(pos, b);
        return b;
    }

    private static void writeUnit(Transport c, byte[] unit) {
        Batch b = new Batch();
        b.wrap(io.zmux.carrier.OutboundBatch.of(ByteBuffer.wrap(unit)));
        c.write(b);
    }

    /** The local endpoint's handshake on a JOINING attachment: nothing read reaches the session before the Reply verifies. */
    private final class JoinerGate extends TimerHeap.Timer implements Inbound {
        final Transport c;
        final long id;
        final int dir;
        final Runnable edge;
        final CompletableFuture<Long> future;
        final long due = Time.add(Clock.now(), joinWait());
        final io.zmux.internal.join.Framing framing;
        boolean prepared, helloSent, helloWritten, verified, confirming, done, inputEnded, endedGracefully;
        io.zmux.internal.join.Result result;
        byte[] confirm;

        JoinerGate(Transport c, long id, int dir, Runnable edge, CompletableFuture<Long> future) {
            this.c = c;
            this.id = id;
            this.dir = dir;
            this.edge = edge;
            this.future = future;
            framing = joinFraming(c);
        }

        void begin() {
            joinerGates.put(id, this);
            c.taken();
            c.start(this);
            hello();
        }

        private void hello() {
            if (done) return;
            String message;
            try {
                message = core.joinPrepare(id, Clock.now());
            } catch (CoreException e) {
                fail(e);
                return;
            }
            markDirty();
            if (message == null) {
                loop.schedule(this, Math.min(due, Time.add(Clock.now(), JOIN_HOLD_POLL)));
                return;
            }
            prepared = true;
            loop.schedule(this, due);
            joinPlan(c, message, () -> !done, this::assemble);
        }

        private void assemble(io.zmux.carrier.HandshakeShaper.UnitPlan plan) {
            byte[] h;
            try {
                h = core.joinHello(id, framing, plan, c.info().carrier().writeOverhead(), Clock.now());
            } catch (io.zmux.carrier.SecureCarrierException | CoreException e) {
                fail(e);
                return;
            }
            helloSent = true;
            writeUnit(c, h);
            markDirty();
        }

        @Override void fire(long now) {
            // Proven: the attachment is the core's, whatever the Confirm's write takes.
            if (done || result != null) return;
            if (now - due >= 0) fail(new java.io.IOException("pipe " + id + " not joined: handshake timeout"));
            else if (!prepared) hello();
        }

        /** Fails the attachment and the carrier. */
        void fail(Throwable cause) {
            if (done) return;
            end(cause);
            try { core.removePipe(id, false); } catch (CoreException gone) { /* Already retired. */ }
            markDirty();
        }

        /** The attachment failed or was removed in the core. */
        void end(Throwable cause) {
            if (done) return;
            done = true;
            joinerGates.remove(id);
            loop.schedule(this, Time.NEVER);
            c.close(null);
            edge.run();
            completions.execute(() -> future.completeExceptionally(cause));
        }

        private int step(byte[] b) {
            if (done) return GONE;
            Joins.JoinStep st = core.verifyReply(id, b, 0, b.length, Clock.now());
            switch (st.status()) {
                case MORE -> { }
                case PROVEN -> {
                    verified = true;
                    c.pauseInput();
                    if (core.pipeMode(id) == io.zmux.internal.join.Mode.BOUND) finish(io.zmux.carrier.HandshakeShaper.UnitPlan.NONE);
                    else joinPlan(c, "confirm", () -> !done, this::finish);
                }
                case DECLINED -> end(CoreException.of(CoreException.Reason.JOIN_REFUSED));
                case FAILED -> end(new java.io.IOException("pipe " + id + " not joined"));
            }
            markDirty();
            return st.consumed();
        }

        private void finish(io.zmux.carrier.HandshakeShaper.UnitPlan plan) {
            Joins.Finished f = core.joinFinish(id, plan, c.info().carrier().writeOverhead(), Clock.now());
            markDirty();
            if (f == null) {
                end(new java.io.IOException("pipe " + id + " not joined"));
                return;
            }
            result = f.result();
            confirm = f.confirm();
            loop.schedule(this, Time.NEVER);
            if (helloWritten) proven();
        }

        /** Proven with the Hello's write complete (a Reply can overtake its report): the carrier's only write is this one. */
        private void proven() {
            if (confirm != null && confirm.length > 0) {
                confirming = true;
                writeUnit(c, confirm);
            } else {
                post(this::attach);
            }
        }

        private void attach() {
            if (done) return;
            done = true;
            joinerGates.remove(id);
            loop.schedule(this, Time.NEVER);
            // The session may have ended, or the attachment failed, while the Confirm was written.
            boolean ended = finished || core.state().compareTo(SessionState.CLOSING) >= 0;
            if (ended || !core.pipeAttached(id)) {
                c.close(null);
                edge.run();
                // Proven made the attachment live in the core, so a close waits for it to take
                // CONNECTION_CLOSE: it has no carrier now (as in endHandshakes).
                if (ended) core.closingWithoutPipe(id);
                completions.execute(() -> future.completeExceptionally(ended ? CoreException.session(terminal()) : new java.io.IOException("pipe " + id + " not joined")));
                markDirty();
                return;
            }
            Transport t = result.mode() == io.zmux.internal.join.Mode.SEALED ? new SealedTransport(c, result) : c;
            t.restrict(dir == 0 ? Action.DIR_OUT : dir == 1 ? Action.DIR_IN : Action.DIR_BIDI);
            core.spendHandshake(t.handshakePadding());
            Inbound in = carriers.attach(t, id, 0, edge);
            c.resumeInput();
            if (inputEnded && dir != 0) in.ended(t, endedGracefully, false);
            core.pipes().wakeWriters(id);
            markDirty();
            completions.execute(() -> future.complete(id));
        }

        @Override public boolean session() { return true; }
        @Override public int bytes(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) {
            if (verified) return 0;
            return step(unit(data, pos, lim));
        }
        @Override public void message(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) {
            if (!verified) step(unit(data, pos, lim));
        }
        @Override public void ended(Transport t, boolean graceful, boolean partial) {
            if (verified) { // Proven: the attachment takes it.
                inputEnded = true;
                endedGracefully = graceful;
                return;
            }
            fail(t.inputError() != null ? t.inputError() : new java.io.IOException("pipe " + id + " not joined: carrier ended"));
        }
        @Override public void written(Transport t, Batch b, Throwable cause) {
            b.discard();
            if (cause != null) { fail(cause); return; }
            if (!helloWritten) {
                helloWritten = true;
                if (result != null) proven();
            } else if (confirming) {
                confirming = false;
                post(this::attach);
            }
        }
    }

    /** A Hello that passed the listener, with what it needs once the session goes on. */
    record OfferedHello(Transport c, io.zmux.internal.join.Hello hello, int skip, byte[] binding, Object admitted, String variant,
                        ListenerHub.Counts counts, ListenerHub.JoinSlot slot, java.util.function.BiConsumer<Transport, String> reject, long slotAt,
                        int maxUnconfirmed) {}

    /** The acceptor's handling of a Hello that named this session; on the loop. */
    void acceptHello(OfferedHello o) {
        Transport c = o.c;
        if (finished || core.state().compareTo(SessionState.CLOSING) >= 0 || o.slot.listenerClosed()) {
            o.slot.settled();
            c.close(null);
            o.counts.refused();
            return;
        }
        if (o.hello.mode() == io.zmux.internal.join.Mode.BOUND && o.binding == null) {
            core.helloWithoutBinding();
            markDirty();
            o.slot.settled();
            o.reject.accept(c, "binding_unavailable");
            return;
        }
        var framing = joinFraming(c);
        Joins.Opened opened = core.openHello(o.hello, framing, o.binding == null ? new byte[0] : o.binding);
        markDirty();
        switch (opened.result()) {
            case HOLD, KEY_WAIT -> {
                hold(new HeldJoin(o, Time.add(o.slotAt, joinWait())));
                return;
            }
            case ACCEPTED -> { }
            default -> {
                o.slot.settled();
                o.reject.accept(c, opened.reason());
                return;
            }
        }
        Joins.PeerJoin pj = opened.join();
        long pipe = pj.pipe().id();
        if (!hooks.joinPolicy().allow(c, pj.mode(), o.admitted) || !hooks.joinPolicy().authorize(host, pipe)) {
            o.slot.settled(); // The Pipe ID's bit stays clear.
            o.reject.accept(c, "auth");
            return;
        }
        Runnable edge = Nesting.tryAttach(this, c.info().connection());
        var base = o.variant.isEmpty() || !c.info().carrier().variant().isEmpty() ? c.info().carrier() : c.info().carrier().withVariant(o.variant);
        var carrier = pj.mode() == io.zmux.internal.join.Mode.SEALED ? SealedTransport.sealedInfo(base) : base;
        Joins.JoinCarrier jc = new Joins.JoinCarrier((c.directions() & Action.DIR_IN) != 0, (c.directions() & Action.DIR_OUT) != 0, carrier, carrier, null, edge == null);
        // Waits for its plan off the session; the slot and the edge are held, and the claim checks the session again.
        PlanningHello planning = new PlanningHello(o, edge);
        planningHellos.add(planning);
        loop.schedule(planning, Time.add(o.slotAt, joinWait()));
        joinPlan(c, "reply", () -> !planning.done, plan -> {
            planning.finish();
            claim(o, pj, jc, edge, plan);
        });
    }

    /** A verified Hello waiting for its Reply's plan. */
    private final class PlanningHello extends TimerHeap.Timer {
        final OfferedHello o;
        final Runnable edge;
        boolean done;

        PlanningHello(OfferedHello o, Runnable edge) {
            this.o = o;
            this.edge = edge;
        }

        void finish() {
            done = true;
            planningHellos.remove(this);
            loop.schedule(this, Time.NEVER);
        }

        void drop(ListenerHub.Outcome how) {
            if (done) return;
            finish();
            o.c.close(null);
            if (edge != null) edge.run();
            o.slot.settled();
            o.counts.count(how);
        }

        @Override void fire(long now) { drop(ListenerHub.Outcome.TIMED_OUT); }
    }

    private final java.util.ArrayList<PlanningHello> planningHellos = new java.util.ArrayList<>();

    /** The plan of an outgoing message, off the session and after its delay; next runs on the loop while live holds. */
    private void joinPlan(Transport c, String message, java.util.function.BooleanSupplier live, java.util.function.Consumer<io.zmux.carrier.HandshakeShaper.UnitPlan> next) {
        var shaper = hooks.handshakeShaper();
        if (shaper == null) {
            next.accept(io.zmux.carrier.HandshakeShaper.UnitPlan.NONE);
            return;
        }
        var view = new io.zmux.carrier.HandshakeShaper.View(c.info().carrier().kind(), "join", message);
        completions.execute(() -> {
            boolean[] fault = new boolean[1];
            var asked = io.zmux.carrier.HandshakeShaper.ask(shaper, view, () -> fault[0] = true);
            post(() -> {
                if (fault[0]) core.handshakeFault();
                else if (asked == null) core.handshakeUnplanned();
                if (!live.getAsBoolean()) return;
                var plan = (asked == null ? io.zmux.carrier.HandshakeShaper.UnitPlan.NONE : asked).limited(core.shapingLimitsInForce());
                markDirty();
                long wait = plan.delayNanos();
                if (wait <= 0) {
                    next.accept(plan);
                    return;
                }
                loop.schedule(new TimerHeap.Timer() {
                    @Override void fire(long now) { if (live.getAsBoolean()) next.accept(plan); }
                }, Time.add(Clock.now(), Math.min(wait, joinWait())));
            });
        });
    }

    private void claim(OfferedHello o, Joins.PeerJoin pj, Joins.JoinCarrier jc, Runnable edge, io.zmux.carrier.HandshakeShaper.UnitPlan plan) {
        Transport c = o.c;
        Joins.Claimed claimed = core.claimHello(pj, jc, plan, c.info().carrier().writeOverhead(), Math.max(o.hello.consumed(), o.skip));
        markDirty();
        switch (claimed.result()) {
            case ACCEPTED -> new AcceptorGate(o, pj, jc, edge).begin(claimed.reply());
            case REFUSED -> {
                if (edge != null) edge.run();
                o.slot.settled();
                o.counts.refused();
                c.taken();
                c.start(new Inbound() {
                    @Override public int bytes(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) { return lim - pos; }
                    @Override public void ended(Transport t, boolean graceful, boolean partial) { t.close(null); }
                    @Override public void written(Transport t, Batch b, Throwable cause) { b.discard(); t.close(null); }
                });
                writeUnit(c, claimed.reply());
            }
            default -> {
                if (edge != null) edge.run();
                o.slot.settled();
                o.reject.accept(c, "auth");
            }
        }
    }

    /** An Unconfirmed carrier: it holds no session state until Confirmed. */
    private final class AcceptorGate extends TimerHeap.Timer implements Inbound {
        final OfferedHello o;
        final Joins.PeerJoin pj;
        final Joins.JoinCarrier jc;
        final Runnable edge;
        boolean replyWritten, confirmed, held, done, inputEnded, endedGracefully;
        /** The Hello's bytes, still in the carrier's input: the listener read them before moving it here. */
        int skip;

        AcceptorGate(OfferedHello o, Joins.PeerJoin pj, Joins.JoinCarrier jc, Runnable edge) {
            this.o = o;
            this.pj = pj;
            this.jc = jc;
            this.edge = edge;
            skip = o.skip;
            confirmed = pj.mode() == io.zmux.internal.join.Mode.BOUND;
        }

        void begin(byte[] reply) {
            // The listener's bound on Unconfirmed attachments: a new one closes the oldest.
            if (unconfirmed.size() >= o.maxUnconfirmed) unconfirmed.getFirst().drop(ListenerHub.Outcome.REJECTED);
            unconfirmed.addLast(this);
            loop.schedule(this, Time.add(o.slotAt, joinWait()));
            o.c.taken();
            o.c.start(this);
            if (!done) go();
            if (!done) writeUnit(o.c, reply);
        }

        void drop(ListenerHub.Outcome how) {
            if (done) return;
            done = true;
            unconfirmed.remove(this);
            loop.schedule(this, Time.NEVER);
            o.c.close(null);
            edge.run();
            o.slot.settled();
            o.counts.count(how);
        }

        private void confirmed() {
            if (done) return;
            if (finished || core.state().compareTo(SessionState.CLOSING) >= 0) {
                drop(ListenerHub.Outcome.REFUSED);
                return;
            }
            done = true;
            unconfirmed.remove(this);
            loop.schedule(this, Time.NEVER);
            io.zmux.internal.join.Result result;
            Joins.JoinResult res;
            try {
                result = pj.result();
                res = core.confirmJoin(pj, jc);
            } catch (io.zmux.carrier.SecureCarrierException e) {
                result = null;
                res = Joins.JoinResult.CLOSE;
            }
            o.slot.settled();
            if (res != Joins.JoinResult.ACCEPTED) {
                o.c.close(null);
                edge.run();
                if (res == Joins.JoinResult.REFUSED) o.counts.refused(); else o.counts.rejected();
                markDirty();
                return;
            }
            long pipe = pj.pipe().id();
            int dir = pj.pipe().direction();
            Transport t = pj.mode() == io.zmux.internal.join.Mode.SEALED ? new SealedTransport(o.c, result) : o.c;
            t.restrict(dir == 0 ? Action.DIR_IN : dir == 1 ? Action.DIR_OUT : Action.DIR_BIDI);
            core.pipes().setCarrierConn(pipe, o.c.info().connection());
            core.pipes().setAdmitted(pipe, o.admitted);
            core.spendHandshake(t.handshakePadding());
            Inbound in = carriers.attach(t, pipe, 0, edge);
            o.c.resumeInput();
            if (inputEnded && dir != 1) in.ended(t, endedGracefully, false);
            core.pipes().wakeWriters(pipe);
            o.counts.joined();
            markDirty();
        }

        /** What follows the Confirm (a Bound join's Hello) is the attachment's: it stays unread from here, whenever the Reply's write reports. */
        private void go() {
            if (done || !confirmed || skip > 0) return;
            if (!held) {
                held = true;
                o.c.pauseInput();
            }
            if (replyWritten) post(this::confirmed);
        }

        private int read(byte[] b) {
            if (done || confirmed) return 0;
            int n;
            try {
                n = pj.readConfirm(b, 0, b.length);
            } catch (io.zmux.carrier.SecureCarrierException e) {
                if (e.code() == io.zmux.carrier.SecureCarrierException.Code.INCOMPLETE) return 0;
                drop(ListenerHub.Outcome.REJECTED);
                return GONE;
            }
            confirmed = true;
            go();
            return n;
        }

        @Override void fire(long now) { drop(ListenerHub.Outcome.TIMED_OUT); }
        @Override public boolean session() { return true; }
        @Override public int bytes(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) {
            int skipped = Math.min(skip, lim - pos);
            skip -= skipped;
            if (skip > 0) return skipped;
            if (skipped > 0) go();
            if (pos + skipped == lim) return skipped;
            int n = read(unit(data, pos + skipped, lim));
            return n == GONE ? GONE : skipped + n;
        }
        @Override public void message(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) { read(unit(data, pos, lim)); }
        @Override public void ended(Transport t, boolean graceful, boolean partial) {
            if (!confirmed) {
                drop(ListenerHub.Outcome.REJECTED);
                return;
            }
            inputEnded = true; // Confirmed: the attachment takes it.
            endedGracefully = graceful;
        }
        @Override public void written(Transport t, Batch b, Throwable cause) {
            b.discard();
            if (cause != null) { drop(ListenerHub.Outcome.REJECTED); return; }
            replyWritten = true;
            go();
        }
    }

    private final java.util.HashMap<Long, JoinerGate> joinerGates = new java.util.HashMap<>();
    private final java.util.ArrayDeque<AcceptorGate> unconfirmed = new java.util.ArrayDeque<>();

    /** The configured callbacks, the logger among them. */
    public DriverHooks hooks() { return hooks; }

    /** The application's handle of this session, for the join policy. */
    private volatile Object host;

    public void host(Object h) { host = h; }

    /** A local join over c, which this loop owns and nobody started; completes at Proven. */
    private void addPipe(Transport c, Joins.JoinOptions o, CompletableFuture<Long> f) throws CoreException {
        if (finished) {
            c.close(null);
            throw CoreException.session(terminal());
        }
        if (c.directions() != Action.DIR_BIDI) {
            c.close(null);
            throw new IllegalArgumentException("a join needs both directions of its carrier for its handshake");
        }
        var base = c.info().carrier();
        if (o.mode() == null) {
            io.zmux.internal.join.Mode chosen;
            try {
                chosen = hooks.joinPolicy().choose(c);
            } catch (RuntimeException failed) {
                c.close(failed);
                hooks.log(System.Logger.Level.WARNING, "the record policy failed; the join's carrier is closed", failed);
                throw failed;
            }
            o = o.toBuilder().mode(chosen).build();
        }
        Joins.JoinOptions.Builder ob = o.toBuilder();
        ob.in(o.mode() == io.zmux.internal.join.Mode.SEALED ? SealedTransport.sealedInfo(base) : base);
        ob.out(o.mode() == io.zmux.internal.join.Mode.SEALED ? SealedTransport.sealedInfo(base) : base);
        if (o.fateGroup() == 0) ob.fateGroup(c.info().fateGroup());
        if (o.mode() == io.zmux.internal.join.Mode.BOUND) {
            byte[] binding = c.bindable() ? c.export(Kdf.EXPORTER_JOIN, null, Kdf.BINDING_LEN) : null;
            if (binding == null) {
                c.close(null);
                throw new IllegalArgumentException("Bound on a carrier without a Secure declaration or an exporter");
            }
            ob.binding(binding);
        }
        Runnable edge = Nesting.tryAttach(this, c.info().connection());
        if (edge == null) {
            c.close(null);
            throw CoreException.of(CoreException.Reason.PIPE_REFUSED);
        }
        long r;
        try {
            r = core.addPipe(ob.build());
        } catch (CoreException | RuntimeException refused) {
            edge.run();
            c.close(null);
            throw refused;
        }
        long id = r < 0 ? -r - 1 : r;
        core.pipes().setCarrierConn(id, c.info().connection());
        new JoinerGate(c, id, o.dir(), edge, f).begin();
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

    /** The waiter of a join gave up: its setup and its handshake end, a join the cap or the backoff holds included. */
    public void cancelJoinSetup(CompletableFuture<Long> future) {
        if (future instanceof TlsJoin tls) tls.cancelHandshake.run();
        executeCarrier(() -> {
            for (JoinerGate g : new ArrayList<>(joinerGates.values())) {
                if (g.future == future && !g.verified) g.fail(new java.io.InterruptedIOException("join cancelled"));
            }
        });
    }

    /** Runs the TLS handshake of engine over ch, then joins it. */
    public CompletableFuture<Long> addPipe(java.nio.channels.SocketChannel ch, javax.net.ssl.SSLEngine engine,
                                           long handshakeTimeout, boolean secure, Joins.JoinOptions o, Bootstrap.Presenter presenter) {
        var f = new TlsJoin();
        EventLoop nio = loop instanceof EventLoop l ? l : EventLoopGroup.nextShared();
        f.cancelHandshake = TlsTransport.handshake(nio, ch, engine, handshakeTimeout, secure, (c, err) -> {
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
            addPipe(c, o, f);
        } catch (CoreException | RuntimeException e) {
            completions.execute(() -> f.completeExceptionally(e));
        }
    }

    /** The bootstrap carrier, whose exporter stays readable after it closed. */
    /** The TLS session this process ran for the bootstrap carrier; null without one. */
    private volatile javax.net.ssl.SSLSession bootTls;

    public javax.net.ssl.SSLSession bootstrapTls() { return bootTls; }

    private volatile Transport boot;

    /** The admission's label of the bootstrap carrier, for one whose pipe input declares none; before it attaches. */
    private String bootstrapVariant = "";

    void bootstrapVariant(String v) { bootstrapVariant = v; }

    /** The exporter of the bootstrap carrier ({@link Transport#export}); null while it is unavailable. Any thread. */
    public byte[] exportKeyingMaterial(String label, byte[] context, int length) {
        return boot.export(label, context, length);
    }

    /** Whether the bootstrap carrier has an exporter; any thread. */
    public boolean bootHasExporter() { return boot.hasExporter(); }

    /** The admission value of the session's bootstrap carrier; set before the session starts. */
    private volatile Object admitted;

    void admitted(Object v) { admitted = v; }

    /** The admission value of the session's bootstrap carrier, null without one. */
    public Object admitted() { return admitted; }

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

    private void discard(io.zmux.carrier.PipeIo io) {
        // The host's code: never on the loop.
        completions.execute(() -> {
            try { if (io.reader() != null) io.reader().close(); } catch (RuntimeException ignored) { /* Closing anyway. */ }
            try { if (io.writer() != null) io.writer().abort(ABORTED); } catch (RuntimeException ignored) { /* Closing anyway. */ }
        });
    }

    private record HeldJoin(OfferedHello o, long due) {}

    private long joinWait() { return core.joinDeadline(); }

    private void hold(HeldJoin j) {
        heldJoins.add(j);
        armHeldJoins();
        j.o.slot.held(() -> { if (!loop.tryExecute(() -> evictHeld(j.o.slot))) j.o.slot.settled(); });
    }

    /** Newer joins took the place of this held join; closed silently. */
    private void evictHeld(ListenerHub.JoinSlot slot) {
        heldJoins.removeIf(j -> {
            if (j.o.slot != slot) return false;
            endHeld(j, ListenerHub.Outcome.REJECTED);
            return true;
        });
        armHeldJoins();
    }

    private static void endHeld(HeldJoin j, ListenerHub.Outcome how) {
        j.o.slot.settled();
        j.o.c.close(null);
        j.o.counts.count(how);
    }

    /** Ends what is still in a handshake on this session: nothing the peer did, so no rejection. On the loop. */
    private void endHandshakes(Throwable cause) {
        for (HeldJoin j : heldJoins) endHeld(j, ListenerHub.Outcome.REFUSED);
        heldJoins.clear();
        loop.schedule(heldTimer, Time.NEVER);
        if (!joinerGates.isEmpty()) {
            for (JoinerGate g : new ArrayList<>(joinerGates.values())) {
                long id = g.id;
                g.end(cause);
                // A close waits for every pipe that could take it, and this one has no carrier now.
                core.closingWithoutPipe(id);
            }
        }
        if (!unconfirmed.isEmpty()) for (AcceptorGate g : new ArrayList<>(unconfirmed)) g.drop(ListenerHub.Outcome.REFUSED);
        if (!planningHellos.isEmpty()) for (PlanningHello h : new ArrayList<>(planningHellos)) h.drop(ListenerHub.Outcome.REFUSED);
    }

    /** The listener closed: the joins of this slot end, not an attached pipe. On the loop. */
    void endOffered(ListenerHub.JoinSlot slot) {
        heldJoins.removeIf(j -> {
            if (j.o.slot != slot) return false;
            endHeld(j, ListenerHub.Outcome.REFUSED);
            return true;
        });
        armHeldJoins();
        for (AcceptorGate g : new ArrayList<>(unconfirmed)) if (g.o.slot == slot) g.drop(ListenerHub.Outcome.REFUSED);
        for (PlanningHello h : new ArrayList<>(planningHellos)) if (h.o.slot == slot) h.drop(ListenerHub.Outcome.REFUSED);
        markDirty();
    }

    private void armHeldJoins() {
        long next = Time.NEVER;
        for (HeldJoin j : heldJoins) next = Math.min(next, j.due);
        loop.schedule(heldTimer, next);
    }

    /** Closes the held joins whose wait is over, silently. */
    private final TimerHeap.Timer heldTimer = new TimerHeap.Timer() {
        @Override
        void fire(long now) {
            heldJoins.removeIf(j -> {
                if (j.due - now > 0) return false;
                endHeld(j, ListenerHub.Outcome.TIMED_OUT);
                return true;
            });
            armHeldJoins();
        }
    };

    /** The held joins go on: the session is established, or the join keys now exist. */
    private void retryHeldJoins() {
        List<HeldJoin> h = new ArrayList<>(heldJoins);
        heldJoins.clear();
        loop.schedule(heldTimer, Time.NEVER);
        for (HeldJoin j : h) acceptHello(j.o);
    }


    /** The host proved pipe 0 end to end; on the loop. Held joins go on in the next round. */
    public void declareEndToEnd() {
        core.declareEndToEnd();
        List<Waiter> w = new ArrayList<>(pipeWaiters);
        pipeWaiters.clear();
        for (Waiter op : w) op.wake();
        markDirty();
    }

    /** The deferred PSK arrived; on the loop. */
    public void setJoinSecret(byte[] psk) throws CoreException {
        boolean wasPending = core.joinPending();
        core.setJoinSecret(psk);
        if (wasPending) {
            retryHeldJoins();
            List<Waiter> w = new ArrayList<>(pipeWaiters);
            pipeWaiters.clear();
            for (Waiter op : w) op.wake();
            markDirty();
        }
    }

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
    /** Whether the calling thread may block on this session: not its event loop or callback thread. */
    public boolean mayBlock() {
        return !SessionHost.onHostThread() && !SerialCallbacks.onContext(this);
    }

    public void checkBlocking() {
        if (SessionHost.onHostThread() || SerialCallbacks.onContext(this)) {
            throw new IllegalStateException("blocking zmux call on its event-loop or callback thread");
        }
    }

    // ---- snapshots the loop publishes for other threads ----

    /**
     * What the session negotiated, as of the loop's last round.
     *
     * @param datagramLimit the largest datagram payload the peer accepts now before a flow prefix, 0 when none
     * @param goAway the received GOAWAY watermarks [maxBidi, maxUni, code], null while none was received
     */
    public record Info(boolean established, boolean initiator, boolean peerIsZmux, boolean preamble,
                       boolean priority, boolean datagramFlows, boolean pipeAck, boolean multipath, boolean resume,
                       boolean lanesToPeer, boolean localDatagrams, boolean peerDatagrams, boolean localResetStreamAt,
                       boolean peerResetStreamAt, int datagramLimit, boolean joinPending, boolean joinSecurityUnavailable,
                       boolean suspended, long[] goAway) {}

    private volatile io.zmux.internal.core.SessionStats.Snapshot stats;
    private volatile Info info;
    // Loop only: what the published snapshots were made from.
    private long statsVersion = -1, statsBudget = -1;
    private boolean infoEstablished, infoSuspended, infoSecretPending;
    private long[] infoGoAway;

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
        boolean est = core.established(), pending = core.joinPending(), suspended = core.state() == SessionState.SUSPENDED;
        long[] away = core.peerGoAway();
        if (info == null || est != infoEstablished || !java.util.Arrays.equals(away, infoGoAway) || pending != infoSecretPending
                || suspended != infoSuspended) {
            infoSecretPending = pending;
            infoEstablished = est;
            infoGoAway = away;
            infoSuspended = suspended;
            var p = core.peerParams();
            var local = core.config().local();
            info = new Info(est, core.initiator(), p != null && p.isZmux(), f.preamble(), f.priority(), f.flows(),
                    f.pipeAck(), f.multipath(), f.resume(), f.lanesOut(), core.config().datagrams(),
                    p != null && p.has(io.zmux.internal.wire.TransportParams.DATAGRAM) && p.maxDatagramFrameSize > 0,
                    local.has(io.zmux.internal.wire.TransportParams.RESET_AT),
                    p != null && p.has(io.zmux.internal.wire.TransportParams.RESET_AT),
                    core.datagrams().maxPayload(0), pending, f.joinSecurityUnavailable(), suspended, away);
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
        } catch (RuntimeException | AssertionError | StackOverflowError e) {
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
        if (core.state().compareTo(SessionState.CLOSING) >= 0) endHandshakes(CoreException.session(terminal()));
        publishSnapshots(); // Before anyone woken (an accepted or established session) reads them.
        core.outbox().takeWakes(wakes);
        for (Waiter w : wakes) w.wake();
        wakes.clear();
        core.settle();
        boolean pipeChanged = false;
        for (Event e; (e = core.outbox().pollEvent()) != null; ) {
            pipeChanged |= e.kind() == Event.Kind.PIPE_STATE || e.kind() == Event.Kind.ESTABLISHED || e.kind() == Event.Kind.EPOCH_COMMITTED;
            deliver(e);
        }
        if (pipeChanged) {
            refreshPipes();
            if (!heldJoins.isEmpty() && core.established() && !core.joinPending()) retryHeldJoins();
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
                // The pipe's datagram channel, independent of the batch write in flight.
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
        if (a.kind() == Action.Kind.ABORT || a.kind() == Action.Kind.CLOSE && a.dir() == Action.DIR_BIDI) {
            JoinerGate g = joinerGates.get(a.pipe());
            if (g != null) {
                g.end(new java.io.IOException("pipe " + a.pipe() + " not joined"));
                return;
            }
        }
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

    /** Told of every Locator joins may now name (true) or no longer name (false); the listener's routing. */
    private java.util.function.BiConsumer<byte[], Boolean> locatorListener;

    /** Installs the listener of Locator changes; on the loop or before the session runs. */
    public void locatorListener(java.util.function.BiConsumer<byte[], Boolean> l) { locatorListener = l; }

    private void deliver(Event e) {
        if (e.kind() == Event.Kind.LOCATOR_ADDED || e.kind() == Event.Kind.LOCATOR_REMOVED) {
            if (locatorListener != null) locatorListener.accept(e.frame(), e.kind() == Event.Kind.LOCATOR_ADDED);
            return;
        }
        trace(e);
        if (e.kind() == Event.Kind.CLOSED) return;
        if (e.kind() == Event.Kind.RESUME_UNATTENDED) {
            hooks.warn("zmux session suspended with nothing to redial it (no redial hook, no running pool): it ends at its resume timeout unless the application adds a pipe");
        }
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
     * The retirement summary: pipes retired since it was last read, of
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

    /** A failure inside the endpoint: the session closes with INTERNAL_ERROR. */
    void internalError(Throwable t) {
        if (hooks.logger() != null) hooks.log(System.Logger.Level.ERROR, "zmux session failed", t);
        else java.util.logging.Logger.getLogger(SessionRuntime.class.getName()).log(java.util.logging.Level.WARNING, "zmux session failed", t);
        failInternal(t);
    }

    /**
     * Closes with INTERNAL_ERROR. The CONNECTION_CLOSE goes out as in any close: a write still
     * in flight on the carrier holds it back until it completes, so the session ends once it went
     * out, or after {@link #FAIL_WAIT} at the latest. A second failure meanwhile ends it at once.
     */
    void failInternal(Throwable t) {
        if (finished) return;
        if (failing) { finish(); return; }
        failing = true;
        try {
            if (core.state() != SessionState.CLOSED) core.forceClose(t);
            drive();
        } catch (RuntimeException | AssertionError | StackOverflowError e2) {
            finish();
        }
        if (!finished) loop.schedule(failTimer, Time.add(Clock.now(), FAIL_WAIT));
    }

    /** As {@link #failInternal}, but the session ends now: its loop is gone, or it never started. */
    void failInternalNow(Throwable t) {
        failInternal(t);
        finish();
    }

    /** How long a session closing after a failure inside the endpoint waits for its CONNECTION_CLOSE to go out. */
    static final long FAIL_WAIT = 2 * Time.SECOND;

    /** A failure inside the endpoint closes the session: set once. */
    private boolean failing;

    private final TimerHeap.Timer failTimer = new TimerHeap.Timer() {
        @Override
        void fire(long now) { finish(); }
    };

    /** Releases everything once the session is CLOSED. */
    private void finish() {
        if (finished) return;
        finished = true;
        publishSnapshots();
        loop.schedule(timer, io.zmux.internal.core.Time.NEVER);
        loop.schedule(heldTimer, Time.NEVER);
        loop.schedule(failTimer, Time.NEVER);
        loop.removeShutdownHook(shutdownHook);
        carriers.closeAll();
        endHandshakes(CoreException.session(terminal()));
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
