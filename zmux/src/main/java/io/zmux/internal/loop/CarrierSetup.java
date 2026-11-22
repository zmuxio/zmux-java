package io.zmux.internal.loop;

import io.zmux.internal.spi.AttachmentRelease;

import io.zmux.internal.spi.AdapterPorts;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/** Sole authority for admission ownership. Host calls, cleanup and notifications run outside its monitor. */
public final class CarrierSetup implements io.zmux.internal.spi.AdapterPorts.Setup {
    static final CarrierSetup NONE = new CarrierSetup();
    enum State { RESERVED, PENDING, MATCHED, MATCHED_WITHDRAWN, FALLBACK, WITHDRAWN, PUBLISHING, STALLED, FINISHED, DETACHED, DETACHED_WITHDRAWN, DETACHED_DONE }
    enum Event { ARRIVE, MATCH, CANCEL, STOP, CLAIM, PAIR_CLAIM, FALLBACK, DETACH, STALL, REJECT, LOST, FAIL, SUCCESS }

    /** A null successor means the event lost. Adding either a state or an event needs a decision here. */
    static State next(State from, Event event) {
        return switch (from) {
            case RESERVED -> switch (event) {
                case ARRIVE -> State.PENDING;
                case CANCEL, STOP -> State.WITHDRAWN;
                case REJECT, LOST, FAIL -> State.FINISHED;
                case MATCH, CLAIM, PAIR_CLAIM, FALLBACK, DETACH, STALL, SUCCESS -> null;
            };
            case PENDING -> switch (event) {
                case CANCEL, STOP -> State.WITHDRAWN;
                case CLAIM -> State.PUBLISHING;
                case MATCH -> State.MATCHED;
                case FALLBACK -> State.FALLBACK;
                case STALL -> State.STALLED;
                case REJECT, LOST, FAIL -> State.FINISHED;
                case ARRIVE, PAIR_CLAIM, DETACH, SUCCESS -> null;
            };
            case MATCHED -> switch (event) {
                case CANCEL, STOP -> State.MATCHED_WITHDRAWN;
                case PAIR_CLAIM -> State.PUBLISHING;
                case REJECT, LOST, FAIL -> State.FINISHED;
                case ARRIVE, MATCH, CLAIM, FALLBACK, DETACH, STALL, SUCCESS -> null;
            };
            case MATCHED_WITHDRAWN -> switch (event) {
                case PAIR_CLAIM -> State.PUBLISHING;
                case REJECT, LOST, FAIL -> State.FINISHED;
                case ARRIVE, MATCH, CANCEL, STOP, CLAIM, FALLBACK, DETACH, STALL, SUCCESS -> null;
            };
            case FALLBACK -> switch (event) {
                case DETACH -> State.DETACHED;
                case CANCEL, STOP -> State.WITHDRAWN;
                case REJECT, LOST, FAIL, SUCCESS -> State.FINISHED;
                case ARRIVE, MATCH, CLAIM, PAIR_CLAIM, FALLBACK, STALL -> null;
            };
            case WITHDRAWN, PUBLISHING -> switch (event) {
                case REJECT, LOST, FAIL, SUCCESS -> State.FINISHED;
                case ARRIVE, MATCH, CANCEL, STOP, CLAIM, PAIR_CLAIM, FALLBACK, DETACH, STALL -> null;
            };
            case STALLED -> switch (event) {
                case STOP -> State.WITHDRAWN;
                case REJECT, LOST, FAIL -> State.FINISHED;
                case ARRIVE, MATCH, CANCEL, CLAIM, PAIR_CLAIM, FALLBACK, DETACH, STALL, SUCCESS -> null;
            };
            case DETACHED -> switch (event) {
                case CANCEL, STOP -> State.DETACHED_WITHDRAWN;
                case REJECT, LOST, FAIL, SUCCESS -> State.DETACHED_DONE;
                case ARRIVE, MATCH, CLAIM, PAIR_CLAIM, FALLBACK, DETACH, STALL -> null;
            };
            case DETACHED_WITHDRAWN -> switch (event) {
                case REJECT, LOST, FAIL, SUCCESS -> State.DETACHED_DONE;
                case ARRIVE, MATCH, CANCEL, STOP, CLAIM, PAIR_CLAIM, FALLBACK, DETACH, STALL -> null;
            };
            case FINISHED, DETACHED_DONE -> null;
        };
    }

    private final Object lock = new Object();
    private State state;
    private final CompletableFuture<Void> result = new CompletableFuture<>();
    private final AttachmentRelease attachment;
    private final CarrierSetup uplink, downlink;
    private ListenerHub.AdmissionSlots.Lease slots;
    private Runnable cancellation;
    // Not a record: a record would publish the table lock through an accessor, and only this class may lock it.
    @SuppressWarnings("ClassCanBeRecord")
    private static final class Token {
        private final io.zmux.internal.core.CarrierAdmission.SplitPairing.Token<?> entry;
        private final Object tableLock;
        Token(io.zmux.internal.core.CarrierAdmission.SplitPairing.Token<?> entry, Object tableLock) {
            this.entry = entry;
            this.tableLock = tableLock;
        }
        void seal() { synchronized (tableLock) { entry.seal(); } }
        void retire() { synchronized (tableLock) { entry.close(); } }
    }
    private Token token;
    private sealed interface Holding permits Empty, Input, SocketInput, Owned, Attached {}
    private record Empty() implements Holding { private static final Empty INSTANCE = new Empty(); }
    private record Owned(Bound binding, AttachmentRelease guard) implements Holding {}
    private record Input(io.zmux.carrier.PipeIo io, AttachmentRelease guard) implements Holding {}
    private record SocketInput(java.nio.channels.SocketChannel channel, AttachmentRelease guard) implements Holding {}
    /** The close right belongs to the physical carrier. Sealing and forwarding preserve it. */
    static final class CloseRight {
        final java.util.concurrent.atomic.AtomicBoolean live = new java.util.concurrent.atomic.AtomicBoolean(true);
        volatile CarrierSetup owner = NONE;
        private Bound original;
        synchronized Bound original(Transport transport) {
            if (original == null) original = new Bound(transport, this);
            return original;
        }
    }

    /** An I/O view and the single close right it shares with the physical carrier. */
    public static final class Bound {
        /** Proof that the physical owner authorized this close, detach or move. */
        public static final class Permit { private Permit() {} }
        private static final Permit PERMIT = new Permit();
        final PhysicalTransport carrier;
        private final CloseRight right;
        private Bound(Transport carrier, CloseRight right) { this.carrier = (PhysicalTransport) carrier; this.right = right; }

        Transport move(SessionHost target) {
            if (!right.live.get()) return null;
            Transport next = carrier.movePhysical(PERMIT, target);
            if (next == null) close(null);
            return next;
        }

        java.nio.channels.SocketChannel detachSocket() {
            if (!(carrier instanceof SocketTransport socket) || !socket.canDetach()) return null;
            return right.live.compareAndSet(true, false) ? socket.detachPhysical(PERMIT) : null;
        }

        Bound as(Transport view) { return view == carrier ? this : new Bound(view, right); }
        void close(Throwable cause) {
            if (right.live.compareAndSet(true, false)) carrier.closePhysical(PERMIT, cause);
        }
    }

    /** The candidate session owns close during bootstrap negotiation. */
    private record Attached(CloseRight right) implements Holding {}

    private Holding holding = Empty.INSTANCE;

    public void receive(io.zmux.carrier.PipeIo io) {
        synchronized (lock) {
            if (holding != Empty.INSTANCE) throw new IllegalStateException("carrier already received");
            holding = new Input(java.util.Objects.requireNonNull(io), cleanup());
        }
    }

    void receive(java.nio.channels.SocketChannel channel) {
        synchronized (lock) {
            if (holding != Empty.INSTANCE) throw new IllegalStateException("carrier already received");
            holding = new SocketInput(channel, cleanup());
        }
    }

    /** Take the close/move right. Stale phase callbacks cannot acquire it a second time. */
    private Holding take() {
        synchronized (lock) {
            Holding previous = holding;
            holding = Empty.INSTANCE;
            return previous;
        }
    }

    /** Terminal entry failure, including exceptions constructing the transport. */
    public void discard(Throwable cause, boolean reportOnClose) {
        if (reportOnClose) attachment.failed(cause);
        close(cause);
    }

    private Handback handback;
    private FallbackReturn fallbackReturn;
    private Runnable settled;
    void onSettled(Runnable callback) {
        synchronized (lock) {
            if (!finished()) { settled = callback; return; }
        }
        callback.run();
    }

    /** Consume the reservation before dispatching the carrier, racing stop of an unused slot. */
    public boolean arrive() {
        synchronized (lock) {
            if (!transition(Event.ARRIVE)) return false;
            cancellation = null;
            return true;
        }
    }

    private volatile SessionHost ownerLoop;
    SessionHost ownerLoop() { return ownerLoop; }

    boolean finished() {
        synchronized (lock) { return state == State.FINISHED || state == State.DETACHED_DONE; }
    }

    AttachmentRelease cleanup() {
        synchronized (lock) { return finished() ? AttachmentRelease.inactive() : attachment.fork(); }
    }

    private long authorizedJoin = io.zmux.PipeId.UNASSIGNED.value();

    void authorizedJoin(long pipe) {
        synchronized (lock) { authorizedJoin = pipe; }
    }

    private IOException rejection(String message) {
        return authorizedJoin == io.zmux.PipeId.UNASSIGNED.value() ? new IOException(message)
                : new io.zmux.JoinRefusedException(io.zmux.PipeId.of(authorizedJoin));
    }

    boolean stall() {
        IOException refused;
        synchronized (lock) {
            if (!transition(Event.STALL)) return false;
            refused = rejection("carrier stalled");
        }
        result.completeExceptionally(refused);
        return true;
    }


    public CarrierSetup() { this(State.PENDING, null, null); }
    static CarrierSetup reservation() { return new CarrierSetup(State.RESERVED, null, null); }
    void reserveSlots(ListenerHub.AdmissionSlots.Lease guard) {
        synchronized (lock) {
            if (slots != null) throw new IllegalStateException("arrival already owns its slots");
            if (state == State.RESERVED || state == State.PENDING) {
                slots = guard;
                return;
            }
        }
        guard.close();
    }
    boolean canStall() {
        synchronized (lock) { return next(state, Event.STALL) != null; }
    }
    ListenerHub.AdmissionSlots.Lease slotKey() {
        synchronized (lock) { return slots; }
    }
    private CarrierSetup(State initial, CarrierSetup up, CarrierSetup down) {
        state = initial;
        attachment = new AttachmentRelease(this::retire);
        uplink = up;
        downlink = down;
    }

    void reserveToken(io.zmux.internal.core.CarrierAdmission.SplitPairing.Token<?> reservation, Object tableLock) {
        synchronized (lock) {
            if (token != null) throw new IllegalStateException("already paired");
            token = new Token(reservation, tableLock);
        }
    }

    void sealToken() {
        Token entry;
        synchronized (lock) { entry = token; }
        if (entry != null) entry.seal();
    }

    private ListenerHub.AdmissionSlots.Lease retiringSlots;

    /** The admission lease can outlive the setup result until the attachment is released. */
    public boolean holdsAdmissionSlotForTests() {
        synchronized (lock) { return slots != null || retiringSlots != null; }
    }

    public CompletableFuture<Void> advanceTimersForTests(long elapsedNanos) {
        return ownerLoop().advanceTimersForTests(elapsedNanos);
    }

    /** A response-head operation was interrupted. Its adapter may still be completing that write. */
    void retainSlotsUntilClosed() {
        synchronized (lock) {
            if (slots != null) { retiringSlots = slots; slots = null; }
        }
    }

    private void releaseRetiringSlots() {
        ListenerHub.AdmissionSlots.Lease lease;
        synchronized (lock) { lease = retiringSlots; retiringSlots = null; }
        if (lease != null) lease.close();
    }

    private void retire() {
        Token reservation;
        synchronized (lock) {
            reservation = token; token = null;
        }
        releaseRetiringSlots();
        if (reservation != null) reservation.retire();
    }

    /** Protocol phases borrow the carrier on its owner loop. Adoption and moves install the same close authority. */
    <T extends Transport> T own(Supplier<T> factory) {
        Holding input;
        AttachmentRelease guard;
        synchronized (lock) {
            if (finished()) return null;
            input = holding;
            guard = switch (input) {
                case Input raw -> raw.guard;
                case SocketInput raw -> raw.guard;
                case Empty ignored -> cleanup();
                case Owned ignored -> throw new IllegalStateException("carrier already owned");
                case Attached ignored -> throw new IllegalStateException("carrier already attached");
            };
            holding = Empty.INSTANCE;
        }
        T transport;
        try {
            transport = factory.get();
        } catch (RuntimeException failure) {
            if (input == Empty.INSTANCE) guard.close(); else dispose(input, failure);
            fail(failure);
            throw failure;
        }
        Bound bound = binding(transport);
        guard.track(transport);
        boolean adopted;
        synchronized (lock) {
            adopted = !finished();
            if (adopted) {
                bound.right.owner = this;
                ownerLoop = transport.loop();
                holding = new Owned(bound, guard);
            }
        }
        if (!adopted) { dispose(new Owned(bound, guard), null); return null; }
        if (!bound.right.live.get()) { transportClosed(transport); return null; }
        return transport;
    }

    static Bound binding(Transport view) { return ((PhysicalTransport) view).closeRight(); }

    static Transport moveUnowned(Transport carrier, SessionHost target) {
        if (carrier.setup() != NONE) throw new IllegalStateException("carrier already owned");
        return binding(carrier).move(target);
    }

    boolean adoptTracked(Transport t) {
        var binding = binding(t);
        synchronized (lock) {
            if (finished()) return false;
            ownerLoop = t.loop();
            binding.right.owner = this;
            var guard = switch (holding) {
                case Input input -> input.guard;
                case SocketInput input -> input.guard;
                case Owned owned -> owned.guard;
                case Empty ignored -> cleanup();
                case Attached ignored -> throw new IllegalStateException("carrier already attached");
            };
            holding = new Owned(binding, guard);
            return true;
        }
    }

    /** Converts a received half-pair under this close right, including native movement and sealing. */
    Transport lane(io.zmux.carrier.PipeIo input, SessionHost host, boolean keepHost) {
        receive(input);
        try {
            Transport carrier = own(() -> {
                Transport nativeCarrier = Transport.claim(input);
                return nativeCarrier == null ? new ExternalTransport(host, input) : nativeCarrier;
            });
            if (carrier == null) return null;
            if (!keepHost && carrier.loop() != host) carrier = move(host);
            if (carrier == null) return null;
            Transport sealed = Transport.seal(carrier, input);
            return adoptTracked(sealed) ? sealed : null;
        } catch (RuntimeException failed) {
            close(failed);
            return null;
        }
    }

    /** The matching pair consumes the two idle external halves on the uplink loop. */
    ExternalTransport combine() {
        AttachmentRelease converting;
        synchronized (lock) {
            converting = state == State.PENDING ? cleanup() : null;
        }
        if (converting == null) { refused(); return null; }
        var up = uplink.take();
        var down = downlink.take();
        try (converting) {
            if (up instanceof Owned(var in, var upGuard) && down instanceof Owned(var out, var downGuard)) {
                var combined = ((ExternalTransport) in.carrier).pairWith((ExternalTransport) out.carrier);
                if (combined != null) {
                    var owned = new Owned(binding(combined), converting.fork());
                    converting.track(combined);
                    upGuard.close();
                    downGuard.close();
                    synchronized (lock) {
                        if (state == State.PENDING) {
                            owned.binding.right.owner = this;
                            holding = owned;
                            return combined;
                        }
                    }
                    dispose(owned, null);
                    refused();
                    return null;
                }
            }
        } catch (RuntimeException failed) {
            fail(failed);
        }
        try { dispose(up, null); } finally { dispose(down, null); }
        refused();
        return null;
    }

    Transport move(SessionHost target) {
        Bound bound;
        AttachmentRelease moving;
        synchronized (lock) {
            if (!(holding instanceof Owned(var ownedBinding, var ownedGuard))) return null;
            bound = ownedBinding;
            moving = ownedGuard;
            holding = Empty.INSTANCE;
        }
        try {
            Transport moved = bound.move(target);
            if (moved == null) { moving.close(); refused(); return null; }
            moving.track(moved);
            var binding = binding(moved);
            synchronized (lock) {
                if (state == State.PENDING || state == State.MATCHED || state == State.MATCHED_WITHDRAWN) {
                    binding.right.owner = this;
                    ownerLoop = target;
                    holding = new Owned(binding, moving);
                    return moved;
                }
            }
            dispose(new Owned(binding, moving), null);
            refused();
            return null;
        } catch (RuntimeException failure) {
            try { dispose(new Owned(bound, moving), failure); } finally { fail(failure); }
            return null;
        }
    }

    /** No host or transport operation is performed under the state lock. */
    void close(Throwable cause) {
        close(take(), cause);
    }

    void endStall(boolean graceful) {
        Holding resource = take();
        try { if (graceful && resource instanceof Owned owned) owned.binding.carrier.shutdownOutput(); }
        finally { close(resource, null); }
    }

    void reject(Transport carrier, Throwable cause) {
        if (this == NONE) { binding(carrier).close(cause); return; }
        var right = binding(carrier).right;
        Holding resource;
        synchronized (lock) {
            if (!(holding instanceof Owned owned) || owned.binding.right != right) return;
            resource = take();
        }
        close(resource, cause);
    }

    private void close(Holding resource, Throwable cause) {
        try { dispose(resource, cause); }
        finally { if (cause == null) refused(); else fail(cause); }
    }

    private static void dispose(Holding resource, Throwable cause) {
        switch (resource) {
            case Empty ignored -> { }
            case Attached ignored -> { }
            case Owned(var bound, var guard) -> bound.carrier.loop().release(() -> {
                try { bound.close(cause); } finally { guard.close(); }
            });
            case SocketInput input -> {
                try { input.channel.close(); }
                catch (IOException ignored) { /* Already disconnected. */ }
                finally { input.guard.close(); }
            }
            case Input input -> Callbacks.EXECUTOR.execute(() -> {
                try { Bootstrap.discard(input.io, cause); }
                finally { input.guard.until(AdapterPorts.released(input.io)); }
            });
        }
    }

    /** Move close authority to a bounded final reply before reporting the refusal. */
    Drain drain() {
        synchronized (lock) {
            if (!(holding instanceof Owned(var bound, var guard)) || !claimHandoff()) return null;
            var drain = new Drain(bound, guard);
            holding = Empty.INSTANCE;
            return drain;
        }
    }

    static final class Drain implements AutoCloseable {
        final Bound binding;
        private final AttachmentRelease guard;
        private boolean closed;
        private Drain(Bound binding, AttachmentRelease guard) { this.binding = binding; this.guard = guard; }
        @Override public void close() { close(null); }
        void close(Throwable cause) {
            if (closed) return;
            closed = true;
            try { binding.close(cause); } finally { guard.untilClosed(binding.carrier); }
        }
    }

    /** A host may block or throw in cancel; no carrier owner or serial completion thread calls it. */
    private static CompletableFuture<Void> cancelAsync(CompletableFuture<?> stage, Throwable reason) {
        return CompletableFuture.runAsync(() -> {
            try { stage.cancel(false); }
            catch (RuntimeException failed) {
                stage.completeExceptionally(failed);
                if (reason != null && reason != failed) reason.addSuppressed(failed);
            }
        }, Callbacks.EXECUTOR);
    }

    void cancelStage(CompletableFuture<?> stage, Throwable reason) {
        if (stage != null && !stage.isDone()) track(cancelAsync(stage, reason));
    }

    /** One owner-loop deadline. The first of release, expiry and loop shutdown removes both subscriptions. */
    private static final class OpeningDeadline extends TimerHeap.Timer implements Runnable {
        private final CompletableFuture<Boolean> elapsed = new CompletableFuture<>();

        @Override void fire(long now) { elapsed.complete(true); }
        @Override public void run() { elapsed.complete(false); }

        static void watch(SessionHost loop, CompletableFuture<Void> released, java.util.function.Consumer<Boolean> expired) {
            long fixed = io.zmux.internal.core.Session.lingerForTests;
            long due = io.zmux.internal.core.Time.add(loop.timerNow(),
                    fixed > 0 ? fixed : io.zmux.internal.core.Session.MAX_LINGER);
            Runnable arm = () -> {
                if (released.isDone()) return;
                if (loop.stopping()) { expired.accept(false); return; }
                var timer = new OpeningDeadline();
                loop.onShutdown(timer);
                loop.schedule(timer, due);
                CompletableFuture.anyOf(released, timer.elapsed).thenAccept(result -> {
                    Runnable remove = () -> {
                        loop.schedule(timer, io.zmux.internal.core.Time.NEVER);
                        loop.removeShutdownHook(timer);
                    };
                    if (loop.inLoop()) remove.run(); else loop.tryExecute(remove);
                    if (result instanceof Boolean fired) expired.accept(fired);
                });
            };
            if (loop.inLoop()) arm.run(); else if (!loop.tryExecute(arm)) expired.accept(false);
        }
    }

    /** Opening the carrier and releasing its adapter's physical write are separate boundaries. */
    final class Opening {
        final CompletableFuture<Void> ready = new CompletableFuture<>();
        private final CompletableFuture<?> hostWrite;
        private final boolean acknowledged;

        Opening(io.zmux.adapter.Upgrade step, ListenerHub.Owner listener, boolean accepted) {
            SessionHost loop = ownerLoop();
            var released = new CompletableFuture<Void>();
            track(released);
            boolean observed;
            try {
                observed = host(() -> {
                    var signal = step.released();
                    if (signal == null) return false;
                    signal.whenComplete((v, e) -> released.complete(null));
                    return true;
                });
            } catch (RuntimeException failed) {
                released.complete(null);
                acknowledged = false;
                hostWrite = ready;
                ready.completeExceptionally(failed);
                return;
            }
            acknowledged = observed;
            hostWrite = stage(() -> java.util.Objects.requireNonNull(step.open(), "upgrade gave no stage"));
            if (acknowledged) {
                OpeningDeadline.watch(loop, released, elapsed -> {
                    if (elapsed) Callbacks.EXECUTOR.execute(() -> listener.log(System.Logger.Level.DEBUG,
                            "adapter opening write still awaits release after one linger", null));
                });
            } else {
                if (accepted) boundRelease(loop, released);
                // Expiry also unlinks this observer from an accepted stage that never finishes.
                CompletableFuture.anyOf(ready, released).whenComplete((v, error) -> {
                    if (!ready.isCancelled()) released.complete(null);
                    else if (!accepted) boundRelease(loop, released);
                });
            }
            // A host that ignores cancel retains only the relay after its carrier ends.
            var relay = ready;
            try {
                hostWrite.whenComplete((v, error) -> {
                    if (error == null) relay.complete(null); else relay.completeExceptionally(error);
                });
            } catch (RuntimeException failed) { relay.completeExceptionally(failed); }
        }

        private static void boundRelease(SessionHost loop, CompletableFuture<Void> released) {
            OpeningDeadline.watch(loop, released, elapsed -> released.complete(null));
        }

        void cancel(Throwable reason) {
            if (!ready.completeExceptionally(new java.util.concurrent.CancellationException("carrier opening withdrawn"))) return;
            if (acknowledged) {
                // The adapter can prove retirement. Wait for both that signal and the off-loop cancel call to finish.
                cancelStage(hostWrite, reason);
            } else {
                // A legacy cancel can itself block forever. Dispatch it independently of the bounded attachment share.
                cancelAsync(hostWrite, reason);
            }
        }
    }

    Opening openUpgrade(io.zmux.adapter.Upgrade step, ListenerHub.Owner listener) {
        return new Opening(step, listener, false);
    }

    /** A slow accepted upgrade may still fail the live carrier even after its unacknowledged share has expired. */
    boolean acceptedUpgrade(Transport carrier, io.zmux.adapter.Upgrade step, ListenerHub.Owner listener) {
        var opening = new Opening(step, listener, true);
        var upgrade = opening.ready;
        onResult(error -> { if (error != null) opening.cancel(error); });
        if (upgrade.isCompletedExceptionally()) {
            reject(carrier, null);
            return false;
        }
        // A never-ending host stage must not keep the retired carrier's failure handler.
        CompletableFuture.anyOf(upgrade, closed().handle((v, e) -> null)).whenComplete((v, e) -> {
            if (e != null) carrier.loop().tryExecute(() -> connectionLost(carrier, e));
        });
        return true;
    }

    static void connectionLost(Transport carrier, Throwable cause) { binding(carrier).close(cause); }

    /** I/O or a transport shutdown hook can close the current physical owner independently. */
    public void transportClosed(Transport transport) {
        var right = binding(transport).right;
        right.live.set(false);
        complete(Event.LOST, new IOException("physical carrier closed during setup"), right);
    }

    // The fork is handed to untilClosed/until, which own it and close it when the work ends.
    @SuppressWarnings("resource")
    void track(Transport carrier) { cleanup().untilClosed(carrier); }
    // The fork is handed to untilClosed/until, which own it and close it when the work ends.
    @SuppressWarnings("resource")
    void track(java.util.concurrent.CompletionStage<?> work) { cleanup().until(work); }

    AttachmentRelease attach(Transport carrier, AttachmentRelease parent) {
        AttachmentRelease guard;
        if (this == NONE) guard = new AttachmentRelease();
        else {
            var right = binding(carrier).right;
            synchronized (lock) {
                if (!(holding instanceof Owned(var bound, var share)) || bound.right != right) throw new IllegalStateException("carrier already moved");
                guard = share;
                holding = new Attached(right);
            }
        }
        if (parent != null) parent.track(guard.completion());
        return guard;
    }
    public CompletableFuture<Void> closed() { return attachment.completion(); }

    /** Borrows protocol I/O for use exclusively on the current owner loop. A move invalidates the view. */
    Transport borrow() {
        synchronized (lock) {
            if (holding instanceof Owned owned) return owned.binding.carrier;
            throw new IllegalStateException("carrier ownership ended");
        }
    }

    private boolean transition(Event event) {
        State to = next(state, event);
        if (to == null) return false;
        state = to;
        return true;
    }

    /** A publication may commit exactly once. Every other exit settles the attempt as refused. */
    public static final class Handoff implements AutoCloseable {
        private final CarrierSetup setup;
        private Handoff(CarrierSetup setup) { this.setup = setup; }
        public void commit() { setup.complete(Event.SUCCESS, null); }
        @Override public void close() { setup.refused(); }
    }

    public Handoff claim() { return claimHandoff() ? new Handoff(this) : null; }

    private boolean claimHandoff() {
        if (this == NONE) return true;
        synchronized (lock) {
            if (next(state, Event.CLAIM) == null) return false;
            if (uplink != null) {
                synchronized (uplink.lock) {
                    synchronized (downlink.lock) {
                        if (uplink.interrupted() && downlink.interrupted()) return false;
                        if (next(uplink.state, Event.PAIR_CLAIM) == null || next(downlink.state, Event.PAIR_CLAIM) == null) return false;
                        uplink.transition(Event.PAIR_CLAIM);
                        downlink.transition(Event.PAIR_CLAIM);
                    }
                }
            }
            return transition(Event.CLAIM);
        }
    }

    void phase(SessionHost loop, Runnable abort) {
        if (this == NONE) return;
        onCancel(new Phase(loop, abort, abort));
    }

    private final class Phase implements Runnable {
        private final SessionHost loop;
        private final Runnable abort;
        private final Runnable onLoss;
        Phase(SessionHost loop, Runnable abort, Runnable onLoss) {
            this.loop = loop;
            this.abort = abort;
            this.onLoss = onLoss;
        }
        @Override public void run() {
            loop.tryExecute(() -> {
                synchronized (lock) {
                    if (cancellation != this || !interrupted()) return;
                }
                abort.run();
            });
        }
        void lost(AttachmentRelease sessionTurn) {
            if (sessionTurn == null) loop.release(onLoss);
            else loop.enqueueRelease(() -> {
                try (sessionTurn) { onLoss.run(); }
            });
        }
    }

    void onCancel(Runnable dispatch) {
        boolean pending;
        synchronized (lock) {
            if (this == NONE || state == State.PUBLISHING || state == State.FINISHED) return;
            cancellation = dispatch;
            pending = interrupted();
        }
        if (pending) dispatch.run();
    }

    /** The host stage retains only this clearable link after withdrawal. */
    // A mutable cell cleared on withdrawal, not a value carrier: it wraps the setup itself.
    @SuppressWarnings("ClassCanBeRecord")
    private static final class FallbackReturn implements java.util.function.BiConsumer<Object, Throwable> {
        private final java.util.concurrent.atomic.AtomicReference<CarrierSetup> owner;
        FallbackReturn(CarrierSetup setup) { owner = new java.util.concurrent.atomic.AtomicReference<>(setup); }
        @Override public void accept(Object ignored, Throwable error) {
            CarrierSetup setup = owner.getAndSet(null);
            if (setup == null) return;
            if (error == null) setup.complete(Event.SUCCESS, null); else setup.fail(error);
        }
    }

    void fallback(Handback takeover, io.zmux.Admission.Fallback target, java.util.function.Consumer<Boolean> claimed, java.util.function.Consumer<RuntimeException> log) {
        var worker = cleanup();
        var cell = new FallbackReturn(this);
        synchronized (lock) {
            handback = takeover;
            fallbackReturn = cell;
        }
        var reported = new java.util.concurrent.atomic.AtomicBoolean();
        onResult(error -> {
            if (reported.compareAndSet(false, true)) claimed.accept(false);
        });
        // Both direct host callbacks and a request dispatched on another loop claim ownership
        // just before delivery. Withdrawal and handoff contend for the same setup transition.
        java.util.function.BooleanSupplier handoff = () -> {
            synchronized (lock) {
                if (!transition(Event.FALLBACK)) return false;
                reported.set(true);
            }
            releaseRetiringSlots();
            claimed.accept(true);
            return true;
        };
        onCancel(() -> cell.accept(null, new InterruptedIOException("fallback interrupted")));
        releaseSlots();
        Callbacks.EXECUTOR.execute(() -> {
            try (worker) {
                if (interrupted() || finished()) return;
                var stage = host(() -> {
                    if (target instanceof io.zmux.internal.spi.AdapterPorts.RequestFallback request) {
                        return request.dispatch().apply(takeover, handoff);
                    }
                    return handoff.getAsBoolean() ? target.serve(takeover) : null;
                });
                if (stage == null) cell.accept(null, null);
                else stage.whenComplete(cell);
            } catch (RuntimeException failure) {
                cell.accept(null, failure);
                log.accept(failure);
            }
        });
    }

    boolean detached() {
        synchronized (lock) {
            return state == State.DETACHED || state == State.DETACHED_WITHDRAWN || state == State.DETACHED_DONE;
        }
    }
    boolean reclaimed() {
        synchronized (lock) { return !detached() && (state == State.FINISHED || state == State.WITHDRAWN); }
    }
    boolean detach(boolean open) {
        synchronized (lock) {
            if (detached()) return true;
            if (reclaimed()) throw new IllegalStateException("Takeover.detach after the carrier was reclaimed");
            return open && transition(Event.DETACH);
        }
    }
    public boolean interrupted() {
        synchronized (lock) { return state == State.WITHDRAWN || state == State.MATCHED_WITHDRAWN || state == State.DETACHED_WITHDRAWN; }
    }

    @Override public void interrupt() { withdraw(Event.CANCEL); }
    void stop() { withdraw(Event.STOP); }
    private IOException withdrawal;

    /** Returns whether the setup is withdrawn, by this call or an earlier one; the first reason is kept for {@link #await}. */
    private boolean withdraw(Event event) {
        Runnable abort;
        synchronized (lock) {
            if (this == NONE) return false;
            if (interrupted()) return true;
            if (!transition(event)) return false;
            withdrawal = event == Event.STOP ? new io.zmux.ListenerClosedException() : new InterruptedIOException("carrier setup interrupted");
            abort = cancellation;
        }
        if (abort != null) abort.run();
        return true;
    }

    private IOException withdrawal() {
        synchronized (lock) { return withdrawal; }
    }

    private void complete(Event event, Throwable error) { complete(event, error, null); }

    private void complete(Event event, Throwable error, CloseRight lost) {
        Runnable done, abort;
        Handback reclaim;
        Throwable closeCause;
        Holding remaining;
        boolean transferred;
        AttachmentRelease sessionTurn;
        synchronized (lock) {
            if (lost != null) {
                boolean current = switch (holding) {
                    case Owned owned -> owned.binding.right == lost;
                    case Attached a -> a.right == lost;
                    case Empty ignored -> false;
                    case Input ignored -> false;
                    case SocketInput ignored -> false;
                };
                if (!current || handback != null) return;
            }
            boolean withdrawn = interrupted();
            // A request adapter can end its dispatch without ever claiming the handoff.
            if (event == Event.SUCCESS && state == State.PENDING && handback != null) event = Event.REJECT;
            if (this == NONE || !transition(event)) return;
            if (withdrawn) error = withdrawal;
            else if (event == Event.REJECT && error == null) error = rejection("carrier setup refused");
            reclaim = detached() ? null : handback;
            // A refusal ends the carrier gracefully, including a Join withdrawn while
            // awaiting Confirm. Its setup result still reports the winning withdrawal.
            closeCause = event == Event.FAIL || (withdrawn && event != Event.REJECT) ? error : null;
            handback = null;
            if (fallbackReturn != null) fallbackReturn.owner.set(null);
            fallbackReturn = null;
            abort = lost == null ? null : cancellation;
            // A candidate session already drives the transport. Its close can arrive from
            // inside drive(), whose action list must finish before a second drive can start.
            sessionTurn = holding instanceof Attached && abort instanceof Phase ? attachment.fork() : null;
            cancellation = null;
            transferred = event == Event.SUCCESS || detached() || reclaim != null;
            remaining = holding;
            holding = Empty.INSTANCE;
            done = settled;
            settled = null;
        }
        try {
            if (abort instanceof Phase phase) phase.lost(sessionTurn);
            else if (abort != null) abort.run();
            if (reclaim != null) reclaim.done(closeCause);
            if (transferred && remaining instanceof Owned owned) owned.guard.close();
            else dispose(remaining, closeCause);
        } catch (RuntimeException cleanupError) {
            attachment.failed(cleanupError);
        } finally {
            if (done != null) done.run();
            releaseSlots();
            if (error == null) result.complete(null); else result.completeExceptionally(error);
            attachment.close();
        }
    }

    void releaseSlots() {
        ListenerHub.AdmissionSlots.Lease held;
        synchronized (lock) { held = slots; slots = null; }
        if (held != null) held.close();
    }

    public void fail(Throwable error) { complete(Event.FAIL, java.util.Objects.requireNonNull(error)); }
    @Override public void onFinished(Runnable callback) { result.whenComplete((v, e) -> callback.run()); }
    public void refused() { refused(null); }
    void refused(Throwable reason) { complete(Event.REJECT, reason); }
    void onResult(java.util.function.Consumer<Throwable> callback) { result.whenComplete((v, e) -> callback.accept(e)); }

    /** A setup callback has one exception boundary, before any new resource or publication can escape. */
    <T> T host(Supplier<T> callback) {
        assert !Thread.holdsLock(lock);
        try { return callback.get(); }
        catch (RuntimeException failed) { fail(failed); throw failed; }
    }

    /** Preserve cancellation of the original host stage and route synchronous failures through host(). */
    <T> CompletableFuture<T> stage(Supplier<java.util.concurrent.CompletionStage<T>> callback) {
        try {
            return host(() -> {
                var stage = callback.get();
                return stage == null ? CompletableFuture.completedFuture(null) : stage.toCompletableFuture();
            });
        } catch (RuntimeException failed) {
            return CompletableFuture.failedFuture(failed);
        }
    }

    static CarrierSetup pair(CarrierSetup up, CarrierSetup down, SessionHost loop) {
        CarrierSetup combined = new CarrierSetup(State.PENDING, up, down);
        Runnable cancel = () -> { if (up.interrupted() && down.interrupted()) combined.interrupt(); };
        AttachmentRelease upShare, downShare;
        Bound upCarrier, downCarrier;
        synchronized (combined.lock) {
            synchronized (up.lock) {
                synchronized (down.lock) {
                    if (next(up.state, Event.MATCH) == null || next(down.state, Event.MATCH) == null) return null;
                    if (!(up.holding instanceof Owned incoming) || !(down.holding instanceof Owned outgoing)) return null;
                    upCarrier = incoming.binding;
                    downCarrier = outgoing.binding;
                    upShare = up.cleanup();
                    downShare = down.cleanup();
                    up.transition(Event.MATCH);
                    down.transition(Event.MATCH);
                    up.cancellation = up.new Phase(loop, cancel, combined::stop);
                    down.cancellation = down.new Phase(loop, cancel, combined::stop);
                    combined.slots = up.slots;
                    up.slots = null;
                    combined.token = up.token != null ? up.token : down.token;
                    up.token = down.token = null;
                }
            }
        }
        upShare.until(combined.closed());
        downShare.until(combined.closed());
        combined.track(upCarrier.carrier);
        combined.track(downCarrier.carrier);
        combined.result.whenComplete((v, e) -> {
            Event fate = e == null ? Event.SUCCESS : Event.FAIL;
            up.complete(fate, e);
            down.complete(fate, e);
        });
        return combined;
    }

    public void await() throws IOException {
        boolean restoreInterrupt = false;
        try {
            while (true) {
                try { result.get(); return; }
                catch (InterruptedException e) {
                    restoreInterrupt = true;
                    if (withdraw(Event.CANCEL)) throw withdrawal();
                    // Publication already owns the result, including a possible refusal.
                }
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException io) throw io;
            throw new IOException("carrier setup failed", e.getCause());
        } finally {
            if (restoreInterrupt) Thread.currentThread().interrupt();
        }
    }
}
