package io.zmux;

import java.util.TreeMap;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import io.zmux.SpliceController.Rejection;
import io.zmux.SpliceDirection.Drop;
import io.zmux.internal.core.Session.DatagramAdmission;
import io.zmux.internal.core.Session.DatagramMode;
import io.zmux.internal.core.Session.DatagramOptions;
import io.zmux.SpliceUnit.Origin;
import io.zmux.SpliceUnit.Shape;
import io.zmux.helper.Limiter;
import io.zmux.helper.Permit;
import io.zmux.internal.buffer.CarrierCharge;
import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.Datagrams;
import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.core.StreamRecv;
import io.zmux.internal.core.Time;
import io.zmux.internal.loop.Operation;
import io.zmux.internal.loop.RecvLane;
import io.zmux.internal.loop.SendLane;
import io.zmux.internal.loop.SerialCallbacks;
import io.zmux.internal.wire.Capsule;
import io.zmux.internal.wire.TransportError;
import io.zmux.internal.spi.Spi;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * Owns relay work, never protocol state. All core access goes through the existing owner.
 *
 * <p>A splice runs no thread of its own. Each direction is a chain of asynchronous lane and loop
 * operations of the two sessions; their completions, the limiter's, and the observers of either
 * session or stream only record what happened and {@link #wake} the splice. Waking runs
 * {@link #drain} on the splice's serial context, which advances both directions until each waits
 * again: every field below that is not atomic belongs to that context alone, and no lock is held
 * while anything is awaited. A splice spans two sessions (two loops): it never runs on either loop.
 */
final class Splice {
    private final ZmuxStream a, b;
    private final SpliceOptions options;
    /** A packet relay (EXTENSIONS.md Section 12): the stream bytes are capsules, and datagrams may become capsules. */
    private final boolean packets, controlled;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    /** The code of an abort through the splice's {@link SpliceAbort}; -1 for none (a cancelled stage resets with ZMUX_CANCELLED and stops with 0). */
    private volatile long cancelCode = -1;
    /**
     * Set once the caller's future cancels the splice (its own cancellation, or a completion from outside): that
     * caller completes the future right after, so the splice, which ends on its own thread meanwhile, must not.
     */
    private volatile boolean futureDecides;
    /** The caller's future: cancelling it, or completing it from outside, cancels the splice. */
    private final ZmuxFuture<SpliceResult> result = new ZmuxFuture<>(() -> { futureDecides = true; cancel(); return true; });
    private final Direction forward, reverse;

    /** Where {@link #drain} runs: one at a time, on a callback thread taken only while there is work. */
    private final SerialCallbacks context = new SerialCallbacks(this);
    /** Wakes not yet drained; whoever raises it from 0 schedules the drain. */
    private final AtomicInteger wip = new AtomicInteger();
    private final Runnable drainTask = this::drain;
    /** Every awaited operation's listener: preallocated, so a forwarded chunk allocates none. */
    private final Runnable wakeTask = this::wake;
    private final BiConsumer<Object, Throwable> wakeOnComplete = (v, e) -> wake();

    // Context only.
    private boolean begun, dataDone, cleaning;
    /** Why the first direction to fail tore both streams down; the other direction's local-close faults follow from it. */
    private IOException teardown;
    /** The stream whose side was lost in the failure behind {@link #teardown}; null when the cause names no side. */
    private ZmuxStream teardownLost;

    private Splice(ZmuxStream a, ZmuxStream b, SpliceOptions options, boolean packets, boolean controlled) {
        this.controlled = controlled;
        this.a = a; this.b = b; this.options = options; this.packets = packets;
        forward = new Direction(a, b, options.aToB().orElse(null), options.packetsAToB(),
                SpliceUnit.Direction.A_TO_B, options.hooksAToB());
        reverse = new Direction(b, a, options.bToA().orElse(null), options.packetsBToA(),
                SpliceUnit.Direction.B_TO_A, options.hooksBToA());
    }

    static ZmuxFuture<SpliceResult> start(ZmuxStream a, ZmuxStream b, SpliceOptions options, boolean packets) {
        return start(a, b, options, packets, options.abort().orElse(null), false).result;
    }

    static SpliceController controlled(ZmuxStream a, ZmuxStream b, SpliceOptions options, boolean packets) {
        SpliceAbort abort = options.abort().orElseGet(SpliceAbort::new);
        Splice relay = start(a, b, options, packets, abort, true);
        return new SpliceController(relay.forward.policy, relay.reverse.policy, abort, relay.result);
    }

    private static Splice start(ZmuxStream a, ZmuxStream b, SpliceOptions options, boolean packets, SpliceAbort abort, boolean controlled) {
        Objects.requireNonNull(a); Objects.requireNonNull(b); Objects.requireNonNull(options);
        options.hooksAToB().validate(packets, options.limits().maxHookSteps());
        options.hooksBToA().validate(packets, options.limits().maxHookSteps());
        options.limits().validate(options.chunk(), options.maxPacketPayload(), options.maxOpaqueValue(), packets);
        options.aToB().ifPresent(limiter -> validateBurst(limiter, options.limits()));
        options.bToA().ifPresent(limiter -> validateBurst(limiter, options.limits()));
        if (a.session.driver == b.session.driver && a.st == b.st) throw new IllegalArgumentException("cannot splice a stream into itself");
        a.claimIo();
        try { b.claimIo(); } catch (RuntimeException error) { a.unclaimIo(); throw error; }
        try {
            Splice relay = new Splice(a, b, options, packets, controlled);
            if (abort != null) {
                abort.attach(relay);
                relay.result.whenComplete((r, e) -> abort.detach(relay));
            }
            relay.wake();
            return relay;
        } catch (RuntimeException | Error failure) { a.unclaimIo(); b.unclaimIo(); throw failure; }
    }

    private static void validateBurst(Limiter limiter, SpliceLimits limits) {
        if (limiter.maxBurstBytes() < limits.maxGroupBytes())
            throw new IllegalArgumentException("direction limiter must declare sufficient maximum burst");
    }

    private void cancel() {
        cancelled.set(true); wake();
    }

    /** Aborts with code through the splice's {@link SpliceAbort}; false when already cancelled or ended. */
    boolean cancelWith(long code) {
        if (cancelled.get() || result.isDone()) return false;
        cancelCode = code;
        cancel();
        return true;
    }

    /** Something happened: drains on the context unless a drain is already due; any thread. */
    private void wake() {
        if (wip.getAndIncrement() == 0) context.execute(drainTask);
    }

    /**
     * Queues action on the stream's loop and completes true once it ran there or the session
     * ended (a closing loop may not have run its cleanup hook yet; its end releases the stream),
     * or false after a close's bound (one second): a loop stuck that long must not hold a splice.
     * The action stays queued and runs if the loop resumes. Never blocks.
     */
    static CompletableFuture<Boolean> boundedCleanup(ZmuxStream stream, Runnable action) {
        var done = new CompletableFuture<Void>();
        CompletableFuture<?> ran = stream.session.driver.executeCarrier(() -> {
            try { action.run(); done.complete(null); } catch (Throwable error) { done.completeExceptionally(error); }
        }) ? done : stream.session.driver.closedFuture();
        return ran.handle((v, e) -> true).completeOnTimeout(false, io.zmux.internal.core.Session.closeLimit, TimeUnit.NANOSECONDS);
    }

    private void drain() {
        int missed = 1;
        while (true) {
            try { advance(); }
            catch (RuntimeException | Error error) { fatal(error); }
            missed = wip.addAndGet(-missed);
            if (missed == 0) return;
        }
    }

    /** Advances both directions until each waits; once both streams and datagram paths ended, releases the streams. */
    private void advance() {
        if (!begun) { begun = true; forward.begin(); reverse.begin(); }
        boolean more;
        do {
            boolean f = forward.pump(), r = reverse.pump(); // both, every time
            more = f || r;
            if (!dataDone && forward.streamDone() && reverse.streamDone()) {
                dataDone = true;
                forward.policy.close();
                reverse.policy.close();
                more = true;
            }
            if (dataDone && !cleaning && forward.datagramsDone() && reverse.datagramsDone()) { cleaning = true; release(); }
        } while (more);
    }

    /** A relay bug: the splice fails with it, and both streams are torn down and released. */
    private void fatal(Throwable error) {
        if (!futureDecides) result.publishFailure(error);
        if (cleaning) return;
        cleaning = true; dataDone = true;
        try { forward.abandon(); reverse.abandon(); }
        finally { abortBoth(a, b, io.zmux.carrier.PipeIo.CARRIER_FAILED); release(); }
    }

    /**
     * Both streams' releases run at once, under one bound: the result follows them, or the bound
     * when a loop is stuck; a release still queued then runs if that loop resumes.
     */
    private void release() {
        a.close(); b.close();
        CompletableFuture<Boolean> ca = boundedCleanup(a, () -> releaseOnLoop(a)), cb = boundedCleanup(b, () -> releaseOnLoop(b));
        ca.thenCombine(cb, (x, y) -> x && y).whenComplete((cleaned, e) -> context.execute(() -> publish(Boolean.TRUE.equals(cleaned))));
    }

    private static void releaseOnLoop(ZmuxStream s) {
        s.session.driver.lanes.watchRecvEnd(s.st, null); s.session.driver.lanes.watchSendEnd(s.st, null); s.session.driver.lanes.releaseStream(s.st);
    }

    private void publish(boolean cleaned) {
        if (!cleaned) {
            if (!futureDecides) result.publishFailure(new java.net.SocketTimeoutException("splice cleanup timed out: a stream's event loop did not answer within the close bound; its release stays queued"));
            return;
        }
        if (futureDecides) return;
        IOException failed = forward.callbackFailure != null ? forward.callbackFailure : reverse.callbackFailure;
        if (failed != null) result.publishFailure(failed);
        else result.publish(new SpliceResult(forward.result(), reverse.result()));
    }

    /** An awaited operation that was cancelled: no outcome. */
    private static final Object CANCELLED = new Object();

    /** The outcome of a decided operation, {@link #CANCELLED} when its cancellation won. */
    private static Object outcome(Operation<?> op) {
        return op.isCancelled() ? CANCELLED : op.outcome();
    }

    /** The failure an outcome other than a value stands for. */
    private static IOException failure(Object r) {
        return r == CANCELLED ? new InterruptedIOException("splice cancelled") : Errors.outcome(r, 0);
    }

    /** Submits a one-attempt operation on the stream's loop. */
    private static Operation<Object> onLoop(ZmuxStream s, Operation.Step step) {
        return new Operation<>(s.session.driver, step, null).submit();
    }

    /** Whether an outcome is the success a lane or loop operation reports. */
    private static boolean ok(Object r) {
        return Boolean.TRUE.equals(r);
    }

    private static void unref(StreamRecv.Chunk c) {
        if (c.buf() != null) c.buf().unref();
    }

    enum Disposition { SUBMITTED, DROPPED, DIRECTION_ENDED }

    private enum Read { IDLE, READY, GRANT, CHUNK }
    private enum End { OPEN, FINISH, RESET_QUERY, DRAIN, RESET_SEND, DONE }
    private enum Write { DIRECT, HEADER, BODY, FALLBACK_FIRST, GROUP_HEADER, GROUP_BODY }
    private static final int ROUNDS = 16;

    /** One owner sequences reliable admissions and writes. The datagram path progresses separately. */
    private record Received(Datagrams.OwnedDatagram data, SplicePolicy.QueueCharge charge) {}

    private record HookWork(SpliceGroup group, List<SpliceHook> hooks, Operation<?> resolving) {}

    private record DatagramQuote(DatagramOptions options, DatagramAdmission admission) {}

    private final class Direction {
        // Only the head can run. Its callback owns the group until it posts its return.
        private final ArrayDeque<HookWork> reliableHooks = new ArrayDeque<>();
        private final ArrayDeque<HookWork> datagramHooks = new ArrayDeque<>();

        private final ZmuxStream source, target;
        private final Limiter limiter;
        private final DatagramSendOptions datagramOptions;
        private final SpliceUnit.Direction direction;
        private final SplicePolicy policy;
        private final SpliceLimits limits = options.limits();
        private final SpliceClock clock;
        private final boolean directBytes, directRead;
        private final SpliceMirror mirror;
        private final SpliceSettlement settlement = new SpliceSettlement();
        private final boolean trackSettlement;
        private final java.util.EnumMap<Drop, Long> dropCounts = new java.util.EnumMap<>(Drop.class);
        private final Measures measures = new Measures();
        private final Capsule.Cursor cursor = packets
                ? new Capsule.Cursor(options.maxPacketPayload(), options.maxOpaqueValue(), true) : null;
        private final TreeMap<Long, SpliceGroup> reliable = new TreeMap<>();
        private long fifoOrder, inputOrder;
        private final ArrayList<SpliceGroup> datagrams = new ArrayList<>();
        private final AtomicReference<IOException> readFault = new AtomicReference<>(), writeFault = new AtomicReference<>();
        private final AtomicBoolean resetPoke = new AtomicBoolean();
        private final Operation.Step receiveStep;
        private final Operation.Withdraw receiveWithdraw;
        private final String sourceTag, targetTag;

        // The splice context owns everything below.
        private Runnable closeRead, closeWrite;
        private SplicePolicy.Configuration settings;
        private Read readStage = Read.IDLE;
        private End endStage = End.OPEN;
        private Write writeKind;
        private Operation<?> read, write, terminal, dgReceive, dgRoute, capacity;
        private StreamRecv.Chunk packetPending;
        private final SpliceGrants grants;
        private SpliceGrants.Request grant;
        private Permit inputPermit, memberPermit, writePermit;
        private CarrierCharge memberCharge, writeCharge;
        private long memberChargeOffset;
        private StreamRecv.Chunk direct, batch;
        private SplicePolicy.Configuration batchSettings;
        private long directEnd;
        private SpliceGroup active, dgActive, dgRouting;
        private ZmuxFuture<DatagramAdmission> dgSend;
        private SplicePayload collected;
        private ByteBuffer compacted, readCopy;
        private int readingSize;
        private SplicePayload compactedPayload;
        private List<SpliceHook> inputHooks = List.of();
        private long inputAt, inputStart, inputGeneration, inputSequence, sequence;
        private long resetCode, protectedBytes, targetBytes, bytes, datagramBytes, asCapsules;
        private long timerAt = Time.NEVER;
        private Runnable stopTimer;
        private boolean fin, rawHeader, streaming, writingAtTarget, dgClosed, dgInputClosed, sourcePosition, discardingCapsule;
        private long datagramLength;
        private SpliceEnd end;
        private IOException callbackFailure;

        Direction(ZmuxStream source, ZmuxStream target, Limiter limiter, DatagramSendOptions datagramOptions,
                  SpliceUnit.Direction direction, SpliceHooks hooks) {
            this.source = source;
            this.target = target;
            clock = options.clock() == null ? SpliceClock.on(source.session.driver) : options.clock();
            this.limiter = limiter;
            grants = limiter == null ? null : new SpliceGrants(limiter, limits, context, wakeTask,
                    settlement::unused);
            this.datagramOptions = datagramOptions;
            trackSettlement = controlled || limiter != null || !hooks.empty();
            this.direction = direction;
            mirror = (direction == SpliceUnit.Direction.A_TO_B ? options.mirrorAToB() : options.mirrorBToA()).orElse(null);
            directRead = !controlled && hooks.empty() && mirror == null && (!packets || target.session.info().peerResetStreamAt());
            directBytes = directRead && !packets && limiter == null;
            sourceTag = direction == SpliceUnit.Direction.A_TO_B ? options.aTag() : options.bTag();
            targetTag = direction == SpliceUnit.Direction.A_TO_B ? options.bTag() : options.aTag();
            policy = new SplicePolicy(packets, options, hooks, controlled, () -> context.execute(this::commandsReady), settlement, direction,
                    this::snapshot);
            settings = policy.configuration();
            receiveStep = o -> {
                if (cancelled.get()) throw CoreException.of(CoreException.Reason.INVALID);
                Datagrams queue = source.session.core.datagrams();
                if (controlled || !hooks.datagram().isEmpty() || mirror != null) {
                    class Transfer implements Predicate<ReadBuf> {
                        SplicePolicy.QueueCharge charge;
                        @Override public boolean test(ReadBuf root) {
                            charge = policy.reserveReceive(root);
                            return charge != null;
                        }
                    }
                    var transfer = new Transfer();
                    try {
                        var data = queue.transfer(source.st, source.session.core.receiving().loans(), transfer);
                        if (data != null) return new Received(data, transfer.charge);
                    } catch (CoreException declined) {
                        if (declined.reason() != CoreException.Reason.CHUNK_BUDGET) throw declined;
                        // The head is still owned by receive. Copy-read can consume it independently.
                        return queue.receive(source.st);
                    }
                } else {
                    var data = queue.receive(source.st);
                    if (data != null) return data;
                }
                if (source.session.core.error() != null) throw CoreException.session(source.session.core.error());
                source.session.core.datagrams().waitReceive(source.st, o);
                return Operation.PENDING;
            };
            receiveWithdraw = o -> source.session.core.datagrams().cancelReceiveWait(source.st, o);
        }

        void begin() {
            closeRead = source.session.driver.watchClosed(error -> sourceFailed(new SessionClosedException(Errors.session(error))));
            closeWrite = target.session.driver.watchClosed(error -> targetFailed(new SessionClosedException(Errors.session(error))));
            source.session.driver.post(() -> source.session.driver.lanes.watchRecvEnd(source.st, this::sourceChanged));
            target.session.driver.post(() -> target.session.driver.lanes.watchSendEnd(target.st, e -> targetFailed(ended(e))));
            dgInputClosed = !source.session.info().datagramFlows() || !source.session.driver.info().localDatagrams();
        }

        private void sourceChanged(CoreException error) {
            if (error == null) {
                if (source.in.reliableCut() != Long.MAX_VALUE) resetPoke.set(true);
                wake();
                return;
            }
            IOException cause = ended(error);
            if (cause instanceof StreamResetException) { resetPoke.set(true); wake(); }
            else sourceFailed(cause);
        }

        private void sourceFailed(IOException error) { if (readFault.compareAndSet(null, error)) wake(); }
        private void targetFailed(IOException error) { if (writeFault.compareAndSet(null, error)) wake(); }

        private static IOException ended(CoreException error) {
            try { return Errors.io(error); }
            catch (RuntimeException misuse) { return new IOException("splice side ended: " + error.reason(), misuse); }
        }

        private void listen(Operation<?> op) { if (!op.onDone(wakeTask)) wake(); }

        boolean streamDone() { return endStage == End.DONE; }
        boolean datagramsDone() { return dgClosed && dgSend == null && dgReceive == null && datagrams.isEmpty() && dgActive == null && dgRoute == null; }

        boolean pump() {
            boolean progress = false;
            for (int round = 0; round < ROUNDS; round++) {
                boolean moved;
                try {
                    moved = advanceDirection();
                } catch (SplicePayload.BudgetExceeded failure) {
                    terminate(new SpliceEnd.HookFailed(java.util.Optional.of(failure)), TransportError.CANCELLED, false);
                    moved = true;
                } catch (LimiterFailed failure) {
                    measures.cause = failure.getCause();
                    terminate(new SpliceEnd.LimiterFailed(), TransportError.CANCELLED, false);
                    moved = true;
                } catch (IOException failure) {
                    settle(failure);
                    moved = true;
                }
                if (!directBytes) setTimer();
                if (!moved) return progress;
                progress = true;
            }
            return true;
        }

        private boolean advanceDirection() throws IOException {
            if (endStage != End.DONE) {
                if ((endStage == End.RESET_SEND || endStage == End.FINISH) && terminal.isDone()) return pumpEnd();
                checkFaults();
            }
            boolean moved = false;
            if (endStage == End.OPEN) {
                if (resetPoke.getAndSet(false)) moved |= observeReset();
                if (directBytes) moved |= pumpDirectBytes();
                else {
                    if (!reliable.isEmpty() && expireReliable()) return true;
                    moved |= pumpWriter();
                    moved |= pumpReader();
                }
                if (fin && !hasReliable() && read == null && endStage == End.OPEN) {
                    terminal = target.out.finishAsync(Long.MAX_VALUE);
                    listen(terminal);
                    endStage = End.FINISH;
                    moved = true;
                }
            } else if (endStage != End.DONE) moved |= pumpEnd();
            if (controlled) {
                boolean atBoundary = write == null && (!packets || targetBytes == protectedBytes);
                policy.confirmPause(atBoundary && dgSend == null);
            }
            moved |= pumpDatagrams();
            return moved;
        }

        private void commandsReady() {
            try {
                do {
                    if (endStage != End.DONE) checkFaults();
                } while (pumpControl());
            } catch (IOException fault) { settle(fault); }
            catch (SplicePayload.BudgetExceeded failure) {
                terminate(new SpliceEnd.HookFailed(Optional.of(failure)), TransportError.CANCELLED, false);
            }
            wake();
        }

        private boolean pumpControl() {
            SplicePolicy.Command command = policy.poll();
            if (command == null) return false;
            if (command instanceof SplicePolicy.Inject injection) admitInjection(injection);
            else {
                policy.apply(command);
                settings = policy.configuration();
                if (command instanceof SplicePolicy.Pause) {
                    if (dgReceive != null) {
                        dgReceive.cancel();
                        Object received = outcome(dgReceive);
                        dgReceive = null;
                        admitReceivedDatagram(received);
                    }
                    if (readStage == Read.GRANT) {
                        grant.revoke();
                        grant = null;
                        readStage = Read.IDLE;
                    }
                    if (outputGrant != null) { outputGrant.revoke(); outputGrant = null; }
                }
            }
            return true;
        }

        private void admitInjection(SplicePolicy.Inject injection) {
            if (fin || source.in.finKnown() || source.in.reliableCut() != Long.MAX_VALUE || endStage != End.OPEN) {
                policy.rejected(injection, Rejection.CLOSED);
            } else if (settings.paused()) {
                policy.rejected(injection, Rejection.PAUSED);
            } else {
                Shape shape = injection.shape;
                SpliceGroup group = group(shape, injection.type, injection.context, Origin.INJECTED,
                        ++sequence, settings.generation(), -1, -1, clock.now(),
                        SplicePayload.of(new SplicePayload.Budget(limits), injection.take()), injection, null);
                long cut = source.in.reliableCut();
                if (source.in.finKnown() || cut != Long.MAX_VALUE) {
                    group.close();
                    policy.rejected(injection, Rejection.CLOSED);
                    return;
                }
                measures.payloadCopyBytes += group.output.getFirst().length();
                policy.accepted(group);
                transform(group, settings.hooks().forShape(shape));
            }
        }

        private void checkFaults() throws IOException {
            if (cancelled.get()) throw new InterruptedIOException("splice cancelled");
            IOException fault = writeFault.get();
            if (fault == null && !(end instanceof SpliceEnd.PacketError)) fault = readFault.get();
            if (fault != null) throw fault;
        }

        private boolean observeReset() {
            long cut = source.in.reliableCut();
            reliable.values().removeIf(group -> {
                if (group.sourceEnd >= 0 && group.sourceEnd <= cut) return false;
                failReliable(group);
                return true;
            });
            if (active != null && ((active.sourceEnd < 0 && !active.memberStarted()) || active.sourceEnd > cut)) {
                releaseWrite();
                failReliable(active);
                active = null;
            }
            if (packets && sourcePosition && (inputStart >= cut || (cursor.headerReady()
                    && parsedOffset() + cursor.remaining() > cut))) {
                discardCapsuleTail();
            } else if (write != null && writeKind == Write.DIRECT && directEnd > cut) {
                releaseWrite();
            }
            if (readStage == Read.GRANT) {
                grant.revoke();
                grant = null;
                readStage = Read.IDLE;
                return true;
            }
            return false;
        }

        private boolean hasReliable() {
            return !reliableHooks.isEmpty() || direct != null || batch != null || sourcePosition || write != null || packetPending != null || active != null || !reliable.isEmpty();
        }

        /** The unmetered static byte path has one read or one write in flight. */
        private boolean pumpDirectBytes() throws IOException {
            if (write != null) {
                if (!write.isDone()) return false;
                finishWrite(false);
                return true;
            }
            if (fin) return false;
            if (read == null) {
                startRead(options.chunk());
                return true;
            }
            if (!read.isDone()) return false;
            Object value = takeRead();
            if (value == Operation.PENDING) return true;
            if (value == StreamRecv.FIN) fin = true;
            else if (value instanceof StreamRecv.Chunk chunk) {
                directEnd = source.in.readOffset();
                long suffix = directEnd - source.in.reliableCut();
                if (suffix >= chunk.data().remaining()) unref(chunk);
                else {
                    if (suffix > 0) chunk.data().limit(chunk.data().limit() - (int) suffix);
                    startWrite(chunk, Write.DIRECT);
                }
            } else throw failure(value);
            return true;
        }

        private Object takeRead() {
            Object result = outcome(read);
            read = null;
            readStage = Read.IDLE;
            if (result instanceof CoreException e && e.reason() == CoreException.Reason.CHUNK_BUDGET) {
                // A declined loan has not advanced the lane. Copy directly into a bounded buffer.
                if (collected != null) collected.budget().requireRoom(readingSize);
                readCopy = ByteBuffer.allocate(readingSize);
                read = source.in.readAsync(readCopy, readingSize, Long.MAX_VALUE);
                listen(read);
                readStage = Read.CHUNK;
                return Operation.PENDING;
            }
            if (readCopy != null) {
                if (result instanceof Integer count) {
                    if (count < 0) result = StreamRecv.FIN;
                    else {
                        measures.payloadCopyBytes += count;
                        ByteBuffer data = readCopy.flip();
                        result = new StreamRecv.Chunk(data, new ReadBuf(data, null), false);
                    }
                }
                readCopy = null;
            }
            return result;
        }

        private boolean pumpReader() throws IOException {
            if (fin || endStage != End.OPEN || !reliableHooks.isEmpty()) return false;
            if (packets && sourcePosition && !streaming && (!inputHooks.isEmpty() || mirror != null || !target.session.info().peerResetStreamAt())
                    && clock.now() >= assemblyDeadline()) {
                terminate(new SpliceEnd.WaitTimeout(), TransportError.CANCELLED, false);
                return true;
            }
            if (readStage == Read.CHUNK) {
                if (!read.isDone()) return false;
                Object result = takeRead();
                if (result == Operation.PENDING) return true;
                if (result == StreamRecv.FIN) {
                    closeInputPermit();
                    if (sourcePosition && packets) packetError();
                    else {
                        sourcePosition = false;
                        fin = true;
                    }
                } else if (result instanceof StreamRecv.Chunk chunk) {
                    if (discardingCapsule) unref(chunk);
                    else if (packets) {
                        batch = chunk;
                        batchSettings = controlled ? readBoundary() : settings;
                    } else {
                        var observed = controlled ? readBoundary() : settings;
                        if (!observed.hooks().stream().isEmpty() || mirror != null)
                            beginInput(observed, source.in.readOffset() - chunk.data().remaining());
                        receiveBytes(chunk);
                    }
                } else {
                    closeInputPermit();
                    throw failure(result);
                }
                return true;
            }
            if (direct != null || write != null || packetPending != null || active != null) return false;
            boolean paused = settings.paused() && !streaming && !discardingCapsule;
            if (readStage == Read.GRANT) {
                if (paused || !grant.done()) return false;
                var request = grant;
                grant = null;
                inputPermit = granted(request);
                startRead((int) inputPermit.size());
                return true;
            }
            if (rawHeader || (!sourcePosition && !reliable.isEmpty())) return false;
            if (batch != null) {
                if (paused) return false;
                consumeBatch();
                return true;
            }
            if (readStage == Read.IDLE) {
                if (paused && source.in.readableNow() > 0) return false;
                if (grants != null && !grants.reliableAvailable() && !packets && settings.hooks().stream().isEmpty()
                        && mirror == null) return false;
                read = source.in.awaitReadableAsync(Long.MAX_VALUE);
                listen(read);
                readStage = Read.READY;
                return true;
            }
            if (!read.isDone()) return false;
            Object result = outcome(read);
            if (!ok(result)) throw failure(result);
            long available = RecvLane.readable(read);
            if (paused && available > 0) {
                read = null;
                readStage = Read.IDLE;
                return true;
            }
            read = null;
            readStage = Read.IDLE;
            if (available <= 0) { startRead(1); return true; }
            int count = (int) Math.min(options.chunk(), available);
            if (discardingCapsule) { startRead(count); return true; }
            if (!packets && limiter != null && settings.hooks().stream().isEmpty() && mirror == null) {
                grant = grants.request(1, count, false);
                if (grant == null) return false;
                readStage = Read.GRANT;
            } else startRead(count);
            return true;
        }

        private void startRead(int count) {
            readingSize = count;
            if (directRead) {
                read = source.in.readAsync(null, count, Long.MAX_VALUE);
            } else {
                // The group cannot change while its next source read is pending.
                var budget = collected == null ? null : collected.budget();
                read = source.in.readAsync(null, count, Long.MAX_VALUE, root -> budget == null
                        ? root.capacity() <= limits.maxBackingBytes() : budget.fits(root.root(), root.capacity()));
            }
            listen(read);
            readStage = Read.CHUNK;
        }

        private long parsedOffset() {
            return source.in.readOffset() - (batch == null ? 0 : batch.data().remaining());
        }

        private void consumeBatch() {
            long offset = parsedOffset();
            long room = source.in.reliableCut() - offset;
            if (room <= 0 || discardingCapsule) {
                unref(batch);
                batch = null;
                return;
            }
            if (!sourcePosition) beginInput(batchSettings, offset);
            ByteBuffer data = batch.data();
            int need = cursor.headerReady() ? cursor.remaining() : cursor.headerNeed();
            int n = (int) Math.min(room, Math.min(need, data.remaining()));
            ByteBuffer part = data.slice(data.position(), n);
            data.position(data.position() + n);
            ReadBuf owner = batch.buf();
            if (!data.hasRemaining()) batch = null;
            else if (owner != null) owner.ref();
            receiveCapsule(new StreamRecv.Chunk(part, owner, false));
        }

        private SplicePolicy.Configuration readBoundary() {
            if (options.probe != null) options.probe.accept(direction, SpliceOptions.Work.BOUNDARY_READ);
            return policy.configuration();
        }

        private void beginInput(SplicePolicy.Configuration observed, long offset) {
            sourcePosition = true;
            inputStart = offset;
            inputHooks = observed.hooks().forShape(packets ? Shape.CAPSULE : Shape.STREAM);
            if (inputHooks.isEmpty() && mirror == null && target.session.info().peerResetStreamAt()) return;
            inputOrder = ++fifoOrder;
            inputAt = clock.now();
            inputGeneration = observed.generation();
            inputSequence = ++sequence;
        }

        private long assemblyDeadline() { return limits.deadline(inputAt); }

        private void receiveBytes(StreamRecv.Chunk chunk) {
            if (inputHooks.isEmpty() && mirror == null) {
                direct = chunk;
                directEnd = parsedOffset();
                sourcePosition = false;
                return;
            }
            var backing = new SplicePayload.Budget(limits);
            SplicePayload payload = new SplicePayload(backing);
            try {
                if (!payload.append(chunk)) {
                    byte[] copy = backing.allocate(chunk.data().remaining());
                    chunk.data().duplicate().get(copy);
                    measures.payloadCopyBytes += copy.length;
                    payload = SplicePayload.of(backing, copy);
                }
            } finally {
                unref(chunk);
            }
            SpliceGroup group = group(Shape.STREAM, -1, -1, Origin.FORWARDED, inputSequence,
                    inputGeneration, inputStart, parsedOffset(), inputAt, payload, null, null);
            sourcePosition = false;
            transform(group, inputHooks);
            inputHooks = List.of();
        }

        private void receiveCapsule(StreamRecv.Chunk chunk) {
            int size = chunk.data().remaining();
            if (!cursor.headerReady()) {
                try {
                    if (!cursor.readHeader(chunk.data())) { packetError(); return; }
                } finally {
                    unref(chunk);
                }
                if (cursor.headerReady()) {
                    if (inputHooks.isEmpty() && mirror == null && target.session.info().peerResetStreamAt()) rawHeader = true;
                    else if (cursor.remaining() == 0) finishCollection();
                }
                return;
            }
            cursor.readValue(size);
            if (streaming) {
                direct = chunk;
                directEnd = parsedOffset();
                return;
            }
            try {
                if (collected == null) collected = new SplicePayload(new SplicePayload.Budget(limits));
                if (compacted == null && (collected.segments() >= limits.maxSegments() || !collected.append(chunk))) {
                    var backing = collected.budget();
                    byte[] copy = backing.allocate(collected.length() + size + cursor.remaining());
                    compactedPayload = SplicePayload.of(backing, copy);
                    compacted = ByteBuffer.wrap(copy);
                    collected.copyTo(0, compacted, collected.length());
                    measures.payloadCopyBytes += collected.length();
                    collected.close();
                }
                if (compacted != null) {
                    compacted.put(chunk.data().duplicate());
                    measures.payloadCopyBytes += size;
                }
            } finally {
                unref(chunk);
            }
            if (cursor.remaining() == 0) finishCollection();
        }

        private void finishCollection() {
            SplicePayload payload = compactedPayload != null ? compactedPayload :
                    collected != null ? collected : new SplicePayload(new SplicePayload.Budget(limits));
            payload.budget().requireRoom(cursor.headerSize());
            byte[] header = new byte[cursor.headerSize()];
            cursor.header().get(header);
            SpliceGroup group = group(Shape.CAPSULE, cursor.type(), cursor.context(), Origin.FORWARDED, inputSequence,
                    inputGeneration, inputStart, parsedOffset(), inputAt, payload, null, ByteBuffer.wrap(header));
            collected = null;
            compacted = null;
            compactedPayload = null;
            cursor.clear();
            sourcePosition = false;
            transform(group, inputHooks);
            inputHooks = List.of();
        }

        private SpliceGroup group(Shape shape, long type, long context, Origin origin, long seq, long generation,
                                  long offset, long sourceEnd, long at, SplicePayload payload, SplicePolicy.Inject injection,
                                  ByteBuffer originalHeader) {
            var meta = new SpliceUnit.Metadata(direction, shape, origin, seq, generation,
                    offset < 0 ? OptionalLong.empty() : optional(source.in.applicationOffset(offset)),
                    optional(source.st.idSet() ? source.st.id() : -1), optional(target.st.idSet() ? target.st.id() : -1),
                    optional(type), optional(context), origin == Origin.FORWARDED && source.isReplayable(),
                    source.st.local() ? Preamble.EMPTY : source.preamble(), sourceTag, targetTag);
            long deadline = limits.deadline(at);
            var group = new SpliceGroup(meta, payload, at, sourceEnd, deadline, injection, originalHeader);
            group.probe = options.probe;
            if (shape == Shape.DATAGRAM) group.targetAt = target.session.driver.now();
            else group.fifoOrder = injection == null ? inputOrder : ++fifoOrder;
            return group;
        }

        private static DatagramOptions targetDatagramOptions(DatagramOptions resolved, long deadline, long targetAt, long here) {
            return deadline == Time.NEVER ? resolved : resolved.expiresAt(Time.add(targetAt, Math.max(0, Time.sub(deadline, here))));
        }

        private static long datagramExpiry(long cap, long helperAt, long targetAt, DatagramOptions resolved) {
            if (resolved.deadline() == Time.NEVER) return cap;
            return Math.min(cap, Time.add(helperAt, Math.max(0, Time.sub(resolved.deadline(), targetAt))));
        }

        private static OptionalLong optional(long value) { return value < 0 ? OptionalLong.empty() : OptionalLong.of(value); }

        private void transform(SpliceGroup group, List<SpliceHook> hooks) {
            measures.inputUnits++;
            measures.inputBytes += group.output.getFirst().length();
            mirror(group, SpliceMirror.Phase.INPUT, 0);
            var queue = group.datagram() ? datagramHooks : reliableHooks;
            if (hooks.isEmpty() && queue.isEmpty() && (!group.datagram() || immediatelyRoutable(group))) {
                transformed(group, null);
                return;
            }
            Operation<?> resolving = group.datagram() ? onLoop(target, ignored ->
                    target.session.core.resolveDatagram(target.st, datagramOptions, group.targetAt)) : null;
            queue.addLast(new HookWork(group, hooks, resolving));
            if (queue.size() == 1) startCallback(queue);
        }

        private boolean immediatelyRoutable(SpliceGroup group) {
            return group.injection == null && !settings.paused() && group.ready(clock.now()) && dgRoute == null && dgSend == null;
        }

        private void startCallback(ArrayDeque<HookWork> queue) {
            HookWork work = queue.getFirst();
            if (work.resolving() == null) {
                invokeCallback(queue, work);
                return;
            }
            Runnable ready = () -> context.execute(() -> {
                // The queued hook owns its group until invocation completes or the direction ends.
                //noinspection resource
                var group = work.group();
                Object result = outcome(work.resolving());
                if (result instanceof DatagramOptions resolved) {
                    group.options = resolved;
                    group.expiry = datagramExpiry(group.expiry, group.admittedAt, group.targetAt, resolved);
                    if (clock.now() >= group.expiry) group.dropReason = Drop.DROPPED_EXPIRED;
                } else group.dropReason = result instanceof Throwable failure ? datagramDrop(failure) : Drop.DROPPED_FLOW_CLOSED;
                invokeCallback(queue, work);
            });
            if (!work.resolving().onDone(ready)) ready.run();
        }

        private void invokeCallback(ArrayDeque<HookWork> queue, HookWork work) {
            var group = work.group();
            if (work.hooks().isEmpty() || group.dropReason != null || (group.datagram() && dgClosed)) {
                finishCallback(queue, group, null);
                return;
            }
            int max = policy.maxItem(group.metadata.shape(), group.metadata.capsuleType().orElse(-1));
            Thread.ofVirtual().name("zmux-splice-hook").start(() -> {
                SpliceEnd result;
                try { result = group.transform(work.hooks(), limits, max, wakeTask); }
                catch (Throwable failure) { result = new SpliceEnd.HookFailed(Optional.of(failure)); }
                SpliceEnd decision = result;
                context.execute(() -> finishCallback(queue, group, decision));
            });
        }

        private void finishCallback(ArrayDeque<HookWork> queue, SpliceGroup group, SpliceEnd decision) {
            queue.removeFirst();
            try { transformed(group, decision); }
            catch (SplicePayload.BudgetExceeded failure) {
                terminate(new SpliceEnd.HookFailed(Optional.of(failure)), TransportError.CANCELLED, false);
            }
            if (!queue.isEmpty()) startCallback(queue);
            wake();
        }

        private void transformed(SpliceGroup group, SpliceEnd decision) {
            measures.payloadCopyBytes += group.copied;
            measures.hookCalls += group.hookCalls;
            measures.replacements += group.replacements;
            if (cancelled.get() || writeFault.get() != null || readFault.get() != null
                    || (group.datagram() ? dgClosed : endStage != End.OPEN)
                    || group.sourceEnd > source.in.reliableCut()) {
                dispose(group, Disposition.DIRECTION_ENDED);
                return;
            }
            if (group.dropReason != null) {
                discard(group, group.dropReason);
                return;
            }
            if (decision != null) {
                if (decision instanceof SpliceEnd.HookFailed(var cause)) measures.cause = cause.orElse(null);
                dispose(group, Disposition.DIRECTION_ENDED);
                long code = decision instanceof SpliceEnd.HookReset(var hookCode) ? hookCode : TransportError.CANCELLED;
                terminate(decision, code, false);
                return;
            }
            if (group.output.isEmpty()) {
                discard(group, Drop.DROPPED_HOOK);
                return;
            }
            if (!group.datagram()) {
                try {
                    group.prepare(group.capsule, limits);
                    if (group.capsule && group.originalHeader == null) measures.encodeBytes += group.encodingCapacity;
                }
                catch (SplicePayload.BudgetExceeded failure) {
                    dispose(group, Disposition.DIRECTION_ENDED);
                    throw failure;
                }
            }
            if (!group.ready(clock.now())) group.waitingSince = clock.now();
            if (group.datagram() && immediatelyRoutable(group)) {
                chooseDatagramRoute(group);
            } else if ((group.datagram() || group.injection != null) && !policy.queue(group)) {
                discard(group, Drop.DROPPED_QUEUE_FULL);
            } else if (group.datagram()) {
                datagrams.add(group);
            } else reliable.put(group.fifoOrder, group);
        }

        private void mirror(SpliceGroup group, SpliceMirror.Phase phase, int index) {
            if (mirror == null) return;
            int copied = mirror.publish(group.metadata, phase, index, group.output.get(index));
            if (copied < 0) measures.mirrorDropped++;
            else measures.mirrorCopyBytes += copied;
        }

        private void finishWait(SpliceGroup group) {
            if (group.waitingSince < 0) return;
            measures.waitNanos += clock.now() - group.waitingSince;
            group.waitingSince = -1;
        }

        private void packetError() {
            if (source.in.reliableCut() <= parsedOffset()) discardCapsuleTail();
            else terminate(new SpliceEnd.PacketError(), TransportError.PACKET_ERROR, true);
        }

        private void discardCapsuleTail() {
            releaseWrite();
            if (direct != null) { unref(direct); direct = null; }
            clearCapsule();
            discardingCapsule = true;
        }

        private boolean expireReliable() {
            var iterator = reliable.values().iterator();
            while (iterator.hasNext()) {
                SpliceGroup group = iterator.next();
                if (clock.now() < group.expiry) continue;
                iterator.remove();
                if (group.datagram()) {
                    discard(group, Drop.DROPPED_EXPIRED);
                } else {
                    dispose(group, Disposition.DIRECTION_ENDED);
                    terminate(new SpliceEnd.WaitTimeout(), TransportError.CANCELLED, false);
                }
                return true;
            }
            return false;
        }

        private long writeLength;
        private SpliceGrants.Request outputGrant;

        private boolean pumpWriter() throws IOException {
            if (write != null) {
                if (writeKind == Write.FALLBACK_FIRST && clock.now() >= active.expiry) write.cancel();
                if (settings.paused() && !streaming && (active == null || !active.capsule)) {
                    pauseByteWrite();
                    return true;
                }
                if (!write.isDone()) return false;
                finishWrite(false);
                return true;
            }
            if (packetPending != null) return writePacketRange();
            if (active == null && !sourcePosition && !reliable.isEmpty()) {
                long first = reliable.firstKey();
                boolean earlierCallback = false;
                for (HookWork waiting : reliableHooks) {
                    // The hook work still owns this group while its order is inspected.
                    //noinspection resource
                    if (waiting.group().fifoOrder < first) { earlierCallback = true; break; }
                }
                // Polling transfers the group from the queue to the active write state.
                //noinspection resource
                if (!earlierCallback) active = reliable.pollFirstEntry().getValue();
            }
            if (active != null) return writeGroup();
            if (settings.paused() && !streaming) return false;
            if (direct != null) {
                if (directEnd > source.in.reliableCut()) {
                    long keep = (long) direct.data().remaining() - (directEnd - source.in.reliableCut());
                    if (keep <= 0 || streaming) {
                        if (streaming) clearCapsule();
                        unref(direct);
                        direct = null;
                        closeInputPermit();
                        return true;
                    }
                    direct.data().limit(direct.data().position() + (int) keep);
                }
                int size = direct.data().remaining();
                if (controlled && limiter != null && !streaming && inputPermit == null) {
                    if (!acquireOutput(1, size)) return false;
                    int accepted = (int) memberPermit.size();
                    if (accepted < size) {
                        ReadBuf owner = direct.buf();
                        if (owner != null) owner.ref();
                        ByteBuffer data = direct.data();
                        ByteBuffer part = data.slice(data.position(), accepted);
                        data.position(data.position() + accepted);
                        writePermit = memberPermit;
                        memberPermit = null;
                        startWrite(new StreamRecv.Chunk(part, owner, false), Write.DIRECT);
                        return true;
                    }
                    inputPermit = memberPermit;
                    memberPermit = null;
                }
                StreamRecv.Chunk chunk = direct;
                direct = null;
                writePermit = inputPermit;
                inputPermit = null;
                startWrite(chunk, streaming ? Write.BODY : Write.DIRECT);
                return true;
            }
            if (!rawHeader) return false;
            if (limiter != null) {
                long size = cursor.headerSize() + (long) cursor.remaining();
                if (!acquireOutput(size, size)) return false;
                startMemberCharge();
            }
            rawHeader = false;
            streaming = true;
            startWrite(new StreamRecv.Chunk(cursor.header(), null, false), Write.HEADER);
            return true;
        }

        private void pauseByteWrite() throws IOException {
            if (writeKind == Write.DIRECT) {
                StreamRecv.Chunk paused = SendLane.pauseChunkWrite(write);
                if (paused != null) {
                    if (direct == null) direct = paused;
                    else {
                        // Taking a subrange for the grant left a remainder on the same backing.
                        direct.data().position(direct.data().position() - paused.data().remaining());
                        unref(paused);
                    }
                }
            } else write.cancel();
            finishWrite(write.isCancelled());
        }

        private boolean writeGroup() throws IOException {
            SpliceGroup group = active;
            if (!group.started) {
                if (clock.now() >= group.expiry) {
                    active = null;
                    if (group.datagram()) {
                        discard(group, Drop.DROPPED_EXPIRED);
                    } else {
                        dispose(group, Disposition.DIRECTION_ENDED);
                        terminate(new SpliceEnd.WaitTimeout(), TransportError.CANCELLED, false);
                    }
                    return true;
                }
                if (settings.paused() || !group.ready(clock.now())) return false;
                finishWait(group);
            }
            if (group.member == group.output.size()) {
                active = null;
                dispose(group, Disposition.SUBMITTED);
                return true;
            }
            SplicePayload item = group.output.get(group.member);
            if (group.capsule && !group.started) {
                // Encode exactly the representation that the selected reliable exit will submit.
                group.header = group.headers[group.member];
                int size = group.header.remaining() + item.length();
                if (limiter != null) {
                    if (group.datagram()) memberPermit = group.reservation.split(size);
                    else if (!acquireOutput(size, size)) return false;
                    startMemberCharge();
                }
                group.started = true;
                if (group.datagram() && group.written == 0) {
                    ByteBuffer first = group.header.slice(group.header.position(), 1);
                    group.header.position(group.header.position() + 1);
                    startWrite(new StreamRecv.Chunk(first, null, false), Write.FALLBACK_FIRST);
                } else startWrite(new StreamRecv.Chunk(group.header, null, false), Write.GROUP_HEADER);
                return true;
            }
            if (group.capsule && group.header.hasRemaining()) {
                startWrite(new StreamRecv.Chunk(group.header, null, false), Write.GROUP_HEADER);
                return true;
            }
            if (group.segment == item.segments()) {
                if (group.capsule) protectedBytes = targetBytes;
                if (group.datagram()) { datagramBytes += item.length(); asCapsules++; }
                group.submitted++;
                measures.outputUnits++;
                mirror(group, SpliceMirror.Phase.OUTPUT, group.member);
                nextMember();
                return true;
            }
            int size = Math.min(options.chunk(), item.segmentLength(group.segment) - group.segmentOffset);
            if (size == 0) { group.segment++; return true; }
            if (!group.capsule && limiter != null) {
                if (!acquireOutput(1, size)) return false;
                size = (int) memberPermit.size();
                writePermit = memberPermit;
                memberPermit = null;
            }
            startWrite(item.chunk(group.segment, group.segmentOffset, size), Write.GROUP_BODY);
            return true;
        }

        private boolean acquireOutput(long min, long max) throws LimiterFailed {
            if (outputGrant == null) {
                outputGrant = grants.request(min, max, false);
            }
            if (outputGrant == null || !outputGrant.done()) return false;
            var next = outputGrant;
            outputGrant = null;
            memberPermit = granted(next);
            return true;
        }

        private void startWrite(StreamRecv.Chunk chunk, Write kind) {
            writeKind = kind;
            if (packets) {
                packetPending = chunk;
                return;
            }
            submitWrite(chunk, kind);
        }

        private boolean writePacketRange() throws IOException {
            if (writeKind == Write.FALLBACK_FIRST && clock.now() >= active.expiry) {
                releaseWrite();
                discard(active, Drop.DROPPED_EXPIRED);
                active = null;
                return true;
            }
            if (settings.paused() && targetBytes == protectedBytes) return false;
            if (capacity == null) {
                capacity = new Operation<>(target.session.driver, op -> {
                    target.session.driver.lanes.flushSending(target.st);
                    var send = target.session.core.sending();
                    CoreException error = send.writeError(target.st);
                    if (error != null) throw error;
                    if (!send.waitSendCapacity(target.st, 1, op)) return Operation.PENDING;
                    return send.sendCapacity(target.st);
                }, op -> target.session.core.sending().cancelCapacityWait(target.st, op)).submit();
                listen(capacity);
                return true;
            }
            if (!capacity.isDone()) return false;
            Object result = outcome(capacity);
            capacity = null;
            if (!(result instanceof Long room) || room <= 0) throw failure(result);
            var pending = packetPending;
            int n = (int) Math.min(room, pending.data().remaining());
            if (n == pending.data().remaining()) {
                packetPending = null;
                submitWrite(pending, writeKind);
            } else {
                ByteBuffer data = pending.data();
                ByteBuffer part = data.slice(data.position(), n);
                data.position(data.position() + n);
                if (pending.buf() != null) pending.buf().ref();
                submitWrite(new StreamRecv.Chunk(part, pending.buf(), false), writeKind);
            }
            return true;
        }

        private void submitWrite(StreamRecv.Chunk chunk, Write kind) {
            writeLength = chunk.data().remaining();
            writingAtTarget = true;
            if (writePermit != null || (trackSettlement && memberCharge == null && writeLength > 0)) {
                writeCharge = charge(writePermit, writeLength, false);
                writePermit = null;
            }
            CarrierCharge billing = writeCharge != null ? writeCharge : memberCharge;
            if (kind == Write.HEADER && billing == null) {
                // The transparent cursor reuses its header scratch space after this operation.
                write = target.out.writeAsync(chunk.data(), null, null, Long.MAX_VALUE);
            } else {
                ByteBuffer data = chunk.data();
                if (kind == Write.HEADER) data = ByteBuffer.allocate(data.remaining()).put(data.duplicate()).flip();
                ReadBuf owner = chunk.buf() == null ? new ReadBuf(data, null) : chunk.buf();
                long expiry = kind == Write.FALLBACK_FIRST ? active.expiry : Time.NEVER;
                write = target.out.writeAsync(null, data, owner, Long.MAX_VALUE, billing,
                        writeCharge == null ? memberChargeOffset : 0,
                        expiry == Time.NEVER ? null : () -> clock.now() < expiry);
            }
            listen(write);
        }

        private void finishWrite(boolean abandoning) throws IOException {
            Operation<?> completed = write;
            write = null;
            Object result = outcome(completed);
            long progressed = ok(result) ? writeLength : SendLane.written(completed);
            targetBytes += progressed;
            boolean converted = (writeKind == Write.FALLBACK_FIRST || writeKind == Write.GROUP_HEADER || writeKind == Write.GROUP_BODY) && active.datagram();
            if (!converted) bytes += progressed;
            if (writeCharge != null) {
                writeCharge.release();
                writeCharge = null;
            } else if (memberCharge != null) memberChargeOffset += progressed;
            if (writeKind == Write.FALLBACK_FIRST) {
                if (progressed == 0 && !abandoning && (result == CANCELLED || result == Boolean.FALSE)) {
                    closeMemberPermit();
                    discard(active, Drop.DROPPED_EXPIRED);
                    active = null;
                    writingAtTarget = false;
                    return;
                }
                if (progressed > 0) active.expiry = Time.NEVER;
            }
            if (converted || writeKind == Write.GROUP_HEADER || writeKind == Write.GROUP_BODY) active.written += progressed;
            if (writeKind == Write.DIRECT || (writeKind == Write.GROUP_BODY && !active.capsule)) protectedBytes = targetBytes;
            if (!ok(result) && !(abandoning && result == CANCELLED && writeKind == Write.GROUP_BODY && !active.capsule)) {
                if (!abandoning) throw failure(result);
                return;
            }
            writingAtTarget = false;
            switch (writeKind) {
                case DIRECT -> { /* No capsule boundary to update. */ }
                case HEADER, BODY -> {
                    if (packetPending == null && cursor.remaining() == 0) {
                        protectedBytes = targetBytes;
                        clearCapsule();
                    }
                }
                case FALLBACK_FIRST, GROUP_HEADER -> { /* A reliable capsule still owns the writer. */ }
                case GROUP_BODY -> {
                    SpliceGroup group = active;
                    group.segmentOffset += (int) progressed;
                    if (group.segmentOffset == group.output.get(group.member).segmentLength(group.segment)) {
                        group.segment++;
                        group.segmentOffset = 0;
                    }
                }
            }
        }

        private void nextMember() {
            closeMemberPermit();
            active.header = null;
            active.member++;
            active.segment = 0;
            active.segmentOffset = 0;
            active.started = false;
        }

        private static Permit granted(SpliceGrants.Request request) throws LimiterFailed {
            Permit permit = request.adopt();
            if (permit == null) throw new LimiterFailed(request.failure());
            return permit;
        }

        private void clearCapsule() {
            closeMemberPermit();
            if (collected != null) { collected.close(); collected = null; }
            if (compactedPayload != null) { compactedPayload.close(); compactedPayload = null; }
            compacted = null;
            cursor.clear();
            rawHeader = false;
            streaming = false;
            sourcePosition = false;
        }

        private void closeInputPermit() {
            if (inputPermit != null) { settlement.unused(inputPermit.size()); inputPermit.close(); inputPermit = null; }
        }

        private void closeMemberPermit() {
            if (memberPermit != null) { settlement.unused(memberPermit.size()); memberPermit.close(); memberPermit = null; }
            if (memberCharge != null) {
                memberCharge.release();
                memberCharge = null;
            }
        }

        private void startMemberCharge() {
            memberCharge = charge(memberPermit, memberPermit.size(), false);
            memberPermit = null;
            memberChargeOffset = 0;
        }

        private CarrierCharge charge(Permit permit, long size, boolean atomic) {
            // Refund can invoke application code, so keep it on the splice context.
            return new CarrierCharge(size, atomic, confirmed -> {
                if (permit != null) context.execute(() -> {
                    permit.consume(confirmed);
                    permit.close();
                });
            }, new CarrierCharge.Observer() {
                @Override public void encoded(int bytes) { settlement.encoded(bytes); }
                @Override public void dispatched(boolean nativeChannel) { settlement.dispatched(nativeChannel); }
                @Override public void enqueued(long count) { settlement.enqueue(count); }
                @Override public void handedOff(long count) { settlement.handoff(count); }
                @Override public void finished(long unaccepted, long unsent) {
                    settlement.finish(unaccepted, unsent, permit != null);
                }
            });
        }

        private boolean pumpDatagrams() {
            if (dgSend != null) {
                if (!dgSend.isDone()) return false;
                finishDatagram();
                if (dgClosed) closeDatagrams();
                return true;
            }
            if (dgClosed) return false;
            if (dataDone || cancelled.get() || (endStage == End.DONE && !(end instanceof SpliceEnd.Finished))) {
                closeDatagrams();
                return false;
            }
            boolean pendingDatagrams = !datagrams.isEmpty() || dgActive != null;
            long now = pendingDatagrams ? clock.now() : 0;
            if (pendingDatagrams && expireDatagrams(now)) return true;
            if (settings.paused()) {
                if (dgReceive != null && !dgReceive.isDone()) dgReceive.cancel();
                return false;
            }
            if (dgRoute != null) {
                if (!dgRoute.isDone()) return false;
                routeDatagram();
                return true;
            }
            SpliceGroup candidate = datagrams.isEmpty() ? null : selectDatagram(now);
            if (candidate != null) {
                datagrams.remove(candidate);
                if (candidate.request != null) completeGroupGrant(candidate);
                else chooseDatagramRoute(candidate);
                return true;
            }
            if (dgInputClosed || !datagramHooks.isEmpty()) return false;
            if (dgReceive == null) {
                dgReceive = new Operation<>(source.session.driver, receiveStep, receiveWithdraw).submit();
                listen(dgReceive);
                return true;
            }
            if (!dgReceive.isDone()) return false;
            Object result = outcome(dgReceive);
            dgReceive = null;
            if (result == CANCELLED) return true;
            if (admitReceivedDatagram(result)) return true;
            if (result instanceof RuntimeException failure) {
                callbackFailure = new CallbackFailedException("relay callback failed", failure);
                terminate(new SpliceEnd.HookFailed(java.util.Optional.of(failure)), TransportError.CANCELLED, false);
            }
            dgInputClosed = true;
            return true;
        }

        private boolean admitReceivedDatagram(Object received) {
            SplicePayload input;
            long context, expiry;
            var hooks = settings.hooks().datagram();
            SplicePolicy.QueueCharge charge = null;
            if (received instanceof Received(var data, var queueCharge)) {
                charge = queueCharge;
                StreamRecv.Chunk chunk = data.payload();
                try {
                    context = data.context();
                    expiry = data.expiry();
                    input = new SplicePayload(new SplicePayload.Budget(limits));
                    if (!input.append(chunk)) throw new SplicePayload.BudgetExceeded();
                } finally { unref(chunk); }
            } else if (received instanceof Datagrams.StreamDatagram(var flow, var payload, var expiresAt)) {
                if (!controlled && hooks.isEmpty() && mirror == null && !packets) {
                    sendDatagram(flow, payload);
                    return true;
                }
                context = flow;
                expiry = expiresAt;
                measures.payloadCopyBytes += payload.length;
                input = SplicePayload.of(new SplicePayload.Budget(limits), payload);
            } else return false;
            receiveDatagram(context, input, hooks, expiry, charge);
            return true;
        }

        private void receiveDatagram(long context, SplicePayload input, List<SpliceHook> hooks, long receiveExpiry, SplicePolicy.QueueCharge charge) {
            if (input.length() > policy.maxItem(Shape.DATAGRAM, -1)) {
                if (charge != null) policy.releaseQueue(charge);
                input.close();
                countDrop(Drop.DROPPED_TOO_LARGE);
                return;
            }
            SpliceGroup group = group(Shape.DATAGRAM, -1, context, Origin.FORWARDED, ++sequence,
                    settings.generation(), -1, -1, clock.now(), input, null, null);
            group.queueCharge = charge;
            if (receiveExpiry != Time.NEVER) {
                long remaining = receiveExpiry - source.session.driver.now();
                if (remaining <= 0) { dispose(group, Disposition.DROPPED); return; }
                group.expiry = Math.min(group.expiry, Time.add(group.admittedAt, remaining));
            }
            transform(group, hooks);
        }

        private DatagramMode datagramMode() {
            return packets && !fin && !source.in.finKnown() && source.in.reliableCut() == Long.MAX_VALUE
                    && endStage == End.OPEN ? DatagramMode.DATAGRAM_OR_CAPSULE : DatagramMode.DATAGRAM_ONLY;
        }

        private void chooseDatagramRoute(SpliceGroup group) {
            finishWait(group);
            long context = group.metadata.contextId().orElseThrow();
            dgRouting = group;
            var mode = datagramMode();
            int largest = group.largestDatagram(), count = group.output.size();
            long targetNow = target.session.driver.now(), here = clock.now();
            long expiry = group.expiry, admittedAt = group.targetAt, helperAt = group.admittedAt;
            var options = group.options;
            dgRoute = onLoop(target, ignored -> {
                if (options != null) {
                    var resolved = targetDatagramOptions(options, expiry, targetNow, here);
                    return target.session.core.planDatagram(target.st, context, largest, count, resolved, mode);
                }
                var initial = target.session.core.resolveDatagram(target.st, datagramOptions, admittedAt);
                var resolved = targetDatagramOptions(initial, datagramExpiry(expiry, helperAt, admittedAt, initial), targetNow, here);
                var admission = target.session.core.planDatagram(target.st, context, largest, count, resolved, mode);
                return new DatagramQuote(initial, admission);
            });
            listen(dgRoute);
        }

        private void routeDatagram() {
            SpliceGroup group = dgRouting;
            Object result = outcome(dgRoute);
            dgRouting = null;
            dgRoute = null;
            if (result instanceof DatagramQuote(var quotedOptions, var quotedAdmission)) {
                group.options = quotedOptions;
                group.expiry = datagramExpiry(group.expiry, group.admittedAt, group.targetAt, group.options);
                result = quotedAdmission;
            }
            if (clock.now() >= group.expiry) {
                discard(group, Drop.DROPPED_EXPIRED);
                return;
            }
            if (!(result instanceof DatagramAdmission admission)) {
                discard(group, result instanceof Throwable failure ? datagramDrop(failure) : Drop.DROPPED_FLOW_CLOSED);
                return;
            }
            boolean oversized = admission == DatagramAdmission.CAPSULE;
            if (group.reservation != null) {
                if (group.capsule == oversized) {
                    publishDatagramGroup(group);
                    return;
                }
                settlement.unused(group.reservation.size());
                group.reservation.close();
                group.reservation = null;
                group.headers = null;
            }
            try {
                group.prepare(oversized, limits);
                if (oversized) measures.encodeBytes += group.encodingCapacity;
            }
            catch (SplicePayload.BudgetExceeded failure) {
                dispose(group, Disposition.DIRECTION_ENDED);
                throw failure;
            }
            if (oversized || limiter != null) {
                if (!policy.queue(group)) {
                    discard(group, Drop.DROPPED_QUEUE_FULL);
                    return;
                }
            }
            if (limiter != null && group.logicalBytes > 0) {
                group.request = grants.request(group.logicalBytes, group.logicalBytes, true);
                if (group.request == null) {
                    discard(group, Drop.DROPPED_BLOCKED);
                } else datagrams.add(group);
            } else publishDatagramGroup(group);
        }

        private void completeGroupGrant(SpliceGroup group) {
            group.reservation = group.request.adopt();
            group.request = null;
            if (group.reservation == null) {
                discard(group, Drop.DROPPED_LIMITER);
            } else chooseDatagramRoute(group);
        }

        private void publishDatagramGroup(SpliceGroup group) {
            if (group.capsule) {
                if (fin || source.in.finKnown() || endStage != End.OPEN || source.in.reliableCut() != Long.MAX_VALUE) {
                    discard(group, Drop.DROPPED_FLOW_CLOSED);
                } else reliable.put(++fifoOrder, group);
                return;
            }
            long context = group.metadata.contextId().orElseThrow();
            StreamRecv.Chunk[] payloads = new StreamRecv.Chunk[group.output.size()];
            CarrierCharge[] charges = new CarrierCharge[payloads.length];
            for (int i = 0; i < payloads.length; i++) {
                SplicePayload item = group.output.get(i);
                payloads[i] = item.datagram();
                measures.payloadCopyBytes += item.copyForDatagram();
                if (trackSettlement) {
                    Permit part = group.reservation == null ? null : group.reservation.split(item.length());
                    charges[i] = charge(part, item.length(), true);
                }
            }
            long at = target.session.driver.now();
            datagramLength = group.logicalBytes;
            dgActive = group;
            dgSend = target.session.sendDatagramGroupAsync(target.st, context, payloads, group.largestDatagram(), group.logicalBytes,
                    targetDatagramOptions(group.options, group.expiry, at, clock.now()), at, datagramMode(), charges,
                    () -> !cancelled.get() && writeFault.get() == null && readFault.get() == null);
            dgSend.whenComplete((status, failure) -> {
                for (CarrierCharge charge : charges) if (charge != null) charge.release();
                wake();
            });
        }

        private void sendDatagram(long context, byte[] payload) {
            datagramLength = payload.length;
            CarrierCharge billing = null;
            if (limiter != null && datagramLength > 0) {
                Permit permit = Permit.tryAcquireExact(limiter, datagramLength);
                if (permit.size() != datagramLength) {
                    permit.close();
                    countDrop(Drop.DROPPED_LIMITER);
                    return;
                }
                billing = charge(permit, datagramLength, true);
            } else if (trackSettlement && datagramLength > 0) billing = charge(null, datagramLength, true);
            dgSend = target.session.sendDatagramAsync(target.st, context, payload, datagramOptions, billing);
            if (billing != null) {
                CarrierCharge reservation = billing;
                dgSend.whenComplete((status, error) -> reservation.release());
            }
            dgSend.whenComplete(wakeOnComplete);
        }

        private void finishDatagram() {
            ZmuxFuture<DatagramAdmission> sent = dgSend;
            dgSend = null;
            boolean accepted = false;
            try {
                var status = sent.join();
                if (status == DatagramAdmission.CAPSULE && dgActive != null) {
                    SpliceGroup group = dgActive;
                    dgActive = null;
                    if (group.reservation != null) {
                        group.reservation.close();
                        group.reservation = null;
                    }
                    chooseDatagramRoute(group);
                    return;
                }
                accepted = status == DatagramAdmission.QUEUED;
                if (accepted) datagramBytes += datagramLength;
            } catch (CancellationException | CompletionException failure) {
                Throwable cause = Spi.completionCause(failure);
                declined(datagramDrop(cause));
            } finally {
                if (dgActive != null) {
                    if (accepted) {
                        dgActive.submitted = dgActive.output.size();
                        dgActive.written = datagramLength;
                        measures.outputUnits += dgActive.submitted;
                        for (int i = 0; i < dgActive.submitted; i++) mirror(dgActive, SpliceMirror.Phase.OUTPUT, i);
                    }
                    Disposition status = accepted ? Disposition.SUBMITTED
                            : dgClosed && end != null ? Disposition.DIRECTION_ENDED : Disposition.DROPPED;
                    dispose(dgActive, status);
                    dgActive = null;
                }
            }
        }

        private static Drop datagramDrop(Throwable failure) {
            if (failure instanceof CoreException core) {
                return switch (core.reason()) {
                    case DATAGRAM_TOO_LARGE -> Drop.DROPPED_TOO_LARGE;
                    case NOT_NEGOTIATED -> Drop.DROPPED_NO_PATH;
                    case DATAGRAM_DROPPED -> datagramDrop(DatagramDroppedException.Reason.from(core.datagramDrop()));
                    default -> Drop.DROPPED_FLOW_CLOSED;
                };
            }
            if (failure instanceof DatagramDroppedException drop) return datagramDrop(drop.reason());
            if (failure instanceof DatagramTooLargeException) return Drop.DROPPED_TOO_LARGE;
            if (failure instanceof NotNegotiatedException) return Drop.DROPPED_NO_PATH;
            return Drop.DROPPED_FLOW_CLOSED;
        }

        private static Drop datagramDrop(DatagramDroppedException.Reason reason) {
            return switch (reason) {
                case DEADLINE -> Drop.DROPPED_EXPIRED;
                case NO_NATIVE_PATH -> Drop.DROPPED_NO_PATH;
                case LIMITER -> Drop.DROPPED_LIMITER;
                case QUEUE_OVERFLOW -> Drop.DROPPED_QUEUE_FULL;
                case SEQUENCE_EXHAUSTED -> Drop.DROPPED_SEQUENCE_EXHAUSTED;
            };
        }

        private void declined(Drop reason) {
            if (dgActive != null) dgActive.dropReason = reason;
            countDrop(reason);
        }

        private void countDrop(Drop reason) { dropCounts.merge(reason, 1L, Long::sum); }

        private void discard(SpliceGroup group, Drop reason) {
            group.dropReason = reason;
            if (group.datagram()) countDrop(reason);
            dispose(group, Disposition.DROPPED);
        }

        private boolean expireDatagrams(long now) {
            for (int index = 0; index < datagrams.size(); index++) {
                // The queue owns this group until it expires, then discard disposes it.
                //noinspection resource
                SpliceGroup group = datagrams.get(index);
                if (now < group.expiry) continue;
                // Removing the expired entry transfers its group to discard for release.
                //noinspection resource
                discard(datagrams.remove(index), Drop.DROPPED_EXPIRED);
                return true;
            }
            return false;
        }

        private SpliceGroup selectDatagram(long now) {
            SpliceGroup first = null;
            for (SpliceGroup group : datagrams) {
                if (group.ready(now) && (group.request == null || group.request.done())
                        && (first == null || group.earliest < first.earliest
                        || (group.earliest == first.earliest && group.metadata.sequence() < first.metadata.sequence())))
                    first = group;

            }
            return first;
        }

        private void closeDatagrams() {
            revokeQueuedCallbacks(datagramHooks);
            dgClosed = true;
            if (dgSend != null && !dgSend.isDone()) dgSend.cancel(false);
            if (dgReceive != null) {
                dgReceive.cancel();
                Object value = outcome(dgReceive);
                if (value instanceof Received(var data, var charge)) {
                    unref(data.payload());
                    policy.releaseQueue(charge);
                    countDrop(Drop.DROPPED_FLOW_CLOSED);
                } else if (value instanceof Datagrams.StreamDatagram) countDrop(Drop.DROPPED_FLOW_CLOSED);
                dgReceive = null;
            }
            for (SpliceGroup group : datagrams) {
                endDatagram(group);
            }
            datagrams.clear();
            if (dgRoute != null) {
                dgRoute.cancel();
                dgRoute = null;
                endDatagram(dgRouting);
                dgRouting = null;
            }
            if (dgSend == null && dgActive != null) {
                endDatagram(dgActive);
                dgActive = null;
            }
        }

        private void endDatagram(SpliceGroup group) {
            countDrop(Drop.DROPPED_FLOW_CLOSED);
            dispose(group, Disposition.DIRECTION_ENDED);
        }

        private void failReliable(SpliceGroup group) {
            if (group.datagram()) countDrop(Drop.DROPPED_FLOW_CLOSED);
            dispose(group, Disposition.DIRECTION_ENDED);
        }

        private void dispose(SpliceGroup group, Disposition status) {
            finishWait(group);
            if (group.reservation != null) settlement.unused(group.reservation.size());
            group.close();
            if (group.injection != null) policy.settled(group, status, end);
            if (group.queueCharge != null) policy.releaseQueue(group.queueCharge);
        }

        private void setTimer() {
            if (stopTimer == null && !sourcePosition && active == null && dgActive == null && reliable.isEmpty() && datagrams.isEmpty()) return;
            long when = Time.NEVER;
            if (packets && sourcePosition && !streaming && (!inputHooks.isEmpty() || mirror != null || !target.session.info().peerResetStreamAt())) when = assemblyDeadline();
            if (active != null && (!active.started || (writeKind == Write.FALLBACK_FIRST && (write != null || packetPending != null)))) when = Math.min(when, active.nextWake(clock.now()));
            if (dgActive != null && dgSend == null) when = Math.min(when, dgActive.expiry);
            for (SpliceGroup group : datagrams) when = Math.min(when, group.nextWake(clock.now()));
            for (SpliceGroup group : reliable.values()) when = Math.min(when, group.expiry);
            if (when == timerAt) return;
            if (stopTimer != null) stopTimer.run();
            timerAt = when;
            if (when != Time.NEVER && options.probe != null) options.probe.accept(direction, SpliceOptions.Work.TIMER);
            stopTimer = when == Time.NEVER ? null : clock.schedule(when, wakeTask);
        }

        /** Disposes all reliable work, incorporating any progress reported by an interrupted write. */
        private void revokeQueuedCallbacks(ArrayDeque<HookWork> queue) {
            // The executing callback still owns its borrowed views. It disposes them on return.
            while (queue.size() > 1) {
                var work = queue.removeLast();
                if (work.resolving() != null) work.resolving().cancel();
                dispose(work.group(), Disposition.DIRECTION_ENDED);
            }
        }

        private void closeReliable() {
            revokeQueuedCallbacks(reliableHooks);
            if (grant != null) { grant.revoke(); grant = null; }
            releaseWrite();
            if (read != null) {
                read.cancel();
                if (outcome(read) instanceof StreamRecv.Chunk chunk) unref(chunk);
                read = null;
            }
            if (direct != null) { unref(direct); direct = null; }
            closeInputPermit();
            if (batch != null) { unref(batch); batch = null; }
            if (packets) clearCapsule();
            sourcePosition = false;
            discardingCapsule = false;
            if (active != null) {
                failReliable(active);
                active = null;
            }
            for (SpliceGroup group : reliable.values()) failReliable(group);
            reliable.clear();
        }

        private void releaseWrite() {
            if (capacity != null) { capacity.cancel(); capacity = null; }
            if (packetPending != null) { unref(packetPending); packetPending = null; }
            if (outputGrant != null) { outputGrant.revoke(); outputGrant = null; }
            if (write != null) {
                write.cancel();
                try { finishWrite(true); }
                catch (IOException impossible) { throw new AssertionError(impossible); }
            }
            closeMemberPermit();
        }

        private void terminate(SpliceEnd reason, long code, boolean protect) {
            if (end != null || endStage == End.DONE) return;
            end = reason;
            closeReliable();
            closeDatagrams();
            source.stop(code);
            if (protect) startReset(code);
            else { target.reset(code); finished(); }
        }

        private void settle(IOException failure) {
            closeReliable();
            closeDatagrams();
            if (terminal != null) { terminal.cancel(); terminal = null; }
            if (end != null) {
                // A failure can interrupt the protected-prefix drain, but its cause already won.
                if (endStage != End.DONE && endStage != End.RESET_SEND) target.reset(resetCode);
                finished();
                return;
            }
            IOException fault = writeFault.get();
            if (fault == null) fault = readFault.get();
            IOException cause = fault == null ? failure : fault;
            measures.cause = cause;
            if (cancelled.get() || cause instanceof InterruptedIOException) {
                long code = cancelCode;
                end = code >= 0 ? new SpliceEnd.Cancelled(code) : null;
                target.reset(code >= 0 ? code : TransportError.CANCELLED);
                source.stop(code >= 0 ? code : 0);
                finished();
            } else if (cause instanceof StreamResetException reset) {
                end = new SpliceEnd.Reset(reset.code());
                startReset(reset.code());
            } else if (cause instanceof StreamStoppedException stopped) {
                end = new SpliceEnd.Stopped(stopped.code());
                source.stop(stopped.code());
                finished();
            } else {
                boolean downstream;
                if (teardown != null && cause instanceof StreamClosedException && teardownLost != null)
                    downstream = teardownLost == target;
                else downstream = cause == writeFault.get() || (cause != readFault.get() && writingAtTarget);
                if (teardown == null) { teardown = cause; teardownLost = downstream ? target : source; }
                end = downstream ? new SpliceEnd.DownstreamLost() : new SpliceEnd.UpstreamLost();
                target.reset(io.zmux.carrier.PipeIo.CARRIER_FAILED);
                source.stop(io.zmux.carrier.PipeIo.CARRIER_FAILED);
                finished();
            }
        }

        private void startReset(long code) {
            resetCode = code;
            terminal = onLoop(target, ignored -> target.session.core.sending().resetAtInUse());
            listen(terminal);
            endStage = End.RESET_QUERY;
        }

        private boolean pumpEnd() throws IOException {
            if (!terminal.isDone()) return false;
            Object result = outcome(terminal);
            terminal = null;
            if (endStage == End.RESET_QUERY && result instanceof Boolean resetAt) {
                if (!resetAt && protectedBytes > 0) {
                    terminal = target.out.awaitSent(false, Long.MAX_VALUE, targetBytes - protectedBytes);
                    listen(terminal);
                    endStage = End.DRAIN;
                } else sendReset();
                return true;
            }
            if (!ok(result)) throw failure(result);
            if (endStage == End.DRAIN) sendReset();
            else {
                if (endStage == End.FINISH) end = new SpliceEnd.Finished();
                finished();
            }
            return true;
        }

        private void sendReset() {
            long code = resetCode, trailing = targetBytes - protectedBytes;
            boolean protect = protectedBytes > 0 || trailing > 0;
            terminal = onLoop(target, ignored -> {
                CoreException fault = target.session.core.sending().writeError(target.st);
                if (fault != null) throw fault;
                target.session.driver.lanes.watchSendEnd(target.st, null);
                target.session.driver.lanes.flushSending(target.st);
                if (protect) target.session.core.sending().protectWrittenPrefix(target.st, trailing);
                target.session.driver.lanes.resetSending(target.st, code);
                return true;
            });
            listen(terminal);
            endStage = End.RESET_SEND;
        }

        private void finished() {
            if (endStage == End.DONE) return;
            endStage = End.DONE;
            policy.finish(end);
            if (closeRead != null) { closeRead.run(); closeWrite.run(); }
            if (!(end instanceof SpliceEnd.Finished)) policy.close(end instanceof SpliceEnd.Cancelled
                    ? Rejection.CANCELLED : Rejection.CLOSED);
        }

        void abandon() {
            closeReliable();
            closeDatagrams();
            if (terminal != null) terminal.cancel();
            policy.close();
            finished();
            setTimer();
        }

        private CompletionStage<SpliceDirection> snapshot() {
            var reply = new CompletableFuture<SpliceDirection>();
            context.execute(() -> policy.complete(reply, result()));
            return reply.minimalCompletionStage();
        }

        SpliceDirection result() {
            return new SpliceDirection(bytes, datagramBytes, asCapsules, end,
                    Optional.ofNullable(measures.cause), measures.inputUnits, measures.inputBytes, measures.outputUnits,
                    measures.hookCalls, measures.replacements, settlement.preAdmissionRefundedBytes(),
                    measures.mirrorDropped, Duration.ofNanos(measures.waitNanos), settlement, dropCounts, measures.payloadCopyBytes,
                    measures.encodeBytes + settlement.encodeBytes(), measures.mirrorCopyBytes, settlement.nativeCount(), settlement.recordCount());
        }
    }

    private static final class Measures {
        long inputUnits, inputBytes, outputUnits, hookCalls, replacements;
        long mirrorDropped, waitNanos, mirrorCopyBytes, payloadCopyBytes, encodeBytes;
        Throwable cause;
    }

    /** The direction's limiter failed in acquire or broke its contract: the direction ends {@link SpliceEnd.LimiterFailed}. */
    private static final class LimiterFailed extends IOException {
        LimiterFailed(Throwable cause) { super("limiter failed", cause); }
    }

    private static void abortBoth(ZmuxStream a, ZmuxStream b, long code) {
        abortBoth(a, b, code, code);
    }

    private static void abortBoth(ZmuxStream a, ZmuxStream b, long resetCode, long stopCode) {
        a.stop(stopCode); a.reset(resetCode); b.stop(stopCode); b.reset(resetCode);
    }
}
