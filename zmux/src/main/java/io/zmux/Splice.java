package io.zmux;

import io.zmux.helper.Limiter;
import io.zmux.helper.Permit;
import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.Datagrams;
import io.zmux.internal.core.ReadBuf;
import io.zmux.internal.core.StreamRecv;
import io.zmux.internal.loop.Operation;
import io.zmux.internal.loop.RecvLane;
import io.zmux.internal.loop.SendLane;
import io.zmux.internal.loop.SerialCallbacks;
import io.zmux.internal.wire.Capsule;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
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
    private final boolean packets;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    /** The code of a cancellation through the splice's control; -1 for none (a cancelled stage resets with ZMUX_CANCELLED and stops with 0). */
    private volatile long cancelCode = -1;
    /** The caller's future: cancelling it, or completing it from outside, cancels the splice. */
    private final ZmuxFuture<SpliceResult> result = new ZmuxFuture<>(() -> { cancel(); return true; });
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

    private Splice(ZmuxStream a, ZmuxStream b, SpliceOptions options, boolean packets) {
        this.a = a; this.b = b; this.options = options; this.packets = packets;
        forward = new Direction(a, b, options.aToB()); reverse = new Direction(b, a, options.bToA());
    }

    static ZmuxFuture<SpliceResult> start(ZmuxStream a, ZmuxStream b, SpliceOptions options, boolean packets) {
        Objects.requireNonNull(a); Objects.requireNonNull(b); Objects.requireNonNull(options);
        if (a.session.driver == b.session.driver && a.st == b.st) throw new IllegalArgumentException("cannot splice a stream into itself");
        a.claimIo();
        try { b.claimIo(); } catch (RuntimeException error) { a.unclaimIo(); throw error; }
        try {
            Splice splice = new Splice(a, b, options, packets);
            if (options.control() != null) options.control().attach(splice);
            splice.wake();
            return splice.result;
        } catch (RuntimeException | Error failure) { a.unclaimIo(); b.unclaimIo(); throw failure; }
    }

    private void cancel() {
        cancelled.set(true); wake();
    }

    /** Cancels with code through the control; false when already cancelled or ended. */
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
            if (!dataDone && forward.streamDone() && reverse.streamDone()) { dataDone = true; more = true; }
            if (dataDone && !cleaning && forward.datagramsDone() && reverse.datagramsDone()) { cleaning = true; release(); }
        } while (more);
    }

    /** A relay bug: the splice fails with it, and both streams are torn down and released. */
    private void fatal(Throwable error) {
        result.publishFailure(error);
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
            result.publishFailure(new java.net.SocketTimeoutException("splice cleanup timed out: a stream's event loop did not answer within the close bound; its release stays queued"));
            return;
        }
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
        return new Operation<Object>(s.session.driver, step, null).submit();
    }

    /** Whether an outcome is the success a lane or loop operation reports. */
    private static boolean ok(Object r) {
        return Boolean.TRUE.equals(r);
    }

    private static void unref(StreamRecv.Chunk c) {
        if (c.buf() != null) c.buf().unref();
    }

    // Stream path stages of a direction.
    private static final int IDLE = 0, PEEK = 1, LIMIT = 2, READ = 3, HOLD = 4, END = 5, FINISH = 6,
            RESET_QUERY = 7, RESET_SOURCE = 8, RESET_TARGET = 9, DRAIN = 10, RESET_SEND = 11, DONE = 12;
    // What the target write in progress carries.
    private static final int STREAM = 0, CONVERTED_HEAD = 1, CONVERTED_BODY = 2, CAPSULE_HEAD = 3;
    // Datagram path stages.
    private static final int DG_IDLE = 0, DG_RECEIVE = 1, DG_SEND = 2, DG_DONE = 3;
    /** Rounds a direction advances before the other gets its turn. */
    private static final int ROUNDS = 16;

    /**
     * One direction: the stream path reads the source's receiving lane and writes the target's
     * sending lane, the same lanes as application reads and writes, so forwarding needs no loop
     * hand-off of its own; the datagram path receives the source's flow datagrams and sends them
     * on the target's. Lane and session observers wake the splice when either side fails while
     * the direction waits elsewhere (the limiter, the other lane); the drain then cancels what the
     * direction awaits.
     */
    private final class Direction {
        private final ZmuxStream source, target;
        private final Limiter limiter;
        private final AtomicReference<IOException> readFault = new AtomicReference<>(), writeFault = new AtomicReference<>();
        /** A source reset arrived: the limiter wait in progress, or the next one, must look again, as the reset is read in order. */
        private final AtomicBoolean resetPoke = new AtomicBoolean();
        // Context only from here on.
        private Runnable closeRead, closeWrite;
        private int stage = IDLE;
        /** The stream path's operation in progress (a read, the finish, a reset step), null while none. */
        private Operation<?> op;
        /** The limiter wait in progress, null while none; cancelling it refunds a grant that arrives anyway. */
        private CompletableFuture<Permit> ready;
        /** The grant taken for the chunk being read, null while none; what the read does not fill is refunded. */
        private Permit permit;
        /** The granted bytes behind the chunk held or being written; what is not forwarded is refunded. */
        private long chunkPaid;
        /** The read in progress takes the end that a wait reported: anything else it returns is dropped. */
        private boolean endRead;
        /** A chunk taken from the source, waiting for the target (HOLD); the splice owns its reference. */
        private StreamRecv.Chunk held;
        /** The target write in progress (one at a time), what it carries, and its stream length. */
        private Operation<?> write;
        private int writeKind, writeLength;
        /** The granted bytes behind the target write in progress. */
        private long writePaid;
        /** The reset code being forwarded. */
        private long resetCode;
        private int dgStage = DG_DONE;
        private Operation<?> dgReceive;
        private ZmuxFuture<Boolean> dgSend;
        private Datagrams.StreamDatagram dgSending;
        private final Operation.Step receiveStep;
        private final Operation.Withdraw receiveWithdraw;
        private long bytes, datagramBytes, drops, tooLarge, limited;
        /** Packet relay: the capsule parser of the source's bytes. */
        private final Capsule.Header capsule = new Capsule.Header();
        /** Packet relay: the value bytes of the current capsule not yet read. */
        private long capsuleRemaining;
        /**
         * Packet relay under a limiter: the header of the next capsule, read and parsed before its whole capsule is
         * acquired, then written; empty at a capsule boundary. At most two variable-length integers.
         */
        private final ByteBuffer capsuleHeader = ByteBuffer.allocate(16);
        /** Packet relay under a limiter: capsuleHeader holds a whole header, not yet granted. */
        private boolean headerReady;
        /** Packet relay under a limiter: the read in progress takes header bytes. */
        private boolean headerRead;
        /** Packet relay under a limiter: what the current capsule's grant still covers of its unread value bytes. */
        private long capsulePaid;
        /** Packet relay: datagrams too large for the target, waiting for a capsule boundary; at most {@link #CONVERTED}. */
        private final ArrayDeque<Datagrams.StreamDatagram> converted = new ArrayDeque<>();
        /** Packet relay: the converted datagram being written. */
        private Datagrams.StreamDatagram converting;
        /** Packet relay: the header of a converted datagram; one write at a time uses it. */
        private final ByteBuffer convertedHeader = ByteBuffer.allocate(Capsule.MAX_DATAGRAM_HEADER);
        /** Packet relay: the target can take no more capsules (finished or failed). */
        private boolean targetClosed;
        /** How the direction ended; null while running, when cancelled, or after a callback failure. */
        private SpliceResult.End end;
        /** A limiter callback failed, and the splice completes with it. */
        private IOException callbackFailure;
        /** The stream path is in an operation on the target, so a failure it throws is the downstream side's. */
        private boolean atTarget;

        Direction(ZmuxStream source, ZmuxStream target, Limiter limiter) {
            this.source = source; this.target = target; this.limiter = limiter;
            receiveStep = o -> {
                if (cancelled.get()) throw CoreException.of(CoreException.Reason.INVALID);
                Datagrams.StreamDatagram d = source.session.core.datagrams().receive(source.st);
                if (d != null) return d;
                if (source.session.core.error() != null) throw CoreException.session(source.session.core.error());
                source.session.core.datagrams().waitReceive(source.st, o); return Operation.PENDING;
            };
            receiveWithdraw = o -> source.session.core.datagrams().cancelReceiveWait(source.st, o);
        }

        void begin() {
            closeRead = source.session.driver.watchClosed(error -> fail(new SessionClosedException(Errors.session(error)), true));
            closeWrite = target.session.driver.watchClosed(error -> fail(new SessionClosedException(Errors.session(error)), false));
            source.session.driver.post(() -> source.session.driver.lanes.watchRecvEnd(source.st, this::sourceEnded));
            target.session.driver.post(() -> target.session.driver.lanes.watchSendEnd(target.st, this::targetEnded));
            dgStage = source.session.info().datagramFlows() ? DG_IDLE : DG_DONE;
        }

        /** Records a side's failure (any thread); the drain cancels what the direction awaits. */
        void fail(IOException error, boolean reading) {
            if ((reading ? readFault : writeFault).compareAndSet(null, error)) wake();
        }

        /** The source's receiving lane ended with failure (on its loop). */
        void sourceEnded(CoreException e) {
            IOException error = ended(e);
            // A reset is read in order, after the bytes before its Reliable Size; only a limiter wait must look again.
            if (error instanceof StreamResetException) { resetPoke.set(true); wake(); return; }
            fail(error, true);
        }

        /** The target's sending lane ended (on its loop). */
        void targetEnded(CoreException e) { fail(ended(e), false); }

        /**
         * A lane's end as a failure, never an exception thrown on the loop: the reasons the API reports by
         * throwing (a misuse, such as INVALID or TOO_LATE) end the direction like any other failure.
         */
        private static IOException ended(CoreException e) {
            try {
                return Errors.io(e);
            } catch (RuntimeException misuse) {
                return new IOException("splice side ended: " + e.reason(), misuse);
            }
        }

        /** Whether the forwarding stages must stop waiting: a cancellation or either side's failure. */
        private boolean interrupted() {
            return cancelled.get() || writeFault.get() != null || readFault.get() != null;
        }

        private void check() throws IOException {
            if (cancelled.get()) throw new InterruptedIOException("splice cancelled");
            IOException error = writeFault.get(); if (error != null) throw error;
            error = readFault.get(); if (error != null) throw error;
        }

        /** Cancels op when interrupt holds; whether op is decided. */
        private static boolean awaited(Operation<?> o, boolean interrupt) {
            if (interrupt) o.cancel();
            return o.isDone();
        }

        private void listen(Operation<?> o) {
            o.onDone(wakeTask); // false: decided already, and the running drain sees it
        }

        boolean streamDone() { return stage == DONE; }

        boolean datagramsDone() { return dgStage == DG_DONE; }

        /** Advances the direction a bounded number of rounds; whether anything moved. */
        boolean pump() {
            boolean progress = false;
            for (int round = 0; round < ROUNDS; round++) {
                boolean moved;
                try {
                    boolean wrote = pumpTarget();
                    moved = pumpStream() || wrote;
                } catch (CallbackFailed failed) {
                    stopForwarding(); callbackFailed(failed.cause); finished();
                    moved = true;
                } catch (IOException error) {
                    stopForwarding(); failed(error);
                    moved = true;
                } catch (RuntimeException error) {
                    stopForwarding(); callbackFailed(new CallbackFailedException("relay callback failed", error)); finished();
                    moved = true;
                }
                moved |= pumpDatagrams();
                if (!moved) return progress;
                progress = true;
            }
            return true;
        }

        // ---- stream path ----

        private boolean pumpStream() throws IOException {
            return switch (stage) {
                case IDLE -> {
                    check();
                    if (limiter != null) { op = source.in.awaitReadableAsync(Long.MAX_VALUE); listen(op); stage = PEEK; }
                    else startRead(sized(options.chunkSize()));
                    yield true;
                }
                case PEEK -> {
                    if (!awaited(op, interrupted())) yield false;
                    Operation<?> w = op; op = null;
                    Object r = outcome(w);
                    if (!ok(r)) throw failure(r);
                    long available = RecvLane.readable(w);
                    if (available < 0) { endRead = true; startRead(1); yield true; }
                    if (available == 0) { stage = IDLE; yield true; }
                    int n = sized((int) Math.min(options.chunkSize(), available));
                    if (packets) {
                        // Each whole capsule is acquired at once, (n, n), from its header; its value bytes then flow.
                        if (capsuleRemaining > 0) { startRead((int) Math.min(n, capsuleRemaining)); yield true; }
                        headerRead = true;
                        startRead(Math.min(n, headerNeed()));
                        yield true;
                    }
                    // Nothing was consumed while the source was idle: the grant is taken only for bytes already readable.
                    waitGrant(1, n);
                    yield true;
                }
                case LIMIT -> {
                    boolean poked = resetPoke.getAndSet(false); // Always consumed: a poke left set would repeat the next wait.
                    CompletableFuture<Permit> f = ready;
                    if (!f.isDone() && !poked && !interrupted()) yield false;
                    ready = null;
                    if (poked || !f.isDone()) {
                        Permit.abandon(f); check();
                        // A source reset: read again, up to its end; a held header keeps its capsule's bytes to come.
                        if (headerReady) waitGrant(capsuleHeader.position() + capsuleRemaining, capsuleHeader.position() + capsuleRemaining);
                        else stage = IDLE;
                        yield true;
                    }
                    Permit granted = granted(f);
                    check();
                    if (headerReady) { writeHeader(granted); yield true; }
                    permit = granted;
                    startRead((int) permit.size());
                    yield true;
                }
                case READ -> {
                    if (!awaited(op, interrupted())) yield false;
                    Object r = outcome(op); op = null;
                    if (r == StreamRecv.FIN) { endRead = false; headerRead = false; giveBack(); reachedEnd(); yield true; }
                    if (r instanceof StreamRecv.Chunk c) {
                        if (endRead) { endRead = false; unref(c); giveBack(); stage = IDLE; yield true; }
                        if (headerRead) { headerRead = false; takeHeader(c); yield true; }
                        int length = c.data().remaining();
                        if (permit != null) { chunkPaid = Math.min(length, permit.size()); permit.consume(length); }
                        else if (packets && limiter != null) { chunkPaid = Math.min(length, capsulePaid); capsulePaid -= chunkPaid; }
                        else chunkPaid = 0;
                        giveBack(); // the part of the grant the read did not fill; the chunk's part is forwarded or refunded
                        held = c; stage = HOLD;
                        yield true;
                    }
                    throw failure(r);
                }
                case HOLD -> {
                    if (held != null && interrupted()) check(); // waiting for the target, not yet written
                    yield false;
                }
                case END -> {
                    check();
                    if (write != null || (packets && !converted.isEmpty() && !targetClosed)) yield false; // the waiting datagrams go first
                    if (packets) targetClosed = true; // later ones are dropped
                    atTarget = true;
                    op = target.out.finishAsync(Long.MAX_VALUE); listen(op);
                    stage = FINISH;
                    yield true;
                }
                case FINISH -> {
                    if (!awaited(op, interrupted())) yield false;
                    Object r = outcome(op); op = null;
                    if (!ok(r)) throw failure(r);
                    atTarget = false;
                    end = new SpliceResult.End.Finished(); finished();
                    yield true;
                }
                case RESET_QUERY, RESET_SOURCE, RESET_TARGET, DRAIN, RESET_SEND -> pumpReset();
                default -> false;
            };
        }

        /** The bytes to read now: with converted datagrams waiting, up to the next capsule boundary, where they go in. */
        private int sized(int n) {
            if (packets && !converted.isEmpty()) return (int) Math.min(n, capsuleRemaining > 0 ? capsuleRemaining : capsule.idle() ? n : 1);
            return n;
        }

        private void startRead(int n) {
            op = source.in.readAsync(null, n, Long.MAX_VALUE); listen(op);
            stage = READ;
        }

        /** Waits for a grant of min through max bytes from the direction's limiter (stage LIMIT). */
        private void waitGrant(long min, long max) {
            ready = Permit.acquire(limiter, min, max).toCompletableFuture();
            stage = LIMIT;
            ready.whenComplete(wakeOnComplete);
        }

        /** The decided grant; a failing limiter fails the direction. */
        private static Permit granted(CompletableFuture<Permit> f) throws CallbackFailed {
            try { return f.join(); }
            catch (CompletionException | CancellationException error) {
                Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
                throw new CallbackFailed(cause instanceof CallbackFailedException failed ? failed : new CallbackFailedException("limiter failed", cause));
            }
        }

        /** Packet relay under a limiter: the header bytes still missing, 0 once Type and Length are whole (1 while a length is unknown). */
        private int headerNeed() {
            int have = capsuleHeader.position();
            if (have == 0) return 1;
            int first = 1 << ((capsuleHeader.get(0) & 0xff) >>> 6);
            if (have < first) return first - have;
            if (have == first) return 1;
            return first + (1 << ((capsuleHeader.get(first) & 0xff) >>> 6)) - have;
        }

        /** Packet relay under a limiter: keeps header bytes read; a whole header waits for its capsule's grant. */
        private void takeHeader(StreamRecv.Chunk c) {
            ByteBuffer b = c.data();
            capsuleHeader.put(b.duplicate());
            unref(c);
            if (headerNeed() > 0) { stage = IDLE; return; }
            ByteBuffer parse = capsuleHeader.duplicate().flip();
            if (!capsule.feed(parse)) throw new AssertionError("capsule header");
            capsuleRemaining = capsule.length();
            headerReady = true;
            long total = capsuleHeader.position() + capsuleRemaining;
            waitGrant(total, total);
        }

        /** Packet relay under a limiter: the capsule is granted, its header goes to the target first. */
        private void writeHeader(Permit granted) {
            long g = granted.size();
            int length = capsuleHeader.position();
            granted.consume(g); // the grant stays with the capsule: refunded only for bytes not forwarded
            headerReady = false;
            capsulePaid = Math.max(0, g - length);
            atTarget = true;
            writeKind = CAPSULE_HEAD; writeLength = length; writePaid = Math.min(g, length);
            capsuleHeader.flip();
            write = target.out.writeAsync(capsuleHeader, null, null, Long.MAX_VALUE);
            listen(write);
            stage = HOLD; // the next read follows the header's write
        }

        /** FIN read upstream. */
        private void reachedEnd() {
            if (packets && !atBoundary()) { end = truncated(); finished(); return; }
            stage = END;
        }

        // ---- the target's writes: forwarded chunks and converted datagrams, one at a time ----

        private boolean pumpTarget() throws IOException {
            if (write != null) {
                if (writeKind == STREAM ? interrupted() : cancelled.get()) write.cancel();
                if (!write.isDone()) return false;
                written(false);
                return true;
            }
            if (packets && !converted.isEmpty() && atBoundary() && stage != DONE) {
                if (targetClosed) return false; // counted as dropped in the result
                startConverted(converted.poll());
                return true;
            }
            if (stage == HOLD && held != null && !interrupted()) {
                StreamRecv.Chunk c = held; held = null;
                int length = c.data().remaining();
                if (packets) scan(c.data().duplicate());
                ReadBuf buf = c.buf() != null ? c.buf() : new ReadBuf(c.data(), null);
                atTarget = true;
                writeKind = STREAM; writeLength = length; writePaid = chunkPaid; chunkPaid = 0;
                write = target.out.writeAsync(null, c.data(), buf, Long.MAX_VALUE); // takes the reference over
                listen(write);
                return true;
            }
            return false;
        }

        /** Accounts for the decided target write; a failed forwarding write throws unless settling. */
        private void written(boolean settling) throws IOException {
            Operation<?> w = write; write = null;
            Object r = outcome(w);
            switch (writeKind) {
                case STREAM -> {
                    if (ok(r)) {
                        bytes += writeLength;
                        atTarget = false;
                        if (!settling) stage = IDLE;
                    } else {
                        long forwarded = r == CANCELLED ? SendLane.written(w) : 0;
                        bytes += forwarded;
                        refund(writePaid - Math.min(writePaid, forwarded));
                        if (!settling) throw failure(r);
                    }
                }
                case CAPSULE_HEAD -> {
                    capsuleHeader.clear();
                    if (ok(r)) {
                        bytes += writeLength;
                        atTarget = false;
                        if (!settling) stage = IDLE;
                    } else {
                        refund(writePaid);
                        if (!settling) throw failure(r);
                    }
                }
                case CONVERTED_HEAD -> {
                    if (ok(r) && !settling) {
                        writeKind = CONVERTED_BODY;
                        write = target.out.writeAsync(ByteBuffer.wrap(converting.payload()), null, null, Long.MAX_VALUE);
                        listen(write);
                    } else {
                        // The target failed or the splice ends: the stream path reports it. A capsule cut short can be followed by nothing.
                        if (ok(r) || r == CANCELLED) targetClosed = true;
                        refund(converting.payload().length);
                        converting = null; dropTooLarge();
                    }
                }
                default -> {
                    int n = converting.payload().length;
                    converting = null;
                    if (ok(r)) datagramBytes += n;
                    else { targetClosed = true; refund(n); dropTooLarge(); }
                }
            }
        }

        /** Refunds n bytes granted and not forwarded to the limiter. */
        private void refund(long n) {
            if (limiter != null) Permit.refund(limiter, n);
        }

        /** Gives back what the chunk in flight still holds. */
        private void giveBack() {
            if (permit != null) { permit.close(); permit = null; }
        }

        /** Writes a waiting datagram as a DATAGRAM capsule, at a capsule boundary. */
        private void startConverted(Datagrams.StreamDatagram d) {
            converting = d;
            convertedHeader.clear();
            Capsule.putDatagramHeader(convertedHeader, d.context(), d.payload().length);
            convertedHeader.flip();
            writeKind = CONVERTED_HEAD;
            write = target.out.writeAsync(convertedHeader, null, null, Long.MAX_VALUE);
            listen(write);
        }

        // ---- packet relay ----

        /** Advances the capsule parser over b, the bytes about to be forwarded. */
        private void scan(ByteBuffer b) {
            while (b.hasRemaining()) {
                if (capsuleRemaining > 0) {
                    int k = (int) Math.min(capsuleRemaining, b.remaining());
                    b.position(b.position() + k); capsuleRemaining -= k;
                } else if (capsule.feed(b)) capsuleRemaining = capsule.length();
            }
        }

        private boolean atBoundary() { return capsuleRemaining == 0 && capsule.idle() && capsuleHeader.position() == 0 && !headerReady; }

        private void dropTooLarge() { drops++; tooLarge++; }
        private void dropLimited() { drops++; limited++; }

        /** The source ended inside a capsule: the target is reset and the source stopped with ZMUX_PACKET_ERROR. */
        private SpliceResult.End truncated() {
            long code = io.zmux.internal.wire.TransportError.PACKET_ERROR;
            source.stop(code); target.reset(code);
            return new SpliceResult.End.Reset(code);
        }

        // ---- settling ----

        /** The stream path stops: whatever it awaits is cancelled, a taken chunk released and its unused grant refunded. */
        private void stopForwarding() {
            if (ready != null) { Permit.abandon(ready); ready = null; }
            if (op != null) {
                op.cancel(); // decided once it returns: a lane read's or a loop step's attempt is not left running
                if (outcome(op) instanceof StreamRecv.Chunk c) unref(c);
                op = null;
            }
            if (held != null) { refund(chunkPaid); unref(held); held = null; }
            chunkPaid = 0;
            refund(capsulePaid); capsulePaid = 0; // the rest of a granted capsule is not forwarded
            headerReady = false; headerRead = false; capsuleHeader.clear();
            if (write != null) {
                write.cancel();
                try { written(true); }
                catch (IOException impossible) { throw new AssertionError(impossible); }
                catch (RuntimeException error) { callbackFailed(new CallbackFailedException("relay callback failed", error)); }
            }
            giveBack();
            if (packets) targetClosed = true;
        }

        /** The stream path failed with error: the side's own fault explains it better than the wait it ended. */
        private void failed(IOException error) {
            IOException outcome = error;
            IOException terminal = writeFault.get();
            if (terminal == null) terminal = readFault.get();
            if (terminal != null && !cancelled.get()) outcome = terminal;
            if (teardown != null && outcome instanceof StreamClosedException) outcome = teardown;
            settle(outcome);
        }

        private void settle(IOException error) {
            if (cancelled.get() || error instanceof InterruptedIOException) {
                long code = cancelCode;
                end = code >= 0 ? new SpliceResult.End.Cancelled(code) : null;
                if (code >= 0) abortBoth(source, target, code);
                else abortBoth(source, target, io.zmux.internal.wire.TransportError.CANCELLED, 0); // 7.4: reset with ZMUX_CANCELLED, stop with 0
                finished();
            } else if (error instanceof StreamResetException reset) {
                resetCode = reset.code();
                end = new SpliceResult.End.Reset(resetCode);
                op = onLoop(target, o -> target.session.core.sending().resetAtInUse()); listen(op);
                stage = RESET_QUERY;
            } else if (error instanceof StreamStoppedException stopped) {
                end = new SpliceResult.End.Stopped(stopped.code()); source.stop(stopped.code());
                finished();
            } else {
                // A lost session, or a carrier failure of one side: the side it came from.
                boolean downstream;
                // The other direction's failure tore this one down: its cause names the side that was lost.
                if (error == teardown && teardownLost != null) downstream = teardownLost == target;
                else downstream = error == writeFault.get() || (error != readFault.get() && atTarget);
                if (teardown == null) { teardown = error; teardownLost = downstream ? target : source; }
                end = downstream ? new SpliceResult.End.DownstreamLost() : new SpliceResult.End.UpstreamLost();
                abortBoth(source, target, io.zmux.carrier.PipeIo.CARRIER_FAILED);
                finished();
            }
        }

        /**
         * Forwarding a reset: without RESET_AT a reset drops unsent bytes, so what was taken is
         * transmitted first. Only a cancellation ends these steps early, and side failures the
         * transmission wait.
         */
        private boolean pumpReset() {
            boolean draining = stage == DRAIN;
            if (!awaited(op, draining ? interrupted() : cancelled.get())) return false;
            Object r = outcome(op); op = null;
            if (r instanceof Throwable || r == CANCELLED) { resetFailed(failure(r)); return true; }
            switch (stage) {
                case RESET_QUERY -> {
                    if ((Boolean) r) { sendReset(); return true; }
                    IOException fault = cancelled.get() ? new InterruptedIOException("splice cancelled") : writeFault.get();
                    if (fault == null) fault = readFault.get();
                    if (fault != null) { resetFailed(fault); return true; }
                    op = onLoop(source, o -> {
                        if (source.session.core.error() != null) throw CoreException.session(source.session.core.error());
                        return true;
                    });
                    stage = RESET_SOURCE;
                }
                case RESET_SOURCE -> {
                    op = onLoop(target, o -> {
                        CoreException failure = target.session.core.sending().writeError(target.st);
                        if (failure != null) throw failure;
                        return true;
                    });
                    stage = RESET_TARGET;
                }
                case RESET_TARGET -> { op = target.out.awaitSent(false, Long.MAX_VALUE); stage = DRAIN; }
                case DRAIN -> { sendReset(); return true; }
                default -> { finished(); return true; }
            }
            listen(op);
            return true;
        }

        private void sendReset() {
            long code = resetCode;
            boolean boundary = bytes > 0 && code != io.zmux.carrier.PipeIo.CARRIER_FAILED;
            op = onLoop(target, o -> {
                target.session.driver.lanes.flushSending(target.st);
                if (boundary) target.session.core.sending().setReliableBoundary(target.st);
                target.session.driver.lanes.resetSending(target.st, code); return true;
            });
            listen(op);
            stage = RESET_SEND;
        }

        private void resetFailed(IOException failure) {
            // A fault of either side explains the failure better than the wait it ended.
            IOException next = readFault.get() instanceof SessionClosedException lost ? lost : writeFault.get();
            if (next == null) next = readFault.get();
            if (next == null || cancelled.get()) next = failure;
            if (teardown != null && next instanceof StreamClosedException) next = teardown;
            settle(next);
        }

        /** The stream path ended: its observers go, and the target takes no more capsules. */
        private void finished() {
            stage = DONE;
            if (write != null) stopForwarding();
            targetClosed = true;
            for (Datagrams.StreamDatagram d; (d = converted.poll()) != null;) { refund(d.payload().length); dropTooLarge(); } // never written
            if (closeRead != null) { closeRead.run(); closeWrite.run(); }
        }

        /** A relay bug: everything the direction awaits is dropped. */
        void abandon() {
            stopForwarding();
            if (dgReceive != null) dgReceive.cancel();
            dgStage = DG_DONE;
            finished();
        }

        // ---- datagram path ----

        private boolean pumpDatagrams() {
            try {
                return pumpDatagramsOnce();
            } catch (RuntimeException error) {
                // A limiter callback (or the relay itself) failed: the splice reports it as the stream path's would.
                if (dgReceive != null) { dgReceive.cancel(); dgReceive = null; }
                if (dgSending != null) refund(dgSending.payload().length);
                dgSend = null; dgSending = null; dgStage = DG_DONE;
                if (!cancelled.get()) callbackFailed(new CallbackFailedException("relay callback failed", error));
                return true;
            }
        }

        private boolean pumpDatagramsOnce() {
            return switch (dgStage) {
                case DG_IDLE -> {
                    if (dataDone || cancelled.get()) { dgStage = DG_DONE; yield true; }
                    dgReceive = new Operation<Object>(source.session.driver, receiveStep, receiveWithdraw).submit();
                    listen(dgReceive);
                    dgStage = DG_RECEIVE;
                    yield true;
                }
                case DG_RECEIVE -> {
                    if (!awaited(dgReceive, dataDone || cancelled.get())) yield false;
                    Object r = outcome(dgReceive); dgReceive = null;
                    if (r instanceof RuntimeException error) throw error;
                    if (!(r instanceof Datagrams.StreamDatagram data)) { dgStage = DG_DONE; yield true; } // the stream paths own terminal propagation
                    if (dataDone || cancelled.get()) { drops++; dgStage = DG_DONE; yield true; }
                    int n = data.payload().length;
                    dgStage = DG_IDLE;
                    // tryAcquire(n, n), never a wait while the datagram is held (a packet relay's too, even one it turns
                    // into a capsule); an empty one bypasses the limiter.
                    if (limiter != null && !Permit.police(limiter, n)) { dropLimited(); yield true; } // taken: refunded unless forwarded
                    forward(data);
                    yield true;
                }
                case DG_SEND -> {
                    if (!dgSend.isDone()) yield false;
                    ZmuxFuture<Boolean> f = dgSend; Datagrams.StreamDatagram data = dgSending;
                    dgSend = null; dgSending = null;
                    dgStage = DG_IDLE;
                    try {
                        if (f.join()) datagramBytes += data.payload().length;
                        else { refund(data.payload().length); dropLimited(); }
                    } catch (CompletionException error) {
                        Throwable cause = error.getCause();
                        if (cause instanceof NotNegotiatedException || cause instanceof DatagramTooLargeException) {
                            if (!packets) { refund(data.payload().length); dropTooLarge(); } else convert(data);
                        } else {
                            refund(data.payload().length);
                            if (cause instanceof RuntimeException failure) throw failure;
                            dgStage = DG_DONE;
                        }
                    }
                    yield true;
                }
                default -> false;
            };
        }

        /** Sends a datagram that passed the limiter on the target, or, in a packet relay, as a capsule when it does not fit. */
        private void forward(Datagrams.StreamDatagram data) {
            int max = target.maxDatagramPayload(data.context());
            if (!packets || (max > 0 && data.payload().length <= max)) {
                dgSending = data;
                dgSend = target.session.sendDatagramAsync(target.st, data.context(), data.payload());
                dgStage = DG_SEND;
                dgSend.whenComplete(wakeOnComplete);
            } else convert(data);
        }

        /** A packet relay writes it as a capsule at the next boundary; a bounded wait, never a queue of its own. */
        private void convert(Datagrams.StreamDatagram data) {
            if (targetClosed || converted.size() >= CONVERTED) { refund(data.payload().length); dropTooLarge(); }
            else converted.add(data);
        }

        /**
         * A callback failed: the splice completes exceptionally with cause (the direction's first such failure, of its stream
         * or its datagram path) and both streams are torn down.
         */
        private void callbackFailed(IOException cause) {
            if (callbackFailure == null) callbackFailure = cause;
            if (teardown == null) teardown = cause;
            abortBoth(source, target, io.zmux.carrier.PipeIo.CARRIER_FAILED);
        }

        SpliceResult.Direction result() {
            return new SpliceResult.Direction(bytes, datagramBytes, drops, tooLarge, limited, end);
        }
    }

    /** Packet relay: datagrams that wait for a capsule boundary, per direction; more are dropped as too large. */
    private static final int CONVERTED = 64;

    /** A limiter callback failed or broke its contract; carries what the splice completes with. */
    private static final class CallbackFailed extends IOException {
        final IOException cause;
        CallbackFailed(IOException cause) { super(cause.getMessage(), cause); this.cause = cause; }
    }

    private static void abortBoth(ZmuxStream a, ZmuxStream b, long code) {
        abortBoth(a, b, code, code);
    }

    private static void abortBoth(ZmuxStream a, ZmuxStream b, long resetCode, long stopCode) {
        a.stop(stopCode); a.reset(resetCode); b.stop(stopCode); b.reset(resetCode);
    }
}
