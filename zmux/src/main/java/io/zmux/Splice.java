package io.zmux;

import io.zmux.helper.Limiter;
import io.zmux.internal.core.Datagrams;
import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.ReadBuf;
import io.zmux.internal.core.StreamRecv;
import io.zmux.internal.loop.Operation;
import io.zmux.internal.wire.Capsule;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicReference;

/** Owns relay work, never protocol state. All core access goes through the existing owner. */
final class Splice {
    private final ZmuxStream a, b;
    private final SpliceOptions options;
    /** A packet relay (EXTENSIONS.md Section 12): the stream bytes are capsules, and datagrams may become capsules. */
    private final boolean packets;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private volatile boolean dataDone;
    /** Why the first direction to fail tore both streams down; the other direction's local-close faults follow from it. */
    private final AtomicReference<IOException> teardown = new AtomicReference<>();
    /** The stream whose side was lost in the failure behind {@link #teardown}; guarded by teardownLock, null when the cause names no side. */
    private ZmuxStream teardownLost;
    /** Guards {@link #teardownLost} and the first set of {@link #teardown}. */
    private final Object teardownLock = new Object();
    /** The caller's future: cancelling it, or completing it from outside, cancels the splice. */
    private final ZmuxFuture<SpliceResult> result = new ZmuxFuture<>(() -> { cancel(); return true; });
    private final Direction forward, reverse;

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
            Thread.ofVirtual().name("zmux-splice").start(splice::run);
            return splice.result;
        } catch (RuntimeException | Error failure) { a.unclaimIo(); b.unclaimIo(); throw failure; }
    }

    private void cancel() {
        cancelled.set(true); forward.interrupt(); reverse.interrupt();
    }

    /**
     * Runs action on the stream's loop and waits for it at most one second (a close's bound): a
     * loop that does not answer by then is stuck, and the splice must not hang on it. The
     * action stays queued and runs if the loop resumes. False on that timeout.
     */
    static boolean ownerCleanup(ZmuxStream stream, Runnable action) {
        var done = new CompletableFuture<Void>();
        // A closing loop may not have run its cleanup hook yet: then the session's end is awaited.
        CompletableFuture<?> wait = stream.session.driver.executeCarrier(() -> {
            try { action.run(); done.complete(null); } catch (Throwable error) { done.completeExceptionally(error); }
        }) ? done : stream.session.driver.closedFuture();
        return ZmuxSession.awaitBounded(wait, io.zmux.internal.core.Session.closeLimit);
    }

    private void run() {
        try { runOwned(); } catch (Throwable error) { result.publishFailure(error); }
    }

    private void runOwned() {
        boolean cleaned = true;
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            tasks.submit(forward::datagrams); tasks.submit(reverse::datagrams);
            Future<?> f = tasks.submit(forward::run), r = tasks.submit(reverse::run);
            try { f.get(); r.get(); }
            catch (InterruptedException error) { cancel(); Thread.currentThread().interrupt(); }
            catch (ExecutionException error) { cancel(); }
            finally { dataDone = true; forward.stopDatagrams(); reverse.stopDatagrams(); }
        } finally {
            a.close(); b.close();
            try { cleaned = ownerCleanup(a, () -> { a.session.driver.lanes.watchRecvEnd(a.st, null); a.session.driver.lanes.watchSendEnd(a.st, null); a.session.driver.lanes.releaseStream(a.st); }); }
            finally { cleaned &= ownerCleanup(b, () -> { b.session.driver.lanes.watchRecvEnd(b.st, null); b.session.driver.lanes.watchSendEnd(b.st, null); b.session.driver.lanes.releaseStream(b.st); }); }
        }
        if (!cleaned) {
            result.publishFailure(new java.net.SocketTimeoutException("splice cleanup timed out: a stream's event loop did not answer within 1 s"));
            return;
        }
        IOException failed = forward.callbackFailure != null ? forward.callbackFailure : reverse.callbackFailure;
        if (failed != null) result.publishFailure(failed);
        else result.publish(new SpliceResult(forward.result(), reverse.result()));
    }

    /**
     * One direction: a worker reads the source's receiving lane and writes the target's
     * sending lane, the same channels as application reads and writes, so forwarding needs
     * no loop hand-off of its own. Lane watchers wake the worker when either side fails
     * while it waits elsewhere (the limiter, the other lane).
     */
    private final class Direction {
        private final ZmuxStream source, target;
        private final Limiter limiter;
        private final AtomicReference<IOException> readFault = new AtomicReference<>(), writeFault = new AtomicReference<>();
        private volatile Thread worker;
        /** The worker waits for the limiter: a source reset must wake it to read the end. */
        private volatile boolean limiting;
        /** A source reset arrived: the limiter wait that begins after it must look again too, as it would not be interrupted. */
        private final AtomicBoolean resetPoke = new AtomicBoolean();
        /** The worker writes taken bytes: a source reset waits for that write. */
        private volatile boolean forwarding;
        /** The worker settles the outcome: watchers no longer interrupt it. */
        private volatile boolean settling;
        /** The settling worker waits for the target to transmit before a reset: failures still wake it. */
        private volatile boolean draining;
        private volatile Thread datagramWorker;
        private long bytes, datagramBytes, drops, tooLarge, limited;
        /** Packet relay: the capsule parser of the source's bytes; only the stream worker uses it. */
        private final Capsule.Header capsule = new Capsule.Header();
        /** Packet relay: the value bytes of the current capsule not yet read. */
        private long capsuleRemaining;
        /**
         * Packet relay: the target's capsule turn. The stream worker holds it from the first byte of
         * a capsule it forwards to the capsule's end; whoever holds it at a boundary writes the
         * datagrams waiting in {@link #converted}.
         */
        private final ReentrantLock turn = new ReentrantLock();
        /** Packet relay: datagrams too large for the target, waiting for a capsule boundary. */
        private final ConcurrentLinkedQueue<Datagrams.StreamDatagram> converted = new ConcurrentLinkedQueue<>();
        private final AtomicInteger convertedCount = new AtomicInteger();
        /** Packet relay: the header of a converted datagram; used by the holder of the turn. */
        private final ByteBuffer convertedHeader = ByteBuffer.allocate(Capsule.MAX_DATAGRAM_HEADER);
        /** Packet relay: the target can take no more capsules (finished or failed); guarded by turn. */
        private volatile boolean targetClosed;
        /** How the direction ended; null while running, when cancelled, or after a callback failure. */
        private SpliceResult.End end;
        /** A limiter callback failed, and the splice completes with it. Written under the direction's lock by its workers; read once both have ended. */
        private IOException callbackFailure;
        /** The worker is in an operation on the target, so a failure it throws is the downstream side's. */
        private boolean atTarget;

        Direction(ZmuxStream source, ZmuxStream target, Limiter limiter) {
            this.source = source; this.target = target; this.limiter = limiter;
        }
        void interrupt() { Thread t = worker; if (t != null) t.interrupt(); }
        void fail(IOException error, boolean reading) {
            AtomicReference<IOException> slot = reading ? readFault : writeFault;
            IOException previous = slot.get();
            boolean changed = previous == null && slot.compareAndSet(null, error);
            if (reading && error instanceof SessionClosedException && previous instanceof StreamResetException) {
                changed = slot.compareAndSet(previous, error);
            }
            if (changed && (!settling || draining)) interrupt();
        }
        /** The source's receiving lane ended with failure (on its loop). */
        void sourceEnded(CoreException e) {
            IOException error = Errors.io(e);
            // A reset is read in order, after the bytes before its Reliable Size; only a
            // worker waiting for the limiter must look again.
            if (error instanceof StreamResetException) { resetPoke.set(true); if (limiting && !settling) interrupt(); return; }
            fail(error, true);
        }
        /** The target's sending lane ended (on its loop). */
        void targetEnded(CoreException e) { fail(Errors.io(e), false); }
        void check() throws IOException {
            if (cancelled.get()) throw new InterruptedIOException("splice cancelled");
            IOException error = writeFault.get(); if (error != null) throw error;
            error = readFault.get(); if (error != null && !(forwarding && error instanceof StreamResetException)) throw error;
        }
        void run() {
            worker = Thread.currentThread();
            Runnable closeRead = source.session.driver.watchClosed(error -> fail(new SessionClosedException(Errors.session(error)), true));
            Runnable closeWrite = target.session.driver.watchClosed(error -> fail(new SessionClosedException(Errors.session(error)), false));
            source.session.driver.post(() -> source.session.driver.lanes.watchRecvEnd(source.st, this::sourceEnded));
            target.session.driver.post(() -> target.session.driver.lanes.watchSendEnd(target.st, this::targetEnded));
            try {
                while (true) {
                    check();
                    long available;
                    try { available = source.in.awaitReadable(Long.MAX_VALUE); }
                    catch (CoreException e) { throw Errors.io(e); }
                    if (available < 0) {
                        StreamRecv.Chunk last;
                        try { last = source.in.readChunk(1, Long.MAX_VALUE); }
                        catch (CoreException e) { throw Errors.io(e); }
                        if (last != null) { if (last.buf() != null) last.buf().unref(); continue; }
                        if (packets && !atBoundary()) { end = truncated(); break; }
                        if (packets) { takeTurn(); closeTurn(); }
                        atTarget = true;
                        try { Errors.check(target.out.finish(Long.MAX_VALUE).await(Long.MAX_VALUE)); }
                        catch (CoreException e) { throw Errors.io(e); }
                        atTarget = false;
                        end = new SpliceResult.End.Finished(); break;
                    }
                    if (available == 0) continue;
                    int n = (int) Math.min(options.chunkSize(), available);
                    if (packets && !converted.isEmpty()) {
                        if (atBoundary()) offerTurn();
                        // Stop at the next capsule boundary, where the waiting datagrams go in.
                        if (!converted.isEmpty()) n = (int) Math.min(n, capsuleRemaining > 0 ? capsuleRemaining : capsule.idle() ? n : 1);
                    }
                    if (limiter != null) {
                        int min = Math.min(n, 16384);
                        CompletableFuture<Integer> ready = Objects.requireNonNull(limiter.ready(min, n)).toCompletableFuture();
                        limiting = true;
                        try {
                            if (resetPoke.compareAndSet(true, false)) throw new InterruptedException(); // A reset beat the flag above.
                            int allowed = ready.get();
                            if (allowed < 1 || allowed > n) throw new CallbackFailed(new CallbackFailedException("invalid limiter allowance"));
                            n = allowed;
                        } catch (InterruptedException error) {
                            ready.cancel(true); check();
                            continue; // A source reset: read again, up to its end.
                        } catch (ExecutionException error) {
                            throw new CallbackFailed(new CallbackFailedException("limiter failed", error.getCause()));
                        } finally {
                            limiting = false;
                        }
                        check();
                    }
                    StreamRecv.Chunk chunk;
                    try { chunk = source.in.readChunk(n, Long.MAX_VALUE); }
                    catch (CoreException e) { throw Errors.io(e); }
                    if (chunk == null) continue; // The end: the next pass forwards it.
                    forwarding = true;
                    int length = chunk.data().remaining();
                    if (packets) {
                        try { takeTurn(); }
                        catch (InterruptedIOException interrupted) { forwarding = false; if (chunk.buf() != null) chunk.buf().unref(); throw interrupted; }
                        scan(chunk.data().duplicate());
                    }
                    ReadBuf held = chunk.buf() != null ? chunk.buf() : new ReadBuf(chunk.data(), null);
                    atTarget = true;
                    try {
                        target.out.write(null, chunk.data(), held, Long.MAX_VALUE);
                        bytes += length;
                        if (limiter != null) limiter.charge(length);
                    } catch (InterruptedIOException interrupted) {
                        int forwarded = interrupted.bytesTransferred;
                        bytes += forwarded;
                        if (limiter != null && forwarded > 0) limiter.charge(forwarded);
                        throw interrupted;
                    } catch (CoreException e) {
                        throw Errors.io(e);
                    } finally {
                        forwarding = false;
                        held.unref();
                    }
                    atTarget = false;
                    if (packets && atBoundary()) giveTurn();
                }
            } catch (CallbackFailed failed) {
                settling = true;
                Errors.clearInternalWake();
                callbackFailed(failed.cause);
            } catch (IOException error) {
                settling = true;
                Errors.clearInternalWake();
                IOException outcome = error;
                IOException terminal = writeFault.get();
                if (terminal == null) terminal = readFault.get();
                if (terminal != null && !cancelled.get()) outcome = terminal;
                IOException cause = teardown.get();
                if (cause != null && outcome instanceof StreamClosedException) outcome = cause;
                finish(outcome);
            } catch (RuntimeException error) {
                settling = true;
                Errors.clearInternalWake();
                callbackFailed(new CallbackFailedException("relay callback failed", error));
            } finally {
                settling = true; worker = null; closeRead.run(); closeWrite.run();
                if (packets) {
                    if (!turn.isHeldByCurrentThread()) turn.lock();
                    try { targetClosed = true; } finally { turn.unlock(); }
                }
            }
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

        private boolean atBoundary() { return capsuleRemaining == 0 && capsule.idle(); }

        /** Takes the capsule turn for the stream worker, unless it holds it already; a watcher's interrupt ends the wait. */
        private void takeTurn() throws InterruptedIOException {
            if (turn.isHeldByCurrentThread()) return;
            try { turn.lockInterruptibly(); }
            catch (InterruptedException interrupted) { throw new InterruptedIOException("splice interrupted"); }
        }

        /** At a capsule boundary: writes the waiting datagrams and releases the turn. */
        private void giveTurn() {
            try { writeConverted(); } finally { turn.unlock(); }
            offerTurn();
        }

        /** Before the target finishes: the waiting datagrams go first; later ones are dropped. */
        private void closeTurn() {
            try { writeConverted(); targetClosed = true; } finally { turn.unlock(); }
        }

        /** Writes the waiting datagrams if the turn is free; a datagram queued after the holder's last look is written by whoever comes next. */
        private void offerTurn() {
            if (turn.isHeldByCurrentThread()) return; // Mid-capsule: the boundary writes them.
            while (!converted.isEmpty() && turn.tryLock()) {
                try { writeConverted(); } finally { turn.unlock(); }
            }
        }

        /** Writes the waiting datagrams as DATAGRAM capsules; holds the turn, at a capsule boundary. */
        private void writeConverted() {
            for (Datagrams.StreamDatagram d; (d = converted.poll()) != null; ) {
                convertedCount.decrementAndGet();
                int n = d.payload().length;
                if (targetClosed) { dropTooLarge(); continue; }
                convertedHeader.clear();
                Capsule.putDatagramHeader(convertedHeader, d.context(), n);
                convertedHeader.flip();
                boolean started = false;
                try {
                    target.out.write(convertedHeader, null, null, Long.MAX_VALUE);
                    started = true;
                    target.out.write(ByteBuffer.wrap(d.payload()), null, null, Long.MAX_VALUE);
                    synchronized (this) { datagramBytes += n; }
                    if (limiter != null) limiter.charge(n);
                } catch (CoreException | InterruptedIOException error) {
                    // The target failed or the splice ends: the stream worker reports it. A capsule cut short can be followed by nothing.
                    if (started) targetClosed = true;
                    dropTooLarge();
                    if (error instanceof InterruptedIOException) { targetClosed = true; Thread.currentThread().interrupt(); }
                }
            }
        }

        private synchronized void dropTooLarge() { drops++; tooLarge++; }
        private synchronized void dropLimited() { drops++; limited++; }

        /** The source ended inside a capsule: the target is reset and the source stopped with ZMUX_PACKET_ERROR. */
        private SpliceResult.End truncated() {
            long code = io.zmux.internal.wire.TransportError.PACKET_ERROR;
            source.stop(code); target.reset(code);
            return new SpliceResult.End.Reset(code);
        }
        private void finish(IOException error) {
            if (cancelled.get() || error instanceof InterruptedIOException) {
                end = null; abortBoth(source, target, 0); return;
            }
            if (error instanceof StreamResetException reset) {
                long code = reset.code();
                end = new SpliceResult.End.Reset(code);
                try {
                    boolean reliable = (Boolean)target.session.run(op -> target.session.core.sending().resetAtInUse());
                    // Without RESET_AT a reset drops unsent bytes: forward what was taken first.
                    if (!reliable) {
                        checkDrain();
                        draining = true;
                        try { Errors.check(target.out.awaitSent().await(Long.MAX_VALUE)); } finally { draining = false; }
                    }
                    target.session.run(op -> {
                        target.session.driver.lanes.flushSending(target.st);
                        if (bytes > 0 && code != io.zmux.carrier.PipeIo.CARRIER_FAILED) target.session.core.sending().setReliableBoundary(target.st);
                        target.session.driver.lanes.resetSending(target.st, code); return true;
                    });
                } catch (IOException failure) {
                    Errors.clearInternalWake();
                    // A fault of either side explains the failure better than the wait it interrupted.
                    IOException next = readFault.get() instanceof SessionClosedException lost ? lost : writeFault.get();
                    if (next == null) next = readFault.get();
                    if (next == null || cancelled.get()) next = failure;
                    IOException cause = teardown.get();
                    if (cause != null && next instanceof StreamClosedException) next = cause;
                    finish(next);
                }
            } else if (error instanceof StreamStoppedException stopped) {
                end = new SpliceResult.End.Stopped(stopped.code()); source.stop(stopped.code());
            } else {
                // A lost session, or a carrier failure of one side: the side it came from.
                boolean downstream;
                synchronized (teardownLock) {
                    // The other direction's failure tore this one down: its cause names the side that was lost.
                    if (error == teardown.get() && teardownLost != null) downstream = teardownLost == target;
                    else downstream = error == writeFault.get() || (error != readFault.get() && atTarget);
                    if (teardown.get() == null) { teardown.set(error); teardownLost = downstream ? target : source; }
                }
                end = downstream ? new SpliceResult.End.DownstreamLost() : new SpliceResult.End.UpstreamLost();
                abortBoth(source, target, io.zmux.carrier.PipeIo.CARRIER_FAILED);
            }
        }
        private void checkDrain() throws IOException {
            if (cancelled.get()) throw new InterruptedIOException("splice cancelled");
            IOException error = writeFault.get(); if (error != null) throw error;
            error = readFault.get(); if (error != null && !(error instanceof StreamResetException)) throw error;
            Errors.clearInternalWake(); // Internal observer interrupts have already been classified above.
            source.session.run(op -> {
                if (source.session.core.error() != null) throw CoreException.session(source.session.core.error());
                return true;
            });
            target.session.run(op -> {
                CoreException failure = target.session.core.sending().writeError(target.st);
                if (failure != null) throw failure;
                return true;
            });
        }
        void stopDatagrams() { Thread dg = datagramWorker; if (dg != null) dg.interrupt(); }
        void datagrams() {
            datagramWorker = Thread.currentThread();
            try {
                if (!source.session.info().datagramFlows()) return;
                while (!dataDone && !cancelled.get()) {
                    Datagrams.StreamDatagram data = (Datagrams.StreamDatagram)source.session.call(op -> {
                        if (dataDone || cancelled.get()) throw CoreException.of(CoreException.Reason.INVALID);
                        Datagrams.StreamDatagram d = source.session.core.datagrams().receive(source.st);
                        if (d != null) return d;
                        if (source.session.core.error() != null) throw CoreException.session(source.session.core.error());
                        source.session.core.datagrams().waitReceive(source.st, op); return Operation.PENDING;
                    }, op -> source.session.core.datagrams().cancelReceiveWait(source.st, op), Long.MAX_VALUE);
                    if (dataDone || cancelled.get()) { synchronized (this) { drops++; } break; }
                    int n = data.payload().length;
                    if (limiter != null && limiter.tryReady(n) < n) { dropLimited(); continue; }
                    int max = target.maxDatagramPayload(data.context());
                    if (!packets || (max > 0 && n <= max)) {
                        try {
                            if (target.session.sendDatagram(target.st, data.context(), ByteBuffer.wrap(data.payload()))) {
                                synchronized (this) { datagramBytes += n; }
                                if (limiter != null) limiter.charge(n);
                            } else dropLimited();
                            continue;
                        } catch (NotNegotiatedException | DatagramTooLargeException error) {
                            if (!packets) { dropTooLarge(); continue; }
                        }
                    }
                    // A packet relay writes it as a capsule at the next boundary; a bounded wait, never a queue of its own.
                    if (convertedCount.incrementAndGet() > CONVERTED) { convertedCount.decrementAndGet(); dropTooLarge(); continue; }
                    converted.add(data);
                    offerTurn();
                }
            } catch (IOException error) {
                // Stream workers own terminal propagation; datagrams never add a waiting queue.
            } catch (RuntimeException error) {
                // A limiter callback (or the relay itself) failed: the splice reports it as a stream worker's would.
                if (!cancelled.get()) callbackFailed(new CallbackFailedException("relay callback failed", error));
            } finally { datagramWorker = null; }
        }

        /**
         * A callback failed: the splice completes exceptionally with cause (the direction's first such failure, of its stream
         * or its datagram worker) and both streams are torn down.
         */
        private void callbackFailed(IOException cause) {
            synchronized (this) { if (callbackFailure == null) callbackFailure = cause; }
            teardown.compareAndSet(null, cause);
            abortBoth(source, target, io.zmux.carrier.PipeIo.CARRIER_FAILED);
        }

        SpliceResult.Direction result() {
            synchronized (this) {
                return new SpliceResult.Direction(bytes, datagramBytes, drops + convertedCount.get(), tooLarge + convertedCount.get(), limited, end);
            }
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
        a.stop(code); a.reset(code); b.stop(code); b.reset(code);
    }
}
