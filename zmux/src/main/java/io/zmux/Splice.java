package io.zmux;

import io.zmux.helper.Limiter;
import io.zmux.internal.core.Datagrams;
import io.zmux.internal.core.CoreException;
import io.zmux.internal.core.ReadBuf;
import io.zmux.internal.core.StreamRecv;
import io.zmux.internal.loop.Operation;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Owns relay work, never protocol state. All core access goes through the existing owner. */
final class Splice {
    private final ZmuxStream a, b;
    private final SpliceOptions options;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private volatile boolean dataDone;
    /** Why the first direction to fail tore both streams down; the other direction's local-close faults follow from it. */
    private final AtomicReference<IOException> teardown = new AtomicReference<>();
    /** The caller's stage: cancelling it cancels the splice. */
    private final CompletableFuture<SpliceResult> result = new CompletableFuture<>() {
        @Override public boolean cancel(boolean mayInterruptIfRunning) {
            boolean done = super.cancel(mayInterruptIfRunning);
            if (done) Splice.this.cancel();
            return done;
        }
    };
    private final Direction forward, reverse;

    private Splice(ZmuxStream a, ZmuxStream b, SpliceOptions options) {
        this.a = a; this.b = b; this.options = options;
        forward = new Direction(a, b, options.aToB()); reverse = new Direction(b, a, options.bToA());
    }

    static CompletionStage<SpliceResult> start(ZmuxStream a, ZmuxStream b, SpliceOptions options) {
        Objects.requireNonNull(a); Objects.requireNonNull(b); Objects.requireNonNull(options);
        if (a.session.driver == b.session.driver && a.st == b.st) throw new IllegalArgumentException("cannot splice a stream into itself");
        a.claimIo();
        try { b.claimIo(); } catch (RuntimeException error) { a.unclaimIo(); throw error; }
        try {
            Splice splice = new Splice(a, b, options);
            Thread.ofVirtual().name("zmux-splice").start(splice::run);
            return splice.result;
        } catch (RuntimeException | Error failure) { a.unclaimIo(); b.unclaimIo(); throw failure; }
    }

    private void cancel() {
        cancelled.set(true); forward.interrupt(); reverse.interrupt();
    }

    private static void ownerCleanup(ZmuxStream stream, Runnable action) {
        var done = new CompletableFuture<Void>();
        if (stream.session.driver.executeCarrier(() -> {
            try { action.run(); done.complete(null); } catch (Throwable error) { done.completeExceptionally(error); }
        })) done.join();
        else stream.session.driver.closedFuture().join(); // A closing loop may not have run its cleanup hook yet.
    }

    private void run() {
        try { runOwned(); } catch (Throwable error) { result.completeExceptionally(error); }
    }

    private void runOwned() {
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            tasks.submit(forward::datagrams); tasks.submit(reverse::datagrams);
            Future<?> f = tasks.submit(forward::run), r = tasks.submit(reverse::run);
            try { f.get(); r.get(); }
            catch (InterruptedException error) { cancel(); Thread.currentThread().interrupt(); }
            catch (ExecutionException error) { cancel(); }
            finally { dataDone = true; forward.stopDatagrams(); reverse.stopDatagrams(); }
        } finally {
            a.close(); b.close();
            try { ownerCleanup(a, () -> { a.session.driver.lanes.watchRecvEnd(a.st, null); a.session.driver.lanes.watchSendEnd(a.st, null); a.session.driver.lanes.releaseStream(a.st); }); }
            finally { ownerCleanup(b, () -> { b.session.driver.lanes.watchRecvEnd(b.st, null); b.session.driver.lanes.watchSendEnd(b.st, null); b.session.driver.lanes.releaseStream(b.st); }); }
        }
        IOException failed = forward.callbackFailure != null ? forward.callbackFailure : reverse.callbackFailure;
        if (failed != null) result.completeExceptionally(failed);
        else result.complete(new SpliceResult(forward.result(), reverse.result()));
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
        /** The worker writes taken bytes: a source reset waits for that write. */
        private volatile boolean forwarding;
        /** The worker settles the outcome: watchers no longer interrupt it. */
        private volatile boolean settling;
        /** The settling worker waits for the target to transmit before a reset: failures still wake it. */
        private volatile boolean draining;
        private volatile Thread datagramWorker;
        private long bytes, datagramBytes, drops;
        /** How the direction ended; null while running, when cancelled, or after a callback failure. */
        private SpliceResult.End end;
        /** A limiter callback failed: the splice completes with it. */
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
            if (error instanceof StreamResetException) { if (limiting && !settling) interrupt(); return; }
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
                        atTarget = true;
                        try { Errors.check(target.out.finish(Long.MAX_VALUE).await(Long.MAX_VALUE)); }
                        catch (CoreException e) { throw Errors.io(e); }
                        atTarget = false;
                        end = new SpliceResult.End.Finished(); break;
                    }
                    if (available == 0) continue;
                    int n = (int) Math.min(options.chunkSize(), available);
                    if (limiter != null) {
                        int min = Math.min(n, 16384);
                        CompletableFuture<Integer> ready = Objects.requireNonNull(limiter.ready(min, n)).toCompletableFuture();
                        limiting = true;
                        try {
                            int allowed = ready.get();
                            if (allowed < 1 || allowed > n) throw new CallbackFailed(new IOException("invalid limiter allowance"));
                            n = allowed;
                        } catch (InterruptedException error) {
                            ready.cancel(true); check();
                            continue; // A source reset: read again, up to its end.
                        } catch (ExecutionException error) {
                            throw new CallbackFailed(new IOException("limiter failed", error.getCause()));
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
                }
            } catch (CallbackFailed failed) {
                settling = true;
                Errors.clearInternalWake();
                callbackFailure = failed.cause;
                teardown.compareAndSet(null, failed.cause);
                abortBoth(source, target, io.zmux.carrier.PipeIo.CARRIER_FAILED);
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
                callbackFailure = new IOException("relay callback failed", error);
                teardown.compareAndSet(null, callbackFailure);
                abortBoth(source, target, io.zmux.carrier.PipeIo.CARRIER_FAILED);
            } finally {
                settling = true; worker = null; closeRead.run(); closeWrite.run();
            }
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
                        target.session.driver.lanes.resetSending(target.st, code); return Boolean.TRUE;
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
                boolean downstream = error == writeFault.get() || error != readFault.get() && atTarget;
                end = downstream ? new SpliceResult.End.DownstreamLost() : new SpliceResult.End.UpstreamLost();
                teardown.compareAndSet(null, error);
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
                return Boolean.TRUE;
            });
            target.session.run(op -> {
                CoreException failure = target.session.core.sending().writeError(target.st);
                if (failure != null) throw failure;
                return Boolean.TRUE;
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
                    if (dataDone || cancelled.get()) { drops++; break; }
                    int n = data.payload().length;
                    if (limiter != null && limiter.tryReady(n) < n) { drops++; continue; }
                    try {
                        if (target.session.sendDatagram(target.st, data.context(), java.nio.ByteBuffer.wrap(data.payload()))) {
                            datagramBytes += n; if (limiter != null) limiter.charge(n);
                        } else drops++;
                    } catch (NotNegotiatedException | DatagramTooLargeException error) { drops++; }
                }
            } catch (IOException | RuntimeException error) {
                // Stream workers own terminal propagation; datagrams never add a waiting queue.
            } finally { datagramWorker = null; }
        }

        SpliceResult.Direction result() { return new SpliceResult.Direction(bytes, datagramBytes, drops, end); }
    }

    /** A limiter callback failed or broke its contract; carries what the splice completes with. */
    private static final class CallbackFailed extends IOException {
        final IOException cause;
        CallbackFailed(IOException cause) { super(cause.getMessage(), cause); this.cause = cause; }
    }

    private static void abortBoth(ZmuxStream a, ZmuxStream b, long code) {
        a.stop(code); a.reset(code); b.stop(code); b.reset(code);
    }
}
