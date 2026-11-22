package io.zmux;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

/**
 * The result of an asynchronous zmux operation.
 *
 * <p><b>Cancellation.</b> {@link #cancel} never waits. It returns true when the operation
 * will not complete any more; from then on the operation does not touch a buffer it
 * borrowed, so the caller may reuse it. It returns false once the operation's outcome was
 * decided, even if the future is not completed yet. Completing the future yourself
 * ({@link #complete}, {@link #completeExceptionally}, {@link #orTimeout}…) first cancels the
 * operation in the same way, so the buffer rule holds for every completion.
 *
 * <p><b>Where completions run.</b> A future the operation completes later completes on the
 * session's completion context: one thread at a time, in the order the session decided the
 * outcomes, never an event loop. A dependent stage that blocks there delays the session's
 * later completions; do not wait there for another future of the same session, and use the
 * {@code *Async} stage variants with your own executor for work that blocks. Blocking zmux calls are allowed
 * there, except that a call that would wait for its stream direction's turn behind an asynchronous operation of that
 * direction still in progress throws {@link CompletionThreadBlockedException} (an {@link IllegalStateException}), as that operation needs this thread to go
 * on: a blocking read, {@code readChunk}, write or {@code finish}, {@code tryRead} while data is readable, and a transfer of the
 * stream ({@code asPipe}, {@link Zmux#splice}), which waits for both directions. {@code awaitReadable} never waits for the
 * direction's turn.
 * A future that could complete at once is returned completed, and its dependents run on the calling thread.
 *
 * <p>Dependent stages are plain {@link CompletableFuture}s.
 */
public final class ZmuxFuture<T> extends CompletableFuture<T> {
    private final BooleanSupplier operation;

    static {
        io.zmux.internal.spi.Spi.install(ZmuxFuture::of);
    }

    /**
     * A future completed as stage is; cancelling it runs cancel (null: nothing). A value that
     * arrives after the future was cancelled or completed from outside is discarded.
     */
    static <T> ZmuxFuture<T> of(java.util.concurrent.CompletionStage<T> stage, Runnable cancel) {
        ZmuxFuture<T> f = new ZmuxFuture<>(() -> {
            if (cancel != null) cancel.run();
            return true;
        });
        stage.whenComplete((v, e) -> {
            if (e == null) f.publishOrDiscard(v);
            else f.publishFailure(e instanceof java.util.concurrent.CompletionException c && c.getCause() != null ? c.getCause() : e);
        });
        return f;
    }

    ZmuxFuture(BooleanSupplier cancelOperation) {
        this.operation = cancelOperation;
    }

    static <T> ZmuxFuture<T> completed(T value) {
        ZmuxFuture<T> f = new ZmuxFuture<>(() -> false);
        f.publish(value);
        return f;
    }

    static <T> ZmuxFuture<T> failed(Throwable error) {
        ZmuxFuture<T> f = new ZmuxFuture<>(() -> false);
        f.publishFailure(error);
        return f;
    }

    /** A blocking body run by {@link #onThread}. */
    @FunctionalInterface
    interface Blocking<T> {
        T call() throws Exception;
    }

    /**
     * Runs body on a new virtual thread named name and completes with its result or failure;
     * cancelling the future interrupts that thread. A value the body returns after the future was
     * cancelled or completed from outside is discarded.
     */
    static <T> ZmuxFuture<T> onThread(String name, Blocking<T> body) {
        return onThread(name, body, new WorkerStop());
    }

    /** A worker receives an interrupt as its wake-up and retains the caller's actual failure. */
    static final class WorkerStop implements BooleanSupplier {
        private final java.util.concurrent.atomic.AtomicReference<Throwable> cause = new java.util.concurrent.atomic.AtomicReference<>();
        private Thread runner;

        Throwable reason(Throwable error) {
            Throwable original = cause.get();
            return original != null && error instanceof java.io.InterruptedIOException
                    && !(error instanceof java.net.SocketTimeoutException) ? original : error;
        }

        boolean stop(Throwable why) {
            if (!cause.compareAndSet(null, why)) return false;
            runner.interrupt();
            return true;
        }

        @Override public boolean getAsBoolean() { return stop(new CancellationException("worker stopped")); }
    }

    static <T> ZmuxFuture<T> onThread(String name, Blocking<T> body, WorkerStop stop) {
        ZmuxFuture<T> f = new ZmuxFuture<>(stop);
        stop.runner = Thread.ofVirtual().name(name).unstarted(() -> {
            try {
                T value = body.call();
                if (stop.cause.get() == null) f.publishOrDiscard(value); else discard(value);
            } catch (Throwable error) {
                if (stop.cause.get() == null) f.publishFailure(error);
            }
        });
        stop.runner.start();
        return f;
    }

    /** Preserve a worker's cause before waking it. Other operations settle their own cancellation. */
    private boolean stopOperation(Throwable cause) {
        return (cause != null && operation instanceof WorkerStop worker) ? worker.stop(cause) : operation.getAsBoolean();
    }

    /**
     * Publishes value; when the future was decided already, so that nobody receives value,
     * releases what it holds: a session is aborted, a pipe input's halves are closed, any other
     * closeable is closed.
     */
    void publishOrDiscard(T value) {
        if (!super.complete(value)) discard(value);
    }

    static void discard(Object value) {
        switch (value) {
            case ZmuxSession s -> s.abort(0, "operation cancelled");
            case io.zmux.carrier.PipeIo io -> {
                try {
                    if (io.reader() != null) io.reader().close();
                } finally {
                    if (io.writer() != null) io.writer().abort(new java.io.IOException("operation cancelled"));
                }
            }
            case AutoCloseable c -> {
                try {
                    c.close();
                } catch (Exception e) {
                    // Nobody holds the value any more: there is no one to report a failed close to.
                }
            }
            case null, default -> { }
        }
    }

    void publish(T value) {
        super.complete(value);
    }

    void publishFailure(Throwable error) {
        super.completeExceptionally(error);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (isDone()) return isCancelled();
        var error = new CancellationException("operation cancelled");
        if (!stopOperation(error)) return false;
        return super.completeExceptionally(error) || isCancelled();
    }

    @Override
    public boolean complete(T value) {
        if (isDone()) return false;
        stopOperation(null);
        return super.complete(value);
    }

    @Override
    public boolean completeExceptionally(Throwable error) {
        if (isDone()) return false;
        stopOperation(error);
        return super.completeExceptionally(error);
    }

    @Override
    public void obtrudeValue(T value) {
        stopOperation(null);
        super.obtrudeValue(value);
    }

    @Override
    public void obtrudeException(Throwable error) {
        stopOperation(error);
        super.obtrudeException(error);
    }

    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() {
        return new CompletableFuture<>();
    }
}
