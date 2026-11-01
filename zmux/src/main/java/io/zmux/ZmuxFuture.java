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
 * {@code *Async} stage variants with your own executor for work that blocks. A future that
 * could complete at once is returned completed, and its dependents run on the calling thread.
 *
 * <p>Dependent stages are plain {@link CompletableFuture}s.
 */
public final class ZmuxFuture<T> extends CompletableFuture<T> {
    private final BooleanSupplier operation;

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

    /** Cancels the operation before a completion from outside; whether it was still running does not matter. */
    private void stopOperation() {
        boolean unused = operation.getAsBoolean();
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
        if (!operation.getAsBoolean()) return false;
        return super.completeExceptionally(new CancellationException("operation cancelled")) || isCancelled();
    }

    @Override
    public boolean complete(T value) {
        if (isDone()) return false;
        stopOperation();
        return super.complete(value);
    }

    @Override
    public boolean completeExceptionally(Throwable error) {
        if (isDone()) return false;
        stopOperation();
        return super.completeExceptionally(error);
    }

    @Override
    public void obtrudeValue(T value) {
        stopOperation();
        super.obtrudeValue(value);
    }

    @Override
    public void obtrudeException(Throwable error) {
        stopOperation();
        super.obtrudeException(error);
    }

    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() {
        return new CompletableFuture<>();
    }
}
