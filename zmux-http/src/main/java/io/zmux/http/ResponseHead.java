package io.zmux.http;

import io.zmux.adapter.Upgrade;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/** The admission can stop waiting while Netty still owns the response write. */
final class ResponseHead implements Upgrade {
    private final CompletableFuture<Void> opened = new CompletableFuture<>();
    private final CompletableFuture<Void> released = new CompletableFuture<>();
    private final java.util.concurrent.atomic.AtomicBoolean submitted = new java.util.concurrent.atomic.AtomicBoolean();
    private final Consumer<ResponseHead> submit;

    ResponseHead(Consumer<ResponseHead> submit, Runnable abort) {
        this.submit = submit;
        opened.whenComplete((v, error) -> { if (opened.isCancelled()) abort.run(); });
    }

    @Override public CompletableFuture<Void> open() {
        if (!submitted.compareAndSet(false, true)) return opened;
        try { submit.accept(this); }
        catch (RuntimeException error) { written(error); }
        return opened;
    }

    @Override public CompletionStage<Void> released() { return released.minimalCompletionStage(); }
    boolean cancelled() { return opened.isCancelled(); }

    /** Called by the actual write listener or by the owner that discarded an unsubmitted write. */
    void written(Throwable error) {
        released.complete(null);
        if (error == null) opened.complete(null);
        else opened.completeExceptionally(error);
    }
}
