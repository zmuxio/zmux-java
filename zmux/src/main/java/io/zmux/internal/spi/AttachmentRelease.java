package io.zmux.internal.spi;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

/** A single-use share of an attachment. Only a live share can register another owner. */
public final class AttachmentRelease implements AutoCloseable {
    /** A transport with asynchronous physical close and a possible final write completion. */
    public interface Resource {
        CompletionStage<Void> whenClosed();
        CompletionStage<Void> writesReleased();
    }

    private static final class Lifetime {
        final AtomicInteger owners = new AtomicInteger(1);
        final CompletableFuture<Void> done = new CompletableFuture<>();
        final java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
        Runnable retire;

        Lifetime(Runnable retire) { this.retire = retire; }

        void release() {
            if (owners.decrementAndGet() != 0) return;
            try { if (retire != null) retire.run(); }
            finally {
                retire = null;
                Throwable cause = failure.get();
                if (cause == null) done.complete(null); else done.completeExceptionally(cause);
            }
        }
    }

    private final Lifetime lifetime;
    private boolean held = true;
    public AttachmentRelease() { this((Runnable) null); }
    public AttachmentRelease(Runnable retire) { lifetime = new Lifetime(retire); }
    private AttachmentRelease(Lifetime lifetime) { this.lifetime = lifetime; }

    private static final AttachmentRelease INACTIVE = new AttachmentRelease();
    static { INACTIVE.close(); }

    public static AttachmentRelease inactive() { return INACTIVE; }
    public boolean active() { return this != INACTIVE; }

    public AttachmentRelease fork() {
        synchronized (this) {
            if (this == INACTIVE) return INACTIVE;
            if (!held) throw new IllegalStateException("attachment guard already returned");
            lifetime.owners.incrementAndGet();
            return new AttachmentRelease(lifetime);
        }
    }

    public CompletableFuture<Void> completion() { return lifetime.done.copy(); }
    // The fork is handed to until/untilClosed, which own it and close it when work ends.
    @SuppressWarnings("resource")
    public void track(CompletionStage<?> work) { fork().until(work); }
    // The fork is handed to until/untilClosed, which own it and close it when work ends.
    @SuppressWarnings("resource")
    public void track(Resource carrier) { fork().untilClosed(carrier); }

    public void untilClosed(Resource carrier) {
        if (!active()) return;
        try {
            until(carrier.whenClosed().handle((v, error) -> {
                if (error != null) failed(error);
                return carrier.writesReleased();
            }).thenCompose(work -> work));
        } catch (RuntimeException failure) {
            failed(failure);
            close();
            throw failure;
        }
    }

    public void failed(Throwable cause) { if (active()) lifetime.failure.compareAndSet(null, cause); }

    /** Transfer this share to the work. Closing the old handle cannot release it early. */
    public void until(CompletionStage<?> work) {
        if (!active()) return;
        synchronized (this) {
            if (!held) throw new IllegalStateException("attachment guard already returned");
            held = false;
        }
        var moved = new AttachmentRelease(lifetime);
        try {
            work.whenComplete((v, error) -> {
                if (error != null) failed(error);
                moved.close();
            });
        } catch (RuntimeException failure) {
            failed(failure);
            moved.close();
            throw failure;
        }
    }

    @Override public void close() {
        synchronized (this) {
            if (!held) return;
            held = false;
        }
        lifetime.release();
    }
}
