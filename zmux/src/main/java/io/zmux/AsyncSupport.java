package io.zmux;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

final class AsyncSupport {
    private static final AtomicInteger THREAD_ID = new AtomicInteger();
    private static final ScheduledThreadPoolExecutor EXECUTOR = executor();

    private AsyncSupport() {
    }

    static CompletableFuture<Void> completedVoid() {
        return CompletableFuture.completedFuture(null);
    }

    static <T> CompletableFuture<T> failed(Throwable failure) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(failure);
        return future;
    }

    static void execute(Runnable task) {
        EXECUTOR.execute(task);
    }

    static void schedule(Runnable task, long delayNanos) {
        EXECUTOR.schedule(task, Math.max(0L, delayNanos), TimeUnit.NANOSECONDS);
    }

    private static ScheduledThreadPoolExecutor executor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
                Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
                threadFactory()
        );
        executor.setRemoveOnCancelPolicy(true);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return executor;
    }

    private static ThreadFactory threadFactory() {
        ThreadFactory base = Executors.defaultThreadFactory();
        return runnable -> {
            Thread thread = base.newThread(runnable);
            thread.setName("zmux-async-" + THREAD_ID.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
