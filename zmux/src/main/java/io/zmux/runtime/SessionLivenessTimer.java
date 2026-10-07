package io.zmux.runtime;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared timer for session liveness work that must not depend on the session writer thread: keepalive deadline
 * evaluation (a writer blocked in a transport write cannot run it) and the bounded wait for a final CLOSE.
 *
 * <p>The timer thread itself never runs a task: due tasks are handed to a cached pool of worker threads, so a task
 * that has to wait for a session lock can only delay its own session, never the timer or other sessions. Anything
 * that may block for long, such as finishing a session or emitting events to application handlers, still runs on a
 * short-lived daemon thread via {@link #runDetached}. This timer is separate from {@link AsyncSupport}, whose threads
 * run blocking async API calls and could otherwise starve it.
 */
final class SessionLivenessTimer {
    private static final AtomicInteger THREAD_ID = new AtomicInteger();
    private static final ScheduledThreadPoolExecutor EXECUTOR = executor();
    private static final ThreadPoolExecutor WORKERS = workers();

    private SessionLivenessTimer() {
    }

    static ScheduledFuture<?> schedule(Runnable task, long delayNanos) {
        return EXECUTOR.schedule(() -> WORKERS.execute(task), Math.max(0L, delayNanos), TimeUnit.NANOSECONDS);
    }

    static void runDetached(String name, Runnable task) {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        thread.start();
    }

    private static ScheduledThreadPoolExecutor executor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threadFactory("zmux-liveness-timer-"));
        executor.setRemoveOnCancelPolicy(true);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return executor;
    }

    private static ThreadPoolExecutor workers() {
        return new ThreadPoolExecutor(
                0,
                Integer.MAX_VALUE,
                30L,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                threadFactory("zmux-liveness-worker-")
        );
    }

    private static ThreadFactory threadFactory(String namePrefix) {
        ThreadFactory base = Executors.defaultThreadFactory();
        return runnable -> {
            Thread thread = base.newThread(runnable);
            thread.setName(namePrefix + THREAD_ID.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
