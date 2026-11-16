package io.zmux.internal.spi;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The one way the carrier modules hand work to an event loop they borrow (a Netty loop is an
 * {@link Executor} and a {@link ScheduledExecutorService}). A loop that is shut down refuses
 * tasks and timers; the caller then gets {@code onRejected} on its own thread, where it fails
 * what it alone owns (a write's completion, a reader's sink, a chunk it was handed) and touches
 * nothing of the loop's. What the loop's objects hold ends by their own termination paths: the
 * close of their channel, or the loop's termination future, never only a task or a timer.
 */
public final class Loops {
    private Loops() {}

    /**
     * Queues task on loop; a loop that refuses it runs onRejected on the calling thread instead.
     * Returns whether task was queued.
     */
    public static boolean run(Executor loop, Runnable task, Runnable onRejected) {
        try {
            loop.execute(task);
            return true;
        } catch (RejectedExecutionException shutDown) {
            onRejected.run();
            return false;
        }
    }

    /** Runs task now when the caller is on the loop (onLoop), and queues it as {@link #run} does otherwise. */
    public static void dispatch(Executor loop, boolean onLoop, Runnable task, Runnable onRejected) {
        if (onLoop) task.run();
        else run(loop, task, onRejected);
    }

    /** Runs task on loop after delay; a loop that refuses the timer runs onRejected and gives null. */
    public static ScheduledFuture<?> schedule(ScheduledExecutorService loop, Runnable task, long delay, TimeUnit unit, Runnable onRejected) {
        try {
            return loop.schedule(task, delay, unit);
        } catch (RejectedExecutionException shutDown) {
            onRejected.run();
            return null;
        }
    }

    /** Cancels a timer that {@link #schedule} gave, if any. */
    public static void cancel(ScheduledFuture<?> timer) {
        if (timer != null) timer.cancel(false);
    }
}
