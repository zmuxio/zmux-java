package io.zmux.internal.loop;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The session's one command queue from other threads to its loop (MPSC). A command is
 * submitted to the loop only when the queue goes from idle to busy, so a burst of commands
 * costs one hand-off and at most one selector wake-up. One drain runs the commands queued
 * before it started; later ones get the next drain, after the loop's next I/O.
 */
public final class Mailbox implements Runnable {
    /** A command run on the loop; abandon is called instead when the session already ended. */
    public interface Command {
        void run();

        default void abandon(Throwable terminal) {}
    }

    private final SessionHost loop;
    private final SessionRuntime driver;
    private final ConcurrentLinkedQueue<Command> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queued = new AtomicInteger();
    private final AtomicBoolean scheduled = new AtomicBoolean();
    private volatile boolean closed;

    Mailbox(SessionHost loop, SessionRuntime driver) {
        this.loop = loop;
        this.driver = driver;
    }

    /** Queues c for the loop; false when the session ended (the caller settles c itself). */
    public boolean offer(Command c) {
        if (closed) return false;
        queue.add(c);
        if (closed && queue.remove(c)) return false; // The final drain may already have passed.
        queued.incrementAndGet();
        schedule();
        return true;
    }

    private void schedule() {
        if (!scheduled.get() && scheduled.compareAndSet(false, true) && !loop.tryExecute(this)) {
            // The loop is closing: its shutdown hook ends the session, which drains the queue.
            scheduled.set(false);
        }
    }

    /** Runs the commands queued so far, on the loop. */
    @Override
    public void run() {
        scheduled.set(false);
        if (closed) return;
        for (int n = queued.getAndSet(0); n > 0; n--) {
            Command c = queue.poll();
            if (c == null) break;
            try {
                c.run();
            } catch (RuntimeException e) {
                driver.internalError(e);
            }
            if (closed) return;
        }
        driver.markDirty();
        if (!queue.isEmpty()) schedule();
    }

    /** Accepts nothing more and abandons what is queued; on the loop, once. */
    void close(Throwable terminal) {
        closed = true;
        for (Command c; (c = queue.poll()) != null; ) {
            try {
                c.abandon(terminal);
            } catch (RuntimeException ignored) {
                // Abandoning only settles operations.
            }
        }
    }
}
