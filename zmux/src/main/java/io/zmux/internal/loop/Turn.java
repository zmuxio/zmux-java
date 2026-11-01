package io.zmux.internal.loop;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * FIFO ownership of one stream direction among the application threads and the completion
 * context that read (or write) it: the holder is the lane's only consumer (or producer), so
 * the lane's ring stays single-producer single-consumer. The loop never takes a turn.
 * Releasing passes the turn to the oldest waiting operation that is still live; an operation
 * that queues itself also tries to take the free turn, so a release never strands a waiter.
 */
final class Turn {
    /** A queued operation; granted the turn when {@link #take} returns true. */
    interface Waiting {
        /** Takes the passed turn: false when the operation was cancelled meanwhile. */
        boolean take();
    }

    private final AtomicInteger held = new AtomicInteger();
    private final ConcurrentLinkedQueue<Waiting> queue = new ConcurrentLinkedQueue<>();

    /** Takes the turn when it is free and nobody queues; never waits. */
    boolean tryEnter() {
        return queue.isEmpty() && held.get() == 0 && held.compareAndSet(0, 1);
    }

    /** Queues w behind earlier operations; w is granted the turn through {@link Waiting#take}. */
    void enqueue(Waiting w) {
        queue.add(w);
        pass();
    }

    /** Withdraws a cancelled operation that has not been granted the turn. */
    void remove(Waiting w) {
        queue.remove(w);
    }

    /** Releases the turn and passes it on. */
    void leave() {
        held.set(0);
        pass();
    }

    private void pass() {
        while (!queue.isEmpty() && held.compareAndSet(0, 1)) {
            Waiting w = queue.poll();
            if (w != null && w.take()) return;
            held.set(0);
        }
    }
}
