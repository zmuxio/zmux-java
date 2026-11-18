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

    /** Who holds the turn: {@link #FREE}, {@link #BLOCKING} or {@link #ASYNC}. */
    private final AtomicInteger held = new AtomicInteger();
    private static final int FREE = 0, BLOCKING = 1, ASYNC = 2;
    private final ConcurrentLinkedQueue<Waiting> queue = new ConcurrentLinkedQueue<>();
    /** What the direction's asynchronous operations are, for a refusal's message. */
    final String asyncOperations;

    Turn(String asyncOperations) {
        this.asyncOperations = asyncOperations;
    }

    /** Takes the turn when it is free and nobody queues; never waits. */
    boolean tryEnter() {
        return queue.isEmpty() && held.get() == FREE && held.compareAndSet(FREE, BLOCKING);
    }

    /**
     * {@link #tryEnter} for an asynchronous operation: its later steps run on the session's completion context.
     * The holder kind is set by the same CAS that takes the turn, so {@link #asyncAhead} never misses it.
     */
    boolean tryEnterAsync() {
        return queue.isEmpty() && held.get() == FREE && held.compareAndSet(FREE, ASYNC);
    }

    /**
     * Whether an asynchronous operation holds the turn or queues ahead of the queued w: one w waits behind,
     * whose progress needs the session's completion context. Called after w is queued, so no operation can
     * later get ahead of w; the queue is scanned before the holder, and {@link #pass} sets the new holder's
     * kind before taking it off the queue, so an operation moving from the queue to the turn is always seen.
     */
    boolean asyncAhead(Waiting w) {
        for (Waiting q : queue) {
            if (q == w) break;
            if (q instanceof Operation<?>) return true;
        }
        return held.get() == ASYNC;
    }

    /** Whether an asynchronous operation holds the turn or queues for it. */
    boolean asyncPending() {
        if (held.get() == ASYNC) return true;
        for (Waiting q : queue) if (q instanceof Operation<?>) return true;
        return false;
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
        held.set(FREE);
        pass();
    }

    private void pass() {
        Waiting w;
        while ((w = queue.peek()) != null && held.compareAndSet(FREE, w instanceof Operation<?> ? ASYNC : BLOCKING)) {
            // w stays queued until it holds the turn; one withdrawn meanwhile is skipped.
            if (queue.remove(w) && w.take()) return;
            held.set(FREE);
        }
    }
}
