package io.zmux.internal.core;

/**
 * A wake-up target owned by one waiting party (an operation, a forwarder, a pipe writer).
 * The core queues it in the {@link Outbox} when the condition it waits for may have become
 * true; the driver calls {@link #wake} at the end of its round, after the core work of that
 * round (IMPLEMENTATION.md Section 2.5.3). Queued at most once per round. A waiter is in at
 * most one {@link WaitList} at a time, so withdrawing it is O(1).
 */
public abstract class Waiter {
    /** Written only by {@link Outbox}. */
    boolean queued;
    /** Written only by {@link WaitList}. */
    Waiter prev, next;
    WaitList list;

    /** Delivers the wake-up on the session's loop; runs no application code and never re-enters the core. */
    public abstract void wake();

    /** A waiter that ignores wake-ups (tests and fire-and-forget registrations). */
    public static Waiter inert() {
        return new Waiter() {
            @Override public void wake() {}
        };
    }
}
