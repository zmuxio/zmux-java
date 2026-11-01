package io.zmux.internal.core;

/** An intrusive FIFO of {@link Waiter}s: add, remove and poll are O(1), no allocation. */
final class WaitList {
    private Waiter head, tail;
    private int size;

    int size() {
        return size;
    }

    boolean isEmpty() {
        return head == null;
    }

    /** Appends w; a waiter already in this list keeps its place, one in another list moves here. */
    void add(Waiter w) {
        if (w.list == this) return;
        if (w.list != null) w.list.remove(w);
        w.list = this;
        w.prev = tail;
        w.next = null;
        if (tail == null) head = w; else tail.next = w;
        tail = w;
        size++;
    }

    /** Withdraws w; false when it is not in this list. */
    boolean remove(Waiter w) {
        if (w == null || w.list != this) return false;
        if (w.prev == null) head = w.next; else w.prev.next = w.next;
        if (w.next == null) tail = w.prev; else w.next.prev = w.prev;
        w.prev = w.next = null;
        w.list = null;
        size--;
        return true;
    }

    /** The oldest waiter, still listed, or null. */
    Waiter first() {
        return head;
    }

    /** Removes and returns the oldest waiter, or null. */
    Waiter poll() {
        Waiter w = head;
        remove(w);
        return w;
    }

    /** Queues the oldest waiter, keeping it listed: it withdraws or re-checks when it runs. */
    void wakeFirst(Outbox o) {
        if (head != null) o.wake(head);
    }

    /** Queues every waiter, keeping each listed. */
    void wakeEvery(Outbox o) {
        for (Waiter w = head; w != null; w = w.next) o.wake(w);
    }

    /** Removes every waiter and queues each, oldest first. */
    void wakeAll(Outbox o) {
        for (Waiter w; (w = poll()) != null; ) o.wake(w);
    }
}
