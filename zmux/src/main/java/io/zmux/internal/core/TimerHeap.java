package io.zmux.internal.core;

import java.util.Arrays;

/** A 4-ary min-heap with in-place key updates, owned by one loop. */
public final class TimerHeap {
    /** A key belongs to at most one heap. The saved position supports O(log n) updates. */
    public static class Entry {
        private long deadline = Time.NEVER;
        private int index = -1;

        public final long deadline() { return deadline; }
    }

    /** A key that carries its own deadline action. */
    public abstract static class Timer extends Entry {
        /** Runs on the owner when the deadline passed. */
        public abstract void fire(long now);
    }

    private Entry[] heap = new Entry[4];
    private int n;

    public int size() { return n; }

    public long earliest() { return n == 0 ? Time.NEVER : heap[0].deadline; }

    /** Sets t's deadline: inserts, moves or (NEVER) removes it. */
    public void schedule(Entry t, long deadline) {
        if (deadline == Time.NEVER) {
            remove(t);
            return;
        }
        if (t.index < 0) {
            if (n == heap.length) heap = Arrays.copyOf(heap, n * 2);
            t.deadline = deadline;
            t.index = n;
            heap[n++] = t;
            up(t.index);
            return;
        }
        long old = t.deadline;
        if (deadline == old) return;
        t.deadline = deadline;
        if (deadline < old) up(t.index);
        else down(t.index);
    }

    public void remove(Entry t) {
        int i = t.index;
        if (i < 0) return;
        t.index = -1;
        t.deadline = Time.NEVER;
        Entry last = heap[--n];
        heap[n] = null;
        if (i == n) return;
        heap[i] = last;
        last.index = i;
        up(i);
        down(last.index);
    }

    /** Removes and returns the earliest timer due at now, or null. */
    public Timer pollDue(long now) {
        if (n == 0 || heap[0].deadline > now) return null;
        if (!(heap[0] instanceof Timer t)) throw new IllegalStateException("index key has no timer action");
        remove(t);
        return t;
    }

    /** Fires due actions, including any same-time actions they schedule. */
    public void fireDue(long now) {
        for (Timer t; (t = pollDue(now)) != null; ) {
            t.fire(now);
            if (t.deadline() != Time.NEVER && t.deadline() <= now) {
                throw new IllegalStateException("due timer did not advance its deadline");
            }
        }
    }

    public void clear() {
        for (int i = 0; i < n; i++) {
            heap[i].index = -1;
            heap[i].deadline = Time.NEVER;
            heap[i] = null;
        }
        n = 0;
    }

    private void up(int i) {
        Entry t = heap[i];
        while (i > 0) {
            int p = (i - 1) >>> 2;
            if (heap[p].deadline <= t.deadline) break;
            heap[i] = heap[p];
            heap[i].index = i;
            i = p;
        }
        heap[i] = t;
        t.index = i;
    }

    private void down(int i) {
        Entry t = heap[i];
        while (true) {
            int c = 4 * i + 1;
            if (c >= n) break;
            int best = c;
            for (int k = c + 1; k < Math.min(c + 4, n); k++) if (heap[k].deadline < heap[best].deadline) best = k;
            if (heap[best].deadline >= t.deadline) break;
            heap[i] = heap[best];
            heap[i].index = i;
            i = best;
        }
        heap[i] = t;
        t.index = i;
    }
}
