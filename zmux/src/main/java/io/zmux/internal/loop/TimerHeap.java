package io.zmux.internal.loop;

import io.zmux.internal.core.Time;
import java.util.Arrays;

/** A 4-ary min-heap of timers with in-place updates; one entry per timer, owned by one loop. */
final class TimerHeap {
    /** A timer of the heap; its position is kept for O(log n) updates. */
    abstract static class Timer {
        long deadline = Time.NEVER;
        int index = -1;

        /** Runs on the loop when the deadline passed. */
        abstract void fire(long now);
    }

    private Timer[] heap = new Timer[16];
    private int n;

    int size() { return n; }

    long earliest() { return n == 0 ? Time.NEVER : heap[0].deadline; }

    /** Sets t's deadline: inserts, moves or (NEVER) removes it. */
    void schedule(Timer t, long deadline) {
        if (deadline == Time.NEVER) {
            remove(t);
            t.deadline = Time.NEVER;
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
        t.deadline = deadline;
        if (deadline < old) up(t.index);
        else down(t.index);
    }

    void remove(Timer t) {
        int i = t.index;
        if (i < 0) return;
        t.index = -1;
        Timer last = heap[--n];
        heap[n] = null;
        if (i == n) return;
        heap[i] = last;
        last.index = i;
        up(i);
        down(last.index);
    }

    /** Removes and returns the earliest timer due at now, or null. */
    Timer pollDue(long now) {
        if (n == 0 || heap[0].deadline > now) return null;
        Timer t = heap[0];
        remove(t);
        return t;
    }

    private void up(int i) {
        Timer t = heap[i];
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
        Timer t = heap[i];
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
