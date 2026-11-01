package io.zmux.internal.core;

import java.util.Arrays;

/** A list of (a, b, c) long triples with removal from the front (send-order segments). */
final class LongTriples {
    private long[] v = new long[0];
    private int head;
    private int n;

    int size() { return n - head; }

    long a(int i) { return v[3 * (head + i)]; }
    long b(int i) { return v[3 * (head + i) + 1]; }
    long c(int i) { return v[3 * (head + i) + 2]; }

    void setB(int i, long x) { v[3 * (head + i) + 1] = x; }

    void add(long a, long b, long c) {
        if (3 * n == v.length) {
            if (head > 0) {
                System.arraycopy(v, 3 * head, v, 0, 3 * (n - head));
                n -= head;
                head = 0;
            }
            if (3 * n == v.length) v = Arrays.copyOf(v, Math.max(12, v.length * 2));
        }
        v[3 * n] = a;
        v[3 * n + 1] = b;
        v[3 * n + 2] = c;
        n++;
    }

    void removeFirst(int k) {
        head += k;
        if (head == n) head = n = 0;
    }
}
