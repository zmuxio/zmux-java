package io.zmux.internal.core;

/**
 * Stream scheduling (IMPLEMENTATION.md Section 5): strict priority by urgency,
 * deficit round robin with a 16 KiB quantum within a level. Each level is a ring of
 * the streams' {@link Node}s, whose fields only the scheduler writes.
 */
final class StreamScheduler {
    static final int QUANTUM = 16384;

    /** One stream's place in the scheduler. */
    static final class Node {
        private final Stream st;
        private Node prev, next;
        private boolean in;
        private int level;
        private int deficit;

        Node(Stream st) {
            this.st = st;
        }
    }

    private final Node[] head = new Node[8];
    private int mask;

    /** Whether no stream is ready. */
    boolean isEmpty() {
        return mask == 0;
    }

    void ensure(Stream st) {
        Node x = st.sch;
        if (x.in || !st.sendable()) return;
        int l = st.tab.urgency();
        x.level = l;
        x.in = true;
        x.deficit = 0;
        Node h = head[l];
        if (h == null) {
            x.prev = x;
            x.next = x;
            head[l] = x;
            mask |= 1 << l;
            return;
        }
        Node tail = h.prev;
        x.prev = tail;
        x.next = h;
        tail.next = x;
        h.prev = x;
    }

    void remove(Stream st) {
        Node x = st.sch;
        if (!x.in) return;
        int l = x.level;
        x.in = false;
        if (x.next == x) {
            head[l] = null;
            mask &= ~(1 << l);
        } else {
            x.prev.next = x.next;
            x.next.prev = x.prev;
            if (head[l] == x) head[l] = x.next;
        }
        x.prev = null;
        x.next = null;
    }

    void reprioritize(Stream st) {
        Node x = st.sch;
        if (x.in && x.level != st.tab.urgency()) {
            remove(st);
            ensure(st);
        }
    }

    /** Charges n sent bytes to an incremental stream; a spent quantum ends its turn. */
    void served(Stream st, long n) {
        if (!st.tab.incremental()) return;
        Node x = st.sch;
        x.deficit += (int) n + 1;
        if (x.deficit >= QUANTUM && x.in && head[x.level] == x) {
            head[x.level] = x.next;
            x.deficit = 0;
        }
    }

    void clear() {
        java.util.Arrays.fill(head, null);
        mask = 0;
    }

    /** Iterates ready streams in priority order, each once; reuse one per caller. */
    static final class Iter {
        private StreamScheduler sc;
        private int level;
        private Node cur;
        private Node first;

        Iter reset(StreamScheduler sc) {
            this.sc = sc;
            level = -1;
            cur = null;
            first = null;
            return this;
        }

        Stream next() {
            if (cur != null) {
                Node n = cur.next;
                if (n != first && n != null && n.in && n.level == level) {
                    cur = n;
                    return n.st;
                }
                cur = null;
            }
            do level++; while (level < 8 && (sc.mask & (1 << level)) == 0);
            if (level >= 8) return null;
            first = sc.head[level];
            cur = first;
            return cur.st;
        }
    }
}
