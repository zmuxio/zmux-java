package io.zmux.http;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.LongSupplier;

/**
 * Ended exchange carriers remembered for a fixed time, to answer GONE to late requests. Every
 * entry lives as long, so insertion order is expiry order: each remember drops the expired
 * entries from the head of that order, amortised O(1) per entry.
 */
final class GoneTable<V> {
    private record Entry<V>(ByteBuffer cid, V value, long until) {}

    private final ConcurrentHashMap<ByteBuffer, Entry<V>> map = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Entry<V>> order = new ConcurrentLinkedQueue<>();
    private final long memoryNanos;
    private final LongSupplier clock;

    GoneTable(long memoryNanos, LongSupplier clock) {
        this.memoryNanos = memoryNanos;
        this.clock = clock;
    }

    /** The value remembered for cid; null when none, or when it expired. */
    V get(ByteBuffer cid) {
        Entry<V> e = map.get(cid);
        if (e == null) return null;
        if (clock.getAsLong() - e.until > 0) { map.remove(cid, e); return null; }
        return e.value;
    }

    /** Remembers cid with value unless it is remembered already, and forgets what expired. */
    void remember(ByteBuffer cid, V value) {
        long now = clock.getAsLong();
        Entry<V> e = new Entry<>(cid, value, now + memoryNanos);
        if (map.putIfAbsent(cid, e) == null) order.add(e);
        for (Entry<V> head; (head = order.peek()) != null && now - head.until > 0; ) {
            if (order.remove(head)) map.remove(head.cid, head);
        }
    }

    int size() { return map.size(); }

    /** Entries in the expiry order, expired or not, for tests. */
    int queued() { return order.size(); }
}
