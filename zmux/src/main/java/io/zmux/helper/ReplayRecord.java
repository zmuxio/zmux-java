package io.zmux.helper;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.TreeMap;

/**
 * A bounded record of keys that are accepted once each, such as the nonces of tokens or
 * requests, grouped by a time index (a slot, a second): {@link #add} forgets the groups that
 * are no longer acceptable and then records a key unless its group holds it already or the
 * record is full. Safe for concurrent use.
 */
public final class ReplayRecord {
    private final int capacity;
    private final TreeMap<Long, HashSet<ByteBuffer>> groups = new TreeMap<>();
    private int size;

    /** A record of at most capacity keys. */
    public ReplayRecord(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("capacity must be positive");
        this.capacity = capacity;
    }

    /**
     * Forgets the groups below oldest, then records key in group at. False when the group holds
     * key already, or when the record is full: the key is then not accepted.
     */
    public synchronized boolean add(long at, byte[] key, long oldest) {
        var head = groups.headMap(oldest, false);
        for (HashSet<ByteBuffer> g : head.values()) size -= g.size();
        head.clear();
        ByteBuffer k = ByteBuffer.wrap(key.clone());
        HashSet<ByteBuffer> g = groups.get(at);
        if (g != null && g.contains(k) || size >= capacity) return false;
        if (g == null) groups.put(at, g = new HashSet<>());
        g.add(k);
        size++;
        return true;
    }

    /** The keys recorded. */
    public synchronized int size() { return size; }
}
