package io.zmux.internal.buffer;

import java.util.IdentityHashMap;

/** Identity lookup whose retained table stays proportional to live owners. */
public final class IdentityIndex<K, V> {
    private IdentityHashMap<K, V> values = new IdentityHashMap<>(4);
    private int peak;

    public V get(K key) { return values.get(key); }

    public void put(K key, V value) {
        values.put(key, value);
        peak = Math.max(peak, values.size());
    }

    public V remove(K key) {
        V old = values.remove(key);
        int remaining = values.size();
        if (peak > 4 && remaining <= peak / 2) {
            var next = new IdentityHashMap<K, V>(Math.max(4, remaining));
            next.putAll(values);
            values = next;
            peak = remaining;
        }
        return old;
    }

    public boolean isEmpty() { return values.isEmpty(); }
}
