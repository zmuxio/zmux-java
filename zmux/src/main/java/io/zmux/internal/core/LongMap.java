package io.zmux.internal.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An open-addressing map from long keys to non-null values, without boxing: linear probing,
 * backward-shift deletion, load factor at most 1/2. Not thread-safe; it must not be modified
 * while {@link #values()} is iterated.
 */
public final class LongMap<V> {
    private long[] keys;
    private Object[] vals;
    private int size;
    private int mask;

    public LongMap() { this(16); }

    public LongMap(int expected) {
        int cap = Integer.highestOneBit(Math.max(4, expected * 2 - 1)) << 1;
        keys = new long[cap];
        vals = new Object[cap];
        mask = cap - 1;
    }

    private static int slot(long key, int mask) {
        long h = key * 0x9E3779B97F4A7C15L; // Fibonacci hashing spreads IDs that differ in low bits only.
        return Long.hashCode(h) & mask;
    }

    public int size() { return size; }

    public boolean isEmpty() { return size == 0; }

    @SuppressWarnings("unchecked")
    public V get(long key) {
        for (int i = slot(key, mask);; i = (i + 1) & mask) {
            Object v = vals[i];
            if (v == null) return null;
            if (keys[i] == key) return (V) v;
        }
    }

    public boolean containsKey(long key) { return get(key) != null; }

    /** Associates value with key; returns the previous value or null. */
    @SuppressWarnings("unchecked")
    public V put(long key, V value) {
        if (value == null) throw new NullPointerException("null value");
        int i = slot(key, mask);
        for (;; i = (i + 1) & mask) {
            Object v = vals[i];
            if (v == null) break;
            if (keys[i] == key) { vals[i] = value; return (V) v; }
        }
        keys[i] = key; vals[i] = value;
        if (++size * 2 > keys.length) grow();
        return null;
    }

    /** Removes key; returns its value or null. */
    @SuppressWarnings("unchecked")
    public V remove(long key) {
        int i = slot(key, mask);
        for (;; i = (i + 1) & mask) {
            Object v = vals[i];
            if (v == null) return null;
            if (keys[i] == key) break;
        }
        V old = (V) vals[i];
        // Backward shift: move later entries of the probe run into the hole.
        for (int hole = i, j = (i + 1) & mask;; j = (j + 1) & mask) {
            if (vals[j] == null) { vals[hole] = null; break; }
            int home = slot(keys[j], mask);
            if (((j - home) & mask) >= ((j - hole) & mask)) {
                keys[hole] = keys[j]; vals[hole] = vals[j]; hole = j;
            }
        }
        size--;
        return old;
    }

    public void clear() {
        java.util.Arrays.fill(vals, null);
        size = 0;
    }

    private void grow() {
        long[] oldKeys = keys; Object[] oldVals = vals;
        keys = new long[oldKeys.length * 2]; vals = new Object[oldVals.length * 2]; mask = keys.length - 1;
        for (int i = 0; i < oldKeys.length; i++) {
            if (oldVals[i] == null) continue;
            int j = slot(oldKeys[i], mask);
            while (vals[j] != null) j = (j + 1) & mask;
            keys[j] = oldKeys[i]; vals[j] = oldVals[i];
        }
    }

    /** A live view for iteration; the map must not change during it. */
    public Iterable<V> values() {
        return () -> new Iterator<>() {
            private int next = advance(0);
            private int advance(int from) { while (from < vals.length && vals[from] == null) from++; return from; }
            @Override public boolean hasNext() { return next < vals.length; }
            @SuppressWarnings("unchecked")
            @Override public V next() {
                if (next >= vals.length) throw new NoSuchElementException();
                V v = (V) vals[next]; next = advance(next + 1); return v;
            }
        };
    }

    /** A copy of the values, for loops that change the map. */
    public ArrayList<V> valuesSnapshot() {
        ArrayList<V> out = new ArrayList<>(size);
        for (V v : values()) out.add(v);
        return out;
    }
}
