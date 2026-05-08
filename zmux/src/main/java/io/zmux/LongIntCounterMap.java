package io.zmux;

import java.util.Arrays;

final class LongIntCounterMap {
    private static final int MIN_CAPACITY = 16;
    private static final int MAX_CAPACITY = 1 << 30;
    private static final float MAX_LOAD_FACTOR = 0.5f;

    private long[] keys = new long[MIN_CAPACITY];
    private int[] values = new int[MIN_CAPACITY];
    private int size;

    private static int smear(long value) {
        value ^= value >>> 33;
        value *= 0xff51afd7ed558ccdL;
        value ^= value >>> 33;
        value *= 0xc4ceb9fe1a85ec53L;
        value ^= value >>> 33;
        return (int) value;
    }

    private static int grownCapacity(int current) {
        if (current <= 0) {
            return MIN_CAPACITY;
        }
        if (current >= MAX_CAPACITY) {
            return MAX_CAPACITY;
        }
        return current << 1;
    }

    private static int tableCapacity(int requested) {
        if (requested <= MIN_CAPACITY) {
            return MIN_CAPACITY;
        }
        if (requested >= MAX_CAPACITY) {
            return MAX_CAPACITY;
        }
        return Integer.highestOneBit(requested - 1) << 1;
    }

    void clear() {
        if (size == 0) {
            return;
        }
        Arrays.fill(keys, 0L);
        Arrays.fill(values, 0);
        size = 0;
    }

    int getOrDefault(long key, int defaultValue) {
        if (key <= 0L) {
            return defaultValue;
        }
        int index = findSlot(key);
        return keys[index] == key ? values[index] : defaultValue;
    }

    void put(long key, int value) {
        if (key <= 0L) {
            return;
        }
        ensureInsertCapacity();
        int index = findSlot(key);
        if (keys[index] == 0L) {
            keys[index] = key;
            size++;
        }
        values[index] = value;
    }

    void incrementSaturating(long key) {
        if (key <= 0L) {
            return;
        }
        ensureInsertCapacity();
        int index = findSlot(key);
        if (keys[index] == 0L) {
            keys[index] = key;
            values[index] = 1;
            size++;
            return;
        }
        if (values[index] != Integer.MAX_VALUE) {
            values[index]++;
        }
    }

    private void ensureInsertCapacity() {
        if (size == Integer.MAX_VALUE || keys.length >= MAX_CAPACITY) {
            throw new OutOfMemoryError("long-int counter map is too large");
        }
        if ((float) (size + 1) < keys.length * MAX_LOAD_FACTOR) {
            return;
        }
        rehash(grownCapacity(keys.length));
    }

    private void rehash(int newCapacity) {
        long[] oldKeys = keys;
        int[] oldValues = values;
        keys = new long[tableCapacity(newCapacity)];
        values = new int[keys.length];
        size = 0;
        for (int i = 0; i < oldKeys.length; ++i) {
            long key = oldKeys[i];
            if (key == 0L) {
                continue;
            }
            int index = findSlot(key);
            keys[index] = key;
            values[index] = oldValues[i];
            size++;
        }
    }

    private int findSlot(long key) {
        int mask = keys.length - 1;
        int index = smear(key) & mask;
        while (true) {
            long existing = keys[index];
            if (existing == 0L || existing == key) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }
}
