package io.zmux.runtime;

import java.util.Arrays;

final class LongLongSortedMap {
    private static final int MIN_CAPACITY = 8;
    private static final int MAX_RETAINED_CAPACITY = 512;
    private static final long[] EMPTY = new long[0];

    private long[] keys = EMPTY;
    private long[] values = EMPTY;
    private int size;

    private static int grownCapacity(int current) {
        if (current <= 0) {
            return MIN_CAPACITY;
        }
        int half = current >>> 1;
        if (current > Integer.MAX_VALUE - half) {
            return Integer.MAX_VALUE;
        }
        return current + half;
    }

    boolean isEmpty() {
        return size == 0;
    }

    int size() {
        return size;
    }

    int indexOf(long key) {
        int index = Arrays.binarySearch(keys, 0, size, key);
        return index >= 0 ? index : -1;
    }

    long keyAt(int index) {
        return keys[index];
    }

    long valueAt(int index) {
        return values[index];
    }

    void put(long key, long value) {
        int index = Arrays.binarySearch(keys, 0, size, key);
        if (index >= 0) {
            values[index] = value;
            return;
        }

        int insertAt = -index - 1;
        if (size == Integer.MAX_VALUE) {
            throw new OutOfMemoryError("long-long sorted map is too large");
        }
        ensureCapacity(size + 1);
        int moved = size - insertAt;
        if (moved > 0) {
            System.arraycopy(keys, insertAt, keys, insertAt + 1, moved);
            System.arraycopy(values, insertAt, values, insertAt + 1, moved);
        }
        keys[insertAt] = key;
        values[insertAt] = value;
        ++size;
    }

    void removeAt(int index) {
        int moved = size - index - 1;
        if (moved > 0) {
            System.arraycopy(keys, index + 1, keys, index, moved);
            System.arraycopy(values, index + 1, values, index, moved);
        }
        --size;
        if (size == 0 && keys.length > MAX_RETAINED_CAPACITY) {
            keys = EMPTY;
            values = EMPTY;
        }
    }

    void clear() {
        if (size == 0) {
            return;
        }
        size = 0;
        if (keys.length > MAX_RETAINED_CAPACITY) {
            keys = EMPTY;
            values = EMPTY;
        }
    }

    private void ensureCapacity(int required) {
        int current = keys.length;
        if (required <= current) {
            return;
        }
        int grown = grownCapacity(current);
        int newCapacity = Math.max(required, grown);
        keys = Arrays.copyOf(keys, newCapacity);
        values = Arrays.copyOf(values, newCapacity);
    }
}
