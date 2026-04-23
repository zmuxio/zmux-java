package io.zmux.internal;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

final class InboundPayloadPool {
    private final Map<Integer, ArrayDeque<byte[]>> freeByLength = new HashMap<>();
    private final int maxPooledLength;
    private final int maxRetainedPerLength;
    private final long maxRetainedBytes;
    private long retainedBytes;

    InboundPayloadPool(int maxPooledLength, int maxRetainedPerLength) {
        if (maxPooledLength < 0) {
            throw new IllegalArgumentException("maxPooledLength < 0");
        }
        if (maxRetainedPerLength < 0) {
            throw new IllegalArgumentException("maxRetainedPerLength < 0");
        }
        this.maxPooledLength = maxPooledLength;
        this.maxRetainedPerLength = maxRetainedPerLength;
        this.maxRetainedBytes = saturatingMultiply(maxPooledLength, maxRetainedPerLength);
    }

    private static long saturatingMultiply(int left, int right) {
        if (left <= 0 || right <= 0) {
            return 0L;
        }
        long product = (long) left * (long) right;
        return product < 0L ? Long.MAX_VALUE : product;
    }

    Handle acquire(int length) {
        if (length <= 0 || length > maxPooledLength || maxRetainedPerLength == 0) {
            return null;
        }
        byte[] bytes;
        synchronized (this) {
            ArrayDeque<byte[]> free = freeByLength.get(length);
            bytes = free == null ? null : free.pollFirst();
            if (bytes != null) {
                retainedBytes = Math.max(0L, retainedBytes - length);
                if (free.isEmpty()) {
                    freeByLength.remove(length);
                }
            }
        }
        return new Handle(length, bytes == null ? new byte[length] : bytes);
    }

    private void release(int length, byte[] bytes) {
        if (length <= 0
                || length > maxPooledLength
                || bytes == null
                || bytes.length != length
                || maxRetainedPerLength == 0) {
            return;
        }
        synchronized (this) {
            if (retainedBytes > maxRetainedBytes - length) {
                return;
            }
            ArrayDeque<byte[]> free = freeByLength.computeIfAbsent(length, ignored -> new ArrayDeque<>());
            if (free.size() >= maxRetainedPerLength) {
                return;
            }
            free.addFirst(bytes);
            retainedBytes += length;
        }
    }

    synchronized long retainedBytes() {
        return retainedBytes;
    }

    long maxRetainedBytes() {
        return maxRetainedBytes;
    }

    final class Handle {
        private final int length;
        private final byte[] bytes;
        private boolean released;

        private Handle(int length, byte[] bytes) {
            this.length = length;
            this.bytes = bytes;
        }

        byte[] bytes() {
            return bytes;
        }

        int storageBytes() {
            return length;
        }

        void release() {
            synchronized (this) {
                if (released) {
                    return;
                }
                released = true;
            }
            InboundPayloadPool.this.release(length, bytes);
        }
    }
}
