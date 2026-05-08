package io.zmux.runtime;

import io.zmux.support.RangeChecks;
import java.util.ArrayDeque;
import java.util.Objects;

final class ByteArrayQueue {
    private static final int SHRINK_MIN_STORAGE_BYTES = 256 << 10;
    private static final int SHRINK_MAX_TAIL_BYTES = 64 << 10;
    private static final int RELEASE_EMPTY_CHUNK_DEQUE_MIN_CHUNKS = 1024;

    private ArrayDeque<Chunk> chunks = new ArrayDeque<>();
    private long size;
    private long storageBytes;
    private long lastReadReleasedStorageBytes;
    private int removedChunksSinceDequeReset;

    private static long saturatingAdd(long left, long right) {
        long result = left + right;
        return result < 0L || result < left ? Long.MAX_VALUE : result;
    }

    private static int saturatedInt(long value) {
        return value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) Math.max(0L, value);
    }

    private static int saturatingAddInt(int left, int right) {
        if (right <= 0) {
            return left;
        }
        return left > Integer.MAX_VALUE - right ? Integer.MAX_VALUE : left + right;
    }

    private static boolean shouldTightenAfterConsume(Chunk chunk) {
        if (chunk == null || chunk.length == 0) {
            return false;
        }
        if (chunk.storageBytes < SHRINK_MIN_STORAGE_BYTES) {
            return false;
        }
        return chunk.length <= SHRINK_MAX_TAIL_BYTES;
    }

    public int size() {
        return saturatedInt(size);
    }

    long sizeLong() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0L;
    }

    long storageBytes() {
        return storageBytes;
    }

    public void add(byte[] data) {
        if (data == null) {
            return;
        }
        add(data, 0, data.length);
    }

    public void add(byte[] data, int offset, int length) {
        Objects.requireNonNull(data, "data");
        addRetained(data, offset, length, data.length, null);
    }

    void addRetained(byte[] data, int offset, int length, int storageBytes, Runnable releaseAction) {
        Objects.requireNonNull(data, "data");
        RangeChecks.checkFromIndexSize(offset, length, data.length);
        if (length == 0) {
            if (storageBytes > 0 && releaseAction != null) {
                releaseAction.run();
            }
            return;
        }
        if (storageBytes < length) {
            throw new IllegalArgumentException("storageBytes < length");
        }
        chunks.addLast(new Chunk(data, offset, length, storageBytes, releaseAction));
        size = saturatingAdd(size, length);
        this.storageBytes = saturatingAdd(this.storageBytes, storageBytes);
    }

    public int read(byte[] dst, int offset, int length) {
        return readInternal(dst, offset, length);
    }

    ReadResult readDetailed(byte[] dst, int offset, int length) {
        int read = readInternal(dst, offset, length);
        long releasedStorageBytes = lastReadReleasedStorageBytes;
        if (read == 0 && releasedStorageBytes == 0L) {
            return ReadResult.empty();
        }
        return new ReadResult(read, releasedStorageBytes);
    }

    int readAndTrackReleasedStorage(byte[] dst, int offset, int length) {
        return readInternal(dst, offset, length);
    }

    long lastReadReleasedStorageBytes() {
        return lastReadReleasedStorageBytes;
    }

    private int readInternal(byte[] dst, int offset, int length) {
        Objects.requireNonNull(dst, "dst");
        RangeChecks.checkFromIndexSize(offset, length, dst.length);
        lastReadReleasedStorageBytes = 0L;
        if (length == 0) {
            return 0;
        }
        int remaining = (int) Math.min(length, size);
        int written = 0;
        int removedChunks = 0;
        long releasedStorageBytes = 0L;
        while (remaining > 0 && !chunks.isEmpty()) {
            Chunk head = chunks.peekFirst();
            int copy = Math.min(head.length, remaining);
            System.arraycopy(head.data, head.offset, dst, offset + written, copy);
            written += copy;
            remaining -= copy;
            size -= copy;
            head.offset += copy;
            head.length -= copy;
            if (head.length == 0) {
                chunks.removeFirst();
                removedChunks++;
                this.storageBytes = Math.max(0L, this.storageBytes - head.storageBytes);
                releasedStorageBytes = saturatingAdd(releasedStorageBytes, head.storageBytes);
                if (head.storageBytes > 0 && head.releaseAction != null) {
                    head.releaseAction.run();
                }
            } else if (shouldTightenAfterConsume(head)) {
                releasedStorageBytes = saturatingAdd(releasedStorageBytes, this.tightenHeadStorage(head));
            }
        }
        this.releaseEmptyChunkDequeStorage(removedChunks);
        lastReadReleasedStorageBytes = releasedStorageBytes;
        return written;
    }

    private long tightenHeadStorage(Chunk head) {
        byte[] tail = new byte[head.length];
        System.arraycopy(head.data, head.offset, tail, 0, head.length);

        int oldStorageBytes = head.storageBytes;
        Runnable oldReleaseAction = head.releaseAction;
        head.data = tail;
        head.offset = 0;
        head.storageBytes = tail.length;
        head.releaseAction = null;

        long releasedStorageBytes = Math.max(0, oldStorageBytes - head.storageBytes);
        storageBytes = Math.max(0L, storageBytes - releasedStorageBytes);
        if (oldStorageBytes > 0 && oldReleaseAction != null) {
            oldReleaseAction.run();
        }
        return releasedStorageBytes;
    }

    public int discardAll() {
        return saturatedInt(discardAllDetailed().bytes());
    }

    DiscardResult discardAllDetailed() {
        long discarded = size;
        long releasedStorageBytes = storageBytes;
        int removedChunks = chunks.size();
        while (!chunks.isEmpty()) {
            Chunk head = chunks.removeFirst();
            if (head.storageBytes > 0 && head.releaseAction != null) {
                head.releaseAction.run();
            }
        }
        storageBytes = 0;
        size = 0;
        this.releaseEmptyChunkDequeStorage(removedChunks);
        return new DiscardResult(discarded, releasedStorageBytes);
    }

    private void releaseEmptyChunkDequeStorage(int removedChunks) {
        removedChunksSinceDequeReset = saturatingAddInt(removedChunksSinceDequeReset, removedChunks);
        if (removedChunksSinceDequeReset >= RELEASE_EMPTY_CHUNK_DEQUE_MIN_CHUNKS && chunks.isEmpty()) {
            chunks = new ArrayDeque<>();
            removedChunksSinceDequeReset = 0;
        }
    }

    static final class ReadResult {
        private static final ReadResult EMPTY = new ReadResult(0, 0);

        private final int bytes;
        private final long releasedStorageBytes;

        ReadResult(int bytes, long releasedStorageBytes) {
            this.bytes = bytes;
            this.releasedStorageBytes = releasedStorageBytes;
        }

        static ReadResult empty() {
            return EMPTY;
        }

        int bytes() {
            return bytes;
        }

        long releasedStorageBytes() {
            return releasedStorageBytes;
        }
    }

    static final class DiscardResult {
        private final long bytes;
        private final long releasedStorageBytes;

        DiscardResult(long bytes, long releasedStorageBytes) {
            this.bytes = bytes;
            this.releasedStorageBytes = releasedStorageBytes;
        }

        long bytes() {
            return bytes;
        }

        long releasedStorageBytes() {
            return releasedStorageBytes;
        }
    }

    private static final class Chunk {
        private byte[] data;
        private int storageBytes;
        private Runnable releaseAction;
        private int offset;
        private int length;

        private Chunk(byte[] data, int offset, int length, int storageBytes, Runnable releaseAction) {
            this.data = data;
            this.offset = offset;
            this.length = length;
            this.storageBytes = storageBytes;
            this.releaseAction = releaseAction;
        }
    }
}
