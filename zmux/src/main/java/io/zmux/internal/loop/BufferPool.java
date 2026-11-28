package io.zmux.internal.loop;

import io.zmux.internal.buffer.ReadBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A host's read leases. Heap classes serve authenticated records and the direct class
 * serves batched byte input. Hosts with the default limit share returned storage.
 */
public final class BufferPool {
    static final int IDLE_SIZE = 256;
    static final int SIZE = 128 << 10;
    private static final int FIRST_SHIFT = 8;
    private static final int LAST_SHIFT = 17;
    private static final int TYPICAL_SEALED = 1280;
    private static final int TYPICAL_CLASS = 3;
    private static final int DIRECT_IDLE_CLASS = LAST_SHIFT + 2 - FIRST_SHIFT;
    private static final int DIRECT_CLASS = DIRECT_IDLE_CLASS + 1;
    /** Idle bytes retained across the hosts using the default setting. */
    public static final long DEFAULT_IDLE_BYTES = 8L << 20;

    private static final ReturnedBlocks DEFAULT = new ReturnedBlocks(DEFAULT_IDLE_BYTES);
    private final ReturnedBlocks returned;
    private final java.util.function.Consumer<ReadBuf> recycle = this::give;
    private final AtomicLong allocations = new AtomicLong();
    private final AtomicInteger outstanding = new AtomicInteger();
    /** Blocks taken and not yet returned, when tracking is enabled for tests. */
    private final java.util.Set<ReadBuf> live = ReadBuf.TRACKING ? java.util.concurrent.ConcurrentHashMap.newKeySet() : null;

    BufferPool(long idleBytes) {
        this(new ReturnedBlocks(idleBytes));
    }

    private BufferPool(ReturnedBlocks returned) {
        this.returned = returned;
    }

    static BufferPool forHost(long idleBytes) {
        return idleBytes == DEFAULT_IDLE_BYTES ? new BufferPool(DEFAULT) : new BufferPool(idleBytes);
    }

    /** A direct block for byte-stream batch input. */
    ReadBuf take() {
        return take(DIRECT_CLASS, SIZE);
    }

    ReadBuf takeIdle() {
        return take(DIRECT_IDLE_CLASS, IDLE_SIZE);
    }

    /** A heap block for an authenticated record, including any framing prefix. */
    ReadBuf takeRecord(int length) {
        if (length < 0 || length > 1 << LAST_SHIFT) throw new IllegalArgumentException("record capacity");
        int index = recordClass(length);
        return take(index, capacity(index));
    }

    private static int recordClass(int length) {
        if (length > 1024 && length <= TYPICAL_SEALED) return TYPICAL_CLASS;
        int shift = Math.max(FIRST_SHIFT, Integer.SIZE - Integer.numberOfLeadingZeros(Math.max(1, length) - 1));
        return shift - FIRST_SHIFT + (length > TYPICAL_SEALED ? 1 : 0);
    }

    private static int capacity(int index) {
        if (index == DIRECT_CLASS) return SIZE;
        if (index == DIRECT_IDLE_CLASS) return IDLE_SIZE;
        if (index == TYPICAL_CLASS) return TYPICAL_SEALED;
        return 1 << (FIRST_SHIFT + index - (index > TYPICAL_CLASS ? 1 : 0));
    }

    private ReadBuf take(int index, int capacity) {
        ByteBuffer data = returned.take(index, capacity);
        if (data == null) {
            data = index >= DIRECT_IDLE_CLASS ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
            allocations.incrementAndGet();
        }
        data.clear();
        outstanding.incrementAndGet();
        ReadBuf buf = new ReadBuf(data, recycle);
        if (live != null) live.add(buf);
        return buf;
    }

    private void give(ReadBuf buf) {
        outstanding.decrementAndGet();
        if (live != null) live.remove(buf);
        returned.give(buf.data());
    }

    /** Backing storage allocations, excluding a new owner for each lease. */
    long allocations() { return allocations.get(); }

    long idleBytes() { return returned.idleBytes(); }

    /** Buffers taken and not yet returned (tests: leak checks). */
    int outstanding() { return outstanding.get(); }

    /** The reference histories of the buffers not yet returned; empty unless tracking (tests). */
    String outstandingTrace() {
        if (live == null) return "";
        var out = new StringBuilder();
        for (ReadBuf rb : live) out.append("outstanding read buffer, refs ").append(rb.refs()).append('\n').append(rb.trace());
        return out.toString();
    }

    private static final class ReturnedBlocks {
        private static final class Bin {
            final AtomicReference<ByteBuffer> hot = new AtomicReference<>();
            final ConcurrentLinkedQueue<ByteBuffer> spare = new ConcurrentLinkedQueue<>();
        }

        private final List<Bin> classes = new ArrayList<>(DIRECT_CLASS + 1);
        private final AtomicLong bytes = new AtomicLong();
        private final long limit;
        private final boolean hotSlots;

        ReturnedBlocks(long limit) {
            int reserved = 0;
            for (int i = 0; i <= DIRECT_CLASS; i++) {
                classes.add(new Bin());
                reserved += capacity(i);
            }
            hotSlots = limit >= reserved;
            this.limit = hotSlots ? limit - reserved : limit;
        }

        ByteBuffer take(int index, int capacity) {
            Bin bin = classes.get(index);
            ByteBuffer data = hotSlots ? bin.hot.getAndSet(null) : null;
            if (data != null) return data;
            data = bin.spare.poll();
            if (data != null) bytes.addAndGet(-capacity);
            return data;
        }

        void give(ByteBuffer data) {
            int capacity = data.capacity();
            int index = data.isDirect() ? (capacity == IDLE_SIZE ? DIRECT_IDLE_CLASS : DIRECT_CLASS) : recordClass(capacity);
            Bin bin = classes.get(index);
            if (hotSlots && bin.hot.compareAndSet(null, data)) return;
            if (!reserve(index, capacity)) return;
            try {
                bin.spare.add(data);
            } catch (RuntimeException | Error failure) {
                bytes.addAndGet(-capacity);
                throw failure;
            }
        }

        private boolean reserve(int index, int capacity) {
            if (capacity > limit) return false;
            boolean evicted = false;
            while (true) {
                long current = bytes.get();
                if (capacity <= limit - current) {
                    if (bytes.compareAndSet(current, current + capacity)) return true;
                } else {
                    if (evicted || !evictLarger(index)) return false;
                    evicted = true;
                }
            }
        }

        /** Large returned batches must leave room for classes used by short records. */
        private boolean evictLarger(int index) {
            for (int i = classes.size() - 1; i > index; i--) {
                ByteBuffer victim = classes.get(i).spare.poll();
                if (victim == null) continue;
                bytes.addAndGet(-victim.capacity());
                return true;
            }
            return false;
        }

        long idleBytes() {
            long total = bytes.get();
            if (hotSlots) {
                for (Bin bin : classes) {
                    ByteBuffer data = bin.hot.get();
                    if (data != null) total += data.capacity();
                }
            }
            return total;
        }
    }
}
