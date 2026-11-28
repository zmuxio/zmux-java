package io.zmux.internal.loop;

import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.buffer.RootPins;

/** Input occupancy in units shared by a forwarding producer and an external inbox. */
final class InputCharge {
    /** Queue node, delivery state and proportional share of the root index. */
    static final long ENTRY_BYTES = io.zmux.internal.spi.CarrierBudget.DELIVERY_BYTES;
    static final long BATCH = BufferPool.SIZE + ENTRY_BYTES;

    private final RootPins roots = new RootPins();
    private long entriesAndCopies;

    long bytes() { return entriesAndCopies + roots.bytes(); }

    /** The caller serializes reservation and release. A null owner describes a fresh copy. */
    boolean reserve(ReadBuf owner, int copyBytes, long limit) {
        long available = limit - entriesAndCopies - ENTRY_BYTES;
        if (available < roots.bytes()) return false;
        if (owner == null ? copyBytes > available - roots.bytes() : !roots.pin(owner, available)) return false;
        entriesAndCopies += ENTRY_BYTES + copyBytes;
        return true;
    }

    void adoptCopy(ReadBuf copy) {
        roots.pin(copy, Long.MAX_VALUE);
        entriesAndCopies -= copy.capacity();
    }

    void release(ReadBuf owner, int copyBytes) {
        entriesAndCopies -= ENTRY_BYTES + copyBytes;
        if (owner != null) roots.unpin(owner);
    }
}
