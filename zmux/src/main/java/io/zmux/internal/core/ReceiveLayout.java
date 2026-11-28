package io.zmux.internal.core;

import io.zmux.internal.buffer.OffsetPageBits;

/** Conservative accounting units for Java's receive representation. */
final class ReceiveLayout {
    private ReceiveLayout() {}

    static final long PAGE_METADATA = 2048;
    static final long PAGE_CHARGE = OffsetPageBits.SIZE + 2L * OffsetPageBits.BITMAP_BYTES + PAGE_METADATA;
    static final long CREDIT_COST = (PAGE_CHARGE + OffsetPageBits.SIZE - 1) / OffsetPageBits.SIZE;
    /** Partially covered pages at the two ends, plus the stream's receive state and indexes. */
    static final long STREAM_CHARGE = 2 * PAGE_CHARGE + 4096;

    static final long VIEW_PIECE = 160;
    static final long VIEW_ROOT = 192;
    static final long PACKED_DESCRIPTOR = 2L * OffsetPageBits.BITMAP_BYTES + PAGE_METADATA;
    static final long LOAN_FLOOR = io.zmux.internal.buffer.ReceiveSizes.MAX_BLOCK + io.zmux.internal.buffer.ReceiveLoans.DESCRIPTOR;
    static final long WORKING = 2L * io.zmux.internal.buffer.ReceiveSizes.MAX_BLOCK + LOAN_FLOOR;
}
