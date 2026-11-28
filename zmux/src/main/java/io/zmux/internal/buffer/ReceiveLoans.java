package io.zmux.internal.buffer;

import java.util.function.Predicate;
import java.util.function.Consumer;

/** Capacity and live application leases, independent of the session loop's lifetime. */
public final class ReceiveLoans {
    /** Default external capacity reserved by one session. */
    public static final long DEFAULT_BYTES = 8L << 20;
    /** Default maximum number of independently releasable leases. */
    public static final int DEFAULT_SLOTS = 32_768;

    private final RootPins roots = new RootPins();
    // Allocations retained by either an external loan or a transferred splice reference.
    // The replacement-credit commitment follows the last holder across both domains.
    private final RootPins funded = new RootPins();
    private final Consumer<ReadBuf> release = lease -> release(lease, false);
    private final Consumer<ReadBuf> transferred = lease -> release(lease, true);
    private final long limit;
    private final int slotLimit;
    /** Alias, byte view, public handle and the root index entry. */
    public static final long DESCRIPTOR = 256;

    /** Memory backing charged to an outstanding receive lease. */
    public interface Budget {
        boolean reserveLoan(long backing, long descriptor);
        void releaseLoan(long backing, long descriptor);
    }

    private final Budget budget;
    private volatile Runnable returned;
    private int slots;

    public ReceiveLoans(long limit, int slotLimit) {
        this(limit, slotLimit, null, null);
    }

    public ReceiveLoans(long limit, int slotLimit, Budget budget, Runnable returned) {
        this.limit = limit;
        this.slotLimit = slotLimit;
        this.budget = budget;
        this.returned = returned;
    }

    /**
     * Reserves one descriptor and shares the backing's capacity reservation. When move is true,
     * a successful acquisition takes the caller's source reference. On failure the caller
     * still owns it.
     */
    public synchronized ReadBuf acquire(ReadBuf source, boolean move) {
        return acquire(source, move, null);
    }

    /**
     * Checks external borrow eligibility first, then the receiving owner's bound. The caller
     * commits the source read only on success. The same replacement-credit reservation backs
     * the moved reference, which no longer occupies an application loan slot.
     */
    public synchronized ReadBuf transfer(ReadBuf source, boolean move, Predicate<ReadBuf> fits) {
        return acquire(source, move, java.util.Objects.requireNonNull(fits));
    }

    private ReadBuf acquire(ReadBuf source, boolean move, Predicate<ReadBuf> fits) {
        if (slots >= slotLimit || !roots.pin(source, limit)) return null;
        long before = funded.bytes();
        funded.pin(source, Long.MAX_VALUE);
        long replacement = source.claimsReceiveCredit() ? funded.bytes() - before : 0;
        if (budget != null && !budget.reserveLoan(replacement, DESCRIPTOR)) {
            funded.unpin(source);
            roots.unpin(source);
            return null;
        }
        boolean success = false;
        try {
            if (fits != null && !fits.test(source)) return null;
            Consumer<ReadBuf> returnedTo = fits == null ? release : transferred;
            ReadBuf lease = move && source.moveIntoLoan(returnedTo) ? source : source.alias(returnedTo, move);
            if (fits == null) slots++;
            else roots.unpin(source);
            success = true;
            return lease;
        } finally {
            if (!success) {
                roots.unpin(source);
                removeFunding(source);
            }
        }
    }

    private void release(ReadBuf lease, boolean moved) {
        synchronized (this) {
            if (!moved) {
                roots.unpin(lease);
                slots--;
            }
            removeFunding(lease);
        }
        Runnable notify = returned;
        if (notify != null) notify.run();
    }

    /** The accounting stays live for external leases after the session owner stops. */
    public void stopNotifications() { returned = null; }

    private void removeFunding(ReadBuf lease) {
        long before = funded.bytes();
        funded.unpin(lease);
        if (budget != null) budget.releaseLoan(lease.claimsReceiveCredit() ? before - funded.bytes() : 0, DESCRIPTOR);
    }

    /** Bytes reserved by active application leases. */
    public synchronized long bytes() { return roots.bytes(); }

    public long capacityLimit() { return limit; }

    public int leaseLimit() { return slotLimit; }
}
