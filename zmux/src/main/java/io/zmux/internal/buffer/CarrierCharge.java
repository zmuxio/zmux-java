package io.zmux.internal.buffer;

import io.zmux.internal.memory.SharedMemory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

import static io.zmux.internal.buffer.OffsetPageBits.SIZE;
import static io.zmux.internal.buffer.OffsetPageBits.base;

/** A splice grant shared by queued ranges and by the carrier attempts that can settle it. */
public final class CarrierCharge {
    // Grant (64), AtomicInteger (24), coverage (40), tree (64) and settlement closure (32).
    private static final long GRANT_CHARGE = 256;
    // Payload positions cost one bit each. The rest covers the bitmap, key and tree entry.
    public static final long PAGE_CHARGE = OffsetPageBits.BITMAP_BYTES + 256;

    private final AtomicInteger references = new AtomicInteger(1);
    private final LongConsumer settle;
    private final long bytes;
    private final boolean atomic;
    /** Local, nonblocking accounting at the queue and carrier commit points. */
    public interface Observer {
        void encoded(int bytes);
        void dispatched(boolean nativeChannel);
        void enqueued(long bytes);
        void handedOff(long bytes);
        void finished(long unaccepted, long unsent);
    }

    private final Observer observer;
    private long enqueued;
    // The receipt monitor guards all accounting state. Budget updates and settlement run outside it.
    private SharedMemory.Receive account;
    private boolean prepaid;
    private boolean settled;
    private OffsetCoverage coverage;

    public CarrierCharge(long bytes, boolean atomic, LongConsumer settle) {
        this(bytes, atomic, settle, null);
    }

    public CarrierCharge(long bytes, boolean atomic, LongConsumer settle, Observer observer) {
        this.bytes = bytes;
        this.observer = observer;
        this.atomic = atomic;
        this.settle = settle;
    }

    public void encoded(int count) {
        if (observer != null) observer.encoded(count);
    }

    public void dispatched(boolean nativeChannel) {
        if (observer != null) observer.dispatched(nativeChannel);
    }

    public synchronized void enqueued(long count) {
        enqueued += count;
        if (observer != null) observer.enqueued(count);
    }

    public long backingBytes() {
        return GRANT_CHARGE + (bytes == 0 ? 0 : (bytes - 1) / SIZE + 1) * PAGE_CHARGE;
    }

    /**
     * Commits the entire stream grant before taking its first byte. A stalled earlier receipt
     * can then coexist with arbitrarily split later ranges without another admission.
     */
    public boolean prepare(SharedMemory.Receive target) {
        synchronized (this) {
            if (settled) return true;
            if (prepaid) {
                checkAccount(target);
                return true;
            }
        }
        long charge = backingBytes();
        if (target != null && !target.reserveView(charge)) return false;
        try {
            synchronized (this) {
                if (!settled && !prepaid) {
                    prepaid = true;
                    account = target;
                    target = null; // The grant now owns the commitment.
                } else if (!settled) {
                    checkAccount(target);
                }
                return true;
            }
        } finally {
            if (target != null) target.releaseView(charge);
        }
    }

    private void checkAccount(SharedMemory.Receive target) {
        if (account != target) throw new IllegalStateException("splice grant crossed target accounts");
    }

    public void retain() { references.incrementAndGet(); }

    /** Releases an owner and refunds unconfirmed bytes when no attempt can still cover them. */
    public void release() {
        if (references.decrementAndGet() != 0) return;
        SharedMemory.Receive funding;
        long refund, result;
        synchronized (this) {
            funding = account;
            result = settled ? -1 : atomic || coverage == null ? 0 : coverage.total();
            if (!settled && observer != null) observer.finished(bytes - enqueued, enqueued - result);
            refund = settled ? 0 : finishCoverage();
            if (prepaid) refund += GRANT_CHARGE;
            account = null;
        }
        refund(funding, refund);
        if (result >= 0) settle.accept(result);
    }

    /** Applies the complete portion of an existing carrier receipt. Overlaps count once. */
    public void accepted(long start, long end) {
        SharedMemory.Receive funding;
        long refund;
        boolean complete;
        synchronized (this) {
            if (settled) return;
            funding = account;
            long before = coverage == null ? 0 : coverage.total();
            if (start == 0 && end == bytes) {
                // Atomic datagrams and one-record writes need no sparse index.
                refund = finishCoverage();
                complete = true;
            } else {
                if (!prepaid || start < 0 || end > bytes || end < start) {
                    throw new IllegalStateException("partial splice receipt outside its prepaid grant");
                }
                if (start == end) return;
                if (coverage == null) coverage = new OffsetCoverage();
                long oldBase = base(coverage.prefix());
                coverage.add(start, end);
                complete = coverage.total() == bytes;
                refund = (base(coverage.prefix()) - oldBase) / SIZE * PAGE_CHARGE;
                if (complete) refund += finishCoverage();
            }
            if (observer != null) observer.handedOff((complete ? bytes : coverage.total()) - before);
        }
        refund(funding, refund);
        if (complete) settle.accept(bytes);
    }

    private long finishCoverage() {
        long charge = prepaid ? backingBytes() - GRANT_CHARGE : 0;
        if (coverage != null) charge -= coverage.prefix() / SIZE * PAGE_CHARGE;
        settled = true;
        coverage = null;
        return charge;
    }

    private static void refund(SharedMemory.Receive funding, long charge) {
        if (funding != null && charge != 0) funding.releaseView(charge);
    }

    /** Associates a send-lane range with its offset in the logical grant. */
    public record Slice(CarrierCharge charge, long offset) {}
}
