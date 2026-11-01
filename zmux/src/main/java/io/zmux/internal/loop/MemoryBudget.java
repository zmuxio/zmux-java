package io.zmux.internal.loop;

import io.zmux.internal.core.SharedMemory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A memory budget shared by the sessions of a listener (API.md Section 4.4): the sum
 * over them of buffered receive data, retained data and session window growth.
 * Sessions charge it with atomics only; pressure starts at the limit and ends below
 * three quarters of it, and each transition runs {@link #onChange} (which must only
 * hand the work to a serial context).
 */
public final class MemoryBudget implements SharedMemory {
    private final long limit;
    private final AtomicLong used = new AtomicLong();
    private final AtomicBoolean over = new AtomicBoolean();
    private final Runnable onChange;
    private final java.util.IdentityHashMap<Object, CarrierCharge> carriers = new java.util.IdentityHashMap<>();
    private static final class CarrierCharge { long bytes; int refs; CarrierCharge(long bytes) { this.bytes = bytes; } }

    /** Charges one shared connection once, even when several sessions or lanes use it. */
    @Override
    public synchronized Runnable acquireCarrier(Object connection, long bytes) {
        if (connection == null) return () -> {};
        if (bytes < 0) throw new IllegalArgumentException("negative carrier estimate");
        CarrierCharge charge = carriers.get(connection);
        if (charge == null) { charge = new CarrierCharge(bytes); carriers.put(connection, charge); add(bytes); }
        else if (bytes > charge.bytes) { add(bytes - charge.bytes); charge.bytes = bytes; }
        charge.refs++;
        CarrierCharge held = charge;
        AtomicBoolean released = new AtomicBoolean();
        return () -> {
            if (!released.compareAndSet(false, true)) return;
            synchronized (MemoryBudget.this) {
                if (--held.refs == 0) { carriers.remove(connection); add(-held.bytes); }
            }
        };
    }

    /** @param limit bytes, {@link io.zmux.internal.core.CoreConfig#UNLIMITED} = unlimited */
    public MemoryBudget(long limit, Runnable onChange) {
        this.limit = limit;
        this.onChange = onChange;
    }

    @Override public long limit() { return limit; }

    @Override public long used() { return Math.max(used.get(), 0); }

    @Override public boolean pressure() { return over.get(); }

    @Override
    public void add(long d) {
        long u = used.addAndGet(d);
        if (limit == io.zmux.internal.core.CoreConfig.UNLIMITED) return;
        if (u >= limit && !over.get()) {
            if (over.compareAndSet(false, true) && onChange != null) onChange.run();
        } else if (u < limit / 4 * 3 && over.get()) {
            if (over.compareAndSet(true, false) && onChange != null) onChange.run();
        }
    }
}
