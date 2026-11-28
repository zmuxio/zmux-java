package io.zmux.internal.spi;

import io.zmux.internal.memory.MemoryBudget;

/** Pre-attachment reservations on the same physical-carrier account used at handoff. */
@FunctionalInterface
public interface CarrierBudget {
    long DELIVERY_BYTES = 256;

    Runnable acquire(Object physical, long providerBytes, long privateBytes);

    static CarrierBudget standalone(long capacity) {
        return new MemoryBudget(capacity, () -> { })::acquireCarrier;
    }

    /** A multiplexed extension may have a distinct namespace for its lanes. */
    interface Namespace {
        Object physicalConnection();
    }

    static Object physical(Object namespace) {
        return namespace instanceof Namespace scoped ? scoped.physicalConnection() : namespace;
    }
}
