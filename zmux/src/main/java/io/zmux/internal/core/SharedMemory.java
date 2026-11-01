package io.zmux.internal.core;

/**
 * A memory budget shared by several sessions (a listener's, API.md Section 4.4); the core only
 * charges and reads it. Implementations are thread-safe: the sessions run on different loops.
 */
public interface SharedMemory {
    /** Bytes, 0 = unlimited. */
    long limit();

    long used();

    /** At or above the limit, until usage falls below three quarters of it. */
    boolean pressure();

    /** Charges delta bytes (negative releases). */
    void add(long delta);

    /** Charges one shared carrier connection once however many sessions use it; the result releases this use. */
    Runnable acquireCarrier(Object connection, long bytes);
}
