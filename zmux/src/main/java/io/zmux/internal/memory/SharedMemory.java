package io.zmux.internal.memory;

/**
 * A memory budget shared by several sessions (a listener's); the core only
 * charges and reads it. Implementations are thread-safe: the sessions run on different loops.
 */
public interface SharedMemory {
    long UNLIMITED = Long.MAX_VALUE;
    /** Bytes, {@link SharedMemory#UNLIMITED} = unlimited. */
    long limit();

    long used();

    /** At or above the limit, until usage falls below three quarters of it. */
    boolean pressure();

    /** Commits an optional physical allocation if there is headroom. */
    boolean tryCharge(long capacity);

    /** The configured minimum receive credit and mandatory stream slot are reserved atomically. */
    final class Unavailable extends IllegalStateException {
        public Unavailable() { super("session memory budget exhausted"); }
    }

    /** Opens an account, or returns null if the minimum commitments cannot be backed. */
    Receive reserveReceive(long minimum, long wanted, long cost, long working, long loanFloor, long receive, long send);

    /** A window reservation shared with the physical holdings it backs. */
    interface Receive extends io.zmux.internal.buffer.ReceiveLoans.Budget {
        Object domain();
        long window();
        /** Covers up to wanted unresolved offsets, while protecting offsets already advertised. */
        long commit(long floor, long wanted);
        long reserveStreams(long wanted, long receive, long send);
        /** Holds a prepaid sending direction through its last pin, even after the account closes. */
        void claimSendFloor(long bytes);
        void returnSendFloor(long bytes);
        /** Notifies the registered owner when it can retry a denied commitment. */
        void onRoom(Runnable ready);
        /** Reconciles the send wait index with the set of waiting directions under the budget lock. */
        void waitForBacking(java.util.function.LongSupplier minimum);
        /** The account's actual held and reserved bytes, excluding shared carrier backing. */
        long reservedBytes();
        boolean pressure();
        void releaseStream(long receive, long send);
        long viewRoom();
        boolean reserveView(long bytes);
        void releaseView(long bytes);
        /** Charges storage backed by the existing commitment, with lifetime through final release. */
        boolean tryRetainReceive(long bytes);
        void releaseRetainedReceive(long bytes);
        void close();
    }

    /** Releases a previously committed optional allocation. */
    void release(long bytes);

    /** Atomically commits the shared connection and one private workspace; null when they will not fit. */
    Runnable acquireCarrier(Object connection, long sharedBytes, long privateBytes);
}
