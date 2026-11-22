package io.zmux.internal.loop;

/** Operations reserved to the physical owner. Protocol phases receive only Transport. */
abstract sealed class PhysicalTransport implements Transport permits AbstractTransport, SealedTransport {
    /** All I/O views of a physical transport share its one close right. */
    abstract CarrierSetup.Bound closeRight();

    /** Closes both halves; abortive when cause is not null. Idempotent. */
    abstract void closePhysical(CarrierSetup.Bound.Permit permit, Throwable cause);

    /** The escalation for an asynchronous graceful close, or null for a synchronous close. */
    protected java.util.function.Consumer<Throwable> closeEscalation() { return null; }

    /**
     * Leaves this loop without closing the connection: the returned transport, not yet
     * started, owns it on to with the unread input; this one becomes inert. Only for an idle
     * gated transport (nothing written). Null when it cannot move (it is closed then).
     */
    abstract Transport movePhysical(CarrierSetup.Bound.Permit permit, SessionHost to);

}
