package io.zmux.internal.loop;

/** Operations reserved to the physical owner. Protocol phases receive only Transport. */
abstract sealed class PhysicalTransport implements Transport permits AbstractTransport, SealedTransport {
    private io.zmux.internal.buffer.WriteWorkspace nextSeal;
    private io.zmux.internal.buffer.WriteWorkspace encoder;
    private io.zmux.internal.buffer.WriteWorkspace history;

    /** The space reserved for adding a layer, before a Join publishes the new carrier. */
    final io.zmux.internal.buffer.WriteWorkspace nextSeal() {
        if (nextSeal == null) nextSeal = new io.zmux.internal.buffer.WriteWorkspace();
        return nextSeal;
    }

    io.zmux.internal.buffer.WriteWorkspace encoder() {
        if (encoder == null) encoder = new io.zmux.internal.buffer.WriteWorkspace();
        return encoder;
    }

    io.zmux.internal.buffer.WriteWorkspace history() {
        if (history == null) history = new io.zmux.internal.buffer.WriteWorkspace();
        return history;
    }

    /** TLS record storage shares one physical lifetime even through a bridge. */
    java.util.List<TlsTransport.RecordWorkspace> tlsWorkspaces() { return java.util.List.of(); }

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
