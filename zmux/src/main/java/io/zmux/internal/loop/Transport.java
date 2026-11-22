package io.zmux.internal.loop;

import io.zmux.internal.spi.AttachmentRelease;
import io.zmux.internal.spi.CarrierProvider;

import io.zmux.internal.core.Batch;

/**
 * The carrier of one pipe lane (both halves of a bidirectional carrier), owned by one loop
 * and used only there. A transport knows nothing of its owner: what it reads, and the
 * outcome of what it writes, go to the {@link Inbound} it is bound to. A listener binds a
 * new transport to the gates that classify it and then to a session's attachment
 * ({@link CarrierSet}); the binding changes only on the loop, between deliveries or from
 * inside one (the delivery then reports {@link Inbound#GONE} when the transport left).
 * Each half is closed exactly once; failures abort.
 */
public sealed interface Transport extends AttachmentRelease.Resource, CarrierProvider permits PhysicalTransport {
    /** Immutable properties: framing, connection identity, budget, protocol. */
    TransportInfo info();

    @Override default Object carrierIdentity() { return info().connection(); }

    SessionHost loop();

    /** Close a carrier still in admission. Session and outbound transports have their own owner. */
    default void reject(Throwable cause) {
        CarrierSetup owner = setup();
        owner.reject(this, cause);
    }

    CarrierSetup setup();

    /** Binds in and starts reading the first time; later calls only rebind. */
    void start(Inbound in);

    /** Binds in without starting (a paused or not yet started transport). */
    void rebind(Inbound in);

    /** The inbound this transport delivers to (null before the first binding). */
    Inbound inbound();

    /** Why the input half failed, as its carrier reported it; null while it has not, or when it ended gracefully. */
    default Throwable inputError() { return null; }

    /** Carrier halves present; lane identifiers are independent in each direction. */
    int directions();

    /** Restricts I/O to the directions an admitted attachment owns, ending the others. */
    void restrict(int required);

    /** Whether a new batch may be handed over now (no write in flight, output open). */
    boolean writable();

    /**
     * Writes b; the outcome goes to the inbound's {@link Inbound#written} (possibly before
     * this returns). A transport that closes with the write in flight releases b itself.
     */
    void write(Batch b);

    /**
     * The bytes written that the carrier holds unsent, or -1 when it cannot tell (the socket
     * transports: a write completes once the socket took every byte, and the JDK has no query
     * for the kernel's unsent bytes). On the loop, outside the core.
     */
    default long unsent() { return -1; }

    /** Stops reading until {@link #resumeInput}; what was read stays buffered. */
    void pauseInput();

    /** Reads again, after delivering what is buffered (and an end of input deferred meanwhile). */
    void resumeInput();

    /** Delivers the buffered input again, to an inbound that replaced the one that left it. */
    void redeliver();

    /** Gracefully ends the write side once the write in flight is done. */
    void shutdownOutput();

    /** Stops only the input half. */
    void shutdownInput();

    /** Whether the transport is closed (or moved away). */
    boolean isClosed();

    /** The exporter of this carrier's innermost end-to-end handshake, read now; null without one or while it is incomplete. Any thread. */
    default byte[] export(String label, byte[] context, int length) { return null; }

    /** Whether this carrier declares the Bound contract; an exporter alone is no such declaration. */
    default boolean isSecure() { return false; }

    /** Whether a Bound Join can run here: a split carrier cannot at present. */
    default boolean bindable() { return isSecure() && info().carrier().kind() != io.zmux.CarrierKind.SPLIT; }

    /** The record mode of an Establish that ran on this carrier, which is then a secure first pipe; null (or UNMANAGED) otherwise. */
    default io.zmux.RecordMode firstPipeMode() { return null; }

    /** The peer an Establish on this carrier authenticated, the session's host value; null otherwise. */
    default Object carrierPeer() { return null; }

    /** The padding bytes a handshake run on the carrier before this transport added. */
    default long handshakePadding() { return 0; }

    /** Whether this carrier has an exporter, whether or not its value exists yet. */
    default boolean exporterPresent() { return false; }

    /** The TLS session of the handshake this process ran for the carrier; null without one. */
    default javax.net.ssl.SSLSession tlsSession() { return null; }

    /** Whether a prefix read can continue after its admission deadline without closing the input. */
    default boolean resumesAfterPeekTimeout() { return false; }

    /** The admission's bound of a first message ({@link io.zmux.carrier.PipeReader#limitFirstMessage}), before it starts. */
    default void limitFirstMessage(int limit) {}

    /** The datagram channel of the carrier, the same as its info's; null when it has none. */
    default CarrierChannel channel() { return null; }

    /** Opens an independent outbound lane on the same connection, or null when the carrier cannot. */
    default java.util.concurrent.CompletionStage<io.zmux.carrier.PipeIo> openLane() { return null; }

    /** Whether half (a PipeReader or PipeWriter) is one of this transport's carrier halves. */
    default boolean ownsHalf(Object half) { return false; }

    /**
     * Completes once the carrier is released (both halves closed, their tasks ended); at once for
     * a transport that releases its carrier when it closes.
     */
    @Override default java.util.concurrent.CompletionStage<Void> whenClosed() {
        return RELEASED;
    }

    /** After close, the pending write's release if its completion still owns a carrier batch. */
    @Override default java.util.concurrent.CompletionStage<Void> writesReleased() { return RELEASED; }

    /** The release of a transport that releases its carrier as it closes. */
    java.util.concurrent.CompletionStage<Void> RELEASED = java.util.concurrent.CompletableFuture.completedStage(null);

    /**
     * The session this transport carries ended with e (its close follows): a carrier shared by
     * several pipes, such as a QUIC connection, learns the code to close itself with. On the loop.
     */
    default void sessionEnded(io.zmux.internal.core.SessionError e) {}

    /**
     * A session took this carrier: it was attached to the session's carriers ({@link
     * CarrierSet}), the one place that does so. Before it, every write is the listener's
     * refusal path, which writes nothing.
     */
    default void taken() {}

    /**
     * Completes once a session took the carrier (true) or the carrier was closed before one did
     * (false: the listener refused it and wrote nothing on it); for carriers of pipe inputs.
     */
    default java.util.concurrent.CompletionStage<Boolean> whenTaken() {
        throw new UnsupportedOperationException("not a pipe input's carrier");
    }

    /**
     * The transport of a pipe input for a session on host: the input's own carrier when it
     * offers one ({@link NativePipe}; moved to host when it lives elsewhere), else an
     * {@link ExternalTransport} on host. Null when the native carrier closed meanwhile.
     */
    static Transport of(SessionHost host, io.zmux.carrier.PipeIo io) {
        return of(host, io, null);
    }

    /** Register the transport before sealing can fail and close the returned wrapper. */
    static Transport of(SessionHost host, io.zmux.carrier.PipeIo io, AttachmentRelease attachment) {
        Transport t = claim(io);
        if (t == null) t = new ExternalTransport(host, io);
        else if (t.loop() != host) t = CarrierSetup.moveUnowned(t, host);
        if (t != null && attachment != null) attachment.track(t);
        if (t == null) return null;
        try { return seal(t, io); }
        catch (IllegalArgumentException belowFloor) { t.reject(belowFloor); throw belowFloor; }
    }

    static Transport seal(Transport carrier, io.zmux.carrier.PipeIo io) {
        var records = PipeIoAccess.get().records(io);
        return records == null ? carrier : new SealedTransport(carrier, records, io.capabilities().exporter());
    }

    /** The input's own transport (on its own host, not started), or null. */
    static Transport claim(io.zmux.carrier.PipeIo io) {
        Object half = io.reader() != null ? io.reader() : io.writer();
        return half instanceof NativePipe n ? n.claim(io) : null;
    }
}
