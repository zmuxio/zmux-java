package io.zmux.internal.loop;

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
public interface Transport {
    /** Immutable properties: framing, connection identity, budget, protocol. */
    TransportInfo info();

    SessionHost loop();

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

    /**
     * The bytes the carrier retransmitted so far, or -1 when it cannot tell (the socket
     * transports: the JDK has no query for the kernel's count). On the loop, outside the core.
     */
    default long retransmitted() { return -1; }

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

    /** Closes both halves; abortive when cause is not null. Idempotent. */
    void close(Throwable cause);

    /** Whether the transport is closed (or moved away). */
    boolean closed();

    /**
     * Leaves this loop without closing the connection: the returned transport, not yet
     * started, owns it on to with the unread input; this one becomes inert. Only for an idle
     * gated transport (nothing written). Null when it cannot move (it is closed then).
     */
    Transport moveTo(SessionHost to);

    /** The exporter of this carrier's innermost end-to-end handshake, read now; null without one or while it is incomplete. Any thread. */
    default byte[] export(String label, byte[] context, int length) { return null; }

    /** Whether this carrier declares the Bound contract; an exporter alone is no such declaration. */
    default boolean secure() { return false; }

    /** Whether a Bound Join can run here: a split carrier cannot at present. */
    default boolean bindable() { return secure() && info().carrier().kind() != io.zmux.CarrierKind.SPLIT; }

    /** The record mode of an Establish that ran on this carrier, which is then a secure first pipe; null (or UNMANAGED) otherwise. */
    default io.zmux.RecordMode firstPipeMode() { return null; }

    /** The peer an Establish on this carrier authenticated, the session's host value; null otherwise. */
    default Object carrierPeer() { return null; }

    /** The padding bytes a handshake run on the carrier before this transport added. */
    default long handshakePadding() { return 0; }

    /** Whether this carrier has an exporter, whether or not its value exists yet. */
    default boolean hasExporter() { return false; }

    /** The TLS session of the handshake this process ran for the carrier; null without one. */
    default javax.net.ssl.SSLSession tlsSession() { return null; }

    /** The admission's bound of a first message ({@link io.zmux.carrier.PipeReader#limitFirstMessage}), before it starts. */
    default void limitFirstMessage(int limit) {}

    /** The datagram channel of the carrier, the same as its info's; null when it has none. */
    default CarrierChannel channel() { return null; }

    /** Opens an independent outbound lane on the same connection, or null when the carrier cannot. */
    default java.util.concurrent.CompletionStage<io.zmux.carrier.PipeIo> openLane() { return null; }

    /** Whether half (a PipeReader or PipeWriter) is one of this transport's carrier halves. */
    default boolean ownsHalf(Object half) { return false; }

    /** Completes once the carrier is released (both halves closed); for carriers of pipe inputs. */
    default java.util.concurrent.CompletionStage<Void> whenClosed() {
        throw new UnsupportedOperationException("not a pipe input's carrier");
    }

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
        Transport t = claim(io);
        if (t == null) t = new ExternalTransport(host, io);
        else if (t.loop() != host) t = t.moveTo(host);
        return t == null || io.capabilities().seal() == null ? t : new SealedTransport(t, io.capabilities().seal(), io.capabilities().exporter());
    }

    /** The input's own transport (on its own host, not started), or null. */
    static Transport claim(io.zmux.carrier.PipeIo io) {
        Object half = io.reader() != null ? io.reader() : io.writer();
        return half instanceof NativePipe n ? n.claim(io) : null;
    }
}
