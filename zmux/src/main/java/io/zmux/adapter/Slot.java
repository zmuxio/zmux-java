package io.zmux.adapter;

import io.zmux.Admission;
import io.zmux.JoinHint;
import io.zmux.ListenerConfig;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.spi.AdapterPorts;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A slot of a carrier's source ({@link ListenerConfig#maxPendingPerSource()}) with one of the
 * listener ({@link ListenerConfig#maxPending()}), taken by {@link ListenerAdapter#reserve} when an
 * entry accepts a connection, before its outer handshake; it is used once, by
 * {@link #handle(PipeIo, JoinHint, Admission.Fallback)}, {@link #handshakeFailed()} or
 * {@link #release()}.
 */
public final class Slot {
    private final AdapterPorts.Listener port;
    private final InetSocketAddress peer, source;
    private final AtomicBoolean used = new AtomicBoolean();

    Slot(AdapterPorts.Listener port, InetSocketAddress peer, InetSocketAddress source) {
        this.port = port;
        this.peer = peer;
        this.source = source;
    }

    /** The address the carrier arrived from directly. */
    public InetSocketAddress peer() { return peer; }

    /** The carrier's source: the one given, else the peer. */
    public InetSocketAddress source() { return source; }

    /**
     * Hands the carrier over, as {@link ListenerAdapter#handle(PipeIo, JoinHint, Admission.Fallback)}
     * does after taking a slot; hint's addresses are this slot's.
     */
    public Handled handle(PipeIo io, JoinHint hint, Admission.Fallback fallback) {
        take();
        return new Handled(port, port.transfer(io, new JoinHint(hint.pairing(), hint.uplink(), source, peer, hint.request()),
                false, null, fallback));
    }

    /** The entry gave the carrier up before handing it over (its connection ended unused): the slot goes, counted nowhere. */
    public void release() {
        take();
        port.release(source);
    }

    /** The carrier's outer handshake (TLS, QUIC) failed or timed out: the slot goes, counted as such. */
    public void handshakeFailed() {
        take();
        port.handshakeFailed(source);
    }

    private void take() {
        if (!used.compareAndSet(false, true)) throw new IllegalStateException("slot used");
    }
}
