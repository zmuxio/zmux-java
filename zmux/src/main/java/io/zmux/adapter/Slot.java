package io.zmux.adapter;

import io.zmux.JoinHint;
import io.zmux.ListenerConfig;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.loop.ListenerHub;
import io.zmux.internal.spi.AdapterPorts;
import java.net.InetSocketAddress;
import io.zmux.internal.loop.CarrierSetup;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A slot of a carrier's source ({@link ListenerConfig#maxPendingPerSource()}) with one of the
 * listener ({@link ListenerConfig#maxPending()}), taken by {@link ListenerAdapter#reserve} when an
 * entry accepts a connection, before its outer handshake; it is used once, by
 * {@link #handle(PipeIo, JoinHint)}, {@link #handshakeFailed()} or
 * {@link #release()}.
 */
public final class Slot {
    private final AdapterPorts.Listener port;
    private final InetSocketAddress peer, source;
    private final AtomicReference<CarrierSetup> reservation;

    Slot(AdapterPorts.Listener port, InetSocketAddress peer, InetSocketAddress source, CarrierSetup setup) {
        reservation = new AtomicReference<>(setup);
        this.port = port;
        this.peer = peer;
        this.source = source;
    }

    CarrierSetup reservation() { return java.util.Objects.requireNonNull(reservation.get()); }

    /** The address the carrier arrived from directly. */
    public InetSocketAddress peer() { return peer; }

    /** The carrier's source: the one given, else the peer. */
    public InetSocketAddress source() { return source; }

    /**
     * Hands the carrier over, as {@link ListenerAdapter#handle(PipeIo, JoinHint)}
     * does after taking a slot; hint's addresses are this slot's.
     */
    public Handled handle(PipeIo io, JoinHint hint) {
        return hand(io, hint, ListenerHub.Entry.Stream.CARRIER);
    }

    /** Uses this reservation for a new carrier classified on its authenticated HTTP request head. */
    public Handled handleRequest(PipeIo io, JoinHint hint, io.zmux.Admission.Fallback fallback, Upgrade takeoverHead) {
        return hand(io, hint, new ListenerHub.Entry.Request(true, fallback, takeoverHead));
    }

    private Handled hand(PipeIo io, JoinHint hint, ListenerHub.Entry entry) {
        java.util.Objects.requireNonNull(io);
        var setup = reservation.getAndSet(null);
        if (setup == null) {
            port.refused(io);
            throw new IllegalStateException("slot used");
        }
        ListenerAdapter.requireEntry(port, io, hint, entry, setup);
        return new Handled(port, port.transfer(setup, io, new JoinHint(hint.pairing(), hint.uplink(), source, peer, hint.request()),
                false, null, entry));
    }

    /** The entry gave the carrier up before handing it over (its connection ended unused): the slot goes, counted nowhere. */
    public void release() {
        take().refused();
    }

    /** The carrier's outer handshake (TLS, QUIC) failed or timed out: the slot goes, counted as such. */
    public void handshakeFailed() {
        port.handshakeFailed(take());
    }

    private CarrierSetup take() {
        var setup = reservation.getAndSet(null);
        if (setup == null) throw new IllegalStateException("slot used");
        return setup;
    }
}
