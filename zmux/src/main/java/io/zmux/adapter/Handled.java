package io.zmux.adapter;

import io.zmux.AdmissionVerdict;
import io.zmux.JoinHint;
import io.zmux.ZmuxFuture;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.loop.ListenerHub;
import io.zmux.internal.spi.AdapterPorts;
import io.zmux.internal.spi.Spi;
import java.util.Objects;

/**
 * A carrier handed to a listener ({@link ListenerAdapter#handle}, {@link Slot#handle}).
 * {@link #verdict()} is the carrier's final admission verdict, never {@link AdmissionVerdict#DEFER}
 * or {@link AdmissionVerdict#MORE} ({@link AdmissionVerdict#ACCEPT} without
 * an admission; a {@link AdmissionVerdict#FALLBACK} without a target is reported as
 * {@link AdmissionVerdict#STALL}, a stall beyond its source's share as
 * {@link AdmissionVerdict#CLOSE}); {@link #closed()} ends with the carrier's lifetime, which keeps
 * the handler of an HTTP carrier alive; {@link #taken()} completes with true once a session took
 * the carrier, or with false when the carrier ended before one did: the listener (or the
 * admission) refused it, and wrote nothing on it. Cancelling one of the futures changes nothing
 * for the carrier.
 */
public final class Handled {
    private final AdapterPorts.Listener port;
    private final AdapterPorts.Carrier carrier;

    Handled(AdapterPorts.Listener port, AdapterPorts.Carrier carrier) {
        this.port = port;
        this.carrier = carrier;
    }

    public ZmuxFuture<AdmissionVerdict> verdict() { return Spi.future(carrier.verdict(), null); }

    public ZmuxFuture<Boolean> taken() { return Spi.future(carrier.taken(), null); }

    io.zmux.internal.loop.CarrierSetup setup() { return carrier.setup; }

    /** Waits only for setup. Interruption withdraws this request before handoff. */
    public void awaitSetup() throws java.io.IOException {
        if (io.zmux.internal.loop.SessionHost.onHostThread()) throw new IllegalStateException("setup wait on owner loop");
        carrier.setup.await();
    }

    /**
     * Both handles of a paired request await retirement of the whole attachment. Its token is reusable
     * on completion. A writer without a release acknowledgement can delay this notification by up to
     * one linger interval while zmux waits to escalate its graceful close.
     */
    public ZmuxFuture<Void> closed() { return Spi.future(carrier.closed(), null); }

    /**
     * Whether the listener's fallback ({@link io.zmux.ListenerConfig#fallback()}) took the carrier over as a
     * {@link io.zmux.Takeover}, once {@link #verdict()} completed with {@link AdmissionVerdict#TAKEOVER} or byte-stream {@link AdmissionVerdict#FALLBACK}; false for a
     * request given back to its adapter's own fallback.
     */
    public boolean takenOver() { return carrier.takenOver(); }

    /**
     * Whether the host detached the carrier it took over ({@link io.zmux.Takeover#detach()}): the adapter then leaves
     * what the carrier runs on (its QUIC connection, its HTTP request) to the host instead of ending it with the
     * carrier. Final once {@link #closed()} completed, since a detach precedes zmux's own close of the carrier.
     */
    public boolean detached() { return carrier.detached(); }

    /**
     * Hands over a further carrier of the connection this carrier's admission accepted (a
     * further stream of a QUIC connection, admitted by its first): it takes its own source
     * slot and is classified like this one, without being decided again. Only after an
     * {@link AdmissionVerdict#ACCEPT}; a carrier refused at its slot is closed.
     *
     * @throws IllegalStateException when this carrier was not accepted
     */
    public Handled sibling(PipeIo io, JoinHint hint) {
        Objects.requireNonNull(io);
        if (carrier.decided() != AdmissionVerdict.ACCEPT) {
            port.refused(io);
            throw new IllegalStateException("the carrier was not accepted");
        }
        ListenerAdapter.requireHint(port, io, hint);
        var setup = port.reserve(hint.source());
        if (setup == null) return new Handled(port, port.refused(io));
        return new Handled(port, port.transfer(setup, io, hint, true, carrier.value(), ListenerHub.Entry.Stream.CARRIER));
    }
}
