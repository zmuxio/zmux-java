package io.zmux.adapter;

import io.zmux.Admission;
import io.zmux.AdmissionVerdict;
import io.zmux.JoinHint;
import io.zmux.ListenerConfig;
import io.zmux.ListenerStats;
import io.zmux.ZmuxListener;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.spi.AdapterPorts;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;

/**
 * The operations of a {@link ZmuxListener} that only a carrier adapter, or a host running its own
 * accept loop, needs: arrival slots, handing carriers over, join lookups, setup-failure counting,
 * and the listener's own bounds. Reached through {@link ZmuxListener#adapter()}; thread-safe.
 */
public final class ListenerAdapter {
    static {
        AdapterPorts.install(new AdapterPorts.Views() {
            @Override public ListenerAdapter listener(AdapterPorts.Listener port) { return new ListenerAdapter(port); }
            @Override public SessionAdapter session(AdapterPorts.Session port) { return new SessionAdapter(port); }
        });
    }

    private final AdapterPorts.Listener port;

    private ListenerAdapter(AdapterPorts.Listener port) {
        this.port = port;
    }

    /**
     * The duration of a {@link AdmissionVerdict#STALL} for a carrier from source: within the
     * admission's bounds and the same for every carrier of one client (its source prefix,
     * {@link ListenerConfig#sourcePrefixV4()} and {@link ListenerConfig#sourcePrefixV6()}, the port
     * ignored) for the listener's lifetime; 0 without an admission.
     */
    public Duration stallDuration(InetSocketAddress source) {
        return Duration.ofNanos(port.stallNanos(source));
    }

    /**
     * For use in an {@link Admission.Decider}: whether prefix begins a Join Hello whose Locator
     * names a current session of this listener. A prefix that is not a Join or is malformed gives
     * (false, 0). A prefix that ends before the Hello's head (through the nonce) gives
     * (false, need): need is the total length to return as {@link Admission.Decision#more}, which
     * {@link Admission#maxPeek()} must allow (at most 291 for the suites defined). The answer is
     * advisory: the join still authenticates, and one whose session ended meanwhile is an unknown
     * Locator for the reject policy.
     */
    public JoinLookup knownJoin(ByteBuffer prefix) {
        return port.knownJoin(prefix);
    }

    /**
     * Takes a slot of source (null: peer) and one of the listener ({@link
     * ListenerConfig#maxPending()}) for a carrier an entry just accepted, before its outer
     * handshake; null when the source has its share of pending carriers or the listener takes
     * no carrier any more (counted in {@link ListenerStats#refused()}), or when every slot of the
     * listener is taken and none is stalled (counted in {@link ListenerStats#rejected()}): the
     * entry then closes the connection at once, without a handshake, a read or a decision.
     */
    public Slot reserve(InetSocketAddress peer, InetSocketAddress source) {
        InetSocketAddress s = source == null ? peer : source;
        return port.reserve(s) ? new Slot(port, peer, s) : null;
    }

    /**
     * Hands a carrier to the listener, the one entry for carriers its own sockets do not
     * accept (HTTP and WebSocket requests, QUIC connections, a host's own pipes): it takes a
     * slot of hint's source, then the listener's {@link ListenerConfig#admission() admission},
     * when configured, decides on it with hint's addresses and {@link JoinHint#request()
     * request} (a carrier decided on its request head is decided at once, without a prefix) and
     * the TLS metadata of io's capabilities; an accepted carrier is classified by hint's join or
     * pairing or by its in-band first record. fallback (null: none) takes a carrier the
     * admission gives back, with every byte read replayed. A carrier refused at its slot is closed.
     */
    public Handled handle(PipeIo io, JoinHint hint, Admission.Fallback fallback) {
        Objects.requireNonNull(io); Objects.requireNonNull(hint);
        if (!port.reserve(hint.source())) return new Handled(port, port.refused(io));
        return new Handled(port, port.transfer(io, hint, false, null, fallback));
    }

    /** {@link #handle(PipeIo, JoinHint, Admission.Fallback)} without a fallback target. */
    public Handled handle(PipeIo io, JoinHint hint) {
        return handle(io, hint, null);
    }

    /**
     * Counts a connection that an adapter running its own accept loop could not make a pipe input
     * of (a failed TLS handshake or its timeout, a failed HTTP or WebSocket upgrade) in
     * {@link ListenerStats#setupFailures()}; any thread.
     */
    public void countSetupFailure() {
        port.countSetupFailure();
    }

    /**
     * How long a new carrier may take to send its first record ({@link
     * ListenerConfig#firstRecordTimeout()}); adapters that read a header of their own before the
     * listener sees the carrier (an exchange request's authentication header) bound it with this.
     */
    public Duration firstRecordTimeout() {
        return port.firstRecordTimeout();
    }

    /** The bytes the listener's sessions hold against its memory budget. */
    public long memoryUsed() {
        return port.memoryUsed();
    }
}
