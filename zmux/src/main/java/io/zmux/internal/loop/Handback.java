package io.zmux.internal.loop;

import io.zmux.Admission;
import io.zmux.Takeover;
import io.zmux.carrier.PipeIo;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A carrier the admission handed back, as the host's fallback takes it: one {@link Forwarded} pipe view, and for a
 * plain TCP carrier its socket instead; {@link #done} ends whichever the host took, once.
 */
final class Handback implements Takeover {
    private final Admission.CarrierInfo info;
    private final Forwarded forwarded;
    private final PipeIo pipe;
    private final AtomicBoolean ended = new AtomicBoolean();
    /** The socket the host took, closed by {@link #done}; null while none was. */
    private volatile Socket socket;

    /** On the carrier's loop: takes c over, with replay (or what its read buffer holds) in front. */
    Handback(Transport c, Admission.CarrierInfo info, Object conn, byte[] replay, boolean ended, boolean graceful, boolean messages) {
        this.info = info;
        forwarded = new Forwarded(c, replay, ended, graceful, messages);
        PipeIo p = forwarded.pipe(info.applicationProtocol()).withCarrierConn(conn);
        if (info.exporter() != null) p = p.withExporter(info.exporter()).withTerminatedTls(info.terminatedTls());
        if (!info.serverName().isEmpty()) p = p.withServerName(info.serverName());
        pipe = p;
        this.carrier = c;
    }

    private final Transport carrier;

    @Override public Admission.CarrierInfo info() { return info; }

    @Override public PipeIo pipe() { return pipe; }

    @Override public Optional<Socket> socket() {
        if (ended.get() || socket != null || !(carrier instanceof SocketTransport)) return Optional.empty();
        var taken = new CompletableFuture<Socket>();
        if (carrier.loop().inLoop()) taken.complete(forwarded.detachSocket());
        else if (!carrier.loop().tryExecute(() -> taken.complete(forwarded.detachSocket()))) return Optional.empty();
        Socket s = taken.join();
        if (s == null) return Optional.empty();
        socket = s;
        // A fallback that ended while the socket was taken has it closed here.
        if (ended.get()) closeSocket(s);
        return Optional.of(s);
    }

    /** The host's fallback ended (failed with cause, or null): the carrier is closed, once. */
    void done(Throwable cause) {
        if (!ended.compareAndSet(false, true)) return;
        Socket s = socket;
        if (s != null) { closeSocket(s); return; }
        if (cause != null) forwarded.abort(cause);
        else forwarded.closeBoth();
    }

    private static void closeSocket(Socket s) {
        try { s.channel().close(); } catch (java.io.IOException ignored) { /* Gone. */ }
    }
}
