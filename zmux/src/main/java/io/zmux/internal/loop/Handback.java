package io.zmux.internal.loop;

import io.zmux.Admission;
import io.zmux.Takeover;
import io.zmux.carrier.PipeIo;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A carrier the admission handed back, as the host's fallback takes it: one {@link Forwarded} pipe view, and for a
 * plain TCP carrier its socket instead. Its state decides the one close of the carrier: {@link #done} closes what the
 * host took on withdrawal or fallback completion, unless {@link #detach} made the carrier the host's first; each half
 * is closed once, whoever closes it (Forwarded's halves are idempotent).
 */
public final class Handback implements Takeover {
    private final Admission.CarrierInfo info;
    private final Object value;
    private final SessionHost socketLoop;
    private final Forwarded forwarded;
    private final PipeIo pipe;
    private final CarrierSetup setup;
    /** The socket the host took, closed by {@link #done}; null while none was. */
    private volatile Socket socket;

    /** On the carrier's loop: takes c over, with replay (or what its read buffer holds) in front. */
    Handback(Transport c, Admission.CarrierInfo info, Object conn, byte[] replay, boolean ended, boolean graceful, boolean messages,
             Object value) {
        this.info = info;
        this.value = value;
        socketLoop = c instanceof SocketTransport ? c.loop() : null;
        setup = c.setup();
        forwarded = new Forwarded(c, replay, ended, graceful, messages);
        pipe = forwarded.pipe(info, conn);
    }

    @Override public Admission.CarrierInfo info() { return info; }

    @Override public Object value() { return value; }

    @Override public PipeIo pipe() { return pipe; }

    @Override public Optional<Messages> messages() {
        return pipe.framing() == io.zmux.carrier.Framing.MESSAGES ? Optional.of(new Messages(pipe.reader(), pipe.writer())) : Optional.empty();
    }

    @Override public Optional<Socket> socket() {
        if (setup.reclaimed() || socket != null || socketLoop == null) return Optional.empty();
        var taken = new CompletableFuture<Socket>();
        if (socketLoop.inLoop()) taken.complete(forwarded.detachSocket());
        else if (!socketLoop.tryExecute(() -> taken.complete(forwarded.detachSocket()))) return Optional.empty();
        Socket s = taken.join();
        if (s == null) return Optional.empty();
        socket = s;
        // Reclamation may have raced the socket transfer.
        if (setup.reclaimed()) closeSocket(s);
        return Optional.of(s);
    }

    @Override public boolean detach() {
        return setup.detach(!hostClosed());
    }

    /** Whether the host closed what it took: the socket's channel, or every half of the pipe view. */
    private boolean hostClosed() {
        Socket s = socket;
        //noinspection resource: a query on the channel the host owns, not a new resource
        return s != null ? !s.channel().isOpen() : forwarded.halvesClosed();
    }

    /** Whether the host detached the carrier: its adapter leaves what the carrier runs on to the host. */
    public boolean detached() { return setup.detached(); }

    /** Withdrawal or fallback completion reclaims the carrier once, unless already detached. */
    void done(Throwable cause) {
        Socket s = socket;
        if (s != null) { closeSocket(s); return; }
        if (cause != null) forwarded.abort(cause);
        else forwarded.closeBoth();
    }

    private static void closeSocket(Socket s) {
        try { s.channel().close(); } catch (java.io.IOException ignored) { /* Gone. */ }
    }
}
