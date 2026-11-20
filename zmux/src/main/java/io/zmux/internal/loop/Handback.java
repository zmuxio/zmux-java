package io.zmux.internal.loop;

import io.zmux.Admission;
import io.zmux.Takeover;
import io.zmux.carrier.PipeIo;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A carrier the admission handed back, as the host's fallback takes it: one {@link Forwarded} pipe view, and for a
 * plain TCP carrier its socket instead. Its state decides the one close of the carrier: {@link #done} closes what the
 * host took when the fallback's stage completes, unless {@link #detach} made the carrier the host's first; each half
 * is closed once, whoever closes it (Forwarded's halves are idempotent).
 */
public final class Handback implements Takeover {
    /** The fallback runs: zmux closes the carrier when its stage completes. */
    private static final int HELD = 0;
    /** The host owns the carrier. */
    private static final int DETACHED = 1;
    /** The stage completed without a detach: zmux closed the carrier. */
    private static final int RETURNED = 2;

    private final Admission.CarrierInfo info;
    private final Object value;
    private final Transport carrier;
    private final Forwarded forwarded;
    private final PipeIo pipe;
    private final AtomicInteger state = new AtomicInteger(HELD);
    /** The socket the host took, closed by {@link #done}; null while none was. */
    private volatile Socket socket;

    /** On the carrier's loop: takes c over, with replay (or what its read buffer holds) in front. */
    Handback(Transport c, Admission.CarrierInfo info, Object conn, byte[] replay, boolean ended, boolean graceful, boolean messages,
             Object value) {
        this.info = info;
        this.value = value;
        carrier = c;
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
        if (state.get() == RETURNED || socket != null || !(carrier instanceof SocketTransport)) return Optional.empty();
        var taken = new CompletableFuture<Socket>();
        if (carrier.loop().inLoop()) taken.complete(forwarded.detachSocket());
        else if (!carrier.loop().tryExecute(() -> taken.complete(forwarded.detachSocket()))) return Optional.empty();
        Socket s = taken.join();
        if (s == null) return Optional.empty();
        socket = s;
        // A fallback whose stage completed while the socket was taken has it closed here.
        if (state.get() == RETURNED) closeSocket(s);
        return Optional.of(s);
    }

    @Override public boolean detach() {
        while (true) {
            int st = state.get();
            if (st == DETACHED) return true;
            if (st == RETURNED) throw new IllegalStateException("Takeover.detach after the fallback's stage completed: the carrier is closed");
            if (hostClosed()) return false;
            if (state.compareAndSet(HELD, DETACHED)) return true;
        }
    }

    /** Whether the host closed what it took: the socket's channel, or every half of the pipe view. */
    private boolean hostClosed() {
        Socket s = socket;
        //noinspection resource: a query on the channel the host owns, not a new resource
        return s != null ? !s.channel().isOpen() : forwarded.halvesClosed();
    }

    /** Whether the host detached the carrier: its adapter leaves what the carrier runs on to the host. */
    public boolean detached() { return state.get() == DETACHED; }

    /** The host's fallback stage completed (failed with cause, or null): the carrier is closed once, unless detached. */
    void done(Throwable cause) {
        if (!state.compareAndSet(HELD, RETURNED)) return;
        Socket s = socket;
        if (s != null) { closeSocket(s); return; }
        if (cause != null) forwarded.abort(cause);
        else forwarded.closeBoth();
    }

    private static void closeSocket(Socket s) {
        try { s.channel().close(); } catch (java.io.IOException ignored) { /* Gone. */ }
    }
}
