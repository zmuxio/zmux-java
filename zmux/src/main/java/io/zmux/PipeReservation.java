package io.zmux;

import io.zmux.carrier.PipeIo;
import java.io.IOException;
import java.util.Objects;

/** A single-use out-of-band join. Close gives it up until attach transfers its ownership. */
public final class PipeReservation implements AutoCloseable {
    private final ZmuxSession session;
    private final long id;
    private static final java.lang.ref.Cleaner CLEANER = java.lang.ref.Cleaner.create(Thread.ofVirtual().name("zmux-reservation-cleaner").factory());
    private final String header;
    private final java.lang.ref.Cleaner.Cleanable cleanable;
    PipeReservation(ZmuxSession session, long id, String header) {
        this.session = session; this.id = id; this.header = header;
        var driver = session.driver;
        cleanable = CLEANER.register(this, () -> driver.cancelReservation(id));
    }
    public long id() { return id; }
    /** The unpadded base64url Zmux-Pipe-Join header value. Treat it as a bearer credential. */
    public String header() { return header; }
    /** Transfers the carrier once and waits for join confirmation. Failure closes its halves. */
    public ZmuxPipe attach(PipeIo io) throws IOException {
        session.driver.checkBlocking();
        try { return session.joined(session.driver.attachReserved(id, Objects.requireNonNull(io))); }
        finally { cleanable.clean(); java.lang.ref.Reference.reachabilityFence(this); }
    }
    public void cancel() { cleanable.clean(); }
    @Override public void close() { cancel(); }
}
