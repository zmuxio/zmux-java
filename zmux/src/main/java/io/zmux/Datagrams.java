package io.zmux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;

/** The session datagrams of a {@link ZmuxSession} (EXTENSIONS.md Section 6). */
public final class Datagrams {
    private final ZmuxSession session;

    Datagrams(ZmuxSession session) { this.session = session; }

    /**
     * Queues a datagram; never blocks (a full queue drops its oldest datagram). False when
     * the datagram limiter dropped it (counted as {@code datagrams_dropped_limit}).
     */
    public boolean send(ByteBuffer payload) throws IOException {
        return session.sendSessionDatagram(payload);
    }

    /**
     * Waits at most timeout (null: no limit) for send-queue space, never for limiter tokens.
     * False when the datagram limiter dropped it.
     */
    public boolean send(ByteBuffer payload, Duration timeout) throws IOException {
        return session.sendSessionDatagram(payload, timeout);
    }

    /** Waits for the next datagram. */
    public byte[] receive() throws IOException {
        return session.receiveSessionDatagram();
    }

    /** Waits at most timeout (null: no limit); {@link java.net.SocketTimeoutException} after it. */
    public byte[] receive(Duration timeout) throws IOException {
        return session.receiveSessionDatagram(timeout);
    }

    /** The largest payload as of the loop's last round; 0 until negotiated. */
    public int maxPayload() {
        return session.info().maxDatagramPayload();
    }
}
