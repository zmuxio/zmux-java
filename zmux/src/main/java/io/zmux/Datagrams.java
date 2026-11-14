package io.zmux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;

/** The session datagrams of a {@link ZmuxSession}. */
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
     * Queues a datagram, waiting for send-queue space instead of dropping the oldest datagram;
     * never waits for limiter tokens. False when the datagram limiter dropped it.
     */
    public boolean sendWait(ByteBuffer payload) throws IOException {
        return session.sendSessionDatagram(payload, Long.MAX_VALUE);
    }

    /** {@link #sendWait(ByteBuffer)} waiting at most timeout; {@link java.net.SocketTimeoutException} after it. */
    public boolean sendWait(ByteBuffer payload, Duration timeout) throws IOException {
        return session.sendSessionDatagram(payload, ZmuxSession.deadline(timeout));
    }

    /** Waits for the next datagram. */
    public byte[] receive() throws IOException {
        return session.receiveSessionDatagram(Long.MAX_VALUE);
    }

    /** Waits at most timeout for the next datagram; {@link java.net.SocketTimeoutException} after it. */
    public byte[] receive(Duration timeout) throws IOException {
        return session.receiveSessionDatagram(ZmuxSession.deadline(timeout));
    }

    /** The largest payload as of the loop's last round; 0 until negotiated. */
    public int maxPayload() {
        return session.info().maxDatagramPayload();
    }
}
