package io.zmux;

import io.zmux.internal.core.Session.DatagramMode;
import io.zmux.internal.core.Time;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;

/** The session datagrams of a {@link ZmuxSession}. */
public final class Datagrams {
    private final ZmuxSession session;

    Datagrams(ZmuxSession session) { this.session = session; }

    /** Queues a datagram with inherited options, evicting the oldest entry when full. */
    public void send(ByteBuffer payload) throws IOException {
        send(payload, DatagramSendOptions.INHERIT);
    }

    /**
     * Queues a datagram with hop-local options. Throws {@link DatagramDroppedException} when
     * local admission drops the datagram.
     */
    public void send(ByteBuffer payload, DatagramSendOptions options) throws IOException {
        session.submitDatagram(null, 0, payload, options, DatagramMode.DATAGRAM_ONLY, false, Time.NEVER);
    }

    /** Queues a datagram, waiting for send-queue space. */
    public void sendWait(ByteBuffer payload) throws IOException {
        session.submitDatagram(null, 0, payload, DatagramSendOptions.INHERIT, DatagramMode.DATAGRAM_ONLY, true, Time.NEVER);
    }

    /** Queues a datagram, waiting at most timeout for send-queue space. */
    public void sendWait(ByteBuffer payload, Duration timeout) throws IOException {
        session.submitDatagram(null, 0, payload, DatagramSendOptions.INHERIT, DatagramMode.DATAGRAM_ONLY, true,
                ZmuxSession.deadline(timeout));
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

    /** The largest payload on an eligible path, as of the loop's last round. */
    public int pathPayload() { return session.info().pathDatagramPayload(); }

    /** The largest native-channel payload, zero when unavailable. */
    public int nativePayload() { return session.info().nativeDatagramPayload(); }

    /** Whether a native channel can carry an original, even if only an empty datagram fits. */
    public boolean nativeAvailable() { return session.info().nativeDatagramAvailable(); }

    /** Feedback in the send direction from the reported pipe and channel. */
    public java.util.Optional<DatagramPathFeedback> feedback(PipeId pipe, DatagramPathFeedback.Channel channel) throws IOException {
        java.util.Objects.requireNonNull(pipe);
        java.util.Objects.requireNonNull(channel);
        return java.util.Optional.ofNullable(session.datagramFeedback(pipe, channel));
    }
}
