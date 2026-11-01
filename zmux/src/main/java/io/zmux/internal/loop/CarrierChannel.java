package io.zmux.internal.loop;

import java.nio.ByteBuffer;

/**
 * The datagram channel of a transport's carrier (EXTENSIONS.md Section 6.8) as the session's
 * loop uses it: what the core decides goes there is sent at once, never behind a batch write,
 * and what arrives for the pipe goes to the core. Used only on the transport's loop.
 */
public interface CarrierChannel {
    /** Receives the channel datagrams for a pipe: b[pos, end), Pipe Stream ID removed, valid during the call. */
    @FunctionalInterface
    interface Sink {
        void datagram(ByteBuffer b, int pos, int end);
    }

    /**
     * The Pipe Stream ID that starts the pipe's channel datagrams: the carrier stream ID of the
     * pipe on channels shared by the streams of a connection (QUIC, WebTransport); -1 on a
     * channel of the pipe's own stream.
     */
    long channelStreamId();

    /** The largest channel datagram, Pipe Stream ID included, the channel takes now; 0 while none. Cheap. */
    int maxChannelDatagram();

    /** Sends one channel datagram; refused runs on the loop when the carrier refuses it, possibly before this returns. */
    void send(byte[] datagram, Runnable refused);

    /** Hands the channel datagrams received for the pipe to sink until the returned stop runs. */
    Runnable receive(Sink sink);
}
