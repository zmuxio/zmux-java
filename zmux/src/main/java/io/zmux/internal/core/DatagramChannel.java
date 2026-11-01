package io.zmux.internal.core;

/**
 * A pipe carrier's datagram channel as the core sees it (EXTENSIONS.md Section 6.8): an
 * unreliable datagram service of the carrier besides the reliable stream that is the pipe,
 * such as the QUIC DATAGRAM frames of the connection a QUIC stream belongs to. The core
 * decides per DATAGRAM frame whether it goes there ({@link Session#pollChannel}); the driver
 * sends what it returns and hands what the channel receives to
 * {@link Session#receiveChannelDatagram}.
 */
public interface DatagramChannel {
    /**
     * The Pipe Stream ID that starts the pipe's channel datagrams: the carrier stream ID of
     * the pipe on channels shared by the streams of a connection (QUIC, WebTransport);
     * {@link io.zmux.internal.wire.ChannelDatagram#NO_ID} on a channel of the pipe's own stream.
     */
    long channelStreamId();

    /**
     * The largest channel datagram, Pipe Stream ID included, the channel takes now; 0 while it
     * takes none (the carrier did not negotiate datagrams, or not yet). Cheap, never blocks.
     */
    int maxChannelDatagram();
}
