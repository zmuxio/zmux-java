package io.zmux.carrier;

import java.nio.ByteBuffer;

/**
 * A carrier's unreliable message channel. Capacity queries are cheap and thread-safe.
 * An external carrier can complete a send and deliver a borrowed input on its own I/O thread.
 */
public interface CarrierChannel {
    /** The carrier's local result, before any peer feedback. */
    enum Outcome { ACCEPTED, REJECTED, EXPIRED, UNKNOWN }

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

    /**
     * Structural datagram capacity, independent of transient room. Return a positive size limit,
     * -1 when the structural limit is unknown, or 0 when there is no native channel.
     */
    default int channelDatagramLimit(int room) { return -1; }

    /** Submits a datagram before expiry and reports its outcome once. Expiry uses the monotonic session clock. */
    void send(byte[] datagram, long expiry, java.util.function.Consumer<Outcome> completed);

    /** Tests a submission's expiry immediately before giving it to the underlying carrier. */
    static boolean expired(long expiry) {
        return expiry != Long.MAX_VALUE && io.zmux.internal.loop.Clock.now() >= expiry;
    }

    /** Watches capacity or writability until the returned stop runs. Null when the carrier has no such signal. */
    default Runnable watch(Runnable changed) { return null; }

    /** Hands the channel datagrams received for the pipe to sink until the returned stop runs. */
    Runnable receive(Sink sink);
}
