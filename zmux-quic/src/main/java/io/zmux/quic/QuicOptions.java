package io.zmux.quic;

import io.netty.handler.codec.quic.QuicCodecBuilder;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Transport windows and peer stream credit of a QUIC connection. These are carrier limits, not
 * zmux stream limits. Immutable; built by {@link #builder()}, {@link #defaults()} the defaults.
 */
public final class QuicOptions {
    private static final long MAX = (1L << 62) - 1;
    /** Datagrams the QUIC connection queues in each direction. */
    static final int DATAGRAM_QUEUE = 1024;
    /** The default receive window of each stream, in bytes. */
    public static final long DEFAULT_STREAM_WINDOW = 1_250_000;
    /** The default count of bidirectional streams the peer may open. */
    public static final long DEFAULT_BIDIRECTIONAL_STREAMS = 4;
    /** The default count of unidirectional lanes the peer may open. */
    public static final long DEFAULT_LANES = 4;
    /** The default idle timeout. */
    public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofSeconds(30);

    private static final QuicOptions DEFAULTS = builder().build();

    private final long streamWindow, bidirectionalStreams, lanes;
    private final Duration idleTimeout;
    private final boolean datagrams;

    private QuicOptions(Builder b) {
        streamWindow = b.streamWindow; bidirectionalStreams = b.bidirectionalStreams; lanes = b.lanes;
        idleTimeout = b.idleTimeout; datagrams = b.datagrams;
        // Both counts are at most 2^60 (checked by the builder), so their sum cannot overflow.
        if (streamWindow > MAX / (bidirectionalStreams + lanes)) {
            throw new IllegalArgumentException("connection receive window exceeds varint62");
        }
    }

    public static QuicOptions defaults() { return DEFAULTS; }

    public static Builder builder() { return new Builder(); }

    /** A builder holding these options. */
    public Builder toBuilder() {
        return new Builder().streamWindow(streamWindow).bidirectionalStreams(bidirectionalStreams).lanes(lanes)
                .idleTimeout(idleTimeout).datagrams(datagrams);
    }

    /** The receive window of each stream, in bytes. */
    public long streamWindow() { return streamWindow; }

    /** The bidirectional streams the peer may open: its pipes. */
    public long bidirectionalStreams() { return bidirectionalStreams; }

    /** The unidirectional streams the peer may open: its lanes. */
    public long lanes() { return lanes; }

    /** The connection's idle timeout; zero: none. */
    public Duration idleTimeout() { return idleTimeout; }

    /**
     * Whether the connection negotiates QUIC DATAGRAM frames (RFC 9221). A connection always offers them, and a
     * session uses them when it enables datagrams ({@link io.zmux.ZmuxConfig.Builder#enableDatagrams}, the one
     * switch); only this module's tests turn them off, to stand for a peer without the extension.
     */
    boolean isDatagrams() { return datagrams; }

    /** Enough connection credit for all configured receive streams to fill their own windows. */
    public long connectionWindow() { return streamWindow * (bidirectionalStreams + lanes); }

    <B extends QuicCodecBuilder<B>> B apply(B builder) {
        long millis;
        try { millis = idleTimeout.toMillis(); } catch (ArithmeticException e) { millis = Long.MAX_VALUE; }
        builder.maxIdleTimeout(millis, TimeUnit.MILLISECONDS).initialMaxData(connectionWindow())
                .initialMaxStreamDataBidirectionalLocal(streamWindow).initialMaxStreamDataBidirectionalRemote(streamWindow)
                .initialMaxStreamDataUnidirectional(streamWindow).initialMaxStreamsBidirectional(bidirectionalStreams)
                .initialMaxStreamsUnidirectional(lanes);
        if (datagrams) builder.datagram(DATAGRAM_QUEUE, DATAGRAM_QUEUE);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof QuicOptions q && streamWindow == q.streamWindow && bidirectionalStreams == q.bidirectionalStreams
                && lanes == q.lanes && idleTimeout.equals(q.idleTimeout) && datagrams == q.datagrams;
    }

    @Override
    public int hashCode() { return Objects.hash(streamWindow, bidirectionalStreams, lanes, idleTimeout, datagrams); }

    @Override
    public String toString() {
        return "QuicOptions[streamWindow=" + streamWindow + ", bidirectionalStreams=" + bidirectionalStreams + ", lanes=" + lanes
                + ", idleTimeout=" + idleTimeout + ", datagrams=" + datagrams + "]";
    }

    /** Builds {@link QuicOptions}; every setter validates its value, {@link #build()} their combination. */
    public static final class Builder {
        private long streamWindow = DEFAULT_STREAM_WINDOW, bidirectionalStreams = DEFAULT_BIDIRECTIONAL_STREAMS, lanes = DEFAULT_LANES;
        private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
        private boolean datagrams = true;

        private Builder() {}

        /** The receive window of each stream, from 1 to 2^62 - 1 bytes; default {@link #DEFAULT_STREAM_WINDOW}. */
        public Builder streamWindow(long bytes) {
            if (bytes < 1 || bytes > MAX) throw new IllegalArgumentException("streamWindow outside 1 to 2^62 - 1");
            streamWindow = bytes;
            return this;
        }

        /** The bidirectional streams the peer may open, from 1 to 2^60; default {@link #DEFAULT_BIDIRECTIONAL_STREAMS}. */
        public Builder bidirectionalStreams(long n) {
            if (n < 1 || n > (1L << 60)) throw new IllegalArgumentException("bidirectionalStreams outside 1 to 2^60");
            bidirectionalStreams = n;
            return this;
        }

        /** The unidirectional lanes the peer may open, from 0 to 2^60; default {@link #DEFAULT_LANES}. */
        public Builder lanes(long n) {
            if (n < 0 || n > (1L << 60)) throw new IllegalArgumentException("lanes outside 0 to 2^60");
            lanes = n;
            return this;
        }

        /** The connection's idle timeout, zero or more (zero: none); default {@link #DEFAULT_IDLE_TIMEOUT}. */
        public Builder idleTimeout(Duration d) {
            if (Objects.requireNonNull(d, "idleTimeout").isNegative()) throw new IllegalArgumentException("negative idleTimeout");
            idleTimeout = d;
            return this;
        }

        /** Tests only: a connection without QUIC DATAGRAM frames, standing for a peer without the extension. */
        Builder datagrams(boolean on) { datagrams = on; return this; }

        /**
         * Builds the options.
         *
         * @throws IllegalArgumentException when the connection window the settings give exceeds 2^62 - 1 */
        public QuicOptions build() { return new QuicOptions(this); }
    }
}
