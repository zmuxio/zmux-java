package io.zmux.quic;

import io.netty.handler.codec.quic.QuicCodecBuilder;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Transport windows and peer stream credit. These are carrier limits, not zmux stream limits.
 *
 * @param datagrams the connection negotiates QUIC DATAGRAM frames (RFC 9221), the datagram
 *     channel of its pipes: a session that enables datagrams sends those that fit on it
 *     instead of in records (EXTENSIONS.md Section 6.8). On by default.
 */
public record QuicOptions(long streamWindow, long bidirectionalStreams, long lanes, Duration idleTimeout, boolean datagrams) {
    private static final long MAX = (1L << 62) - 1;
    /** Datagrams the QUIC connection queues in each direction. */
    static final int DATAGRAM_QUEUE = 1024;

    public QuicOptions {
        Objects.requireNonNull(idleTimeout);
        if (streamWindow < 1 || streamWindow > MAX || bidirectionalStreams < 1 || lanes < 0
                || bidirectionalStreams > (1L << 60) || lanes > (1L << 60) || idleTimeout.isNegative()) {
            throw new IllegalArgumentException("invalid QUIC limits");
        }
        // Both counts are at most 2^60 (above), so their sum cannot overflow.
        if (streamWindow > MAX / (bidirectionalStreams + lanes)) {
            throw new IllegalArgumentException("connection receive window exceeds varint62");
        }
    }

    /** Options with QUIC datagrams negotiated. */
    public QuicOptions(long streamWindow, long bidirectionalStreams, long lanes, Duration idleTimeout) {
        this(streamWindow, bidirectionalStreams, lanes, idleTimeout, true);
    }

    public static QuicOptions defaults() { return new QuicOptions(1_250_000, 4, 4, Duration.ofSeconds(30)); }

    /** These options with QUIC datagrams negotiated or not. */
    public QuicOptions withDatagrams(boolean on) {
        return new QuicOptions(streamWindow, bidirectionalStreams, lanes, idleTimeout, on);
    }

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
}
