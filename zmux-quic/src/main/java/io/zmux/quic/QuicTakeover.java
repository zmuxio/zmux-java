package io.zmux.quic;

import io.netty.handler.codec.quic.QuicChannel;
import io.zmux.Takeover;
import java.util.Optional;

/**
 * The QUIC side of a {@link Takeover}: a QUIC stream the listener's admission gave back
 * ({@link io.zmux.ListenerConfig#fallback()}) belongs to a connection that the host serves along
 * with it. The connection's other streams were closed, and zmux closes the connection once the
 * fallback is done with the stream.
 */
public final class QuicTakeover {
    private QuicTakeover() {}

    /** The QUIC connection of the stream t carries; empty when t is not a QUIC stream's. */
    public static Optional<QuicChannel> connection(Takeover t) {
        return t.pipe().carrierConn() instanceof QuicChannel c ? Optional.of(c) : Optional.empty();
    }
}
