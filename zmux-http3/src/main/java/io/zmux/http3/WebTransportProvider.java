package io.zmux.http3;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.zmux.quic.QuicOptions;
import java.time.Duration;
import java.util.function.Consumer;

/** Operations that must be performed in QUIC's handshake, stream and recovery engines. */
interface WebTransportProvider {
    QuicOptions receiveLimits(QuicChannel connection);
    /** Attested upper bound for receive allocations and outstanding transport credit. */
    long receiveCapacity(QuicChannel connection);
    Duration reorderTimeout(QuicChannel connection);
    ChannelFuture stop(QuicStreamChannel stream, long wireCode);
    ChannelFuture reset(QuicStreamChannel stream, long wireCode);
    /** Reports terminal offsets only when the transport supplies them. */
    void observeTermination(QuicStreamChannel stream, Consumer<WebTransportStreamException> listener);

    /** Takes ownership of the header. A successful write is local submission. */
    ChannelFuture writePreface(QuicStreamChannel stream, ByteBuf header);

    static WebTransportProvider linked(QuicOptions options) {
        return new NettyWebTransportProvider(options);
    }

    static WebTransportProvider require(WebTransportProvider provider) {
        if (provider == null) {
            throw new UnsupportedOperationException(
                    "supply the host QUIC receive limits when binding WebTransport");
        }
        return provider;
    }
}
