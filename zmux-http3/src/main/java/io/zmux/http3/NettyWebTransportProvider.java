package io.zmux.http3;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamResetException;
import io.zmux.quic.QuicOptions;
import io.zmux.quic.internal.QuicVarInts;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

/** The transport capabilities exposed by Netty's quiche binding. */
final class NettyWebTransportProvider implements WebTransportProvider {
    private static final System.Logger LOG = System.getLogger(NettyWebTransportProvider.class.getName());
    // quiche 0.30.1 auto-tunes receive credit up to this ceiling, even for a small initial window.
    private static final long MAX_CONNECTION_WINDOW = 24L << 20;
    private final QuicOptions options;

    NettyWebTransportProvider(QuicOptions options) { this.options = Objects.requireNonNull(options); }

    @Override public QuicOptions receiveLimits(QuicChannel connection) { return options; }

    @Override public long receiveCapacity(QuicChannel connection) {
        long bidi = Math.multiplyExact(options.bidirectionalStreams(), options.streamWindow());
        long uniStreams = Math.max(options.lanes(), Http3.MIN_INITIAL_MAX_STREAMS_UNIDIRECTIONAL);
        long uniWindow = Math.max(options.streamWindow(), Http3.MIN_INITIAL_MAX_STREAM_DATA_UNIDIRECTIONAL);
        // QuicOptions.apply caps the augmented H3 control credit at QUIC's varint maximum.
        if (uniStreams > (QuicVarInts.MAX - bidi) / uniWindow) return QuicVarInts.MAX;
        return Math.max(MAX_CONNECTION_WINDOW, bidi + uniStreams * uniWindow);
    }

    @Override public Duration reorderTimeout(QuicChannel connection) {
        return options.idleTimeout().isZero() ? QuicOptions.DEFAULT_IDLE_TIMEOUT : options.idleTimeout();
    }

    @Override public ChannelFuture writePreface(QuicStreamChannel stream, ByteBuf header) {
        return stream.writeAndFlush(header);
    }

    static int sendCode(long wireCode) {
        // Netty widens a signed int to quiche's long. Narrowing a WT application code would alias another code.
        return wireCode >= 0 && wireCode <= Integer.MAX_VALUE ? (int) wireCode : Http3ErrorCode.H3_REQUEST_CANCELLED.code();
    }

    private static int sendCode(QuicStreamChannel stream, String operation, long requested) {
        int actual = sendCode(requested);
        if (actual != requested) {
            LOG.log(System.Logger.Level.WARNING, "{0} on stream {1}: wire code {2} mapped to {3}",
                    operation, Long.toString(stream.streamId()), Long.toHexString(requested), Integer.toHexString(actual));
        }
        return actual;
    }

    @Override public ChannelFuture stop(QuicStreamChannel stream, long wireCode) {
        return stream.shutdownInput(sendCode(stream, "STOP_SENDING", wireCode));
    }

    @Override public ChannelFuture reset(QuicStreamChannel stream, long wireCode) {
        // The linked Netty QUIC binding only exposes ordinary stream shutdown.
        return stream.shutdownOutput(sendCode(stream, "RESET_STREAM", wireCode));
    }

    @Override public void observeTermination(QuicStreamChannel stream, Consumer<WebTransportStreamException> listener) {
        stream.pipeline().addFirst(new ChannelInboundHandlerAdapter() {
            @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                if (cause instanceof QuicStreamResetException reset) {
                    listener.accept(new WebTransportStreamException(false, reset.applicationProtocolCode(), -1, -1));
                } else ctx.fireExceptionCaught(cause);
            }
        });
    }
}
