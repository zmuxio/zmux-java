package io.zmux.http3;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.http3.Http3FrameToHttpObjectCodec;
import io.netty.handler.codec.http3.Http3HeadersFrame;
import io.netty.handler.codec.http3.Http3HeadersValidationException;
import io.netty.handler.codec.quic.QuicCodecBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.zmux.carrier.PipeIo;
import io.zmux.http.internal.HttpTransport;
import io.zmux.internal.spi.CarrierProvider;
import io.zmux.internal.spi.Loops;
import io.zmux.quic.QuicExporter;
import io.zmux.quic.QuicOptions;
import io.zmux.quic.internal.QuicCarrierStats;
import io.zmux.quic.internal.QuicHandshake;
import io.netty.util.AttributeKey;
import java.net.InetSocketAddress;
import java.util.List;
import javax.net.ssl.SSLEngine;

/** Adapts each H3 request stream to the messages used by the shared HTTP carrier. */
final class Http3Streams {
    static HttpTransport.Stream requestStream(QuicStreamChannel stream) {
        var request = HttpTransport.stream(stream);
        assert request != null : "HTTP/3 request transport is installed before request handlers";
        return request;
    }

    private Http3Streams() { }

    static <B extends QuicCodecBuilder<B>> B configure(QuicOptions options, B builder) {
        return options.apply(builder, Http3.MIN_INITIAL_MAX_STREAMS_UNIDIRECTIONAL,
                Http3.MIN_INITIAL_MAX_STREAM_DATA_UNIDIRECTIONAL);
    }

    static void install(ChannelPipeline pipeline, boolean server) {
        var stream = (QuicStreamChannel) pipeline.channel();
        var wt = WebTransportConnection.get(stream.parent());
        if (stream.isLocalCreated() && wt != null) {
            wt.observeRequest(stream);
            pipeline.addFirst(new WebTransportHttpFrames(0));
        }
        HttpTransport.attach(stream, new RequestStream(stream, QuicHandshake.observe(stream)));
        pipeline.addLast(WireHeaders.INSTANCE, new Http3FrameToHttpObjectCodec(server), NoChunkedEncoding.INSTANCE);
    }

    static void raw(QuicStreamChannel stream, ChannelHandler handler) {
        var pipeline = stream.pipeline();
        var conversion = pipeline.context(WireHeaders.INSTANCE);
        if (conversion == null) pipeline.addLast(handler);
        else pipeline.addBefore(conversion.name(), null, handler);
    }

    private record RequestStream(QuicStreamChannel stream, io.zmux.Admission.OuterSecurity outerSecurity) implements HttpTransport.Stream {
        @Override public Channel connection() { return stream.parent(); }
        @Override public Object carrierIdentity() { return connection(); }
        @Override public CarrierProvider.Query carrierQuery(Object identity) {
            return QuicCarrierStats.query(stream.parent(), identity);
        }

        @Override public InetSocketAddress peer() {
            return stream.parent().remoteSocketAddress() instanceof InetSocketAddress peer ? peer : null;
        }

        @Override public SSLEngine tls() { return stream.parent().sslEngine(); }

        @Override public PipeIo.Exporter exporter() {
            SSLEngine engine = tls();
            return (label, context, length) -> QuicExporter.export(engine, label, context, length);
        }

        @Override public void abort() {
            stream.shutdown(Http3ErrorCode.H3_REQUEST_CANCELLED.code()).addListener(ChannelFutureListener.CLOSE);
        }
    }

    private static final String PROTOCOL_FIELD = "x-http3-protocol";
    private static final AttributeKey<List<CharSequence>> WIRE_PROTOCOL_FIELDS =
            AttributeKey.valueOf(Http3Streams.class, "wireProtocolFields");

    @ChannelHandler.Sharable
    private static final class WireHeaders extends io.netty.channel.ChannelDuplexHandler {
        static final WireHeaders INSTANCE = new WireHeaders();

        @Override public void write(ChannelHandlerContext ctx, Object message, io.netty.channel.ChannelPromise promise) {
            if (message instanceof Http3HeadersFrame frame && frame.headers().method() != null) {
                var stream = (QuicStreamChannel) ctx.channel();
                var wt = WebTransportConnection.get(stream.parent());
                if (wt != null) wt.ordinary(stream);
            }
            ctx.write(message, promise);
        }

        @Override public void channelActive(ChannelHandlerContext ctx) {
            // The QUIC codec runs host stream initializers during registration, after the
            // HTTP/3 initializer is added. Set the request invariant before auto-read starts.
            ((QuicStreamChannel) ctx.channel()).config().setAllowHalfClosure(true);
            ctx.fireChannelActive();
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (cause instanceof Http3HeadersValidationException) {
                // The frame codec sends H3_MESSAGE_ERROR after delivering this exception.
                // Close the remaining request input after that stream reset has been written.
                Loops.run(ctx.executor(), ctx::close, () -> {});
            } else {
                ctx.fireExceptionCaught(cause);
            }
        }

        @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
            if (message instanceof Http3HeadersFrame frame) {
                var headers = frame.headers();
                if (headers.method() != null) {
                    var protocol = headers.protocol();
                    if (protocol != null) HttpTransport.protocol(ctx.channel(), protocol.toString());
                    // Keep the ordinary field apart from the codec's translation of :protocol.
                    ctx.channel().attr(WIRE_PROTOCOL_FIELDS).set(headers.getAll(PROTOCOL_FIELD));
                    headers.remove(PROTOCOL_FIELD);
                    try {
                        ctx.fireChannelRead(message);
                    } finally {
                        ctx.channel().attr(WIRE_PROTOCOL_FIELDS).set(null);
                    }
                    return;
                }
            }
            ctx.fireChannelRead(message);
        }
    }

    @ChannelHandler.Sharable
    private static final class NoChunkedEncoding extends ChannelInboundHandlerAdapter {
        static final NoChunkedEncoding INSTANCE = new NoChunkedEncoding();

        @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
            // Netty rejects wire Transfer-Encoding while decoding the raw headers. The
            // HTTP-object codec synthesizes it for HTTP/1.1 aggregators; our router uses FIN.
            if (message instanceof HttpMessage head) head.headers().remove(HttpHeaderNames.TRANSFER_ENCODING);
            if (message instanceof HttpRequest request) {
                var original = ctx.channel().attr(WIRE_PROTOCOL_FIELDS).get();
                if (original != null) request.headers().set(PROTOCOL_FIELD, original);
            }
            ctx.fireChannelRead(message);
        }
    }
}
