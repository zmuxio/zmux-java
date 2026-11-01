package io.zmux.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.ReferenceCountUtil;
import io.zmux.ZmuxListener;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Serves HTTP carriers into a {@link ZmuxListener} on Netty (API.md Section 4.8). Each request is classified by its head alone
 * ({@link HttpClass#classify}) and served by the adapter of its class: streaming carriers
 * (downlinks, uplinks, bidirectional requests), the exchange carrier, WebSockets. Each adapter
 * is enabled on its own; an endpoint with one enabled is that adapter alone. A request of a class
 * without an adapter, a request that is not a carrier, and one that its carrier's admission does
 * not accept goes on to the host's handler, with every body byte read replayed.
 *
 * <p>Every streaming and WebSocket carrier passes the listener's admission
 * ({@link ZmuxListener#admission()}); nothing is written before it is accepted. An exchange
 * carrier is authenticated per request by its {@link ExchangeConfig} PSK instead (the session's
 * credential is not presented on it), and a request that is not authenticated goes to the
 * host's handler with its body replayed. A request that an intermediary forwarded from TLS early data
 * ({@code Early-Data: 1}) is answered 425 before any admission. A response that carries a
 * downlink is sent with {@code Cache-Control: no-store} and {@code X-Accel-Buffering: no} and
 * without a Content-Length; an HTTP/1.1 connection is closed after a carrier.
 *
 * <p>{@link #handler()} mounts the endpoint in a host's own Netty pipeline: after the
 * {@code HttpServerCodec} of an HTTP/1.1 connection, and after the
 * {@code Http2StreamFrameToHttpObjectCodec} of each HTTP/2 stream; the handlers after it are the
 * host's. {@link #initializer} builds a whole server pipeline: TLS with ALPN (h2, http/1.1), or
 * cleartext HTTP/1.1 and HTTP/2 by prior knowledge (a reverse proxy that speaks HTTP/2 to the
 * server without TLS), and the PROXY protocol from trusted intermediaries.
 */
public final class HttpEndpoint implements AutoCloseable {
    final ZmuxListener listener;
    final boolean streaming;
    final ExchangeServer exchange;
    final WebSocketServer webSocket;
    final TrustedProxies trusted;

    /**
     * The source address of the requests of a connection (or of one HTTP/2 stream), for a host
     * that resolves it itself: set on the channel before its first request reaches the
     * endpoint, it replaces the connection's peer as the requests' source address. Without
     * {@link Builder#trusted} zmux uses it as given; with it, it is the address a trusted
     * intermediary's forwarding field is read from. A trusted PROXY protocol header sets it.
     * The peer of a carrier ({@link io.zmux.Admission.CarrierInfo#peer()}) stays the connection's.
     */
    public static final io.netty.util.AttributeKey<InetSocketAddress> SOURCE = io.netty.util.AttributeKey.valueOf("zmux.source");

    private HttpEndpoint(Builder b) {
        listener = b.listener;
        streaming = b.streaming;
        trusted = b.trusted;
        exchange = b.exchange == null ? null : new ExchangeServer(this, b.exchange);
        webSocket = b.webSocket == null ? null : new WebSocketServer(this, b.webSocket);
    }

    public static Builder builder(ZmuxListener listener) { return new Builder(listener); }

    /** A server SslContext from builder that selects h2 or http/1.1 by ALPN. */
    public static SslContext serverSsl(io.netty.handler.ssl.SslContextBuilder builder) throws javax.net.ssl.SSLException {
        return builder.applicationProtocolConfig(new io.netty.handler.ssl.ApplicationProtocolConfig(
                io.netty.handler.ssl.ApplicationProtocolConfig.Protocol.ALPN,
                io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                ApplicationProtocolNames.HTTP_2, ApplicationProtocolNames.HTTP_1_1)).build();
    }

    /** A new handler for one channel's pipeline (an HTTP/1.1 connection or an HTTP/2 stream), after its HTTP codec. */
    public ChannelHandler handler() { return new Router(this); }

    /**
     * A server pipeline for accepted connections: ssl (null: cleartext, HTTP/1.1 or HTTP/2 by
     * prior knowledge), the endpoint, then a handler from host for each connection and HTTP/2
     * stream (null: a 404 response to every request that reaches it).
     */
    public ChannelInitializer<Channel> initializer(SslContext ssl, Supplier<ChannelHandler> host) {
        Supplier<ChannelHandler> h = host == null ? NotFound::new : host;
        return new ChannelInitializer<>() {
            @Override protected void initChannel(Channel ch) {
                ChannelPipeline p = ch.pipeline();
                if (trusted != null && trusted.proxyProtocol()) p.addLast(new ProxyProtocolHandler(trusted));
                if (ssl != null) {
                    p.addLast(ssl.newHandler(ch.alloc()));
                    p.addLast(new ApplicationProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_1_1) {
                        @Override protected void configurePipeline(ChannelHandlerContext ctx, String protocol) {
                            if (ApplicationProtocolNames.HTTP_2.equals(protocol)) http2(ctx.pipeline(), h);
                            else http1(ctx.pipeline(), h);
                        }
                    });
                } else {
                    p.addLast(new PrefaceDetector(h));
                }
            }
        };
    }

    private void http1(ChannelPipeline p, Supplier<ChannelHandler> host) {
        p.addLast(new HttpServerCodec());
        p.addLast(handler());
        p.addLast(host.get());
    }

    private void http2(ChannelPipeline p, Supplier<ChannelHandler> host) {
        p.addLast(Http2FrameCodecBuilder.forServer().build());
        p.addLast(new Http2MultiplexHandler(new ChannelInitializer<>() {
            @Override protected void initChannel(Channel ch) {
                ch.pipeline().addLast(new Http2StreamFrameToHttpObjectCodec(true));
                ch.pipeline().addLast(handler());
                ch.pipeline().addLast(host.get());
            }
        }));
    }

    /** Chooses HTTP/2 by its connection preface, HTTP/1.1 otherwise. */
    private final class PrefaceDetector extends ByteToMessageDecoder {
        private final Supplier<ChannelHandler> host;

        PrefaceDetector(Supplier<ChannelHandler> host) { this.host = host; }

        @Override protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            ByteBuf preface = Http2CodecUtil.connectionPrefaceBuf();
            try {
                int n = Math.min(in.readableBytes(), preface.readableBytes());
                boolean h2 = ByteBufUtil.equals(in, in.readerIndex(), preface, 0, n);
                if (h2 && n < preface.readableBytes()) return; // more to tell
                ChannelPipeline p = ctx.pipeline();
                if (h2) http2(p, host);
                else http1(p, host);
                p.remove(this);
            } finally {
                preface.release();
            }
        }
    }

    /** The host's handler when none is given: 404 to every request. */
    @ChannelHandler.Sharable
    static final class NotFound extends ChannelInboundHandlerAdapter {
        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpRequest r) {
                var resp = new DefaultFullHttpResponse(r.protocolVersion(), HttpResponseStatus.NOT_FOUND);
                HttpUtil.setContentLength(resp, 0);
                if (!HttpUtil.isKeepAlive(r)) resp.headers().set(HttpHeaderNames.CONNECTION, "close");
                ctx.writeAndFlush(resp);
            }
            if (msg instanceof HttpContent || msg instanceof HttpRequest) ReferenceCountUtil.release(msg); // LastHttpContent is an HttpContent
            else ctx.fireChannelRead(msg);
        }
    }

    /** Fails every exchange carrier; carriers serving sessions as requests go on until their sessions end. */
    @Override public void close() {
        if (exchange != null) exchange.close();
    }

    /** Configures an {@link HttpEndpoint}; every adapter is off until enabled. */
    public static final class Builder {
        private final ZmuxListener listener;
        private boolean streaming;
        private ExchangeConfig exchange;
        private List<String> webSocket;
        private TrustedProxies trusted;

        private Builder(ZmuxListener listener) { this.listener = Objects.requireNonNull(listener); }

        /** Serves streaming carriers: downlinks, uplinks (split bootstrap, out-of-band joins) and bidirectional requests. */
        public Builder streaming() { streaming = true; return this; }

        /** Serves the exchange carrier with config. */
        public Builder exchange(ExchangeConfig config) { exchange = Objects.requireNonNull(config); return this; }

        /**
         * Serves WebSocket carriers (HTTP/1.1 upgrades) with these subprotocols, by preference (none:
         * the listener's application protocols), each also as its draft-01 alias.
         */
        public Builder webSocket(String... protocols) { webSocket = List.of(protocols); return this; }

        /** The intermediaries whose report gives a carrier's source (null: the peer is the source). */
        public Builder trusted(TrustedProxies t) { trusted = t; return this; }

        public HttpEndpoint build() { return new HttpEndpoint(this); }
    }

    /** The PROXY protocol header of a trusted intermediary, read before anything else on its connection. */
    static final class ProxyProtocolHandler extends ByteToMessageDecoder {
        private final TrustedProxies trusted;

        ProxyProtocolHandler(TrustedProxies trusted) { this.trusted = trusted; }

        @Override protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            if (!(ctx.channel().remoteAddress() instanceof InetSocketAddress peer) || !trusted.trusts(peer.getAddress())) {
                ctx.pipeline().remove(this); // other peers are passed through unread
                return;
            }
            ProxyHeader h;
            try {
                h = ProxyHeader.parse(in.nioBuffer());
            } catch (java.net.ProtocolException e) {
                in.skipBytes(in.readableBytes());
                ctx.close();
                return;
            }
            if (h == null) return;
            in.skipBytes(h.length());
            if (h.source() != null) ctx.channel().attr(SOURCE).set(h.source());
            ctx.pipeline().remove(this);
        }
    }
}
