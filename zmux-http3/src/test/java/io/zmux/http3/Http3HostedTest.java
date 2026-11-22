package io.zmux.http3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.http3.Http3DataFrame;
import io.netty.handler.codec.http3.Http3ErrorCode;
import io.netty.handler.codec.http3.Http3Exception;
import io.netty.handler.codec.http3.Http3RequestStreamFrame;
import io.netty.handler.codec.http3.Http3ServerConnectionHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamResetException;
import io.netty.handler.codec.quic.QuicStreamType;
import io.zmux.Admission;
import io.zmux.EventLoops;
import io.zmux.ListenerConfig;
import io.zmux.PipeOptions;
import io.zmux.Role;
import io.zmux.Zmux;
import io.zmux.ZmuxConfig;
import io.zmux.ZmuxFuture;
import io.zmux.http.HttpEndpoint;
import io.zmux.http.internal.HttpTransport;
import io.zmux.internal.spi.AdapterTestSupport;
import io.zmux.quic.QuicExporter;
import io.zmux.quic.QuicOptions;
import java.io.IOException;
import java.util.List;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class Http3HostedTest {
    private static final Duration WAIT = Duration.ofSeconds(5);

    @Test void cancellingServeLeavesAcceptedCarrierAndHostRequestsAlive() throws Exception {
        var hostRequest = new CompletableFuture<String>();
        Supplier<ChannelHandler> host = () -> new ChannelInitializer<Channel>() {
            @Override protected void initChannel(Channel stream) {
                stream.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        if (msg instanceof HttpRequest request) hostRequest.complete(request.uri());
                        ctx.fireChannelRead(msg);
                    }
                }, HttpTransport.notFound());
            }
        };
        var netty = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (var owners = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(owners).build();
            try (var listener = Zmux.listener(ListenerConfig.builder().session(config).build());
                 var endpoint = HttpEndpoint.builder(listener).streaming().build();
                 var server = bind(netty, endpoint, host)) {
                var carrier = server.uri().resolve("/carrier");
                try (var client = Http3Client.builder(QuicSslContextBuilder.forClient().trustManager(H3TestSupport.tmf()), QuicOptions.defaults())
                        .timeout(WAIT).build();
                     var dialled = Zmux.open(client.pipe(carrier, Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null),
                             Role.CLIENT, config);
                     var accepted = listener.accept(WAIT)) {
                    assertNotNull(accepted);
                    assertTrue(server.observed().get(5, TimeUnit.SECONDS).cancel(false));
                    H3TestSupport.exchange(dialled, accepted, 2301);
                    H3TestSupport.exchange(accepted, dialled, 2302);
                    var raw = client.pipe(carrier.resolve("/host"), Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null);
                    try {
                        assertEquals("/host", hostRequest.get(5, TimeUnit.SECONDS));
                    } finally {
                        raw.reader().close();
                        raw.writer().close();
                    }
                }
            }
        } finally {
            netty.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
    }

    @Test void closingThePeerControlStreamReportsAConnectionError() throws Exception {
        var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (var listener = Zmux.listener(ListenerConfig.builder().build());
             var endpoint = HttpEndpoint.builder(listener).build();
             var server = bind(group, endpoint, null)) {
            var conn = connect(group, server.uri());
            try {
                var control = conn.eventLoop().submit(() -> Http3.getLocalControlStream(conn))
                        .get(5, TimeUnit.SECONDS);
                assertNotNull(control);
                assertTrue(control.shutdownOutput().await(5, TimeUnit.SECONDS));
                var observed = server.observed().get(5, TimeUnit.SECONDS);
                var error = assertThrows(ExecutionException.class, () -> observed.get(5, TimeUnit.SECONDS));
                assertEquals(Http3ErrorCode.H3_CLOSED_CRITICAL_STREAM,
                        assertInstanceOf(Http3Exception.class, error.getCause()).errorCode());
            } finally {
                conn.close().awaitUninterruptibly(5, TimeUnit.SECONDS);
            }
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
    }

    @Test void requestInitializerReenablesOutputAfterPeerFin() throws Exception {
        var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        var serverStream = new CompletableFuture<QuicStreamChannel>();
        Supplier<ChannelHandler> host = () -> new SimpleChannelInboundHandler<HttpObject>() {
            @Override protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
                if (msg instanceof LastHttpContent) {
                    serverStream.complete((QuicStreamChannel) ctx.channel());
                    reply(ctx, "answer after request FIN");
                }
            }
        };
        try (var listener = Zmux.listener(ListenerConfig.builder().build());
             var endpoint = HttpEndpoint.builder(listener).build()) {
            var tls = serverTls();
            var codec = Http3Streams.configure(QuicOptions.defaults(), new QuicServerCodecBuilder()).sslContext(tls)
                    .streamHandler(new ChannelInitializer<QuicStreamChannel>() {
                        @Override protected void initChannel(QuicStreamChannel stream) {
                            if (stream.type() == QuicStreamType.BIDIRECTIONAL) stream.config().setAllowHalfClosure(false);
                        }
                    })
                    .handler(new ChannelInitializer<QuicChannel>() {
                        @Override protected void initChannel(QuicChannel connection) {
                            connection.pipeline().addLast(new Http3ServerConnectionHandler(
                                    Http3Server.requestInitializer(endpoint, host)));
                        }
                    }).build();
            try (var socket = bind(group, codec, new CompletableFuture<>())) {
                var client = connect(group, socket.uri());
                try {
                    assertEquals("answer after request FIN", request(client, headers("GET", "/"), true).body().get(5, TimeUnit.SECONDS));
                    assertTrue(serverStream.get(5, TimeUnit.SECONDS).config().isAllowHalfClosure());
                } finally {
                    client.close().awaitUninterruptibly(5, TimeUnit.SECONDS);
                }
            }
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
    }

    @Test void cancellingServeDoesNotRetireAnOwnerStallEarly() throws Exception {
        var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        var incoming = new CompletableFuture<Channel>();
        var closed = new AtomicInteger();
        var stall = Duration.ofMinutes(1);
        var admission = Admission.builder(0, Admission.Decider.immediate((info, prefix) -> Admission.Decision.STALL))
                .stall(stall, stall).build();
        Supplier<ChannelHandler> host = () -> new ChannelInitializer<Channel>() {
            @Override protected void initChannel(Channel stream) {
                stream.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
                    @Override public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
                        closed.incrementAndGet();
                        ctx.close(promise);
                    }
                });
                incoming.complete(stream);
                stream.pipeline().addLast(HttpTransport.notFound());
            }
        };
        try (var loops = EventLoops.create(1);
             var listener = Zmux.listener(ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).build())
                     .admission(admission).build());
             var endpoint = HttpEndpoint.builder(listener).streaming().build();
             var server = bind(group, endpoint, host)) {
            var client = connect(group, server.uri());
            try {
                request(client, headers("POST", "/stall"), false);
                var stream = incoming.get(5, TimeUnit.SECONDS);
                H3TestSupport.await(() -> listener.stats().admission().stalled() == 1);
                assertTrue(server.observed().get(5, TimeUnit.SECONDS).cancel(false));
                // Enqueue behind the stop task so this assertion cannot race the cancellation.
                stream.eventLoop().submit(() -> assertTrue(stream.isActive())).get(5, TimeUnit.SECONDS);
                assertEquals(0, closed.get());
                AdapterTestSupport.advanceTimers(loops, stall).get(5, TimeUnit.SECONDS);
                assertTrue(stream.closeFuture().await(5, TimeUnit.SECONDS));
                stream.eventLoop().submit(() -> assertEquals(1, closed.get())).get(5, TimeUnit.SECONDS);
                assertTrue(client.isActive());
            } finally {
                client.close().awaitUninterruptibly(5, TimeUnit.SECONDS);
            }
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
    }

    @Test void hostedDriverExceptionLeavesConnectionOwnedByHost() throws Exception {
        var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        var failure = new IOException("host driver failure");
        Supplier<ChannelHandler> host = () -> new SimpleChannelInboundHandler<HttpObject>() {
            @Override protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
                if (msg instanceof HttpRequest request && request.uri().equals("/report")) {
                    ctx.channel().parent().pipeline().fireExceptionCaught(failure);
                }
                if (msg instanceof LastHttpContent) reply(ctx, "host owns connection");
            }
        };
        try (var listener = Zmux.listener(ListenerConfig.builder().build());
             var endpoint = HttpEndpoint.builder(listener).build();
             var server = bind(group, endpoint, host)) {
            var client = connect(group, server.uri());
            try {
                assertEquals("host owns connection",
                        request(client, headers("GET", "/report"), true).body().get(5, TimeUnit.SECONDS));
                var observed = server.observed().get(5, TimeUnit.SECONDS);
                var error = assertThrows(ExecutionException.class, () -> observed.get(5, TimeUnit.SECONDS));
                assertSame(failure, error.getCause());
                assertEquals("host owns connection",
                        request(client, headers("GET", "/another"), true).body().get(5, TimeUnit.SECONDS));
            } finally {
                client.close().awaitUninterruptibly(5, TimeUnit.SECONDS);
            }
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
    }

    @Test void onlyRawPseudoHeaderDeterminesExtendedConnectProtocol() throws Exception {
        var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        var observed = new LinkedBlockingQueue<Protocol>();
        Supplier<ChannelHandler> host = () -> new SimpleChannelInboundHandler<HttpObject>() {
            @Override protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
                if (msg instanceof HttpRequest request) {
                    observed.add(new Protocol(HttpTransport.protocol(ctx.channel()),
                            request.headers().getAll("x-http3-protocol")));
                }
                if (msg instanceof LastHttpContent) reply(ctx, "host");
            }
        };
        try (var listener = Zmux.listener(ListenerConfig.builder().build());
             var endpoint = HttpEndpoint.builder(listener).streaming().build();
             var server = bind(group, endpoint, host)) {
            var client = connect(group, server.uri());
            try {
                var connect = new DefaultHttp3HeadersFrame();
                connect.headers().method("CONNECT").authority("localhost:443").add("x-http3-protocol", "webtransport");
                assertEquals("host", request(client, connect, true).body().get(5, TimeUnit.SECONDS));
                assertEquals(new Protocol("", List.of("webtransport")), observed.poll(5, TimeUnit.SECONDS));

                var extended = headers("CONNECT", "/connect");
                extended.headers().protocol("websocket").add("x-http3-protocol", "webtransport").add("x-http3-protocol", "other");
                assertEquals("host", request(client, extended, true).body().get(5, TimeUnit.SECONDS));
                assertEquals(new Protocol("websocket", List.of("webtransport", "other")), observed.poll(5, TimeUnit.SECONDS));
            } finally {
                client.close().awaitUninterruptibly(5, TimeUnit.SECONDS);
            }
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
    }

    private record Protocol(String wire, List<String> fields) { }

    @Test void actualTransferEncodingIsRejectedButSyntheticEncodingIsHidden() throws Exception {
        var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        var observed = new LinkedBlockingQueue<Boolean>();
        Supplier<ChannelHandler> host = () -> new SimpleChannelInboundHandler<HttpObject>() {
            @Override protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
                if (msg instanceof HttpRequest request) {
                    observed.add(request.headers().contains(HttpHeaderNames.TRANSFER_ENCODING));
                }
                if (msg instanceof LastHttpContent) reply(ctx, "ok");
            }
        };
        try (var listener = Zmux.listener(ListenerConfig.builder().build());
             var endpoint = HttpEndpoint.builder(listener).build();
             var server = bind(group, endpoint, host)) {
            var client = connect(group, server.uri());
            try {
                var bad = headers("POST", "/bad");
                bad.headers().add(HttpHeaderNames.TRANSFER_ENCODING, "chunked");
                var exchange = request(client, bad, true);
                var error = assertThrows(ExecutionException.class, () -> exchange.body().get(5, TimeUnit.SECONDS));
                assertEquals(Http3ErrorCode.H3_MESSAGE_ERROR.code(),
                        assertInstanceOf(QuicStreamResetException.class, error.getCause()).applicationProtocolCode());
                assertTrue(observed.isEmpty());
                assertEquals("ok", request(client, headers("POST", "/ok"), true).body().get(5, TimeUnit.SECONDS));
                var transferEncoding = observed.poll(5, TimeUnit.SECONDS);
                assertNotNull(transferEncoding);
                assertFalse(transferEncoding);
            } finally {
                client.close().awaitUninterruptibly(5, TimeUnit.SECONDS);
            }
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
    }

    @SuppressWarnings("try") // Exercise explicit close from the event loop and an external caller.
    @Test void boundCloseCanStartOnItsLoopAndAnotherCallerObservesShutdown() throws Exception {
        var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        var udp = new CompletableFuture<Channel>();
        Supplier<ChannelHandler> host = () -> new SimpleChannelInboundHandler<HttpObject>() {
            @Override protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
                if (msg instanceof LastHttpContent) {
                    udp.complete(ctx.channel().parent().parent());
                    reply(ctx, "ok");
                }
            }
        };
        try (var listener = Zmux.listener(ListenerConfig.builder().build());
             var endpoint = HttpEndpoint.builder(listener).build();
             var server = Http3Server.bind(new InetSocketAddress("127.0.0.1", 0),
                     QuicSslContextBuilder.forServer(H3TestSupport.kmf(), "changeit"), QuicOptions.defaults(), endpoint, host)) {
            var client = connect(group, uri(server.address().getPort()));
            try {
                assertEquals("ok", request(client, headers("GET", "/"), true).body().get(5, TimeUnit.SECONDS));
                var socket = udp.get(5, TimeUnit.SECONDS);
                var paused = new CompletableFuture<Runnable>();
                socket.eventLoop().submit(() -> {
                    socket.pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
                        @Override public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
                            if (!paused.complete(() -> ctx.close(promise))) ctx.close(promise);
                        }
                    });
                    server.close();
                }).get(5, TimeUnit.SECONDS);
                Runnable resume = paused.get(5, TimeUnit.SECONDS);
                var closed = new CompletableFuture<Void>();
                Thread.ofVirtual().start(() -> {
                    server.close();
                    closed.complete(null);
                });
                try {
                    closed.get(5, TimeUnit.SECONDS);
                    assertFalse(socket.closeFuture().isDone());
                } finally {
                    socket.eventLoop().execute(resume);
                }
                socket.closeFuture().get(5, TimeUnit.SECONDS);
                socket.eventLoop().terminationFuture().get(5, TimeUnit.SECONDS);
            } finally {
                client.close().awaitUninterruptibly(5, TimeUnit.SECONDS);
            }
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).awaitUninterruptibly(5, TimeUnit.SECONDS);
        }
    }

    private static QuicChannel connect(EventLoopGroup group, URI uri) throws Exception {
        var client = new Http3ClientTransport(
                QuicSslContextBuilder.forClient().trustManager(H3TestSupport.tmf()), QuicOptions.defaults());
        return (QuicChannel) client.connect(group, uri, WAIT, ignored -> {}).get(5, TimeUnit.SECONDS);
    }

    private static DefaultHttp3HeadersFrame headers(String method, String path) {
        var frame = new DefaultHttp3HeadersFrame();
        frame.headers().method(method).scheme("https").authority("localhost:443").path(path);
        return frame;
    }

    private static Response request(QuicChannel connection, DefaultHttp3HeadersFrame headers, boolean fin) throws Exception {
        var body = new CompletableFuture<String>();
        var stream = Http3.newRequestStream(connection, new SimpleChannelInboundHandler<Http3RequestStreamFrame>() {
            private final StringBuilder received = new StringBuilder();

            @Override protected void channelRead0(ChannelHandlerContext ctx, Http3RequestStreamFrame msg) {
                if (msg instanceof Http3DataFrame data) received.append(data.content().toString(StandardCharsets.US_ASCII));
            }

            @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                if (event == ChannelInputShutdownEvent.INSTANCE) body.complete(received.toString());
                ctx.fireUserEventTriggered(event);
            }

            @Override public void channelInactive(ChannelHandlerContext ctx) {
                body.completeExceptionally(new IOException("request stream closed before FIN"));
                ctx.fireChannelInactive();
            }

            @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                body.completeExceptionally(cause);
                ctx.close();
            }
        }).get(5, TimeUnit.SECONDS);
        stream.writeAndFlush(headers).get(5, TimeUnit.SECONDS);
        if (fin && !stream.isOutputShutdown()) {
            // A peer may reset malformed request headers before the test submits its FIN.
            assertTrue(stream.shutdownOutput().await(5, TimeUnit.SECONDS));
        }
        return new Response(stream, body);
    }

    private record Response(QuicStreamChannel stream, CompletableFuture<String> body) { }

    private static void reply(ChannelHandlerContext ctx, String text) {
        var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.copiedBuffer(text, StandardCharsets.US_ASCII));
        HttpUtil.setContentLength(response, text.length());
        ctx.writeAndFlush(response);
    }

    private static URI uri(int port) { return URI.create("https://localhost:" + port + "/"); }

    static HostSocket bind(EventLoopGroup group, HttpEndpoint endpoint, Supplier<ChannelHandler> host)
            throws Exception {
        var observed = new CompletableFuture<ZmuxFuture<Void>>();
        var codec = Http3Streams.configure(QuicOptions.defaults(), new QuicServerCodecBuilder()).sslContext(serverTls())
                .handler(new ChannelInitializer<QuicChannel>() {
                    @Override protected void initChannel(QuicChannel connection) {
                        observed.complete(Http3Server.serve(connection, endpoint, host));
                    }
                }).build();
        return bind(group, codec, observed);
    }

    private static QuicSslContext serverTls() throws Exception {
        return QuicSslContextBuilder.forServer(H3TestSupport.kmf(), "changeit").applicationProtocols("h3")
                .earlyData(false).keylog(QuicExporter.KEYLOG).build();
    }

    private static HostSocket bind(EventLoopGroup group, ChannelHandler codec, CompletableFuture<ZmuxFuture<Void>> observed)
            throws Exception {
        var bound = new Bootstrap().group(group).channel(NioDatagramChannel.class).handler(codec)
                .bind(new InetSocketAddress("127.0.0.1", 0));
        assertTrue(bound.await(5, TimeUnit.SECONDS));
        assertTrue(bound.isSuccess(), () -> String.valueOf(bound.cause()));
        return new HostSocket(bound.channel(), observed);
    }

    record HostSocket(Channel datagram, CompletableFuture<ZmuxFuture<Void>> observed) implements AutoCloseable {
        URI uri() {
            int port = ((InetSocketAddress) datagram.localAddress()).getPort();
            return Http3HostedTest.uri(port);
        }

        @Override public void close() { datagram.close().awaitUninterruptibly(5, TimeUnit.SECONDS); }
    }
}
