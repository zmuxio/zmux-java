package io.zmux.http3;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http3.Http3;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicClientCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.ReferenceCountUtil;
import io.zmux.CarrierCounts;
import io.zmux.ListenerConfig;
import io.zmux.QuicCounters;
import io.zmux.Zmux;
import io.zmux.http.HttpCarrierClient;
import io.zmux.http.HttpEndpoint;
import io.zmux.http.internal.HttpTransport;
import io.zmux.quic.QuicExporter;
import io.zmux.quic.QuicOptions;
import io.zmux.quic.QuicServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Timeout;

@Timeout(90)
class NextBatchTransportTest {
    record Bound(QuicChannel connection, HttpCarrierClient.Builder builder) {}

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
    void providerFollowsTheActualRequestAndBorrowedConnection(boolean nativeDatagrams) throws Exception {
        var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        var requests = new LinkedBlockingQueue<Channel>();
        var zmuxConfig = io.zmux.ZmuxConfig.builder().enableDatagrams(true).build();
        try (var listener = Zmux.listener(ListenerConfig.of(zmuxConfig));
             var endpoint = HttpEndpoint.builder(listener).streaming().build();
             var server = Http3HostedTest.bind(group, endpoint, () -> new ChannelInboundHandlerAdapter() {
                 @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
                     if (message instanceof HttpRequest) requests.add(ctx.channel());
                     ReferenceCountUtil.release(message);
                 }
             }, nativeDatagrams ? QuicOptions.defaults() : null)) {
            var tls = QuicSslContextBuilder.forClient().trustManager(H3TestSupport.tmf())
                    .applicationProtocols("h3").earlyData(false).keylog(QuicExporter.KEYLOG).build();
            var codec = Http3Streams.configure(QuicOptions.defaults(), new QuicClientCodecBuilder())
                    .sslEngineProvider(ch -> tls.newEngine(ch.alloc(), "localhost", server.uri().getPort())).build();
            var socket = new Bootstrap().group(group).channel(NioDatagramChannel.class).handler(codec)
                    .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
            try {
                URI origin = server.uri();
                var first = dial(socket, server.datagram().localAddress(), origin);
                var second = dial(socket, server.datagram().localAddress(), origin);
                assertSame(first.connection.parent(), second.connection.parent());
                assertNotSame(first.connection, second.connection);

                try (var a = first.builder.build(); var b = second.builder.build();
                     var same = Http3Client.builder(first.connection, origin).build()) {
                    var a0 = open(a, origin);
                    var a1 = open(same, origin);
                    var b0 = open(b, origin);
                    assertSame(first.connection, a0.parent());
                    assertSame(first.connection, a1.parent());
                    assertSame(second.connection, b0.parent());
                    var control = Http3.getLocalControlStream(first.connection);
                    assertNotNull(control);

                    var sa = send(a0, origin, requests);
                    var sa1 = send(a1, origin, requests);
                    var sb = send(b0, origin, requests);
                    assertSame(sa.parent(), sa1.parent());
                    assertNotSame(sa.parent(), sb.parent());
                    for (Channel request : new Channel[] {a0, a1, b0, sa, sa1, sb}) {
                        var provider = HttpTransport.stream(request);
                        assertSame(request.parent(), provider.carrierIdentity());
                        assertNull(provider.carrierQuery(new Object()));
                        assertInstanceOf(QuicCounters.class, sample(provider));
                    }

                    assertEquals(nativeDatagrams,
                            H3Datagrams.peerEnabled(first.connection).toCompletableFuture().get(20, TimeUnit.SECONDS));
                    assertEquals(nativeDatagrams,
                            H3Datagrams.peerEnabled((QuicChannel) sa.parent()).toCompletableFuture().get(20, TimeUnit.SECONDS));
                    sessionDatagram(a, origin, listener, zmuxConfig);

                    URI wrong = URI.create("https://other.invalid:" + origin.getPort() + "/");
                    assertInstanceOf(IllegalArgumentException.class, assertThrows(ExecutionException.class,
                            () -> open(a, wrong)).getCause());
                    try (var mismatched = Http3Client.builder(first.connection, wrong).build()) {
                        assertInstanceOf(IllegalArgumentException.class, assertThrows(ExecutionException.class,
                                () -> open(mismatched, wrong)).getCause());
                    }
                    var rejected = open(a, origin);
                    var invalid = request("other.invalid");
                    var write = rejected.writeAndFlush(invalid);
                    assertInstanceOf(IllegalArgumentException.class, assertThrows(ExecutionException.class,
                            () -> write.get(20, TimeUnit.SECONDS)).getCause());
                    assertEquals(0, invalid.refCnt());
                    a.closeAsync().get(20, TimeUnit.SECONDS);
                    assertFalse(a0.isOpen());
                    assertTrue(a1.isOpen());
                    assertTrue(first.connection.isActive());
                    assertSame(control, Http3.getLocalControlStream(first.connection));
                    assertInstanceOf(QuicCounters.class, sample(HttpTransport.stream(a0)));
                    first.connection.close().get(20, TimeUnit.SECONDS);
                    sa.parent().closeFuture().get(20, TimeUnit.SECONDS);
                    assertNull(sample(HttpTransport.stream(a0)));
                    assertNull(sample(HttpTransport.stream(sa)));
                    assertTrue(second.connection.isActive());
                }
            } finally {
                socket.close().get(20, TimeUnit.SECONDS);
            }
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).get(20, TimeUnit.SECONDS);
        }
    }

    private static void sessionDatagram(HttpCarrierClient carrier, URI origin, io.zmux.ZmuxListener listener,
                                        io.zmux.ZmuxConfig config) throws Exception {
        var pipe = carrier.pipe(origin.resolve("/carrier"), java.util.Map.of(), io.zmux.PipeOptions.Direction.BIDIRECTIONAL, null);
        try (var client = Zmux.open(pipe, io.zmux.Role.CLIENT, config);
             var server = listener.accept(java.time.Duration.ofSeconds(20))) {
            assertNotNull(server);
            H3TestSupport.exchange(server, client, 2301);
            byte[] payload = {0x61, 0};
            for (var direction : java.util.List.of(java.util.List.of(client, server), java.util.List.of(server, client))) {
                var from = direction.get(0);
                var to = direction.get(1);
                from.datagrams().send(java.nio.ByteBuffer.wrap(payload));
                assertArrayEquals(payload, to.datagrams().receive());
                assertEquals(1, to.stats().datagramsReceived());
                assertEquals(0, to.stats().datagramsReceivedChannel(), "ordinary POST must carry the packet in a record");
            }
        }
    }

    static Bound dial(Channel socket, java.net.SocketAddress peer, URI origin) throws Exception {
        var binding = new CompletableFuture<Bound>();
        QuicChannel.newBootstrap(socket).handler(new io.netty.channel.ChannelInitializer<QuicChannel>() {
            @Override protected void initChannel(QuicChannel connection) {
                QuicServer.observeDatagramCapacity(connection);
                connection.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                    @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                        if (event instanceof SslHandshakeCompletionEvent handshake) {
                            if (handshake.isSuccess()) {
                                var transport = new Http3ClientTransport(connection, origin, true);
                                var builder = HttpTransport.clientBuilder(transport).group(connection.eventLoop());
                                binding.complete(new Bound(connection, builder));
                            } else {
                                binding.completeExceptionally(handshake.cause());
                            }
                        }
                        ctx.fireUserEventTriggered(event);
                    }
                });
            }
        }).remoteAddress(peer).connect().get(20, TimeUnit.SECONDS);
        return binding.get(20, TimeUnit.SECONDS);
    }

    private static Channel open(HttpCarrierClient client, URI uri) throws Exception {
        return HttpTransport.openRequest(client, uri).get(20, TimeUnit.SECONDS);
    }

    private static Channel send(Channel request, URI origin, LinkedBlockingQueue<Channel> requests) throws Exception {
        request.writeAndFlush(request(origin.getRawAuthority())).get(20, TimeUnit.SECONDS);
        return requests.take();
    }

    private static DefaultFullHttpRequest request(String authority) {
        var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/health", io.netty.buffer.Unpooled.buffer(0, 1));
        request.headers().set(HttpHeaderNames.HOST, authority);
        return request;
    }

    private static CarrierCounts sample(HttpTransport.Stream provider) throws Exception {
        var result = new CompletableFuture<CarrierCounts>();
        provider.carrierQuery(provider.carrierIdentity()).collect().accept((time, counts) -> result.complete(counts));
        return result.get(20, TimeUnit.SECONDS);
    }
}
