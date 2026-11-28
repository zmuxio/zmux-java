package io.zmux.quic;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.*;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.zmux.quic.internal.QuicHandshake;
import io.zmux.*;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** One established QUIC connection served with a listener: the same admission path as a QuicServer's. */
@Timeout(60)
class QuicServeTest {
    @BeforeAll static void tls() throws Exception { TestTls.init(); }

    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    @Test void anEstablishedConnectionIsAdmittedOnceAndServedUntilItEnds() throws Exception {
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var decisions = new AtomicInteger();
            var lc = ListenerConfig.builder().session(config)
                    .admission(Admission.builder(0, Admission.Decider.immediate((i, p) -> { decisions.incrementAndGet(); return Admission.Decision.accept(0, "q"); })).build())
                    .build();
            var served = new CompletableFuture<QuicConnectionService>();
            var evidence = new CompletableFuture<java.util.List<Admission.OuterSecurity>>();
            try (var listener = Zmux.listener(lc)) {
                QuicOptions options = QuicOptions.defaults();
                ChannelHandler codec = options.apply(new QuicServerCodecBuilder()).sslContext(TestTls.server.build())
                        .handler(new ChannelInboundHandlerAdapter() {
                            private QuicStreamChannel buffered;
                            private Admission.OuterSecurity arrival;

                            @Override public void handlerAdded(ChannelHandlerContext ctx) {
                                buffered = streamEvidence((QuicChannel) ctx.channel());
                                arrival = QuicHandshake.observe(buffered);
                            }

                            @Override public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                                if (evt instanceof SslHandshakeCompletionEvent e && e.isSuccess()) {
                                    var connection = (QuicChannel) ctx.channel();
                                    evidence.complete(java.util.List.of(arrival, QuicHandshake.observe(buffered),
                                            QuicHandshake.observe(streamEvidence(connection))));
                                    served.complete(QuicServer.serve(connection, options, listener));
                                }
                                ctx.fireUserEventTriggered(evt);
                            }
                            @Override public boolean isSharable() { return true; }
                        })
                        .streamHandler(QuicServer.streamHandler()).build();
                Channel udp = new Bootstrap().group(group).channel(NioDatagramChannel.class).handler(codec)
                        .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
                try (var endpoint = new QuicClient(TestTls.client, options)) {
                    ZmuxSession client = endpoint.connectAsync((InetSocketAddress) udp.localAddress(), "localhost", config)
                            .toCompletableFuture().get(10, TimeUnit.SECONDS);
                    ZmuxSession accepted = listener.accept(Duration.ofSeconds(10));
                    assertNotNull(accepted);
                    assertEquals(java.util.Optional.of("q"), accepted.admitted());
                    assertEquals(java.util.List.of(Admission.OuterSecurity.EARLY_DATA, Admission.OuterSecurity.EARLY_DATA,
                            Admission.OuterSecurity.QUIC), evidence.get(5, TimeUnit.SECONDS));
                    var st = client.openStream();
                    st.write(new byte[] {7});
                    st.finish();
                    var in = accepted.acceptStream();
                    var b = new byte[4];
                    assertEquals(1, in.read(b, 0, 4));
                    assertEquals(1, decisions.get(), "one decision per connection");
                    assertEquals(1, listener.stats().admission().accepted());
                    var done = served.get(5, TimeUnit.SECONDS).whenDrained().toCompletableFuture();
                    assertFalse(done.isDone());
                    client.close();
                    accepted.awaitClosed();
                    endpoint.close();
                    done.get(20, TimeUnit.SECONDS);
                } finally {
                    udp.close().sync();
                }
            }
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync();
        }
    }

    private static QuicStreamChannel streamEvidence(QuicChannel parent) {
        var attributes = new io.netty.util.DefaultAttributeMap();
        return (QuicStreamChannel) java.lang.reflect.Proxy.newProxyInstance(QuicStreamChannel.class.getClassLoader(),
                new Class<?>[] {QuicStreamChannel.class}, (ignored, method, args) -> switch (method.getName()) {
                    case "parent" -> parent;
                    case "attr" -> attributes.attr((io.netty.util.AttributeKey<?>) args[0]);
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    @Test void streamAndConnectionCloseCancelAnUnreadTransform() throws Exception {
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (EventLoops loops = EventLoops.create(1)) {
            var config = ZmuxConfig.builder().eventLoops(loops).noHandshakeTimeout().build();
            var transforms = new java.util.concurrent.LinkedBlockingQueue<CompletableFuture<io.zmux.carrier.PipeIo>>();
            var streams = new java.util.concurrent.LinkedBlockingQueue<QuicStreamChannel>();
            var lc = ListenerConfig.builder().session(config).maxPendingPerSource(2).inboundTransform((io, context) -> {
                var waiting = new CompletableFuture<io.zmux.carrier.PipeIo>();
                transforms.add(waiting);
                return waiting;
            }).build();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(lc); var endpoint = new QuicClient(TestTls.client, options)) {
                var codec = options.apply(new QuicServerCodecBuilder()).sslContext(TestTls.server.build())
                        .handler(new ChannelInboundHandlerAdapter() {
                            @Override public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                                if (evt instanceof SslHandshakeCompletionEvent e && e.isSuccess())
                                    QuicServer.serve((QuicChannel) ctx.channel(), options, listener);
                                ctx.fireUserEventTriggered(evt);
                            }
                            @Override public boolean isSharable() { return true; }
                        }).streamHandler(new ChannelInitializer<QuicStreamChannel>() {
                            @Override protected void initChannel(QuicStreamChannel ch) {
                                ch.pipeline().addLast(QuicServer.streamHandler());
                                streams.add(ch);
                            }
                        }).build();
                Channel udp = new Bootstrap().group(group).channel(NioDatagramChannel.class).handler(codec)
                        .bind(QuicTest.any()).sync().channel();
                try {
                    var bound = (InetSocketAddress) udp.localAddress();
                    for (boolean closeConnection : new boolean[]{false, true}) {
                        var dialing = endpoint.connectAsync(bound, bound.getHostName(), config);
                        var stream = streams.poll(5, TimeUnit.SECONDS);
                        assertNotNull(stream);
                        var handshake = transforms.poll(5, TimeUnit.SECONDS);
                        assertNotNull(handshake);
                        var peer = (InetSocketAddress) stream.parent().remoteSocketAddress();
                        Channel closing = closeConnection ? stream.parent() : stream;
                        closing.close().sync();
                        assertThrows(java.util.concurrent.CancellationException.class, () -> handshake.get(5, TimeUnit.SECONDS));
                        stream.eventLoop().submit(() -> {}).sync();
                        var slot = listener.adapter().reserve(peer, null);
                        assertNotNull(slot);
                        var second = listener.adapter().reserve(peer, null);
                        assertNotNull(second);
                        slot.release();
                        second.release();
                        dialing.cancel(false);
                    }
                } finally { udp.close().sync(); }
            }
        } finally { group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync(); }
    }

    @Test void stoppingAdmissionWithdrawsSetupBeforeTheConnectionLoopCanRunAgain() throws Exception {
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).noHandshakeTimeout().build();
            var takeover = new CompletableFuture<Takeover>();
            var stage = new CompletableFuture<Void>();
            var lc = ListenerConfig.builder().session(cfg)
                    .admission(Admission.builder(0, Admission.Decider.immediate((i, b) -> Admission.Decision.FALLBACK)).build())
                    .fallback(t -> { takeover.complete(t); return stage; }).build();
            var served = new CompletableFuture<QuicConnectionService>();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(lc); var client = new QuicClient(TestTls.client, options)) {
                var codec = options.apply(new QuicServerCodecBuilder()).sslContext(TestTls.server.build())
                        .handler(new ChannelInboundHandlerAdapter() {
                            @Override public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                                if (evt instanceof SslHandshakeCompletionEvent e && e.isSuccess())
                                    served.complete(QuicServer.serve((QuicChannel) ctx.channel(), options, listener));
                                ctx.fireUserEventTriggered(evt);
                            }
                            @Override public boolean isSharable() { return true; }
                        }).streamHandler(QuicServer.streamHandler()).build();
                Channel udp = new Bootstrap().group(group).channel(NioDatagramChannel.class).handler(codec)
                        .bind(QuicTest.any()).sync().channel();
                var resume = new CompletableFuture<Void>();
                try {
                    var address = (InetSocketAddress) udp.localAddress();
                    var dialing = client.connectAsync(address, address.getHostName(), cfg);
                    var pending = takeover.get(5, TimeUnit.SECONDS);
                    var connection = QuicTakeover.connection(pending).orElseThrow();
                    var paused = new java.util.concurrent.CountDownLatch(1);
                    connection.eventLoop().execute(() -> { paused.countDown(); resume.join(); });
                    assertTrue(paused.await(5, TimeUnit.SECONDS));
                    served.get(5, TimeUnit.SECONDS).stopAdmission();
                    // stop has won even though the stream's loop has not processed a single stop task.
                    assertThrows(IllegalStateException.class, pending::detach);
                    assertFalse(stage.isDone());
                    dialing.cancel(false);
                } finally { resume.complete(null); udp.close().sync(); }
            }
        } finally { group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync(); }
    }

    // Complete QUIC negotiation but never open stream zero. Stop and expiry each close the reservation.
    @SuppressWarnings("try")
    @Test void waitingForTheFirstStreamIsBoundedByStopAndDeadline() throws Exception {
        // HS-57.
        for (String ending : java.util.List.of("stop", "first record", "handshake")) {
            boolean stop = ending.equals("stop");
            var lc = ListenerConfig.builder().maxPendingPerSource(2)
                    .session(io.zmux.ZmuxConfig.builder().handshakeTimeout(Duration.ofSeconds(ending.equals("handshake") ? 1 : 20)).build())
                    .firstRecordTimeout(Duration.ofSeconds(ending.equals("first record") ? 1 : 20)).build();
            var options = QuicOptions.defaults();
            try (var l = Zmux.listener(lc); var server = new QuicServer(QuicTest.any(), TestTls.server, options, l);
                 var client = new QuicClient(TestTls.client, options)) {
                var first = client.connectCarrier(server.address(), server.address().getHostName(), null, ConnectionCarrier.POOLED)
                        .toCompletableFuture().get(5, TimeUnit.SECONDS);
                var accepted = server.connections().getFirst();
                accepted.eventLoop().submit(() -> {}).sync();
                var peer = (InetSocketAddress) accepted.remoteSocketAddress();
                var spare = l.adapter().reserve(peer, null);
                assertNotNull(spare);
                assertNull(l.adapter().reserve(peer, null));
                spare.release();
                if (stop) l.close();
                assertTrue(accepted.closeFuture().await(4, TimeUnit.SECONDS));
                assertTrue(first.channel().closeFuture().await(4, TimeUnit.SECONDS));
                if (!stop) {
                    assertEquals(1, l.stats().setupFailures());
                    var returned = l.adapter().reserve(peer, null);
                    assertNotNull(returned);
                    returned.release();
                }
            }
        }
    }

    @Test void aClosedListenerClosesTheConnectionAndFailsTheCall() throws Exception {
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var served = new CompletableFuture<QuicConnectionService>();
            var listener = Zmux.listener(ListenerConfig.of(config));
            listener.close();
            QuicOptions options = QuicOptions.defaults();
            ChannelHandler codec = options.apply(new QuicServerCodecBuilder()).sslContext(TestTls.server.build())
                    .handler(new ChannelInboundHandlerAdapter() {
                        @Override public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                            if (evt instanceof SslHandshakeCompletionEvent e && e.isSuccess())
                                served.complete(QuicServer.serve((QuicChannel) ctx.channel(), options, listener));
                            ctx.fireUserEventTriggered(evt);
                        }
                        @Override public boolean isSharable() { return true; }
                    })
                    .streamHandler(QuicServer.streamHandler()).build();
            Channel udp = new Bootstrap().group(group).channel(NioDatagramChannel.class).handler(codec)
                    .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
            try (var endpoint = new QuicClient(TestTls.client, options)) {
                var dialing = endpoint.connectAsync((InetSocketAddress) udp.localAddress(), "localhost", config);
                var done = served.get(10, TimeUnit.SECONDS).whenDrained().toCompletableFuture();
                var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> done.get(10, TimeUnit.SECONDS));
                assertInstanceOf(ListenerClosedException.class, failure.getCause());
                dialing.toCompletableFuture().cancel(true);
            } finally {
                udp.close().sync();
            }
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync();
        }
    }
}
