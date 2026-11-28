package io.zmux.quic;

import io.zmux.CapacityLimits;
import io.zmux.SessionPolicy;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.zmux.EventLoops;
import io.zmux.ListenerConfig;
import io.zmux.PipeAuth;
import io.zmux.PipeOptions;
import io.zmux.RecordMode;
import io.zmux.Zmux;
import io.zmux.ZmuxConfig;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(90)
class NextBatchTransportTest {
    private static final Duration WAIT = Duration.ofSeconds(20);

    @BeforeAll static void tls() throws Exception { TestTls.init(); }

    @Test void managedAdmissionStopsWhileAttachedPipesContinue() throws Exception {
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, listener);
                 var client = new QuicClient(TestTls.client, options)) {
                try (var outgoing = client.connectAsync(server.address(), "localhost", config).get(20, TimeUnit.SECONDS);
                     var incoming = listener.accept(WAIT)) {
                    var connection = server.connections().getFirst();
                    var service = connection.attr(ConnectionCarrier.KEY).get();
                    connection.eventLoop().submit(server::stopAdmission).get(20, TimeUnit.SECONDS);
                    assertTrue(connection.isActive());
                    assertTrue(server.openPipeAsync(connection).toCompletableFuture().isCompletedExceptionally());
                    QuicTest.transfer(workers, outgoing, incoming, 8, 128 << 10);
                    assertTrue(incoming.pipeStats().getFirst().lanesIn() > 1);
                    outgoing.close(42, "drained");
                    assertEquals(new io.zmux.SessionError.ApplicationClosed(42, "drained", true),
                            incoming.closed().get(20, TimeUnit.SECONDS));
                    service.whenDrained().toCompletableFuture().get(20, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test void externalBoundPipeUsesCapturedDatagramCapacityInBothDirections() throws Exception {
        var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (var loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).sessionPolicy(new SessionPolicy(new CapacityLimits(4))).noProbation()
                    .pipeAuth(PipeAuth.exporter().withEndToEnd()).allowRecordMode((c, m, p) -> true).build();
            var options = QuicOptions.defaults();
            var services = new java.util.concurrent.LinkedBlockingQueue<QuicConnectionService>();
            try (var listener = Zmux.listener(ListenerConfig.of(config)); var client = new QuicClient(TestTls.client, options)) {
                var codec = options.apply(new QuicServerCodecBuilder())
                        .sslContext(TestTls.server.keylog(QuicExporter.KEYLOG).build())
                        .handler(new ChannelInitializer<QuicChannel>() {
                            @Override protected void initChannel(QuicChannel connection) {
                                QuicServer.observeDatagramCapacity(connection);
                                connection.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                    @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                                        if (event instanceof SslHandshakeCompletionEvent handshake && handshake.isSuccess()) {
                                            services.add(QuicServer.serve(connection, options, listener));
                                        }
                                        ctx.fireUserEventTriggered(event);
                                    }
                                });
                            }
                        }).streamHandler(QuicServer.streamHandler()).build();
                Channel socket = new Bootstrap().group(group).channel(NioDatagramChannel.class)
                        .handler(codec).bind(QuicTest.any()).sync().channel();
                try {
                    var address = (InetSocketAddress) socket.localAddress();
                    try (var outgoing = client.connectAsync(address, "localhost", config).get(20, TimeUnit.SECONDS);
                         var incoming = listener.accept(WAIT)) {
                        var first = outgoing.pipes().getFirst();
                        var bound = outgoing.addPipe(client.dialAsync(address, "localhost", outgoing).get(20, TimeUnit.SECONDS),
                                PipeOptions.builder().recordMode(RecordMode.BOUND).build());
                        bound.awaitAttached(WAIT);
                        assertEquals(RecordMode.BOUND, bound.stats().orElseThrow().recordMode());
                        var firstService = services.take();
                        var boundService = services.take();
                        first.abort();
                        first.awaitClosed(WAIT);
                        firstService.whenDrained().toCompletableFuture().get(20, TimeUnit.SECONDS);
                        boundService.stopAdmission();
                        assertFalse(boundService.whenDrained().toCompletableFuture().isDone());

                        byte[] up = {1, 2, 3}, down = {4, 5}, large = new byte[4000];
                        outgoing.datagrams().send(ByteBuffer.wrap(up));
                        assertArrayEquals(up, incoming.datagrams().receive(WAIT));
                        incoming.datagrams().send(ByteBuffer.wrap(down));
                        assertArrayEquals(down, outgoing.datagrams().receive(WAIT));
                        outgoing.datagrams().send(ByteBuffer.wrap(large));
                        assertArrayEquals(large, incoming.datagrams().receive(WAIT));
                        assertEquals(1, outgoing.stats().datagramsSentChannel());
                        assertEquals(1, outgoing.stats().datagramsReceivedChannel());
                        assertEquals(1, incoming.stats().datagramsSentChannel());
                        assertEquals(1, incoming.stats().datagramsReceivedChannel());
                        assertEquals(2, outgoing.stats().datagramsSent());
                        outgoing.close(42, "drained");
                        assertEquals(new io.zmux.SessionError.ApplicationClosed(42, "drained", true),
                                incoming.closed().get(20, TimeUnit.SECONDS));
                        boundService.whenDrained().toCompletableFuture().get(20, TimeUnit.SECONDS);
                    }
                } finally {
                    socket.close().get(20, TimeUnit.SECONDS);
                }
            }
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).get(20, TimeUnit.SECONDS);
        }
    }
}
