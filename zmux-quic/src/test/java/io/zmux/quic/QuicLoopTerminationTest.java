package io.zmux.quic;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.*;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.zmux.*;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A QUIC loop that terminates without anything closing its host first. */
@Timeout(60)
class QuicLoopTerminationTest {
    @BeforeAll static void tls() throws Exception { TestTls.init(); }

    /**
     * The shutdown hooks of a loop's host that terminated without close run as the host's thread: they find
     * themselves in the loop, and so run their loop-side work inline instead of posting it to the stopped loop.
     */
    @Test void shutdownHooksOfATerminatedLoopRunInTheHostsLoop() throws Exception {
        var loop = new DefaultEventLoop();
        NettyHost host = new NettyHost(loop);
        var seen = new java.util.concurrent.atomic.AtomicReference<boolean[]>();
        var hooked = new CountDownLatch(1);
        host.onShutdown(() -> {
            seen.set(new boolean[] {host.inLoop(), io.zmux.internal.loop.SessionHost.onHostThread()});
            hooked.countDown();
        });
        assertTrue(loop.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).await(10, TimeUnit.SECONDS));
        assertTrue(hooked.await(10, TimeUnit.SECONDS), "the hook did not run");
        assertTrue(seen.get()[0], "the hook ran outside the host's loop");
        assertTrue(seen.get()[1], "the hook's thread was not a host thread");
        // The stand-in thread is only that while the hooks run.
        assertFalse(host.inLoop());
    }

    /** A served connection's loop leaves the registry of hosts when it terminates (a weak map would keep it: its host references the loop). */
    @Test void aTerminatedLoopLeavesTheHostRegistry() throws Exception {
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (EventLoops loops = EventLoops.create(1)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var served = new CompletableFuture<EventLoop>();
            try (var listener = Zmux.listener(ListenerConfig.of(config))) {
                QuicOptions options = QuicOptions.defaults();
                ChannelHandler codec = options.apply(new QuicServerCodecBuilder()).sslContext(TestTls.server.build())
                        .handler(new ChannelInboundHandlerAdapter() {
                            @Override public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                                if (evt instanceof SslHandshakeCompletionEvent e && e.isSuccess()) {
                                    QuicServer.serve((QuicChannel) ctx.channel(), options, listener);
                                    served.complete(ctx.channel().eventLoop());
                                }
                                ctx.fireUserEventTriggered(evt);
                            }
                            @Override public boolean isSharable() { return true; }
                        })
                        .streamHandler(QuicServer.streamHandler()).build();
                Channel udp = new Bootstrap().group(group).channel(NioDatagramChannel.class).handler(codec)
                        .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
                try (var endpoint = new QuicClient(TestTls.client, options)) {
                    endpoint.connectAsync((InetSocketAddress) udp.localAddress(), "localhost", config).get(10, TimeUnit.SECONDS);
                    EventLoop loop = served.get(10, TimeUnit.SECONDS);
                    Map<?, ?> hosts = hosts();
                    assertTrue(hosts.containsKey(loop), "a served loop has its host");
                    // Registered after the registry's own listener of the same future: it runs after it.
                    var after = new CountDownLatch(1);
                    loop.terminationFuture().addListener(f -> after.countDown());
                    udp.close().sync();
                    group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync();
                    assertTrue(after.await(10, TimeUnit.SECONDS));
                    assertFalse(hosts.containsKey(loop), "a terminated loop stays in the registry");
                }
            }
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync();
        }
    }

    private static Map<?, ?> hosts() throws ReflectiveOperationException {
        var field = QuicServer.class.getDeclaredField("HOSTS");
        field.setAccessible(true);
        return (Map<?, ?>) field.get(null);
    }
}
