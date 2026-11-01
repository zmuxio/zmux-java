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

    @Test void anEstablishedConnectionIsAdmittedOnceAndServedUntilItEnds() throws Exception {
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var decisions = new AtomicInteger();
            var lc = ListenerConfig.builder().session(config)
                    .admission(Admission.of(0, Admission.Decider.immediate((i, p) -> { decisions.incrementAndGet(); return Admission.Decision.accept(0, "q"); })))
                    .build();
            var served = new CompletableFuture<java.util.concurrent.CompletionStage<Void>>();
            try (var listener = Zmux.listener(lc)) {
                QuicOptions options = QuicOptions.defaults();
                ChannelHandler codec = options.apply(new QuicServerCodecBuilder()).sslContext(TestTls.server.build())
                        .handler(new ChannelInboundHandlerAdapter() {
                            @Override public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
                                if (evt instanceof SslHandshakeCompletionEvent e && e.isSuccess())
                                    served.complete(QuicServer.serve((QuicChannel) ctx.channel(), options, listener, null));
                                ctx.fireUserEventTriggered(evt);
                            }
                            @Override public boolean isSharable() { return true; }
                        })
                        .streamHandler(QuicServer.streamHandler()).build();
                Channel udp = new Bootstrap().group(group).channel(NioDatagramChannel.class).handler(codec)
                        .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
                try (var endpoint = new QuicClient(TestTls.client, options)) {
                    ZmuxSession client = endpoint.connect((InetSocketAddress) udp.localAddress(), "localhost", config)
                            .toCompletableFuture().get(10, TimeUnit.SECONDS);
                    ZmuxSession accepted = listener.accept(Duration.ofSeconds(10));
                    assertNotNull(accepted);
                    assertEquals(java.util.Optional.of("q"), accepted.admitted());
                    var st = client.openStream();
                    st.write(new byte[] {7});
                    st.finish();
                    var in = accepted.acceptStream();
                    var b = new byte[4];
                    assertEquals(1, in.read(b, 0, 4));
                    assertEquals(1, decisions.get(), "one decision per connection");
                    assertEquals(1, listener.stats().admission().accepted());
                    var done = served.get(5, TimeUnit.SECONDS).toCompletableFuture();
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
}
