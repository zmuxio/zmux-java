package io.zmux.http;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.zmux.ListenerConfig;
import io.zmux.PipeOptions;
import io.zmux.Role;
import io.zmux.Zmux;
import io.zmux.ZmuxListener;
import io.zmux.ZmuxSession;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The minimal example of zmux-http/README.md, kept runnable. */
final class ReadmeExampleTest {
    @Test @Timeout(30) void streamingCarrierEcho() throws Exception {
        var group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        try (ZmuxListener listener = ZmuxListener.of(ListenerConfig.defaults());
             HttpEndpoint endpoint = HttpEndpoint.builder(listener).streaming().build()) {
            var server = new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                    .childHandler(endpoint.initializer(null, null))            // cleartext HTTP/1.1 and h2c
                    .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
            int port = ((InetSocketAddress) server.localAddress()).getPort();

            Thread.ofVirtual().start(() -> {                                    // the server side: echo one stream
                try (ZmuxSession session = listener.accept(); var stream = session.acceptStream()) {
                    stream.write(stream.readAll(1 << 20));
                    stream.finish();
                    session.awaitClosed();
                } catch (Exception e) {
                    // The client ended first.
                }
            });

            try (HttpCarrierClient client = HttpCarrierClient.builder().group(group).build();
                 ZmuxSession session = Zmux.open(client.pipe(URI.create("http://127.0.0.1:" + port + "/zmux"), Map.of(),
                         PipeOptions.Direction.BIDIRECTIONAL, null), Role.CLIENT, listener.config());
                 var stream = session.openStream()) {
                stream.write("hello".getBytes(StandardCharsets.UTF_8));
                stream.finish();
                assertEquals("hello", new String(stream.readAll(1 << 20), StandardCharsets.UTF_8));
            }
            server.close().sync();
        } finally {
            group.shutdownGracefully(0, 1, java.util.concurrent.TimeUnit.SECONDS).sync();
        }
    }
}
