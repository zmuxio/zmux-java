package io.zmux.http;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.zmux.ListenerConfig;
import io.zmux.Zmux;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A peer's reset ends an incomplete request quietly at every stage of HTTP classification. */
@Timeout(90)
class RouterResetTest {
    private record Read(Channel channel, int bytes) {}

    @Test void aResetMidRequestClosesTheConnectionQuietly() throws Exception {
        var group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        var reads = new LinkedBlockingQueue<Read>();
        try (var listener = Zmux.listener(ListenerConfig.builder().build());
             var endpoint = HttpEndpoint.builder(listener).build()) {
            var socket = new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<Channel>() {
                        @Override protected void initChannel(Channel channel) {
                            channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
                                    int bytes = message instanceof ByteBuf buf ? buf.readableBytes() : 0;
                                    ctx.fireChannelRead(message);
                                    if (bytes != 0) reads.add(new Read(channel, bytes));
                                }
                            }, endpoint.initializer(null, null));
                        }
                    }).bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
            String[] starts = {"POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\npart", "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n", "GE"};
            try {
                for (String start : starts) {
                    Channel child = null;
                    byte[] data = start.getBytes(StandardCharsets.ISO_8859_1);
                    var address = (InetSocketAddress) socket.localAddress();
                    try (var client = new Socket(address.getAddress(), address.getPort())) {
                        client.setSoLinger(true, 0);
                        client.getOutputStream().write(data);
                        client.getOutputStream().flush();
                        for (int received = 0; received < data.length; ) {
                            var read = reads.poll(20, TimeUnit.SECONDS);
                            assertNotNull(read);
                            child = read.channel();
                            received += read.bytes();
                        }
                    }
                    assertNotNull(child);
                    child.closeFuture().get(20, TimeUnit.SECONDS);
                    child.eventLoop().submit(() -> { }).get(20, TimeUnit.SECONDS);
                }
                assertTrue(socket.isActive());
            } finally {
                socket.close().get(20, TimeUnit.SECONDS);
            }
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).get(20, TimeUnit.SECONDS);
        }
    }
}
