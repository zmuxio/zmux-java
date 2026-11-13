package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.zmux.PipeAuth;
import io.zmux.PipeOptions;
import io.zmux.RecordMode;
import io.zmux.Role;
import io.zmux.Zmux;
import io.zmux.ZmuxPipe;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A join on a real WebSocket whose server reports its Reply's write late: the Confirm and the records after it arrive first. */
@Timeout(60)
class WebSocketJoinTest {
    private static final long DELAY = 300;

    /** Holds the report of what the channel writes first after its upgrade's answer; the bytes go at once. */
    private static final class LateReply extends ChannelOutboundHandlerAdapter {
        private long until = -1;

        @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            boolean upgrade = msg instanceof ByteBuf b && b.isReadable() && b.getByte(b.readerIndex()) == 'H' && until < 0;
            long now = System.nanoTime();
            if (upgrade) { ctx.write(msg, promise); until = 0; return; }
            if (until == 0) until = now + TimeUnit.MILLISECONDS.toNanos(DELAY);
            if (until <= now || promise.isVoid()) { ctx.write(msg, promise); return; }
            long wait = until - now;
            ctx.write(msg).addListener(r -> ctx.executor().schedule(() -> {
                if (r.isSuccess()) promise.trySuccess();
                else promise.tryFailure(r.cause());
            }, wait, TimeUnit.NANOSECONDS));
        }
    }

    @Test void aLateReplyReportLosesNothingOnMessages() throws Exception {
        try (var f = new Fixture(b -> b.maxPipes(4).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation(),
                b -> b, b -> b.webSocket(), false, null); var c = f.client(b -> b)) {
            var inner = f.endpoint.initializer(null, () -> new HostHandler(new java.util.concurrent.LinkedBlockingQueue<>()));
            Channel late = new ServerBootstrap().group(f.group).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<>() {
                @Override protected void initChannel(Channel ch) {
                    ch.pipeline().addLast(new LateReply());
                    ch.pipeline().addLast(inner);
                }
            }).bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
            try (var client = Zmux.open(c.webSocketPipe(WebSocketTest.ws(f.base), Map.of(), List.of(), null), Role.CLIENT, f.config);
                 var server = f.listener.accept(Duration.ofSeconds(5))) {
                URI url = URI.create("ws://localhost:" + ((InetSocketAddress) late.localAddress()).getPort() + "/z");
                ZmuxPipe pipe = client.addPipe(c.webSocketPipe(url, Map.of(), List.of(), null), PipeOptions.builder().recordMode(RecordMode.SEALED).build());
                long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DELAY * 2);
                while (System.nanoTime() < end) Fixture.exchange(client, server, 20_000);
                Thread.sleep(DELAY);
                Fixture.exchange(client, server, 200_000);
                assertEquals(0, server.stats().pipeFailures(), "acceptor");
                assertEquals(0, client.stats().pipeFailures(), "joiner");
                assertEquals(2, server.pipes().size(), "the join is attached");
                assertTrue(pipe.stats().orElseThrow().bytesSent() > 0, "the pipe carried records");
            } finally {
                late.close().sync();
            }
        }
    }
}
