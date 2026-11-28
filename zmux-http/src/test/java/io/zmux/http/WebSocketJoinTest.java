package io.zmux.http;

import io.zmux.CapacityLimits;
import io.zmux.SessionPolicy;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelDuplexHandler;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A join on a real WebSocket whose server reports its Reply's write late: the Confirm and the records after it arrive first. */
@Timeout(60)
class WebSocketJoinTest {
    /** Sends the first carrier message but holds its write receipt until the peer answers it. */
    private static final class LateReply extends ChannelDuplexHandler {
        final CompletableFuture<Runnable> release = new CompletableFuture<>();
        final CompletableFuture<Void> confirm = new CompletableFuture<>();
        private boolean upgraded;
        private boolean held;

        @Override public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
            if (!upgraded && message instanceof ByteBuf bytes && bytes.isReadable() && bytes.getByte(bytes.readerIndex()) == 'H') {
                upgraded = true;
            } else if (upgraded && !held && !promise.isVoid()) {
                held = true;
                ctx.write(message).addListener(result -> release.complete(() -> {
                    if (result.isSuccess()) promise.trySuccess();
                    else promise.tryFailure(result.cause());
                }));
                return;
            }
            ctx.write(message, promise);
        }

        @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
            boolean afterReply = held;
            ctx.fireChannelRead(message);
            if (afterReply) confirm.complete(null);
        }
    }

    @Test void aLateReplyReportLosesNothingOnMessages() throws Exception {
        try (var f = new Fixture(b -> b.sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation(),
                b -> b, b -> b.webSocket(), false, null); var c = f.client(b -> b)) {
            var inner = f.endpoint.initializer(null, () -> new HostHandler(new java.util.concurrent.LinkedBlockingQueue<>()));
            var gate = new LateReply();
            Channel late = new ServerBootstrap().group(f.group).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<>() {
                @Override protected void initChannel(Channel ch) {
                    ch.pipeline().addLast(gate);
                    ch.pipeline().addLast(inner);
                }
            }).bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
            try (var client = Zmux.open(c.webSocketPipe(WebSocketTest.ws(f.base), Map.of(), List.of(), null), Role.CLIENT, f.config);
                 var server = f.listener.accept(Duration.ofSeconds(5))) {
                URI url = URI.create("ws://localhost:" + ((InetSocketAddress) late.localAddress()).getPort() + "/z");
                ZmuxPipe pipe = client.addPipe(c.webSocketPipe(url, Map.of(), List.of(), null),
                        PipeOptions.builder().recordMode(RecordMode.SEALED).returnOnReply(true).build());
                var release = gate.release.get(20, TimeUnit.SECONDS);
                try { gate.confirm.get(20, TimeUnit.SECONDS); }
                finally { release.run(); }
                pipe.awaitAttached(Duration.ofSeconds(20));
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
