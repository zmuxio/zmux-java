package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.zmux.Role;
import io.zmux.Zmux;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A WebSocket carrier's writes never bypass the frame encoder, however the channel orders the
 * completion of the upgrade request's write against the arrival of the upgrade response: here
 * the request's write completes only after the response was read and handled (a later loop
 * task), which Netty permits and the handshake's own encoder installation depends on.
 */
@Timeout(60)
class WebSocketHandshakeOrderTest {
    /** Holds the completion of the first write until the first read was passed on, then completes it as a later loop task. */
    private static final class LateWriteCompletion extends ChannelDuplexHandler {
        private final boolean untilClosed;
        private ChannelPromise held;
        private boolean read;

        /** untilClosed: the completion waits for the connection's close, not just the first read. */
        LateWriteCompletion(boolean untilClosed) { this.untilClosed = untilClosed; }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            ctx.fireChannelInactive();
            ChannelPromise p = held;
            if (untilClosed && p != null) ctx.executor().execute(() -> p.trySuccess());
        }

        @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            if (held != null || read) { ctx.write(msg, promise); return; }
            held = promise;
            ChannelPromise inner = ctx.newPromise();
            inner.addListener(f -> { if (!f.isSuccess()) promise.tryFailure(f.cause()); });
            ctx.write(msg, inner);
        }

        @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ctx.fireChannelRead(msg);
            if (read) return;
            read = true;
            ChannelPromise p = held;
            if (!untilClosed && p != null) ctx.executor().execute(() -> p.trySuccess());
        }
    }

    @AfterEach void clear() { WebSocketClient.connectedForTests = null; }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void closingUpgradedRequestCancelsUnreadTransform(boolean closeFrame) throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var handshake = new java.util.concurrent.CompletableFuture<io.zmux.carrier.PipeIo>();
        var channel = new java.util.concurrent.CompletableFuture<Channel>();
        WebSocketClient.connectedForTests = channel::complete;
        try (var f = new Fixture(b -> b.noHandshakeTimeout(), b -> b.maxPendingPerSource(2)
                .inboundTransform((io, context) -> { entered.countDown(); return handshake; }), b -> b.webSocket(), false, null);
             var c = f.client(b -> b)) {
            c.webSocketPipe(WebSocketTest.ws(f.base), Map.of(), List.of(), null);
            assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
            var ws = channel.get(5, java.util.concurrent.TimeUnit.SECONDS);
            if (closeFrame) ws.writeAndFlush(new io.netty.handler.codec.http.websocketx.CloseWebSocketFrame()).sync();
            else ws.close().sync();
            assertThrows(java.util.concurrent.CancellationException.class, () -> handshake.get(5, java.util.concurrent.TimeUnit.SECONDS));
        }
    }

    @Test void writesAfterAnUpgradeWhoseRequestWriteCompletesLateStillGoThroughTheEncoder() throws Exception {
        try (var f = new Fixture(b -> b, b -> b, b -> b.webSocket(), false, null); var c = f.client(b -> b)) {
            WebSocketClient.connectedForTests = (Channel ch) -> ch.pipeline().addFirst("late-write", new LateWriteCompletion(false));
            var p = c.webSocketPipe(WebSocketTest.ws(f.base), Map.of(), List.of(), null);
            try (var client = Zmux.open(p, Role.CLIENT, f.config); var server = f.listener.accept(Duration.ofSeconds(5))) {
                assertNotNull(server);
                var out = client.openStream();
                out.write(new byte[] {1, 2, 3});
                var in = server.acceptStream(Duration.ofSeconds(5));
                assertNotNull(in);
                byte[] b = new byte[3];
                for (int n = 0, r; n < 3; n += r) { r = in.read(b, n, 3 - n); assertTrue(r > 0); }
                assertArrayEquals(new byte[] {1, 2, 3}, b);
            }
        }
    }

    @Test void aRedirectAnsweredBeforeTheRequestWriteCompletedIsReportedAsTheRedirectEvenWhenTheServerCloses() throws Exception {
        try (var f = new Fixture(b -> b, b -> b, b -> b.webSocket(), false, null); var c = f.client(b -> b.http1());
             var away = new RedirectTest.Front(f.base, 302, u -> "http://other.example/z")) {
            WebSocketClient.connectedForTests = (Channel ch) -> ch.pipeline().addFirst("late-write", new LateWriteCompletion(true));
            var e = assertThrows(io.zmux.HttpRedirectException.class,
                    () -> c.webSocketPipe(WebSocketTest.ws(away.base), Map.of(), List.of(), null));
            assertEquals("another host", e.reason());
        }
    }
}
