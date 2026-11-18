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
        private ChannelPromise held;
        private boolean read;

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
            if (p != null) ctx.executor().execute(() -> p.trySuccess());
        }
    }

    @AfterEach void clear() { WebSocketClient.connectedForTests = null; }

    @Test void writesAfterAnUpgradeWhoseRequestWriteCompletesLateStillGoThroughTheEncoder() throws Exception {
        try (var f = new Fixture(b -> b, b -> b, b -> b.webSocket(), false, null); var c = f.client(b -> b)) {
            WebSocketClient.connectedForTests = (Channel ch) -> ch.pipeline().addFirst("late-write", new LateWriteCompletion());
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
}
