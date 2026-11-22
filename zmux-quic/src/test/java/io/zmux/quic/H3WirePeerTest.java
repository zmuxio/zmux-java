package io.zmux.quic;

import static io.zmux.quic.H3WirePeer.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.ChannelOutputShutdownException;
import io.netty.handler.codec.http3.Http3;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class H3WirePeerTest {
    private static File cert;
    private static File key;

    @BeforeAll static void tls() throws Exception {
        TestTls.init();
        var pem = QuicChannelInteropTest.pem();
        cert = pem[0].toFile();
        key = pem[1].toFile();
    }

    @Test void requestsThenStreamReset() throws Exception {
        try (var server = H3WirePeer.Endpoint.server(0, cert, key, false);
             var client = H3WirePeer.Endpoint.client(server.port(), cert, false)) {
            client.exercise(false);
            client.exerciseReset();
        }
    }

    @Test void encodedHeadPromiseFailsAndReleasesBufferWhenOutputEnds() throws Exception {
        try (var server = H3WirePeer.Endpoint.server(0, cert, key, false);
             var client = H3WirePeer.Endpoint.client(server.port(), cert, false)) {
            var state = client.state();
            await(state.settings);
            var stream = await(Http3.newRequestStream(state.channel, new H3WirePeer.Response()));
            var gate = new HoldEncodedHead();
            await(stream.eventLoop().submit(() -> stream.pipeline().addFirst(gate)));
            var write = stream.writeAndFlush(state.requestHeaders("POST", "/stream"));
            var encoded = await(gate.captured);
            assertSame(write, encoded.promise());
            assertFalse(write.isDone());
            // Keep a test reference so Netty cannot recycle the pooled buffer before inspection.
            encoded.buffer().retain();
            try {
                await(stream.shutdownOutput());
                await(stream.eventLoop().submit(gate::forward));
                assertTrue(write.await(5, TimeUnit.SECONDS));
                assertInstanceOf(ChannelOutputShutdownException.class, write.cause());
                assertEquals(1, encoded.buffer().refCnt());
            } finally {
                encoded.buffer().release();
            }
            client.health();
        }
    }

    @Test void webTransportAndSharedConnection() throws Exception {
        try (var server = H3WirePeer.Endpoint.server(0, cert, key, true);
             var client = H3WirePeer.Endpoint.client(server.port(), cert, true)) {
            client.exercise(true);
        }
    }

    record EncodedHead(ByteBuf buffer, ChannelPromise promise) { }

    private static final class HoldEncodedHead extends ChannelOutboundHandlerAdapter {
        final CompletableFuture<EncodedHead> captured = new CompletableFuture<>();
        private ChannelHandlerContext context;
        private EncodedHead pending;

        @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            if (pending != null || !(msg instanceof ByteBuf buffer)) {
                ctx.write(msg, promise);
                return;
            }
            context = ctx;
            pending = new EncodedHead(buffer, promise);
            captured.complete(pending);
        }

        void forward() {
            EncodedHead forward = pending;
            pending = null;
            context.writeAndFlush(forward.buffer(), forward.promise());
        }

        @Override public void handlerRemoved(ChannelHandlerContext ctx) {
            if (pending != null) {
                ReferenceCountUtil.release(pending.buffer());
                pending.promise().tryFailure(new ClosedChannelException());
                pending = null;
            }
        }
    }
}
