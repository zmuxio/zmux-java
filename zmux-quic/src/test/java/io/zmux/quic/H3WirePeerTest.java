package io.zmux.quic;

import static io.zmux.quic.H3WirePeer.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.ChannelOutputShutdownException;
import io.netty.handler.codec.http3.DefaultHttp3DataFrame;
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
        var pem = io.zmux.nettytest.TestTls.pem();
        cert = pem[0].toFile();
        key = pem[1].toFile();
    }

    @Test void requestsThenStreamReset() throws Exception {
        try (var server = H3WirePeer.Endpoint.server(0, cert, key);
             var client = H3WirePeer.Endpoint.client(server.port(), cert)) {
            client.exercise();
            client.exerciseReset();
        }
    }

    @Test void bodylessRequestSendsFinWithHeaders() throws Exception {
        try (var server = H3WirePeer.Endpoint.server(0, cert, key);
             var client = H3WirePeer.Endpoint.client(server.port(), cert)) {
            var state = client.state();
            await(state.settings);
            var response = new H3WirePeer.Response();
            var stream = state.requestWithFin(state.requestHeaders("GET", "/health"), response);
            assertTrue(stream.isOutputShutdown());
            response.expect("ok");
        }
    }

    @Test void finishCompletesWhenPeerStopHasAlreadyCollectedTheStream() throws Exception {
        try (var server = H3WirePeer.Endpoint.server(0, cert, key);
             var client = H3WirePeer.Endpoint.client(server.port(), cert)) {
            var state = client.state();
            await(state.settings);
            var response = new H3WirePeer.Response();
            var stream = state.request(state.requestHeaders("GET", "/health-stop"), response);
            response.expect("ok");

            var collected = new CompletableFuture<Void>();
            var capacity = state.channel.getClass().getDeclaredMethod("streamCapacity", long.class);
            capacity.setAccessible(true);
            var probe = new ChannelInboundHandlerAdapter() {
                @Override public void channelReadComplete(ChannelHandlerContext ctx) {
                    ctx.fireChannelReadComplete();
                    check();
                }

                void check() {
                    try {
                        // QUICHE_ERR_INVALID_STREAM_STATE confirms native collection.
                        if ((long) capacity.invoke(state.channel, stream.streamId()) == -7) collected.complete(null);
                    } catch (ReflectiveOperationException failure) {
                        collected.completeExceptionally(failure);
                    }
                }
            };
            var udp = state.channel.parent().pipeline();
            await(stream.eventLoop().submit(() -> {
                udp.addFirst(probe);
                probe.check();
            }));
            try {
                await(collected);
                var fin = await(stream.eventLoop().submit(() -> {
                    assertTrue(stream.isActive());
                    assertTrue(stream.isInputShutdown());
                    assertFalse(stream.isOutputShutdown());
                    return QuicStreamPipe.finish(stream);
                }));
                assertTrue(fin.await(5, TimeUnit.SECONDS));
                assertInstanceOf(ChannelOutputShutdownException.class, fin.cause());
                await(stream.closeFuture());
                client.health();
            } finally {
                await(stream.eventLoop().submit(() -> { udp.remove(probe); }));
            }
        }
    }

    @Test void finishWaitsForFlowControlledDataBeforeSendingFin() throws Exception {
        var options = QuicOptions.defaults().toBuilder().streamWindow(4096).build();
        try (var server = H3WirePeer.Endpoint.server(0, cert, key, options);
             var client = H3WirePeer.Endpoint.client(server.port(), cert)) {
            var state = client.state();
            await(state.settings);
            var udp = server.datagramChannel();
            await(udp.eventLoop().submit(() -> udp.config().setAutoRead(false)));

            var response = new H3WirePeer.Response();
            var stream = state.request(state.requestHeaders("POST", "/exchange"), response);
            String payload = "0123456789abcdef".repeat(2048);
            var write = stream.writeAndFlush(new DefaultHttp3DataFrame(H3WirePeer.bytes(payload)));
            var fin = QuicStreamPipe.finish(stream);
            var checked = new CompletableFuture<Void>();
            NettyHost.afterIteration(stream.eventLoop()).execute(() -> {
                try {
                    assertFalse(stream.isWritable());
                    assertFalse(write.isDone());
                    assertFalse(fin.isDone());
                    assertTrue(stream.isActive());
                    checked.complete(null);
                } catch (AssertionError failure) {
                    checked.completeExceptionally(failure);
                }
            });
            await(checked);

            await(udp.eventLoop().submit(() -> udp.config().setAutoRead(true)));
            await(write);
            await(fin);
            response.expect(payload);
        }
    }

    @Test void encodedHeadPromiseFailsAndReleasesBufferWhenOutputEnds() throws Exception {
        try (var server = H3WirePeer.Endpoint.server(0, cert, key);
             var client = H3WirePeer.Endpoint.client(server.port(), cert)) {
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
