package io.zmux.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.zmux.Chunk;
import io.zmux.JoinHint;
import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class BodyCarrierLeakTest {
    private static BodyCarrier carrier(io.netty.channel.Channel ch) {
        return new BodyCarrier(ch, Framing.BYTE_STREAM, true, true, new BodyCarrier.Out() {
            @Override public Object content(ByteBuf data) { return data; }
            @Override public Object end() { return null; }
        }, () -> {}, e -> null);
    }

    @Test void takeoverOpeningObservesTheActualNettyHeadWrite() throws Exception {
        // HS-15, HS-16, HS-16a.
        for (String ending : List.of("commit", "fail", "throw", "withdraw")) {
            var intercepted = new CompletableFuture<ChannelPromise>();
            var failure = new IllegalStateException("head write failed");
            var ch = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
                @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                    io.netty.util.ReferenceCountUtil.release(msg);
                    intercepted.complete(promise);
                }
            });
            try {
                var c = new BodyCarrier(ch, Framing.BYTE_STREAM, true, true, new BodyCarrier.Out() {
                    @Override public io.netty.channel.ChannelFuture head() {
                        if (ending.equals("throw")) throw failure;
                        return ch.writeAndFlush(new io.netty.handler.codec.http.DefaultHttpResponse(
                                io.netty.handler.codec.http.HttpVersion.HTTP_1_1, io.netty.handler.codec.http.HttpResponseStatus.OK));
                    }
                    @Override public Object content(ByteBuf data) { return data; }
                    @Override public Object end() { return null; }
                }, () -> {}, cause -> null);
                var head = c.opening();
                var opened = head.open();
                if (!ending.equals("throw")) {
                    var promise = intercepted.get(3, TimeUnit.SECONDS);
                    assertFalse(opened.isDone());
                    if (ending.equals("withdraw")) {
                        assertTrue(opened.cancel(false));
                        ch.runPendingTasks();
                        assertFalse(head.released().toCompletableFuture().isDone());
                        promise.setFailure(failure);
                    } else if (ending.equals("commit")) promise.setSuccess();
                    else promise.setFailure(failure);
                    ch.runPendingTasks();
                }
                head.released().toCompletableFuture().get(3, TimeUnit.SECONDS);
                if (ending.equals("withdraw")) assertTrue(opened.isCancelled());
                else if (ending.equals("commit")) opened.get(3, TimeUnit.SECONDS);
                else org.junit.jupiter.api.Assertions.assertSame(failure, assertThrows(java.util.concurrent.ExecutionException.class,
                        () -> opened.get(3, TimeUnit.SECONDS)).getCause());
            } finally { ch.finishAndReleaseAll(); }
        }
    }

    @Timeout(25)
    @Test void splitHandlesWaitForTheHttpResponseTailBeforeReusingTheToken() throws Exception {
        // HS-44.
        String token = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[16]);
        try (var f = new Fixture(b -> b.noHandshakeTimeout(), b -> b.inboundTransform((io, ctx) ->
                CompletableFuture.completedFuture(io)), b -> b.streaming(), false, null)) {
            for (int use = 0; use < 2; use++) {
                var tail = new CompletableFuture<ChannelPromise>();
                var ch = new Bootstrap().group(f.group).channel(NioSocketChannel.class)
                        .handler(new ChannelOutboundHandlerAdapter() {
                            @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise done) {
                                io.netty.util.ReferenceCountUtil.release(msg);
                                tail.complete(done);
                            }
                        }).connect(f.server.localAddress()).sync().channel();
                try {
                    var downBody = ch.eventLoop().submit(() -> {
                        BodyCarrier[] self = new BodyCarrier[1];
                        self[0] = new BodyCarrier(ch, Framing.BYTE_STREAM, false, true, new BodyCarrier.Out() {
                            @Override public Object content(ByteBuf bytes) { return bytes; }
                            @Override public Object end() { return new io.netty.handler.codec.http.DefaultLastHttpContent(); }
                        }, () -> self[0].afterWrites(ch::close), error -> { ch.close(); return null; });
                        return self[0];
                    }).get(5, TimeUnit.SECONDS);
                    var started = new CompletableFuture<PipeReader.Sink>();
                    PipeReader reader = new PipeReader() {
                        @Override public void start(Sink sink) { started.complete(sink); }
                        @Override public void close() {}
                    };
                    var up = f.listener.adapter().handle(PipeIo.of(reader, null), JoinHint.pair(token, true, null));
                    var down = f.listener.adapter().handle(PipeIo.of(null, downBody.writer()), JoinHint.pair(token, false, null));
                    started.get(5, TimeUnit.SECONDS).onLent(java.nio.ByteBuffer.wrap(new byte[]{0}), () -> {});
                    var write = tail.get(5, TimeUnit.SECONDS);
                    try {
                        assertThrows(java.io.IOException.class, up::awaitSetup);
                        var spare = MemoryPipe.pair(Framing.BYTE_STREAM)[1];
                        var replay = f.listener.adapter().handle(PipeIo.of(null, spare.writer()), JoinHint.pair(token, false, null));
                        replay.closed().get(5, TimeUnit.SECONDS);
                        assertThrows(java.io.IOException.class, replay::awaitSetup);
                        assertFalse(up.closed().isDone());
                        assertFalse(down.closed().isDone());
                    } finally { write.trySuccess(); }
                    up.closed().get(5, TimeUnit.SECONDS);
                    down.closed().get(5, TimeUnit.SECONDS);
                } finally { ch.close().sync(); }
            }
        }
    }

    /** A carrier kept for a replay is lost before zmux started its reader: nothing stays referenced. */
    @Test void lostBeforeReaderStartReleasesQueue() {
        EmbeddedChannel ch = new EmbeddedChannel();
        BodyCarrier c = carrier(ch);
        PipeReader r = c.reader();
        c.keepForReplay();
        ByteBuf a = Unpooled.buffer().writeBytes(new byte[] {1, 2, 3});
        ByteBuf b = Unpooled.buffer().writeBytes(new byte[] {4});
        c.in(a);
        c.in(b);
        c.lost(new ClosedChannelException());
        assertEquals(0, a.refCnt());
        assertEquals(0, b.refCnt());
        // A late start sees the error only.
        List<Throwable> errors = new ArrayList<>();
        r.start(new PipeReader.Sink() {
            @Override public void onChunk(Chunk chunk, Runnable consumed) { throw new AssertionError("data after loss"); }
            @Override public void onEnd() { throw new AssertionError("end after loss"); }
            @Override public void onError(Throwable t) { errors.add(t); }
        });
        assertEquals(1, errors.size());
        assertTrue(errors.get(0) instanceof ClosedChannelException);
        r.close();
        ch.finishAndReleaseAll();
    }

    /**
     * The whole body arrived (its end too) before zmux started the reader, then the carrier was
     * lost: the queue goes at the loss, and a late reader sees the loss.
     */
    @Test void endedThenLostBeforeReaderStartReleasesQueue() {
        EmbeddedChannel ch = new EmbeddedChannel();
        BodyCarrier c = carrier(ch);
        PipeReader r = c.reader();
        ByteBuf a = Unpooled.buffer().writeBytes(new byte[] {1, 2, 3});
        ByteBuf b = Unpooled.buffer().writeBytes(new byte[] {4});
        c.in(a);
        c.in(b);
        c.inEnd();
        assertEquals(1, a.refCnt());
        c.lost(new ClosedChannelException());
        assertEquals(0, a.refCnt());
        assertEquals(0, b.refCnt());
        LoopShutdownTest.ErrorSink sink = new LoopShutdownTest.ErrorSink();
        r.start(sink);
        assertEquals(1, sink.errors.size());
        assertTrue(sink.errors.getFirst() instanceof ClosedChannelException);
        r.close();
        ch.finishAndReleaseAll();
    }

    /** A reader that is taking a complete body keeps it when the carrier is lost meanwhile: the rest and the end still arrive. */
    @Test void endedThenLostWhileReadingDeliversTheRest() {
        EmbeddedChannel ch = new EmbeddedChannel();
        BodyCarrier c = carrier(ch);
        List<Chunk> got = new ArrayList<>();
        List<String> ends = new ArrayList<>();
        List<Runnable> consumed = new ArrayList<>();
        c.reader().start(new PipeReader.Sink() {
            @Override public void onChunk(Chunk chunk, Runnable c) { got.add(chunk); consumed.add(c); }
            @Override public void onEnd() { ends.add("end"); }
            @Override public void onError(Throwable t) { ends.add("error"); }
        });
        // Over the receive memory: the second piece waits for the first to be consumed.
        ByteBuf a = Unpooled.buffer().writeBytes(new byte[BodyCarrier.RECEIVE_MEMORY]);
        ByteBuf b = Unpooled.buffer().writeBytes(new byte[] {4});
        c.in(a);
        c.in(b);
        c.inEnd();
        c.lost(new ClosedChannelException());
        assertEquals(1, got.size());
        assertEquals(1, b.refCnt(), "the rest of a complete body stays for the reader");
        got.getFirst().close();
        consumed.getFirst().run();
        assertEquals(2, got.size());
        assertEquals(List.of("end"), ends);
        got.get(1).close();
        assertEquals(0, a.refCnt());
        assertEquals(0, b.refCnt());
        ch.finishAndReleaseAll();
    }

    /**
     * The loop of a carrier with queued input shuts down: closing its channel releases the
     * queue, and zmux's later calls fail what they brought at once.
     */
    @Test void loopShutDownReleasesQueueAndFailsCalls() throws Exception {
        var group = new io.netty.channel.MultiThreadIoEventLoopGroup(1, io.netty.channel.nio.NioIoHandler.newFactory());
        try {
            var server = new io.netty.bootstrap.ServerBootstrap().group(group).channel(io.netty.channel.socket.nio.NioServerSocketChannel.class)
                    .childHandler(new io.netty.channel.ChannelInboundHandlerAdapter())
                    .bind(java.net.InetAddress.getLoopbackAddress(), 0).sync().channel();
            BodyCarrier[] carrier = new BodyCarrier[1];
            var ch = new io.netty.bootstrap.Bootstrap().group(group).channel(io.netty.channel.socket.nio.NioSocketChannel.class)
                    .handler(new io.netty.channel.ChannelInboundHandlerAdapter() {
                        @Override public void channelInactive(io.netty.channel.ChannelHandlerContext ctx) {
                            carrier[0].lost(new ClosedChannelException()); // as the router does
                        }
                    })
                    .connect(server.localAddress()).sync().channel();
            ByteBuf data = Unpooled.directBuffer().writeBytes(new byte[] {1, 2, 3});
            ch.eventLoop().submit(() -> {
                carrier[0] = carrier(ch);
                carrier[0].in(data);
            }).sync();
            assertEquals(1, data.refCnt(), "queued for a reader that has not started");
            assertTrue(group.shutdownGracefully(0, 0, java.util.concurrent.TimeUnit.SECONDS).await(5, java.util.concurrent.TimeUnit.SECONDS));
            assertEquals(0, data.refCnt(), "the loop closed the channel, and its loss released the queue");
            LoopShutdownTest.assertCallsFailAtOnce(new io.zmux.carrier.PipeIo(carrier[0].reader(), carrier[0].writer(), Framing.BYTE_STREAM, "test", null));
        } finally {
            group.shutdownGracefully(0, 0, java.util.concurrent.TimeUnit.SECONDS);
        }
    }
}
