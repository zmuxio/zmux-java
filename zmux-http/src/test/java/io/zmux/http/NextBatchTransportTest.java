package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class NextBatchTransportTest {
    @Test void reentrantWriterAndCloseStayBehindThePendingResponseHead() {
        var channel = new EmbeddedChannel();
        try {
            var head = channel.newPromise();
            var heads = new AtomicInteger();
            var writer = new AtomicReference<PipeWriter>();
            var first = new CompletableFuture<Void>();
            var nested = new CompletableFuture<Void>();
            var afterClose = new CompletableFuture<Void>();
            var body = new BodyCarrier(channel, Framing.BYTE_STREAM, false, true, new BodyCarrier.Out() {
                @Override public ChannelPromise head() {
                    assertEquals(1, heads.incrementAndGet());
                    write(writer.get(), nested, (byte) 2);
                    return head;
                }
                @Override public Object content(ByteBuf data) { return data; }
                @Override public Object end() { return "fin"; }
            }, () -> {}, cause -> null);
            writer.set(body.writer());

            write(writer.get(), first, (byte) 1);
            writer.get().close();
            write(writer.get(), afterClose, (byte) 3);
            assertNull(channel.readOutbound());
            assertFalse(first.isDone());
            assertFalse(nested.isDone());
            assertTrue(afterClose.isCompletedExceptionally());

            head.setSuccess();
            first.join();
            nested.join();
            assertByte(channel, 1);
            assertByte(channel, 2);
            Object fin = channel.readOutbound();
            assertEquals("fin", fin);
            assertNull(channel.readOutbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test void anEarlierMessageFailureFailsTheBatchAfterNettyReleasesTheLastWrite() {
        var failure = new IOException("first message failed");
        var retained = new AtomicReference<ByteBuf>();
        var lastWrite = new AtomicReference<ChannelPromise>();
        var writes = new AtomicInteger();
        var channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
                var bytes = (ByteBuf) message;
                if (writes.incrementAndGet() == 1) {
                    bytes.release();
                    promise.setFailure(failure);
                } else {
                    retained.set(bytes);
                    lastWrite.set(promise);
                }
            }
        });
        try {
            var body = new BodyCarrier(channel, Framing.MESSAGES, false, true, new BodyCarrier.Out() {
                @Override public Object content(ByteBuf data) { return data; }
                @Override public Object end() { return null; }
            }, () -> {}, cause -> null);
            var completion = new CompletableFuture<Void>();
            body.writer().write(OutboundBatch.of(ByteBuffer.wrap(new byte[]{1}), ByteBuffer.wrap(new byte[]{2})),
                    completion(completion));
            assertEquals(2, writes.get());
            assertFalse(completion.isDone());
            channel.close();
            assertEquals(1, retained.get().refCnt());
            retained.get().release();
            lastWrite.get().setSuccess();
            assertSame(failure, assertThrows(CompletionException.class, completion::join).getCause());
            assertEquals(0, retained.get().refCnt());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test void closingDuringTheFirstMessageQueuesFinBehindTheWholeBatch() {
        var writer = new AtomicReference<PipeWriter>();
        var channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            boolean closing;

            @Override public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
                if (!closing) {
                    closing = true;
                    writer.get().close();
                }
                ctx.write(message, promise);
            }
        });
        try {
            var body = new BodyCarrier(channel, Framing.MESSAGES, false, true, new BodyCarrier.Out() {
                @Override public Object content(ByteBuf data) { return data; }
                @Override public Object end() { return "fin"; }
            }, () -> {}, cause -> null);
            writer.set(body.writer());
            var complete = new CompletableFuture<Void>();
            writer.get().write(OutboundBatch.of(ByteBuffer.wrap(new byte[]{1}), ByteBuffer.wrap(new byte[]{2})), completion(complete));
            complete.join();
            assertByte(channel, 1);
            assertByte(channel, 2);
            Object fin = channel.readOutbound();
            assertEquals("fin", fin);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static void assertByte(EmbeddedChannel channel, int expected) {
        ByteBuf data = channel.readOutbound();
        try { assertEquals(expected, data.readUnsignedByte()); }
        finally { data.release(); }
    }

    private static void write(PipeWriter writer, CompletableFuture<Void> result, byte value) {
        writer.write(OutboundBatch.of(ByteBuffer.wrap(new byte[]{value})), completion(result));
    }

    private static PipeWriter.Completion completion(CompletableFuture<Void> result) {
        return new PipeWriter.Completion() {
            @Override public void completed() { result.complete(null); }
            @Override public void failed(Throwable cause) { result.completeExceptionally(cause); }
        };
    }
}
