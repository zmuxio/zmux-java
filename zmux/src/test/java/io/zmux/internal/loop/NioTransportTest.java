package io.zmux.internal.loop;

import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.core.Action;
import io.zmux.carrier.PipeReader;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(30)
class NioTransportTest {
    @Test void aByteStreamBridgeLendsTheOriginalBacking() {
        var host = new SessionHost(BufferPool.DEFAULT_IDLE_BYTES) {
            @Override public boolean inLoop() { return true; }
            @Override public boolean tryExecute(Runnable task) { task.run(); return true; }
            @Override protected void requestRoundEnd() {}
            @Override protected void wakeAt(long deadline) {}
        };
        var lower = new CarrierSetup().own(() -> new CarrierActionsTest.Carrier(host, Action.DIR_IN, null, 0));
        var bridge = new Forwarded(lower, null, false, false);
        var reader = bridge.pipe(null, "", "", false, io.zmux.Admission.OuterSecurity.CLEAR, null).reader();
        var data = ByteBuffer.wrap(new byte[] {11, 22, 33});
        int[] released = {0};
        var backing = new ReadBuf(data, ignored -> released[0]++);
        io.zmux.internal.core.StreamRecv.Chunk[] received = {null};
        reader.start(new PipeReader.Sink() {
            @Override public void onChunk(io.zmux.Chunk chunk, Runnable consumed) {
                received[0] = ChunkAccess.take(chunk);
                consumed.run();
            }
            @Override public void onEnd() {}
            @Override public void onError(Throwable cause) { throw new AssertionError(cause); }
        });
        try {
            assertEquals(2, bridge.bytes(lower, backing, data, 1, 3));
        } finally {
            backing.unref();
            reader.close();
        }
        var chunk = java.util.Objects.requireNonNull(received[0]);
        try {
            assertSame(backing, chunk.buf());
            assertEquals(22, chunk.data().get(0));
            assertEquals(0, released[0]);
        } finally {
            chunk.buf().unref();
        }
        assertEquals(1, released[0]);
    }

    @Test void anUnreadRangeTransfersBetweenPoolsWithTheOriginalOwner() {
        var first = new BufferPool(BufferPool.DEFAULT_IDLE_BYTES);
        var second = new BufferPool(BufferPool.DEFAULT_IDLE_BYTES);
        var source = new ChannelReader(first);
        var target = new ChannelReader(second);
        source.room().put(new byte[] {11, 22, 33});
        source.lim = 3;
        source.consumed(1, false);
        ReadBuf original = source.buf;
        original.ref();
        try {
            source.moveTo(target);
            assertFalse(source.hasPartial());
            assertSame(original, target.buf);
            assertEquals(22, target.buf.data().get(target.pos));
            assertEquals(2, target.lim - target.pos);
            assertEquals(1, first.outstanding());
            assertEquals(0, second.allocations());
            target.consumed(2, false);
            assertEquals(1, first.outstanding());
        } finally {
            target.release();
            source.release();
            original.unref();
        }
        assertEquals(0, first.outstanding());
        assertEquals(0, second.outstanding());
    }

    @Test void pausedEofFollowsTheBufferedBytesAndIsDeliveredOnce() throws Exception {
        var loop = new EventLoop("paused-end", true, BufferPool.DEFAULT_IDLE_BYTES);
        try (var socket = SocketChannel.open()) {
            ExternalTransportTest.run(loop, () -> {
                var carrier = new SocketTransport(loop, socket);
                var received = new ArrayList<String>();
                carrier.start(new Inbound() {
                    @Override public int bytes(Transport transport, ReadBuf buf, ByteBuffer b, int from, int to) {
                        received.add("bytes " + (to - from));
                        return to - from;
                    }

                    @Override public void ended(Transport transport, boolean graceful, boolean partial) {
                        received.add("end " + graceful + " " + partial);
                    }
                });
                carrier.pauseInput();
                carrier.reader.room().put((byte) 42);
                carrier.reader.lim++;
                carrier.endInput(true);
                assertTrue(received.isEmpty());
                assertEquals(1, loop.pool.outstanding());

                carrier.resumeInput();
                carrier.endInput(false);
                assertEquals(List.of("bytes 1", "end true false"), received);
                assertEquals(0, loop.pool.outstanding());
                carrier.reject(null);
            });
        } finally {
            loop.close();
        }
    }
}
