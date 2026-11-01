package io.zmux.internal.loop;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import io.zmux.internal.core.BatchFixture;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class ExternalTransportTest {
    static void run(EventLoop loop, Runnable task) throws Exception {
        CompletableFuture<Void> done = new CompletableFuture<>();
        loop.execute(() -> {
            try { task.run(); done.complete(null); }
            catch (Throwable error) { done.completeExceptionally(error); }
        });
        done.get(5, TimeUnit.SECONDS);
    }
    @Test void closeKeepsBorrowedOutputAliveUntilTheWriterCompletesAfterLoopShutdown() throws Exception {
        EventLoop loop = new EventLoop("write-lifetime", true, BufferPool.DEFAULT_IDLE_BYTES);
        AtomicReference<PipeWriter.Completion> completion = new AtomicReference<>();
        PipeWriter writer = new PipeWriter() {
            @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) { completion.set(done); }
            @Override public void close() {}
        };
        try {
            run(loop, () -> {
                ExternalTransport carrier = new ExternalTransport(loop, PipeIo.of(null, writer));
                var batch = new io.zmux.internal.core.Batch();
                var payload = loop.pool.take();
                BatchFixture.pin(batch, payload); payload.unref();
                carrier.write(batch); carrier.close(null);
                assertEquals(1, loop.pool.outstanding(), "writer still owns the referenced output");
            });
            loop.close();
            completion.get().completed();
            assertEquals(0, loop.pool.outstanding());
            completion.get().failed(new IllegalStateException("duplicate completion"));
            assertEquals(0, loop.pool.outstanding());
        } finally { loop.close(); }
    }
    @Test void migrationPreservesTheRestOfALargeInputAndAcknowledgesItOnce() throws Exception {
        EventLoop source = new EventLoop("source", true, BufferPool.DEFAULT_IDLE_BYTES), target = new EventLoop("target", true, BufferPool.DEFAULT_IDLE_BYTES);
        AtomicReference<PipeReader.Sink> sink = new AtomicReference<>();
        AtomicInteger count = new AtomicInteger(), releases = new AtomicInteger();
        CompletableFuture<Void> received = new CompletableFuture<>();
        int size = BufferPool.SIZE * 3 + 17;
        PipeReader reader = new PipeReader() {
            @Override public void start(Sink input) { assertTrue(sink.compareAndSet(null, input)); }
            @Override public void close() {}
        };
        try {
            run(source, () -> {
                ExternalTransport carrier = new ExternalTransport(source, PipeIo.of(reader, null));
                carrier.start(new Inbound() {
                    @Override public int bytes(Transport c, io.zmux.internal.core.ReadBuf b, ByteBuffer data, int pos, int lim) {
                        Transport moved = c.moveTo(target);
                        Inbound next = new Inbound() {
                            @Override public int bytes(Transport n, io.zmux.internal.core.ReadBuf rb, ByteBuffer bytes, int from, int to) {
                                for (int i=from; i<to; i++) assertEquals((byte) (count.getAndIncrement() % 251), bytes.get(i));
                                if (count.get() == size) received.complete(null);
                                return to-from;
                            }
                            @Override public void ended(Transport n, boolean graceful, boolean partial) { n.close(null); }
                        };
                        target.execute(() -> moved.start(next));
                        return Inbound.GONE;
                    }
                    @Override public void ended(Transport c, boolean graceful, boolean partial) { c.close(null); }
                });
            });
            ByteBuffer input = ByteBuffer.allocate(size);
            while (input.hasRemaining()) input.put((byte) (input.position() % 251));
            input.flip();
            sink.get().onData(input, releases::incrementAndGet);
            sink.get().onEnd();
            received.get(5, TimeUnit.SECONDS);
            run(source, () -> {}); run(target, () -> {});
            assertEquals(1, releases.get());
            assertEquals(size, count.get());
        } finally { source.close(); target.close(); }
        assertEquals(0, source.pool.outstanding()); assertEquals(0, target.pool.outstanding());
    }
    @Test void loopShutdownClosesPendingFirstRecordGatesAndRejectedCarriers() throws Exception {
        for (boolean knownLane : new boolean[] {false, true}) {
            EventLoop loop = new EventLoop("pending-gate", true, BufferPool.DEFAULT_IDLE_BYTES);
            AtomicReference<PipeReader.Sink> sink = new AtomicReference<>();
            AtomicInteger closed = new AtomicInteger();
            PipeReader reader = new PipeReader() {
                @Override public void start(Sink input) { sink.set(input); }
                @Override public void close() { closed.incrementAndGet(); }
            };
            var hub = new ListenerHub(new ListenerHub.Owner() {
                @Override public io.zmux.internal.core.CoreConfig config() { return io.zmux.internal.core.CoreConfig.defaults(); }
                @Override public DriverHooks hooks() { return DriverHooks.NONE; }
                @Override public void started(SessionRuntime driver, String protocol) { fail("incomplete first record started a session"); }
            }, null, new byte[0], 8, 0, 0);
            try {
                if (knownLane) {
                    var driver = new SessionRuntime(loop, new io.zmux.internal.core.Session(
                            io.zmux.internal.core.CoreConfig.defaults(), Clock.now()), DriverHooks.NONE);
                    ListenerHub.laneInput(driver, PipeIo.of(reader, null));
                } else { hub.reserve(null); hub.external(loop, PipeIo.of(reader, null), io.zmux.JoinHint.inBand(null), false, null, null, null); }
                run(loop, () -> {});
                sink.get().onData(ByteBuffer.wrap(new byte[] {0x40}), () -> {});
                run(loop, () -> assertEquals(1, loop.pool.outstanding()));
                loop.close();
                assertEquals(1, closed.get()); assertEquals(0, loop.pool.outstanding());
                hub.reserve(null); hub.external(loop, PipeIo.of(reader, null), io.zmux.JoinHint.inBand(null), false, null, null, null);
                assertEquals(2, closed.get(), "rejected dispatch must return carrier ownership");
            } finally { hub.close(); loop.close(); }
        }
    }

    @Test void retainedPayloadReferencesOutliveTheReusedTransmitBatch() throws Exception {
        EventLoop loop = new EventLoop("owned-batch-lifetime", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            run(loop, () -> {
                var batch = new io.zmux.internal.core.Batch();
                var payload = loop.pool.take(); payload.data().put(0, (byte) 91);
                BatchFixture.pin(batch, payload); payload.unref();
                var view = batch.segment(0);
                var kept = batch.retain(); batch.discard();
                assertEquals(1, loop.pool.outstanding());
                assertEquals(91, view.get());
                assertTrue(view.isReadOnly());
                kept.close(); kept.close();
                assertEquals(0, loop.pool.outstanding());
            });
        } finally { loop.close(); }
    }

}
