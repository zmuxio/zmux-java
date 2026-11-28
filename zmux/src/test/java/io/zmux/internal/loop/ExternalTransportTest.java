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
        var released = new CompletableFuture<Void>();
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
                carrier.writesReleased().thenRun(() -> released.complete(null));
                carrier.write(batch); carrier.reject(null);
                assertEquals(1, loop.pool.outstanding(), "writer still owns the referenced output");
            });
            loop.close();
            completion.get().completed();
            released.get(5, TimeUnit.SECONDS);
            assertEquals(0, loop.pool.outstanding());
            completion.get().failed(new IllegalStateException("duplicate completion"));
            assertEquals(0, loop.pool.outstanding());
        } finally { loop.close(); }
    }
    @Test void orderedPrefixesBelongToOneWriteUntilTerminalReceipt() throws Exception {
        EventLoop loop = new EventLoop("ordered-prefix", true, BufferPool.DEFAULT_IDLE_BYTES);
        var progress = new java.util.ArrayList<Integer>();
        var callback = new AtomicReference<PipeWriter.Completion>();
        var terminal = new CompletableFuture<Integer>();
        PipeWriter writer = new PipeWriter() {
            @Override public void write(OutboundBatch data, Completion done) { callback.set(done); }
            @Override public void close() {}
        };
        try {
            run(loop, () -> {
                var carrier = new ExternalTransport(loop, PipeIo.of(null, writer));
                carrier.start(new Inbound() {
                    @Override public int bytes(Transport t, io.zmux.internal.buffer.ReadBuf b, ByteBuffer data, int pos, int lim) {
                        return lim - pos;
                    }
                    @Override public void ended(Transport t, boolean graceful, boolean partial) {}
                    @Override public void progress(Transport t, io.zmux.internal.core.Batch b, int n) { progress.add(n); }
                    @Override public void written(Transport t, io.zmux.internal.core.Batch b, int n, Throwable cause) {
                        assertTrue(b.progressUnknown());
                        b.discard();
                        terminal.complete(n);
                    }
                });
                var batch = new io.zmux.internal.core.Batch();
                batch.wrap(OutboundBatch.of(ByteBuffer.allocate(100)));
                carrier.write(batch);
            });
            callback.get().progress(21);
            callback.get().progress(21);
            run(loop, () -> assertEquals(java.util.List.of(21), progress));
            callback.get().failedUnknown(new java.io.IOException("uncertain suffix"), 0);
            assertEquals(21, terminal.get(5, TimeUnit.SECONDS));
            callback.get().progress(100);
            run(loop, () -> assertEquals(java.util.List.of(21), progress));
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
                ExternalTransport carrier = new ExternalTransport(source, PipeIo.of(reader, null).withReceiveMemory(size));
                carrier.start(new Inbound() {
                    @Override public int bytes(Transport c, io.zmux.internal.buffer.ReadBuf b, ByteBuffer data, int pos, int lim) {
                        Transport moved = CarrierSetup.moveUnowned(c, target);
                        Inbound next = new Inbound() {
                            @Override public int bytes(Transport n, io.zmux.internal.buffer.ReadBuf rb, ByteBuffer bytes, int from, int to) {
                                for (int i=from; i<to; i++) assertEquals((byte) (count.getAndIncrement() % 251), bytes.get(i));
                                if (count.get() == size) received.complete(null);
                                return to-from;
                            }
                            @Override public void ended(Transport n, boolean graceful, boolean partial) { n.reject(null); }
                        };
                        target.execute(() -> moved.start(next));
                        // The reader's end arrives now, while the carrier is between its loops and the rest of
                        // the input is still held: a fast reader (EOF right after its last read) on a busy host
                        // does this. The end may neither fail the carrier nor overtake the rest of the input.
                        sink.get().onEnd();
                        return Inbound.GONE;
                    }
                    @Override public void ended(Transport c, boolean graceful, boolean partial) { c.reject(null); }
                });
            });
            ByteBuffer input = ByteBuffer.allocate(size);
            while (input.hasRemaining()) input.put((byte) (input.position() % 251));
            input.flip();
            sink.get().onLent(input, releases::incrementAndGet);
            received.get(5, TimeUnit.SECONDS);
            run(source, () -> {}); run(target, () -> {});
            assertEquals(1, releases.get());
            assertEquals(size, count.get());
        } finally { source.close(); target.close(); }
        assertEquals(0, source.pool.outstanding()); assertEquals(0, target.pool.outstanding());
    }

    @Test void boundsTheFirstOwnedBatchByItsAllocationRatherThanItsView() throws Exception {
        EventLoop loop = new EventLoop("input-root-budget", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            var sink = new AtomicReference<PipeReader.Sink>();
            var rejected = new CompletableFuture<Void>();
            var released = new AtomicInteger();
            var consumed = new AtomicInteger();
            PipeReader reader = new PipeReader() {
                @Override public void start(Sink input) { sink.set(input); }
                @Override public void close() { rejected.complete(null); }
            };
            run(loop, () -> new ExternalTransport(loop, PipeIo.of(reader, null)).start(new Inbound() {
                @Override public int bytes(Transport c, io.zmux.internal.buffer.ReadBuf b, ByteBuffer data, int pos, int lim) {
                    throw new AssertionError("oversized backing reached the parser");
                }
                @Override public void ended(Transport c, boolean graceful, boolean partial) { c.reject(null); }
            }));
            var root = ByteBuffer.allocate(BufferPool.SIZE + 1).limit(1);
            sink.get().onChunk(io.zmux.Chunk.of(root, released::incrementAndGet), consumed::incrementAndGet);
            rejected.get(10, TimeUnit.SECONDS);
            assertEquals(1, released.get());
            assertEquals(1, consumed.get());
        } finally { loop.close(); }
    }

    @Test void sharedRootIsChargedOnceWhileBothViewsAreQueued() throws Exception {
        EventLoop loop = new EventLoop("shared-input-root", true, BufferPool.DEFAULT_IDLE_BYTES);
        var consumed = new CompletableFuture<Void>();
        var released = new AtomicInteger();
        var retained = new AtomicInteger();
        var deliveries = new AtomicInteger();
        try {
            run(loop, () -> {
                PipeReader reader = new PipeReader() {
                    @Override public void start(Sink sink) {
                        var data = ByteBuffer.allocate(BufferPool.SIZE);
                        var provider = new io.zmux.internal.spi.ProviderBackings();
                        sink.onChunk(provider.chunk(data, data.capacity(), data.slice(0, 1),
                                retained::incrementAndGet, released::incrementAndGet), () -> {});
                        sink.onChunk(provider.chunk(data, data.capacity(), data.slice(1, 1),
                                retained::incrementAndGet, released::incrementAndGet), () -> consumed.complete(null));
                    }
                    @Override public void close() {}
                };
                var carrier = new ExternalTransport(loop, PipeIo.of(reader, null).withReceiveMemory(InputCharge.ENTRY_BYTES));
                carrier.start(new Inbound() {
                    @Override public boolean session() { return true; }
                    @Override public int bytes(Transport c, io.zmux.internal.buffer.ReadBuf b, ByteBuffer data, int pos, int lim) {
                        deliveries.incrementAndGet();
                        return lim - pos;
                    }
                    @Override public void ended(Transport c, boolean graceful, boolean partial) { c.reject(null); }
                });
                loop.execute(() -> carrier.reject(null));
            });
            consumed.get(10, TimeUnit.SECONDS);
            assertEquals(2, deliveries.get());
            assertEquals(1, retained.get());
            assertEquals(1, released.get());
        } finally { loop.close(); }
    }

    @Test void sparseCarryDropsItsOversizedBackingsAfterOneCompaction() {
        var pool = new BufferPool(0);
        var reader = new ChannelReader(pool);
        var released = new AtomicInteger();
        try {
            for (int i = 0; i < 3; i++) {
                var data = ByteBuffer.allocate(BufferPool.SIZE).put(0, (byte) i);
                var root = new io.zmux.internal.buffer.ReadBuf(data, ignored -> released.incrementAndGet());
                try { reader.hold(root, data, 0, 1); } finally { root.unref(); }
                assertEquals(i < 2 ? 0 : 3, reader.compactSparseCarry());
            }
            assertTrue(reader.compacted());
            assertEquals(3, released.get());
            assertArrayEquals(new byte[] {0, 1, 2}, reader.copyUnread());
            assertEquals(0, reader.compactSparseCarry());
        } finally { reader.release(); }
        assertEquals(0, pool.outstanding());
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
                @Override public void started(SessionRuntime driver, String protocol, io.zmux.CarrierCredential credential, CarrierSetup setup) { fail("incomplete first record started a session"); }
            }, null, new byte[0], io.zmux.ListenerLimits.builder().maxSuspended(0).build(),
                    io.zmux.internal.core.CarrierAdmission.DEFAULT_PREFIX_V4, io.zmux.internal.core.CarrierAdmission.DEFAULT_PREFIX_V6);
            try {
                if (knownLane) ListenerHub.laneInput(loop, PipeIo.of(reader, null));
                else { hub.external(loop, PipeIo.of(reader, null), io.zmux.JoinHint.inBand(null), false, null, ListenerHub.Entry.Stream.CARRIER, null, PairingShutdownTest.reserved(hub)); }
                run(loop, () -> {});
                var consumed = new CompletableFuture<Void>();
                sink.get().onLent(ByteBuffer.wrap(new byte[] {0x40}), () -> consumed.complete(null));
                consumed.get(30, TimeUnit.SECONDS);
                run(loop, () -> assertEquals(1, loop.pool.outstanding()));
                loop.close();
                // A half's close runs off the loop.
                awaitCount(closed, 1); assertEquals(0, loop.pool.outstanding());
                hub.external(loop, PipeIo.of(reader, null), io.zmux.JoinHint.inBand(null), false, null, ListenerHub.Entry.Stream.CARRIER, null, PairingShutdownTest.reserved(hub));
                awaitCount(closed, 2);
            } finally { hub.close(); loop.close(); }
        }
    }

    /** The process-wide lane entry gives a lane carrier the default first-record timeout (2 s) for its header, not the join timeout. */
    @Test void aLaneCarrierWithoutAHeaderIsClosedAtTheFirstRecordTimeout() throws Exception {
        EventLoop loop = new EventLoop("lane-header", true, BufferPool.DEFAULT_IDLE_BYTES);
        AtomicInteger closed = new AtomicInteger();
        PipeReader reader = new PipeReader() {
            @Override public void start(Sink input) {}
            @Override public void close() { closed.incrementAndGet(); }
        };
        try {
            long start = System.nanoTime();
            ListenerHub.laneInput(loop, PipeIo.of(reader, null));
            awaitCount(closed, 1);
            long took = System.nanoTime() - start;
            assertTrue(took >= io.zmux.ListenerConfig.DEFAULT_FIRST_RECORD_TIMEOUT.toNanos() * 9 / 10, "not before the timeout: " + took);
            assertTrue(took < io.zmux.ZmuxConfig.DEFAULT_JOIN_TIMEOUT.toNanos() / 2, "well before the join timeout: " + took);
        } finally { loop.close(); }
    }

    private static void awaitCount(java.util.concurrent.atomic.AtomicInteger n, int want) throws Exception {
        for (long end = System.nanoTime() + 5_000_000_000L; n.get() < want && System.nanoTime() < end; ) Thread.sleep(1);
        assertEquals(want, n.get());
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
