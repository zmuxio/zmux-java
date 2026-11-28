package io.zmux.internal.loop;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import io.zmux.internal.core.*;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class PairingShutdownTest {
    @Test void downlinkOwnerShutdownRacesTransferWithoutDoubleClosingOrLosingHalves() throws Exception {
        Session peer = new Session(CoreConfig.defaults(), Clock.now());
        CarrierInfo info = new CarrierInfo(io.zmux.internal.core.Framing.BYTES, false, false, false, false);
        peer.pipes().attachBootstrap(info, info); Batch batch = new Batch();
        assertTrue(peer.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, Clock.now()));
        byte[] preface = io.zmux.internal.core.BatchFixture.flat(batch); batch.discard();
        for (int iteration=0; iteration<24; iteration++) {
            EventLoop up = new EventLoop("pair-up", true, BufferPool.DEFAULT_IDLE_BYTES), down = new EventLoop("pair-down", true, BufferPool.DEFAULT_IDLE_BYTES);
            AtomicInteger readsClosed = new AtomicInteger(), writesClosed = new AtomicInteger();
            var hub = hub();
            try {
                PipeReader reader = new PipeReader() {
                    @Override public void start(Sink sink) { sink.onLent(ByteBuffer.wrap(preface), () -> {}); }
                    @Override public void close() { readsClosed.incrementAndGet(); }
                };
                PipeWriter writer = new PipeWriter() {
                    @Override public void write(io.zmux.carrier.OutboundBatch bytes, Completion done) { done.completed(); }
                    @Override public void close() { writesClosed.incrementAndGet(); }
                };
                String token = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[16]);
                var a = hub.external(up, PipeIo.of(reader, null), io.zmux.JoinHint.pair(token, true, null), false, null, ListenerHub.Entry.Stream.CARRIER, null, reserved(hub));
                var b = hub.external(down, PipeIo.of(null, writer), io.zmux.JoinHint.pair(token, false, null), false, null, ListenerHub.Entry.Stream.CARRIER, null, reserved(hub));
                down.close(); hub.close(); up.close();
                a.whenClosed().toCompletableFuture().get(5, TimeUnit.SECONDS); b.whenClosed().toCompletableFuture().get(5, TimeUnit.SECONDS);
                assertEquals(1, readsClosed.get()); assertEquals(1, writesClosed.get());
                assertEquals(0, up.pool.outstanding()); assertEquals(0, down.pool.outstanding());
            } finally { hub.close(); up.close(); down.close(); }
        }
    }

    @Test void physicalLossOfAMatchedHalfEndsBothRequestsBeforeTheFirstRecord() throws Exception {
        var hub = hub();
        var up = new EventLoop("pair-input", true, BufferPool.DEFAULT_IDLE_BYTES);
        var down = new EventLoop("pair-output", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            var halves = MemoryPipe.pair(io.zmux.carrier.Framing.BYTE_STREAM);
            String key = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[16]);
            var first = reserved(hub);
            var second = reserved(hub);
            hub.external(up, PipeIo.of(halves[1].reader(), null), io.zmux.JoinHint.pair(key, true, null),
                    false, null, ListenerHub.Entry.Stream.CARRIER, null, first);
            ExternalTransportTest.run(up, () -> {});
            Transport output = hub.external(down, PipeIo.of(null, halves[1].writer()), io.zmux.JoinHint.pair(key, false, null),
                    false, null, ListenerHub.Entry.Stream.CARRIER, null, second);
            ExternalTransportTest.run(down, () -> CarrierSetup.connectionLost(output, new java.io.IOException("stream reset")));
            first.closed().get(5, TimeUnit.SECONDS);
            second.closed().get(5, TimeUnit.SECONDS);
            assertThrows(java.io.IOException.class, first::await);
            assertThrows(java.io.IOException.class, second::await);
        } finally { hub.close(); down.close(); up.close(); }
    }

    @Test void withdrawalBeforeMatchWinsWhileTheFirstOwnerIsPaused() throws Exception {
        // HS-41.
        var unblock = new java.util.concurrent.CompletableFuture<Void>();
        var up = new EventLoop("pair-match", true, BufferPool.DEFAULT_IDLE_BYTES);
        var down = new EventLoop("pair-withdraw", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            var hub = hub();
            try {
                String key = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[16]);
                var halves = MemoryPipe.pair(io.zmux.carrier.Framing.BYTE_STREAM);
                var first = reserved(hub);
                hub.external(down, PipeIo.of(null, halves[1].writer()), io.zmux.JoinHint.pair(key, false, null),
                        false, null, ListenerHub.Entry.Stream.CARRIER, null, first);
                var paused = new java.util.concurrent.CountDownLatch(1);
                down.execute(() -> { paused.countDown(); unblock.join(); });
                assertTrue(paused.await(5, TimeUnit.SECONDS));
                Thread.currentThread().interrupt();
                try { assertThrows(java.io.InterruptedIOException.class, first::await); }
                finally { Thread.interrupted(); }
                var later = reserved(hub);
                hub.external(up, PipeIo.of(halves[1].reader(), null), io.zmux.JoinHint.pair(key, true, null),
                        false, null, ListenerHub.Entry.Stream.CARRIER, null, later);
                later.closed().get(5, TimeUnit.SECONDS);
                assertThrows(java.io.IOException.class, later::await);
                assertFalse(first.closed().isDone());
                unblock.complete(null);
                first.closed().get(5, TimeUnit.SECONDS);
            } finally { unblock.complete(null); hub.close(); }
        } finally { down.close(); up.close(); }
    }

    private static ListenerHub hub() {
        return new ListenerHub(new ListenerHub.Owner() {
                @Override public CoreConfig config() { return CoreConfig.builder().role(CoreConfig.Role.RESPONDER).transportRole(CoreConfig.Role.RESPONDER).build(); }
                @Override public DriverHooks hooks() { return DriverHooks.NONE; }
                @Override public void started(SessionRuntime driver, String protocol, io.zmux.CarrierCredential credential, CarrierSetup setup) {}
            }, null, new byte[0], io.zmux.ListenerLimits.builder().maxSuspended(0).build(),
                    io.zmux.internal.core.CarrierAdmission.DEFAULT_PREFIX_V4, io.zmux.internal.core.CarrierAdmission.DEFAULT_PREFIX_V6);
    }

    static CarrierSetup reserved(ListenerHub hub) {
        var setup = hub.reserve(null);
        assertNotNull(setup);
        assertTrue(setup.arrive());
        return setup;
    }

    @Test void uplinkExpiryAfterDownlinkMoveUsesTheNewOwner() throws Exception {
        // HS-45.
        var hub = hub();
        var allowMove = new java.util.concurrent.CompletableFuture<Void>();
        var expire = new java.util.concurrent.CompletableFuture<Void>();
        var up = new EventLoop("pair-expiry", true, BufferPool.DEFAULT_IDLE_BYTES);
        var down = new EventLoop("old-owner", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            var key = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[16]);
            var output = reserved(hub);
            var closed = new AtomicInteger();
            var writer = new PipeWriter() {
                @Override public void write(io.zmux.carrier.OutboundBatch b, Completion c) { fail("expired before setup"); }
                @Override public void close() { closed.incrementAndGet(); }
            };
            hub.external(down, PipeIo.of(null, writer), io.zmux.JoinHint.pair(key, false, null),
                    false, null, ListenerHub.Entry.Stream.CARRIER, null, output);
            ExternalTransportTest.run(down, () -> {});
            down.execute(allowMove::join);
            var input = reserved(hub);
            var sink = new java.util.concurrent.CompletableFuture<PipeReader.Sink>();
            var reader = new PipeReader() {
                @Override public void start(Sink s) { sink.complete(s); }
                @Override public void close() {}
            };
            hub.external(up, PipeIo.of(reader, null), io.zmux.JoinHint.pair(key, true, null),
                    false, null, ListenerHub.Entry.Stream.CARRIER, null, input);
            var peer = new Session(CoreConfig.defaults(), Clock.now());
            var info = new CarrierInfo(io.zmux.internal.core.Framing.BYTES, false, false, false, false);
            peer.pipes().attachBootstrap(info, info);
            var batch = new Batch();
            assertTrue(peer.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, Clock.now()));
            byte[] bytes = BatchFixture.flat(batch);
            batch.discard();
            sink.get(5, TimeUnit.SECONDS).onLent(ByteBuffer.wrap(bytes), () -> {});
            // The inbox runs before this barrier, and enqueues combine before the next task.
            ExternalTransportTest.run(up, () -> {});
            var paused = new java.util.concurrent.CountDownLatch(1);
            up.execute(() -> { paused.countDown(); expire.join(); up.runTimers(Long.MAX_VALUE - 1); });
            assertTrue(paused.await(5, TimeUnit.SECONDS));
            allowMove.complete(null);
            ExternalTransportTest.run(down, () -> {});
            assertSame(up, output.ownerLoop());
            down.close();
            expire.complete(null);
            input.closed().get(5, TimeUnit.SECONDS);
            output.closed().get(5, TimeUnit.SECONDS);
            assertEquals(1, closed.get());
            assertEquals(1, hub.counts().timedOutCount());
            var retry = reserved(hub);
            hub.external(up, PipeIo.of(null, writer), io.zmux.JoinHint.pair(key, false, null),
                    false, null, ListenerHub.Entry.Stream.CARRIER, null, retry);
            ExternalTransportTest.run(up, () -> {});
            assertFalse(retry.closed().isDone());
            retry.stop();
            retry.closed().get(5, TimeUnit.SECONDS);
        } finally { allowMove.complete(null); expire.complete(null); hub.close(); up.close(); down.close(); }
    }

}
