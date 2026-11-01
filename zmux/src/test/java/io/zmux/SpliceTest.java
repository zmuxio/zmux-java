package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.helper.Limiter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class SpliceTest {
    static final class Relay implements AutoCloseable {
        final Pair left, right;
        final ZmuxStream endpoint, a, b;
        Relay(ZmuxConfig config) throws Exception { this(config, null); }
        Relay(ZmuxConfig config, io.zmux.carrier.PipeIo[] target) throws Exception {
            left = Pair.memory(config);
            if (target == null) right = Pair.memory(config);
            else {
                var accepting = CompletableFuture.supplyAsync(() -> {
                    try {
                        var core = config.toCore(io.zmux.internal.core.CoreConfig.Role.RESPONDER, io.zmux.internal.core.CoreConfig.Role.RESPONDER);
                        core.local().present &= ~(io.zmux.internal.wire.TransportParams.ANY_ZMUX | io.zmux.internal.wire.TransportParams.RESET_AT);
                        var peer = Zmux.started(io.zmux.internal.loop.Bootstrap.external(() -> config.eventLoops.group().next(), core, target[1], config.hooks), null);
                        peer.awaitEstablished(Long.MAX_VALUE); return peer;
                    } catch (IOException e) { throw new CompletionException(e); }
                }, r -> Thread.ofVirtual().start(r));
                right = new Pair(Zmux.open(target[0], Role.CLIENT, config), accepting.get(3, TimeUnit.SECONDS));
            }
            endpoint = left.client().openStream(); endpoint.write(new byte[] {7});
            a = left.server().acceptStream(Duration.ofSeconds(3)); b = right.client().openStream();
        }
        ZmuxStream destination() throws Exception { return right.server().acceptStream(Duration.ofSeconds(3)); }
        @Override public void close() throws Exception { endpoint.close(); left.close(); right.close(); }
    }
    static ZmuxConfig config(EventLoops loops) { return ZmuxConfig.builder().eventLoops(loops).build(); }

    @Test void chunksCrossOwnersAndFinOnlyEndsItsOwnDirection() throws Exception {
        try (EventLoops loops = EventLoops.create(4); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            try (var relay = new Relay(config(loops))) {
                byte[] payload = new byte[2 << 20]; new Random(42).nextBytes(payload);
                byte[] response = new byte[1 << 20]; new Random(43).nextBytes(response);
                var splice = Zmux.splice(relay.a, relay.b).toCompletableFuture();
                assertThrows(IllegalStateException.class, () -> relay.a.readChunk(1));
                var destination = tasks.submit(() -> {
                    try (var peer = relay.destination()) {
                        byte[] got = SessionTest.readAll(peer);
                        assertEquals(7, got[0]); assertArrayEquals(payload, Arrays.copyOfRange(got, 1, got.length));
                        peer.write(response); peer.finish();
                    }
                    return null;
                });
                var writing = tasks.submit(() -> { relay.endpoint.write(payload); relay.endpoint.finish(); return null; });
                assertArrayEquals(response, SessionTest.readAll(relay.endpoint));
                writing.get(5, TimeUnit.SECONDS); destination.get(5, TimeUnit.SECONDS);
                var result = splice.get(5, TimeUnit.SECONDS);
                assertEquals(payload.length + 1, result.aToB().streamBytes());
                assertEquals(response.length, result.bToA().streamBytes());
                assertEquals(new SpliceResult.End.Finished(), result.aToB().end()); assertEquals(new SpliceResult.End.Finished(), result.bToA().end());
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void cancellationUnregistersDataAndLimiterWaitsBeforeCompletion() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var ready = new CompletableFuture<Integer>();
            var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Integer> ready(int min, int max) { entered.countDown(); return ready; }
                @Override public int tryReady(int max) { return 0; }
                @Override public void charge(int n) { fail("no bytes were allowed"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            assertTrue(entered.await(3, TimeUnit.SECONDS));
            // Cancelling the stage cancels the splice: no result, both streams reset with code 0.
            assertTrue(splice.cancel(true));
            assertThrows(CancellationException.class, () -> splice.get(3, TimeUnit.SECONDS));
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (!ready.isCancelled() && System.nanoTime() < until) Thread.sleep(2);
            assertTrue(ready.isCancelled());
            var reset = assertThrows(StreamResetException.class, () -> SessionTest.readAll(relay.endpoint));
            assertEquals(0, reset.code());
        }
    }

    @Test void upstreamResetCannotHideBehindZeroDownstreamCapacity() throws Exception {
        try (EventLoops loops = EventLoops.create(3)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).flowControl(FlowControl.defaults().withInitialStreamWindow(4096).withMaxStreamWindow(4096)).build();
            try (var relay = new Relay(cfg); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
                var splice = Zmux.splice(relay.a, relay.b).toCompletableFuture();
                try (var peer = relay.destination()) {
                    var sender = tasks.submit(() -> { try { relay.endpoint.write(new byte[32768]); } catch (IOException expected) {} return null; });
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                    while (relay.b.sendCapacity() != 0 && System.nanoTime() < until) Thread.sleep(5);
                    assertEquals(0, relay.b.sendCapacity());
                    relay.endpoint.reset(73);
                    peer.setReadTimeout(Duration.ofSeconds(3));
                    StreamResetException error = assertThrows(StreamResetException.class, () -> SessionTest.readAll(peer));
                    assertEquals(73, error.code()); peer.finish();
                    var result = splice.get(3, TimeUnit.SECONDS);
                    assertEquals(new SpliceResult.End.Reset(73), result.aToB().end());
                    sender.get(3, TimeUnit.SECONDS);
                }
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void reliableResetPreservesTheEntireTailAndCode() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var relay = new Relay(config(loops));
             var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var splice = Zmux.splice(relay.a, relay.b).toCompletableFuture();
            byte[] payload = new byte[100000]; new Random(21).nextBytes(payload);
            var sending = tasks.submit(() -> {
                relay.endpoint.write(payload); relay.endpoint.markReliableBoundary(); relay.endpoint.reset(95); return null;
            });
            try (var peer = relay.destination()) {
                var bytes = new java.io.ByteArrayOutputStream();
                StreamResetException reset = assertThrows(StreamResetException.class, () -> {
                    for (Chunk chunk; (chunk = peer.readChunk(65536)) != null;) {
                        try (Chunk owned = chunk) {
                            ByteBuffer data = owned.buffer(); byte[] part = new byte[data.remaining()]; data.get(part); bytes.write(part);
                        }
                    }
                });
                assertEquals(95, reset.code()); byte[] got = bytes.toByteArray();
                assertEquals(7, got[0]); assertArrayEquals(payload, Arrays.copyOfRange(got, 1, got.length));
                peer.finish(); sending.get(3, TimeUnit.SECONDS);
                var result = splice.get(3, TimeUnit.SECONDS);
                assertEquals(payload.length + 1, result.aToB().streamBytes());
                assertEquals(new SpliceResult.End.Reset(95), result.aToB().end());
            }
        }
    }

    @Test void stopInterruptsALimiterAndReachesTheOriginalSender() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var blocked = new CompletableFuture<Integer>(); var entered = new CountDownLatch(1);
            var calls = new AtomicInteger();
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Integer> ready(int min, int max) {
                    if (calls.getAndIncrement() == 0) return CompletableFuture.completedFuture(1);
                    entered.countDown(); return blocked;
                }
                @Override public int tryReady(int max) { return max; }
                @Override public void charge(int n) {}
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read());
                relay.endpoint.write(new byte[] {42}); assertTrue(entered.await(3, TimeUnit.SECONDS));
                peer.stop(82); peer.finish();
                relay.endpoint.setWriteTimeout(Duration.ofSeconds(3));
                var result = splice.get(3, TimeUnit.SECONDS);
                StreamStoppedException stop = null;
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                while (stop == null && System.nanoTime() < until) {
                    try { relay.endpoint.tryWrite(ByteBuffer.wrap(new byte[] {1})); Thread.sleep(2); }
                    catch (StreamStoppedException error) { stop = error; }
                }
                assertNotNull(stop); assertEquals(82, stop.code());
                assertEquals(new SpliceResult.End.Stopped(82), result.aToB().end());
                assertTrue(blocked.isCancelled());
            }
        }
    }

    @Test void sessionFailureInterruptsLimiterWithoutTakingBufferedData() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var blocked = new CompletableFuture<Integer>(); var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Integer> ready(int min, int max) { entered.countDown(); return blocked; }
                @Override public int tryReady(int max) { return 0; }
                @Override public void charge(int n) { fail("limiter denied forwarding"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            assertTrue(entered.await(3, TimeUnit.SECONDS)); relay.left.client().close(new ApplicationError(32, "gone"));
            var result = splice.get(3, TimeUnit.SECONDS);
            assertEquals(new SpliceResult.End.UpstreamLost(), result.aToB().end());
            assertEquals(0, result.aToB().streamBytes()); assertTrue(blocked.isCancelled());
        }
    }

    @Test void datagramsKeepContextsAndUseOnlyTheSessionsQueues() throws Exception {
        try (EventLoops loops = EventLoops.create(3);
             var relay = new Relay(ZmuxConfig.builder().eventLoops(loops).datagrams(true).build())) {
            var splice = Zmux.splice(relay.a, relay.b).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read());
                relay.endpoint.sendDatagram(42, ByteBuffer.wrap(new byte[] {1, 2, 3}));
                StreamDatagram request = peer.receiveDatagram(); assertEquals(42, request.context());
                assertArrayEquals(new byte[] {1, 2, 3}, request.payload());
                peer.sendDatagram(99, ByteBuffer.wrap(new byte[] {4, 5}));
                StreamDatagram response = relay.endpoint.receiveDatagram(); assertEquals(99, response.context());
                assertArrayEquals(new byte[] {4, 5}, response.payload());
                relay.endpoint.finish();
                assertNull(peer.readChunk(1)); // Reliable FIN does not end the datagram association.
                relay.endpoint.sendDatagram(43, ByteBuffer.wrap(new byte[] {6}));
                StreamDatagram tail = peer.receiveDatagram(); assertEquals(43, tail.context());
                assertArrayEquals(new byte[] {6}, tail.payload()); peer.finish();
                var result = splice.get(3, TimeUnit.SECONDS);
                assertEquals(4, result.aToB().datagramBytes()); assertEquals(2, result.bToA().datagramBytes());
                assertEquals(0, result.aToB().datagramsDropped());
            }
        }
    }

    @Test void resetBeforeTheFirstForwardedByteCompletesBothDirectionsWithoutAnOpener() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var blocked = new CompletableFuture<Integer>(); var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Integer> ready(int min, int max) { entered.countDown(); return blocked; }
                @Override public int tryReady(int max) { return 0; }
                @Override public void charge(int n) { fail("no byte allowed"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            assertTrue(entered.await(3, TimeUnit.SECONDS)); relay.endpoint.reset(77);
            var result = splice.get(3, TimeUnit.SECONDS);
            assertEquals(new SpliceResult.End.Reset(77), result.aToB().end());
            assertEquals(new SpliceResult.End.Finished(), result.bToA().end());
            assertFalse(relay.b.st.idSet()); assertEquals(0, result.aToB().streamBytes()); assertTrue(blocked.isCancelled());
        }
    }

    @Test void losingTheUnopenedTargetSessionWakesTheLimiterAndTheReverseReader() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var blocked = new CompletableFuture<Integer>(); var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Integer> ready(int min, int max) { entered.countDown(); return blocked; }
                @Override public int tryReady(int max) { return 0; }
                @Override public void charge(int n) { fail("no byte allowed"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            assertTrue(entered.await(3, TimeUnit.SECONDS)); relay.right.client().close(new ApplicationError(31, "gone"));
            var result = splice.get(3, TimeUnit.SECONDS);
            assertEquals(new SpliceResult.End.DownstreamLost(), result.aToB().end()); assertTrue(blocked.isCancelled());
        }
    }

    @Test void datagramsDeniedByTheDirectionLimiterAreCountedWithoutAHelperQueue() throws Exception {
        try (EventLoops loops = EventLoops.create(2);
             var relay = new Relay(ZmuxConfig.builder().eventLoops(loops).datagrams(true).build())) {
            var dropped = new CountDownLatch(1); var charged = new AtomicInteger();
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Integer> ready(int min, int max) { return CompletableFuture.completedFuture(max); }
                @Override public int tryReady(int max) { dropped.countDown(); return 0; }
                @Override public void charge(int n) { charged.addAndGet(n); }
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read()); relay.endpoint.sendDatagram(4, ByteBuffer.wrap(new byte[] {1, 2}));
                assertTrue(dropped.await(3, TimeUnit.SECONDS)); relay.endpoint.finish(); peer.finish();
                var result = splice.get(3, TimeUnit.SECONDS);
                assertEquals(1, result.aToB().datagramsDropped()); assertEquals(0, result.aToB().datagramBytes());
                assertEquals(result.aToB().streamBytes(), charged.get());
            }
        }
    }

    @Test void ownerLoopShutdownDoesNotLeaveObserversParked() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var blocked = new CompletableFuture<Integer>(); var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Integer> ready(int min, int max) { entered.countDown(); return blocked; }
                @Override public int tryReady(int max) { return 0; }
                @Override public void charge(int n) { fail("no byte allowed"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            assertTrue(entered.await(3, TimeUnit.SECONDS)); loops.close();
            var result = splice.get(3, TimeUnit.SECONDS);
            assertTrue(result.aToB().end() instanceof SpliceResult.End.UpstreamLost || result.aToB().end() instanceof SpliceResult.End.DownstreamLost,
                    String.valueOf(result.aToB().end())); assertTrue(blocked.isCancelled());
        }
    }

    static final class GateWriter implements io.zmux.carrier.PipeWriter {
        final io.zmux.carrier.PipeWriter delegate;
        Pending pending;
        volatile boolean paused;
        record Pending(io.zmux.carrier.OutboundBatch buffers, Completion done) {}
        GateWriter(io.zmux.carrier.PipeWriter delegate) { this.delegate = delegate; }
        @Override public void write(io.zmux.carrier.OutboundBatch buffers, Completion done) {
            boolean held, overlap;
            synchronized (this) {
                held = paused; overlap = held && pending != null;
                if (held && !overlap) pending = new Pending(buffers, done);
            }
            if (overlap) done.failed(new IOException("overlapping write"));
            else if (!held) delegate.write(buffers, done);
        }
        void release() {
            Pending held;
            synchronized (this) { paused = false; held = pending; pending = null; }
            if (held != null) delegate.write(held.buffers(), held.done());
        }
        @Override public void close() { release(); delegate.close(); }
        @Override public void abort(Throwable error) {
            Pending held;
            synchronized (this) { held = pending; pending = null; }
            if (held != null) held.done().failed(error);
            delegate.abort(error);
        }
    }

    @Test void fallbackDrainPreservesResetStopCancellationAndSessionLossSemantics() throws Exception {
        for (String mode : new String[] {"drain", "cancel", "session", "stop"}) {
            try (EventLoops loops = EventLoops.create(3)) {
                var pipes = io.zmux.carrier.MemoryPipe.pair(io.zmux.carrier.Framing.BYTE_STREAM);
                var gate = new GateWriter(pipes[0].writer());
                pipes[0] = new io.zmux.carrier.PipeIo(pipes[0].reader(), gate, pipes[0].framing(), null, null);
                try (var relay = new Relay(config(loops), pipes)) {
                    assertFalse(relay.right.client().info().peerIsZmux());
                    assertFalse((Boolean)relay.b.session.run(op -> relay.b.session.core.sending().resetAtInUse()));
                    var charged = new AtomicInteger();
                    Limiter limiter = new Limiter() {
                        @Override public CompletionStage<Integer> ready(int min, int max) { return CompletableFuture.completedFuture(max); }
                        @Override public int tryReady(int max) { return max; }
                        @Override public void charge(int n) { charged.addAndGet(n); }
                    };
                    var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(16384, limiter, null)).toCompletableFuture();
                    try (var peer = relay.destination()) {
                        assertEquals(7, peer.inputStream().read()); gate.paused = true;
                        relay.endpoint.write(new byte[100000]); relay.endpoint.markReliableBoundary(); relay.endpoint.reset(95);
                        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                        while (charged.get() < 100001 && System.nanoTime() < until) Thread.sleep(2);
                        assertEquals(100001, charged.get(), mode);
                        // Undelivered: still unsent, or in the batch the paused carrier holds (a batch can take all of it).
                        boolean unsent = (Boolean)relay.b.session.run(op -> relay.b.session.core.sending().holdsUnsent(relay.b.st));
                        synchronized (gate) { assertTrue(unsent || gate.pending != null, mode); }
                        assertNull(relay.b.session.run(op -> relay.b.session.core.sending().writeError(relay.b.st)), "fallback must not reset before draining");
                        if (mode.equals("drain")) {
                            gate.release(); var received = new java.io.ByteArrayOutputStream();
                            var error = assertThrows(StreamResetException.class, () -> {
                                for (Chunk chunk; (chunk = peer.readChunk(65536)) != null;) {
                                    try (Chunk owned = chunk) { received.write(new byte[owned.buffer().remaining()]); }
                                }
                            });
                            assertEquals(95, error.code());
                            // Plain RESET_STREAM may discard already handed-off bytes at its peer.
                            assertTrue(received.size() <= 100000); peer.finish();
                            var result = splice.get(3, TimeUnit.SECONDS);
                            assertEquals(new SpliceResult.End.Reset(95), result.aToB().end());
                        } else {
                            if (mode.equals("cancel")) {
                                assertTrue(splice.cancel(true), mode);
                                assertThrows(CancellationException.class, () -> splice.get(3, TimeUnit.SECONDS), mode);
                            } else {
                                if (mode.equals("session")) relay.left.client().close(new ApplicationError(32, "gone"));
                                if (mode.equals("stop")) { peer.stop(82); peer.finish(); }
                                var result = splice.get(3, TimeUnit.SECONDS);
                                assertEquals(mode.equals("stop") ? new SpliceResult.End.Stopped(82) : new SpliceResult.End.UpstreamLost(),
                                        result.aToB().end(), mode);
                            }
                            gate.release();
                        }
                    } finally { gate.release(); }
                }
                assertEquals(0, loops.outstandingBuffers(), mode);
            }
        }
    }

    @Test void invalidLimiterAllowanceAbortsAndReportsItsFailure() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Integer> ready(int min, int max) { return CompletableFuture.completedFuture(max + 1); }
                @Override public int tryReady(int max) { return 0; }
                @Override public void charge(int n) { fail("invalid allowance must not forward"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            var failed = assertThrows(ExecutionException.class, () -> splice.get(3, TimeUnit.SECONDS));
            assertTrue(failed.getCause() instanceof IOException, String.valueOf(failed.getCause()));
            assertEquals("invalid limiter allowance", failed.getCause().getMessage());
        }
    }

    @Test void failedSecondTransferReturnsTheFirstStreamsOwnership() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var relay = new Relay(config(loops))) {
            var pipe = relay.b.asPipe();
            assertThrows(IllegalStateException.class, () -> Zmux.splice(relay.a, relay.b));
            try (var chunk = relay.a.readChunk(1)) { assertNotNull(chunk); }
            pipe.reader().close(); pipe.writer().close();
        }
    }

    @Test void completedRelaysDoNotAccumulateLifecycleRegistrationsOnLiveSessions() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var tasks = Executors.newVirtualThreadPerTaskExecutor();
             var left = Pair.memory(config(loops)); var right = Pair.memory(config(loops))) {
            for (int i = 0; i < 20; i++) {
                try (var endpoint = left.client().openStream()) {
                    endpoint.write(new byte[] {7});
                    var a = left.server().acceptStream(); var b = right.client().openStream();
                    var splice = Zmux.splice(a, b).toCompletableFuture();
                    var destination = tasks.submit(() -> {
                        try (var peer = right.server().acceptStream()) {
                            assertArrayEquals(new byte[] {7}, SessionTest.readAll(peer)); peer.finish();
                        }
                        return null;
                    });
                    endpoint.finish(); assertEquals(0, SessionTest.readAll(endpoint).length);
                    splice.get(3, TimeUnit.SECONDS); destination.get(3, TimeUnit.SECONDS);
                    assertEquals(0, (Integer)left.server().run(op -> io.zmux.internal.core.ObserverFixture.closedWaiters(left.server().core)));
                    assertEquals(0, (Integer)right.client().run(op -> io.zmux.internal.core.ObserverFixture.closedWaiters(right.client().core)));
                }
            }
        }
    }

    @Test void rejectedSelfSpliceDoesNotTransferOwnership() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var relay = new Relay(config(loops))) {
            assertThrows(IllegalArgumentException.class, () -> Zmux.splice(relay.a, relay.a));
            try (var chunk = relay.a.readChunk(1)) { assertNotNull(chunk); }
        }
    }
}
