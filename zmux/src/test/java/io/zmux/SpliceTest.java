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
        Relay(ZmuxConfig config) throws Exception { this(config, (io.zmux.carrier.PipeIo[]) null); }
        /** The two sessions on separate loop groups. */
        Relay(ZmuxConfig leftConfig, ZmuxConfig rightConfig) throws Exception {
            left = Pair.memory(leftConfig); right = Pair.memory(rightConfig);
            endpoint = left.client().openStream(); endpoint.write(new byte[] {7});
            a = left.server().acceptStream(Duration.ofSeconds(3)); b = right.client().openStream();
        }
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
        @Override public void close() throws IOException { endpoint.close(); left.close(); right.close(); }
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

    /** Both peers see the code a control cancels with, both ways; the result reports it. */
    @Test void aControlEndsBothStreamsWithItsCode() throws Exception {
        try (EventLoops loops = EventLoops.create(4); var relay = new Relay(config(loops))) {
            var control = new SpliceControl();
            var splice = Zmux.splice(relay.a, relay.b, SpliceOptions.defaults().withControl(control)).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read(), "the relay carries the first byte");
                assertTrue(control.cancel(0x77));
                assertFalse(control.cancel(1), "one cancellation");
                var result = splice.get(5, TimeUnit.SECONDS);
                assertEquals(new SpliceResult.End.Cancelled(0x77), result.aToB().end());
                assertEquals(new SpliceResult.End.Cancelled(0x77), result.bToA().end());
                for (ZmuxStream end : new ZmuxStream[] {relay.endpoint, peer}) {
                    var reset = assertThrows(StreamResetException.class, () -> SessionTest.readAll(end));
                    assertEquals(0x77, reset.code());
                    var stopped = assertThrows(StreamStoppedException.class, () -> {
                        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        while (System.nanoTime() < until) { end.write(new byte[1024]); end.flush(); }
                    });
                    assertEquals(0x77, stopped.code());
                }
            }
        }
    }

    @Test void cancellationUnregistersDataAndLimiterWaitsBeforeCompletion() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var ready = new CompletableFuture<Long>();
            var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Long> acquire(long min, long max) { entered.countDown(); return ready; }
                @Override public long tryAcquire(long min, long max) { return 0; }
                @Override public void refund(long n) { fail("no bytes were allowed"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            assertTrue(entered.await(3, TimeUnit.SECONDS));
            // Cancelling the stage cancels the splice: no result, both streams reset with ZMUX_CANCELLED and stopped with 0.
            assertTrue(splice.cancel(true));
            assertThrows(CancellationException.class, () -> splice.get(3, TimeUnit.SECONDS));
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (!ready.isCancelled() && System.nanoTime() < until) Thread.sleep(2);
            assertTrue(ready.isCancelled());
            var reset = assertThrows(StreamResetException.class, () -> SessionTest.readAll(relay.endpoint));
            assertEquals(io.zmux.internal.wire.TransportError.CANCELLED, reset.code());
            // The stopped receiving part: the endpoint's own sending side is stopped with code 0, not ZMUX_CANCELLED.
            var stopped = assertThrows(StreamStoppedException.class, () -> {
                long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                while (System.nanoTime() < limit) { relay.endpoint.write(new byte[1024]); Thread.sleep(2); }
            });
            assertEquals(0, stopped.code());
        }
    }

    @Test void upstreamResetCannotHideBehindZeroDownstreamCapacity() throws Exception {
        try (EventLoops loops = EventLoops.create(3)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).initialStreamReceiveWindow(4096).maxStreamReceiveWindow(4096).build();
            try (var relay = new Relay(cfg); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
                var splice = Zmux.splice(relay.a, relay.b).toCompletableFuture();
                try (var peer = relay.destination()) {
                    var sender = tasks.submit(() -> { try { relay.endpoint.write(new byte[32768]); } catch (IOException expected) {} return null; });
                    // Reached within milliseconds; the bound only limits a failure, and a saturated runner may take seconds.
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
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

    /** A fixed byte budget: allowances are taken when handed out; waiters park until bytes come back. */
    static final class FixedBudget implements Limiter {
        final long total; long left, lowest;
        private record Waiter(CompletableFuture<Long> f, long min, long max) {}
        private final java.util.ArrayDeque<Waiter> waiters = new java.util.ArrayDeque<>();
        FixedBudget(long total) { this.total = total; left = lowest = total; }
        private long take(long max) { long n = Math.min(left, max); left -= n; lowest = Math.min(lowest, left); return n; }
        @Override public synchronized CompletionStage<Long> acquire(long min, long max) {
            if (left >= min) return CompletableFuture.completedFuture(take(max));
            var f = new CompletableFuture<Long>(); waiters.add(new Waiter(f, min, max)); return f;
        }
        @Override public synchronized long tryAcquire(long min, long max) { return left >= min ? take(max) : 0; }
        @Override public void refund(long n) {
            java.util.List<Runnable> wake = new java.util.ArrayList<>();
            synchronized (this) {
                left += n;
                for (var it = waiters.iterator(); it.hasNext();) {
                    Waiter w = it.next(); var f = w.f();
                    if (f.isDone()) { it.remove(); continue; }
                    if (left < w.min()) continue;
                    it.remove(); long got = take(w.max());
                    wake.add(() -> { if (!f.complete(got)) refund(got); });
                }
            }
            wake.forEach(Runnable::run);
        }
        synchronized long left() { return left; }
    }

    @Test void aDirectionHoldsNoGrantWhileItsSourceIsIdle() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var calls = new java.util.concurrent.CopyOnWriteArrayList<long[]>();
            var charged = new java.util.concurrent.atomic.AtomicLong();
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Long> acquire(long min, long max) {
                    calls.add(new long[] {min, max}); charged.addAndGet(max); return CompletableFuture.completedFuture(max);
                }
                @Override public long tryAcquire(long min, long max) { return 0; }
                @Override public void refund(long n) { charged.addAndGet(-n); }
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read());
                Thread.sleep(100); // idle source: the direction waits for readability, not in the limiter
                int seen = calls.size();
                assertEquals(1, charged.get(), "only the forwarded byte stays taken");
                Thread.sleep(100);
                assertEquals(seen, calls.size(), "no grant is asked for while nothing is readable");
                relay.endpoint.write(new byte[] {1, 2, 3});
                byte[] got = peer.inputStream().readNBytes(3);
                assertArrayEquals(new byte[] {1, 2, 3}, got);
                for (long[] c : calls) { assertEquals(1, c[0], "acquire(1, k)"); assertTrue(c[1] >= 1 && c[1] <= 3, "k = the bytes readable"); }
                relay.endpoint.finish(); peer.finish();
                var result = splice.get(3, TimeUnit.SECONDS);
                assertEquals(4, result.aToB().streamBytes());
                assertEquals(4, charged.get(), "granted and not refunded = forwarded");
            }
        }
    }

    @Test void aSharedLimiterNeverOverGrantsAcrossSplices() throws Exception {
        try (EventLoops loops = EventLoops.create(4); var tasks = Executors.newVirtualThreadPerTaskExecutor();
             var one = new Relay(config(loops)); var two = new Relay(config(loops))) {
            FixedBudget budget = new FixedBudget(50000);
            var c1 = new SpliceControl(); var c2 = new SpliceControl();
            var s1 = Zmux.splice(one.a, one.b, new SpliceOptions(8192, budget, null, c1)).toCompletableFuture();
            var s2 = Zmux.splice(two.a, two.b, new SpliceOptions(8192, budget, null, c2)).toCompletableFuture();
            AtomicInteger received = new AtomicInteger();
            for (Relay r : new Relay[] {one, two}) {
                tasks.submit(() -> { r.endpoint.write(new byte[100000]); return null; });
                tasks.submit(() -> {
                    var peer = r.destination(); byte[] b = new byte[65536];
                    for (int n; (n = peer.inputStream().read(b)) > 0;) received.addAndGet(n);
                    return null;
                });
            }
            ListenerResourcesTest.until(() -> budget.left() < 8192 && received.get() >= budget.total - budget.left());
            Thread.sleep(100); // no more allowances: neither direction moves past the budget
            assertTrue(received.get() <= budget.total, "forwarded " + received.get());
            assertTrue(budget.lowest >= 0);
            c1.cancel(1); c2.cancel(1);
            long forwarded = s1.get(3, TimeUnit.SECONDS).aToB().streamBytes() + s2.get(3, TimeUnit.SECONDS).aToB().streamBytes();
            assertEquals(budget.total, budget.left() + forwarded, "what was not forwarded came back");
        }
    }

    @Test void stopInterruptsALimiterAndReachesTheOriginalSender() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var blocked = new CompletableFuture<Long>(); var entered = new CountDownLatch(1);
            var calls = new AtomicInteger();
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Long> acquire(long min, long max) {
                    if (calls.getAndIncrement() == 0) return CompletableFuture.completedFuture(1L);
                    entered.countDown(); return blocked;
                }
                @Override public long tryAcquire(long min, long max) { return max; }
                @Override public void refund(long n) {}
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
            var blocked = new CompletableFuture<Long>(); var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Long> acquire(long min, long max) { entered.countDown(); return blocked; }
                @Override public long tryAcquire(long min, long max) { return 0; }
                @Override public void refund(long n) { fail("limiter denied forwarding"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            assertTrue(entered.await(3, TimeUnit.SECONDS)); relay.left.client().abort(32, "gone");
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
            var blocked = new CompletableFuture<Long>(); var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Long> acquire(long min, long max) { entered.countDown(); return blocked; }
                @Override public long tryAcquire(long min, long max) { return 0; }
                @Override public void refund(long n) { fail("no byte allowed"); }
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
            var blocked = new CompletableFuture<Long>(); var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Long> acquire(long min, long max) { entered.countDown(); return blocked; }
                @Override public long tryAcquire(long min, long max) { return 0; }
                @Override public void refund(long n) { fail("no byte allowed"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            assertTrue(entered.await(3, TimeUnit.SECONDS)); relay.right.client().abort(31, "gone");
            var result = splice.get(3, TimeUnit.SECONDS);
            assertEquals(new SpliceResult.End.DownstreamLost(), result.aToB().end()); assertTrue(blocked.isCancelled());
        }
    }

    @Test void datagramsDeniedByTheDirectionLimiterAreCountedWithoutAHelperQueue() throws Exception {
        try (EventLoops loops = EventLoops.create(2);
             var relay = new Relay(ZmuxConfig.builder().eventLoops(loops).datagrams(true).build())) {
            var dropped = new CountDownLatch(1); var charged = new java.util.concurrent.atomic.AtomicLong();
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Long> acquire(long min, long max) { charged.addAndGet(max); return CompletableFuture.completedFuture(max); }
                @Override public long tryAcquire(long min, long max) { dropped.countDown(); return 0; }
                @Override public void refund(long n) { charged.addAndGet(-n); }
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read()); relay.endpoint.sendDatagram(4, ByteBuffer.wrap(new byte[] {1, 2}));
                assertTrue(dropped.await(3, TimeUnit.SECONDS)); relay.endpoint.finish(); peer.finish();
                var result = splice.get(3, TimeUnit.SECONDS);
                assertEquals(1, result.aToB().datagramsDropped()); assertEquals(0, result.aToB().datagramBytes());
                assertEquals(1, result.aToB().datagramsLimited()); assertEquals(0, result.aToB().datagramsTooLarge());
                assertEquals(result.aToB().streamBytes(), charged.get());
            }
        }
    }

    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    @Test void ownerLoopShutdownDoesNotLeaveObserversParked() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var blocked = new CompletableFuture<Long>(); var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Long> acquire(long min, long max) { entered.countDown(); return blocked; }
                @Override public long tryAcquire(long min, long max) { return 0; }
                @Override public void refund(long n) { fail("no byte allowed"); }
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
                    var charged = new java.util.concurrent.atomic.AtomicLong();
                    Limiter limiter = new Limiter() {
                        @Override public CompletionStage<Long> acquire(long min, long max) { charged.addAndGet(max); return CompletableFuture.completedFuture(max); }
                        @Override public long tryAcquire(long min, long max) { charged.addAndGet(max); return max; }
                        @Override public void refund(long n) { charged.addAndGet(-n); }
                    };
                    var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(16384, limiter, null)).toCompletableFuture();
                    try (var peer = relay.destination()) {
                        assertEquals(7, peer.inputStream().read()); gate.paused = true;
                        relay.endpoint.write(new byte[100000]); relay.endpoint.markReliableBoundary(); relay.endpoint.reset(95);
                        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                        while (charged.get() < 100001 && System.nanoTime() < until) Thread.sleep(2);
                        assertTrue(charged.get() >= 100001, mode); // a read in flight may hold more until it ends
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
                            assertEquals(result.aToB().streamBytes(), charged.get(), "what was not forwarded came back");
                        } else {
                            if (mode.equals("cancel")) {
                                assertTrue(splice.cancel(true), mode);
                                assertThrows(CancellationException.class, () -> splice.get(3, TimeUnit.SECONDS), mode);
                            } else {
                                if (mode.equals("session")) relay.left.client().abort(32, "gone");
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

    @Test void aLimiterFailingOnADatagramDropsItAndTheSpliceGoesOn() throws Exception {
        try (EventLoops loops = EventLoops.create(2);
             var relay = new Relay(ZmuxConfig.builder().eventLoops(loops).datagrams(true).build())) {
            var thrown = new IllegalStateException("datagram limiter broke");
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Long> acquire(long min, long max) { return CompletableFuture.completedFuture(max); }
                @Override public long tryAcquire(long min, long max) { throw thrown; }
                @Override public void refund(long n) { }
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read()); relay.endpoint.sendDatagram(4, ByteBuffer.wrap(new byte[] {1, 2}));
                // tryAcquire failing counts as nothing taken: the datagram is dropped as limited, the stream goes on.
                relay.endpoint.write(new byte[] {9}); relay.endpoint.finish();
                assertEquals(9, peer.inputStream().read());
                peer.finish();
                var result = splice.get(3, TimeUnit.SECONDS);
                assertEquals(1, result.aToB().datagramsDropped());
            }
        }
    }

    @Test void invalidLimiterAllowanceAbortsAndReportsItsFailure() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Long> acquire(long min, long max) { return CompletableFuture.completedFuture(0L); }
                @Override public long tryAcquire(long min, long max) { return 0; }
                @Override public void refund(long n) { fail("an invalid allowance took nothing"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, new SpliceOptions(65536, limiter, null)).toCompletableFuture();
            var failed = assertThrows(ExecutionException.class, () -> splice.get(3, TimeUnit.SECONDS));
            assertTrue(failed.getCause() instanceof IOException, String.valueOf(failed.getCause()));
            assertTrue(failed.getCause().getMessage().startsWith("invalid limiter grant"), failed.getCause().getMessage());
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

    /**
     * Frames of a splice in any thread, virtual ones included: an in-process thread dump (no attach). The platform
     * MXBeans and Thread.getAllStackTraces list no virtual threads, which would make the check vacuous; the
     * HotSpot dump does, and is reached reflectively because the test module does not read jdk.management.
     * The frames come one per line; null when the dump lacks the caller's own frames, which would prove nothing.
     */
    static String spliceFrames() throws Exception {
        var dump = java.nio.file.Path.of("target", "splice-threads-" + System.nanoTime() + ".txt").toAbsolutePath();
        Class<?> bean = Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
        @SuppressWarnings({"unchecked", "rawtypes"}) // the enum class is reached reflectively: no type to name
        Object format = Enum.valueOf((Class) Class.forName("com.sun.management.HotSpotDiagnosticMXBean$ThreadDumpFormat"), "TEXT_PLAIN");
        Object diagnostic = Class.forName("java.lang.management.ManagementFactory").getMethod("getPlatformMXBean", Class.class).invoke(null, bean);
        try {
            bean.getMethod("dumpThreads", String.class, format.getClass()).invoke(diagnostic, dump.toString(), format);
            var text = java.nio.file.Files.readString(dump);
            if (!text.contains("io.zmux.SpliceTest.spliceFrames")) return null;
            return text.lines().filter(l -> l.contains("io.zmux.Splice.") || l.contains("io.zmux.Splice$"))
                    .collect(java.util.stream.Collectors.joining("\n"));
        } finally { java.nio.file.Files.deleteIfExists(dump); }
    }

    /** Waiting splices hold no thread: every wait is a registered operation or limiter future. */
    @Test void waitingSplicesHoldNoThread() throws Exception {
        try (EventLoops loops = EventLoops.create(2);
             var left = Pair.memory(ZmuxConfig.builder().eventLoops(loops).datagrams(true).build());
             var right = Pair.memory(ZmuxConfig.builder().eventLoops(loops).datagrams(true).build())) {
            var blocked = new CompletableFuture<Long>();
            var waiting = new CountDownLatch(8);
            Limiter limiter = new Limiter() {
                @Override public CompletionStage<Long> acquire(long min, long max) { waiting.countDown(); return blocked; }
                @Override public long tryAcquire(long min, long max) { return max; }
                @Override public void refund(long n) {}
            };
            var splices = new java.util.ArrayList<CompletableFuture<SpliceResult>>();
            var endpoints = new java.util.ArrayList<ZmuxStream>();
            for (int i = 0; i < 16; i++) {
                var endpoint = left.client().openStream(); endpoint.write(new byte[] {7}); endpoints.add(endpoint);
                var a = left.server().acceptStream(Duration.ofSeconds(3)); var b = right.client().openStream();
                // Half wait for the limiter, half for data.
                splices.add(Zmux.splice(a, b, new SpliceOptions(65536, i % 2 == 0 ? limiter : null, null)).toCompletableFuture());
            }
            // Every limited direction has reached its limiter wait (the others wait for data once the first byte went).
            assertTrue(waiting.await(20, TimeUnit.SECONDS));
            // A drain finishing its last step may still show in a dump: a thread that holds a splice shows in every one.
            // Stable means two dumps in a row with no splice frame; the bound only has to outlast a slow host.
            String frames = null, previous = null; long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!("".equals(frames) && "".equals(previous)) && System.nanoTime() < until) { Thread.sleep(20); previous = frames; frames = spliceFrames(); }
            assertEquals("", frames, "no thread runs or parks in a waiting splice (null: the dump lacked the caller's own frames)");
            for (var s : splices) assertFalse(s.isDone());
            for (var s : splices) s.cancel(true);
            for (var e : endpoints) e.close();
            // Cancelling a splice's stage completes it at once; its drain then abandons the limiter wait on the splice's
            // own context, so the limiter's stage is cancelled shortly after, not before cancel returns.
            long cancelBy = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!blocked.isCancelled() && System.nanoTime() < cancelBy) Thread.sleep(2);
            assertTrue(blocked.isCancelled());
        }
    }

    /** Cancelling while the direction waits for data, or for the target's capacity, cancels that wait and releases what it holds. */
    @Test void cancellationEndsTheReadAndTheWriteWait() throws Exception {
        for (boolean write : new boolean[] {false, true}) {
            try (EventLoops loops = EventLoops.create(3)) {
                var cfg = ZmuxConfig.builder().eventLoops(loops).initialStreamReceiveWindow(4096).maxStreamReceiveWindow(4096).build();
                try (var relay = new Relay(cfg); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
                    var control = new SpliceControl();
                    var splice = Zmux.splice(relay.a, relay.b, SpliceOptions.defaults().withControl(control)).toCompletableFuture();
                    try (var peer = relay.destination()) {
                        if (write) {
                            tasks.submit(() -> { try { relay.endpoint.write(new byte[32768]); } catch (IOException expected) {} return null; });
                            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                            while (relay.b.sendCapacity() != 0 && System.nanoTime() < until) Thread.sleep(5);
                            assertEquals(0, relay.b.sendCapacity());
                        } else assertEquals(7, peer.inputStream().read());
                        assertTrue(control.cancel(0x55));
                        var result = splice.get(3, TimeUnit.SECONDS);
                        assertEquals(new SpliceResult.End.Cancelled(0x55), result.aToB().end(), "write " + write);
                        assertEquals(new SpliceResult.End.Cancelled(0x55), result.bToA().end(), "write " + write);
                        peer.setReadTimeout(Duration.ofSeconds(3));
                        var reset = assertThrows(StreamResetException.class, () -> SessionTest.readAll(peer));
                        assertEquals(0x55, reset.code());
                    }
                }
                assertEquals(0, loops.outstandingBuffers());
            }
        }
    }

    /** Cancelling the stage while datagram and stream receives wait ends both without a result. */
    @Test void cancellationEndsTheDatagramWait() throws Exception {
        try (EventLoops loops = EventLoops.create(2);
             var relay = new Relay(ZmuxConfig.builder().eventLoops(loops).datagrams(true).build())) {
            var splice = Zmux.splice(relay.a, relay.b).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read());
                assertTrue(splice.cancel(true));
                assertThrows(CancellationException.class, () -> splice.get(3, TimeUnit.SECONDS));
                var reset = assertThrows(StreamResetException.class, () -> SessionTest.readAll(relay.endpoint));
                assertEquals(io.zmux.internal.wire.TransportError.CANCELLED, reset.code());
            }
        }
    }

    /** The target's loop group shuts down under a waiting splice: its operations are refused or ended, and the splice completes. */
    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    @Test void aShutDownTargetLoopEndsTheSpliceInsteadOfHangingIt() throws Exception {
        try (EventLoops upstream = EventLoops.create(1); EventLoops downstream = EventLoops.create(1);
             var relay = new Relay(config(upstream), config(downstream))) {
            var splice = Zmux.splice(relay.a, relay.b).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read());
                downstream.close();
                var result = splice.get(5, TimeUnit.SECONDS);
                assertEquals(new SpliceResult.End.DownstreamLost(), result.aToB().end());
                assertEquals(new SpliceResult.End.UpstreamLost(), result.bToA().end());
                var reset = assertThrows(StreamResetException.class, () -> SessionTest.readAll(relay.endpoint));
                assertEquals(io.zmux.carrier.PipeIo.CARRIER_FAILED, reset.code());
            }
        }
    }
}
