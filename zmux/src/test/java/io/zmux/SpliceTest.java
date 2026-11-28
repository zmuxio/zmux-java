package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.helper.Limiter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class SpliceTest {
    static final class Relay implements AutoCloseable {
        final Pair left, right;
        final ZmuxStream endpoint, a, b;
        Relay(ZmuxConfig config) throws Exception { this(config, (io.zmux.carrier.PipeIo[]) null); }
        /** The two sessions on separate loop groups. */
        Relay(ZmuxConfig leftConfig, ZmuxConfig rightConfig) throws Exception {
            left = Pair.memory(leftConfig); right = Pair.memory(rightConfig);
            endpoint = left.client().openStream(); endpoint.write(new byte[] {7});
            a = left.server().acceptStream(Duration.ofSeconds(GUARD_SECONDS)); b = right.client().openStream();
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
                right = new Pair(Zmux.open(target[0], Role.CLIENT, config), accepting.get(GUARD_SECONDS, TimeUnit.SECONDS));
            }
            endpoint = left.client().openStream(); endpoint.write(new byte[] {7});
            a = left.server().acceptStream(Duration.ofSeconds(GUARD_SECONDS)); b = right.client().openStream();
        }
        ZmuxStream destination() throws Exception { return right.server().acceptStream(Duration.ofSeconds(GUARD_SECONDS)); }
        @Override public void close() throws IOException { endpoint.close(); left.close(); right.close(); }
    }
    static ZmuxConfig config(EventLoops loops) { return ZmuxConfig.builder().eventLoops(loops).build(); }

    private static final long GUARD_SECONDS = 30;

    private static <T> T await(CompletableFuture<T> result) throws Exception {
        return result.get(GUARD_SECONDS, TimeUnit.SECONDS);
    }

    private static CompletableFuture<Throwable> sendEnd(ZmuxStream stream) {
        CompletableFuture<Throwable> stopped = new CompletableFuture<>();
        stream.session.driver.post(() -> stream.session.driver.lanes.watchSendEnd(stream.st,
                error -> stopped.complete(Errors.io(error))));
        return stopped;
    }

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
                writing.get(GUARD_SECONDS, TimeUnit.SECONDS); destination.get(GUARD_SECONDS, TimeUnit.SECONDS);
                var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
                assertEquals(payload.length + 1, result.aToB().bytes());
                assertEquals(response.length, result.bToA().bytes());
                assertEquals(new SpliceEnd.Finished(), result.aToB().end()); assertEquals(new SpliceEnd.Finished(), result.bToA().end());
            }
            Leaks.none(loops);
        }
    }

    /** Both peers see the code a control cancels with, both ways; the result reports it. */
    @Test void aControlEndsBothStreamsWithItsCode() throws Exception {
        try (EventLoops loops = EventLoops.create(4); var relay = new Relay(config(loops))) {
            var control = new SpliceAbort();
            var splice = Zmux.splice(relay.a, relay.b, SpliceOptions.builder().abort(control).build()).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read(), "the relay carries the first byte");
                assertTrue(control.abort(0x77));
                assertFalse(control.abort(1), "the first code decides");
                var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
                assertEquals(new SpliceEnd.Cancelled(0x77), result.aToB().end());
                assertEquals(new SpliceEnd.Cancelled(0x77), result.bToA().end());
                for (ZmuxStream end : new ZmuxStream[] {relay.endpoint, peer}) {
                    var reset = assertThrows(StreamResetException.class, () -> SessionTest.readAll(end));
                    assertEquals(0x77, reset.code());
                    var stopped = assertInstanceOf(StreamStoppedException.class, await(sendEnd(end)));
                    assertEquals(0x77, stopped.code());
                }
            }
        }
    }

    @Test void cancellationUnregistersDataAndLimiterWaitsBeforeCompletion() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var ready = new CompletableFuture<Long>();
            var abandoned = ready.handle((grant, error) -> error);
            var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public long maxBurstBytes() { return Long.MAX_VALUE; }
                @Override public CompletionStage<Long> acquire(long min, long max) { entered.countDown(); return ready; }
                @Override public long tryAcquire(long min, long max) { return 0; }
                @Override public void refund(long n) { fail("no bytes were allowed"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, SpliceOptions.builder().aToB(limiter).build()).toCompletableFuture();
            assertTrue(entered.await(GUARD_SECONDS, TimeUnit.SECONDS));
            // Cancelling the stage cancels the splice: no result, both streams reset with ZMUX_CANCELLED and stopped with 0.
            assertTrue(splice.cancel(true));
            assertThrows(CancellationException.class, () -> splice.get(GUARD_SECONDS, TimeUnit.SECONDS));
            assertInstanceOf(CancellationException.class, await(abandoned));
            var reset = assertThrows(StreamResetException.class, () -> SessionTest.readAll(relay.endpoint));
            assertEquals(io.zmux.internal.wire.TransportError.CANCELLED, reset.code());
            // The stopped receiving part: the endpoint's own sending side is stopped with code 0, not ZMUX_CANCELLED.
            var stopped = assertInstanceOf(StreamStoppedException.class, await(sendEnd(relay.endpoint)));
            assertEquals(0, stopped.code());
        }
    }

    @Test void upstreamResetCannotHideBehindZeroDownstreamCapacity() throws Exception {
        try (EventLoops loops = EventLoops.create(3)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).initialStreamReceiveWindow(4096).maxStreamReceiveWindow(4096).build();
            try (var relay = new Relay(cfg); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
                var limiter = new FixedBudget(4096);
                var splice = Zmux.splice(relay.a, relay.b, SpliceOptions.builder().aToB(limiter).build()).toCompletableFuture();
                try (var peer = relay.destination()) {
                    var sender = tasks.submit(() -> { try { relay.endpoint.write(new byte[32768]); } catch (IOException expected) {} return null; });
                    await(limiter.exhausted);
                    SessionTestSupport.await(relay.b.session, () -> relay.b.sendCapacity() == 0);
                    relay.endpoint.reset(73);
                    peer.setReadTimeout(Duration.ofSeconds(GUARD_SECONDS));
                    StreamResetException error = assertThrows(StreamResetException.class, () -> SessionTest.readAll(peer));
                    assertEquals(73, error.code()); peer.finish();
                    var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
                    assertEquals(new SpliceEnd.Reset(73), result.aToB().end());
                    sender.get(GUARD_SECONDS, TimeUnit.SECONDS);
                }
            }
            Leaks.none(loops);
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
                            bytes.writeBytes(owned.copy());
                        }
                    }
                });
                assertEquals(95, reset.code()); byte[] got = bytes.toByteArray();
                assertEquals(7, got[0]); assertArrayEquals(payload, Arrays.copyOfRange(got, 1, got.length));
                peer.finish(); sending.get(GUARD_SECONDS, TimeUnit.SECONDS);
                var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
                assertEquals(payload.length + 1, result.aToB().bytes());
                assertEquals(new SpliceEnd.Reset(95), result.aToB().end());
            }
        }
    }

    /** A fixed byte budget: allowances are taken when handed out; waiters park until bytes come back. */
    static final class FixedBudget implements Limiter {
        @Override public long maxBurstBytes() { return Long.MAX_VALUE; }
        final long total; long left, lowest;
        final CompletableFuture<Void> exhausted = new CompletableFuture<>();
        private record Waiter(CompletableFuture<Long> f, long min, long max) {}
        private final java.util.ArrayDeque<Waiter> waiters = new java.util.ArrayDeque<>();
        FixedBudget(long total) { this.total = total; left = lowest = total; }
        private long take(long max) { long n = Math.min(left, max); left -= n; lowest = Math.min(lowest, left); return n; }
        @Override public synchronized CompletionStage<Long> acquire(long min, long max) {
            if (left >= min) return CompletableFuture.completedFuture(take(max));
            var f = new CompletableFuture<Long>(); waiters.add(new Waiter(f, min, max)); exhausted.complete(null); return f;
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
                @Override public long maxBurstBytes() { return Long.MAX_VALUE; }
                @Override public CompletionStage<Long> acquire(long min, long max) {
                    calls.add(new long[] {min, max}); charged.addAndGet(max); return CompletableFuture.completedFuture(max);
                }
                @Override public long tryAcquire(long min, long max) { return 0; }
                @Override public void refund(long n) { charged.addAndGet(-n); }
            };
            var splice = Zmux.splice(relay.a, relay.b, SpliceOptions.builder().aToB(limiter).build()).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read());
                assertEquals(1, charged.get(), "only the forwarded byte stays taken");
                relay.endpoint.write(new byte[] {1, 2, 3});
                byte[] got = peer.inputStream().readNBytes(3);
                assertArrayEquals(new byte[] {1, 2, 3}, got);
                for (long[] c : calls) { assertEquals(1, c[0], "acquire(1, k)"); assertTrue(c[1] >= 1 && c[1] <= 3, "k = the bytes readable"); }
                relay.endpoint.finish(); peer.finish();
                var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
                assertEquals(4, result.aToB().bytes());
                assertEquals(4, charged.get(), "granted and not refunded = forwarded");
            }
        }
    }

    @Test void aSharedLimiterNeverOverGrantsAcrossSplices() throws Exception {
        try (EventLoops loops = EventLoops.create(4); var tasks = Executors.newVirtualThreadPerTaskExecutor();
             var one = new Relay(config(loops)); var two = new Relay(config(loops))) {
            FixedBudget budget = new FixedBudget(50000);
            var c1 = new SpliceAbort(); var c2 = new SpliceAbort();
            var s1 = Zmux.splice(one.a, one.b, SpliceOptions.builder().chunk(8192).aToB(budget).abort(c1).build()).toCompletableFuture();
            var s2 = Zmux.splice(two.a, two.b, SpliceOptions.builder().chunk(8192).aToB(budget).abort(c2).build()).toCompletableFuture();
            AtomicInteger received = new AtomicInteger();
            var delivered = new CompletableFuture<Void>();
            for (Relay r : new Relay[] {one, two}) {
                tasks.submit(() -> { r.endpoint.write(new byte[100000]); return null; });
                tasks.submit(() -> {
                    var peer = r.destination(); byte[] b = new byte[65536];
                    for (int n; (n = peer.inputStream().read(b)) > 0;) {
                        if (received.addAndGet(n) >= budget.total) delivered.complete(null);
                    }
                    return null;
                });
            }
            await(budget.exhausted);
            await(delivered);
            assertEquals(budget.total, received.get());
            assertTrue(budget.lowest >= 0);
            c1.abort(1); c2.abort(1);
            s1.get(GUARD_SECONDS, TimeUnit.SECONDS);
            s2.get(GUARD_SECONDS, TimeUnit.SECONDS);
            assertEquals(0, budget.left(), "each byte handed to the carrier stays charged");
        }
    }

    @Test void stopInterruptsALimiterAndReachesTheOriginalSender() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var blocked = new CompletableFuture<Long>(); var entered = new CountDownLatch(1);
            var calls = new AtomicInteger();
            Limiter limiter = new Limiter() {
                @Override public long maxBurstBytes() { return Long.MAX_VALUE; }
                @Override public CompletionStage<Long> acquire(long min, long max) {
                    if (calls.getAndIncrement() == 0) return CompletableFuture.completedFuture(1L);
                    entered.countDown(); return blocked;
                }
                @Override public long tryAcquire(long min, long max) { return max; }
                @Override public void refund(long n) {}
            };
            var splice = Zmux.splice(relay.a, relay.b, SpliceOptions.builder().aToB(limiter).build()).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read());
                relay.endpoint.write(new byte[] {42}); assertTrue(entered.await(GUARD_SECONDS, TimeUnit.SECONDS));
                peer.stop(82); peer.finish();
                relay.endpoint.setWriteTimeout(Duration.ofSeconds(GUARD_SECONDS));
                var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
                var stop = assertInstanceOf(StreamStoppedException.class, await(sendEnd(relay.endpoint)));
                assertEquals(82, stop.code());
                assertEquals(new SpliceEnd.Stopped(82), result.aToB().end());
                awaitCancelled(blocked);
            }
        }
    }

    /**
     * The limiter's pending acquire is cancelled once the splice drops it. The splice may end first: the acquire
     * call runs off the owner, which learns of its future only afterwards and cancels it then.
     */
    private static void awaitCancelled(CompletableFuture<Long> blocked) throws Exception {
        try {
            blocked.get(GUARD_SECONDS, TimeUnit.SECONDS);
            fail("the limiter's acquire completed");
        } catch (java.util.concurrent.CancellationException expected) {
            // cancelled
        }
    }

    @Test void sessionFailureInterruptsLimiterWithoutTakingBufferedData() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var blocked = new CompletableFuture<Long>(); var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public long maxBurstBytes() { return Long.MAX_VALUE; }
                @Override public CompletionStage<Long> acquire(long min, long max) { entered.countDown(); return blocked; }
                @Override public long tryAcquire(long min, long max) { return 0; }
                @Override public void refund(long n) { fail("limiter denied forwarding"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, SpliceOptions.builder().aToB(limiter).build()).toCompletableFuture();
            assertTrue(entered.await(GUARD_SECONDS, TimeUnit.SECONDS)); relay.left.client().abort(32, "gone");
            var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
            assertEquals(new SpliceEnd.UpstreamLost(), result.aToB().end());
            assertEquals(0, result.aToB().bytes()); awaitCancelled(blocked);
        }
    }

    @Test void datagramsKeepContextsAndUseOnlyTheSessionsQueues() throws Exception {
        try (EventLoops loops = EventLoops.create(3);
             var relay = new Relay(ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).build())) {
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
                var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
                assertEquals(4, result.aToB().datagramBytes()); assertEquals(2, result.bToA().datagramBytes());
                assertEquals(0, result.aToB().datagramsDropped());
            }
        }
    }

    @Test void resetBeforeTheFirstForwardedByteCompletesBothDirectionsWithoutAnOpener() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var blocked = new CompletableFuture<Long>(); var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public long maxBurstBytes() { return Long.MAX_VALUE; }
                @Override public CompletionStage<Long> acquire(long min, long max) { entered.countDown(); return blocked; }
                @Override public long tryAcquire(long min, long max) { return 0; }
                @Override public void refund(long n) { fail("no byte allowed"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, SpliceOptions.builder().aToB(limiter).build()).toCompletableFuture();
            assertTrue(entered.await(GUARD_SECONDS, TimeUnit.SECONDS)); relay.endpoint.reset(77);
            var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
            assertEquals(new SpliceEnd.Reset(77), result.aToB().end());
            assertEquals(new SpliceEnd.Finished(), result.bToA().end());
            assertFalse(relay.b.st.idSet()); assertEquals(0, result.aToB().bytes()); awaitCancelled(blocked);
        }
    }

    @Test void losingTheUnopenedTargetSessionWakesTheLimiterAndTheReverseReader() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var blocked = new CompletableFuture<Long>(); var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public long maxBurstBytes() { return Long.MAX_VALUE; }
                @Override public CompletionStage<Long> acquire(long min, long max) { entered.countDown(); return blocked; }
                @Override public long tryAcquire(long min, long max) { return 0; }
                @Override public void refund(long n) { fail("no byte allowed"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, SpliceOptions.builder().aToB(limiter).build()).toCompletableFuture();
            assertTrue(entered.await(GUARD_SECONDS, TimeUnit.SECONDS)); relay.right.client().abort(31, "gone");
            var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
            assertEquals(new SpliceEnd.DownstreamLost(), result.aToB().end()); awaitCancelled(blocked);
        }
    }

    @Test void datagramsDeniedByTheDirectionLimiterAreCountedWithoutAHelperQueue() throws Exception {
        try (EventLoops loops = EventLoops.create(2);
             var relay = new Relay(ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).build())) {
            var dropped = new CountDownLatch(1); var charged = new java.util.concurrent.atomic.AtomicLong();
            Limiter limiter = new Limiter() {
                @Override public long maxBurstBytes() { return Long.MAX_VALUE; }
                @Override public CompletionStage<Long> acquire(long min, long max) { charged.addAndGet(max); return CompletableFuture.completedFuture(max); }
                @Override public long tryAcquire(long min, long max) { dropped.countDown(); return 0; }
                @Override public void refund(long n) { charged.addAndGet(-n); }
            };
            var splice = Zmux.splice(relay.a, relay.b, SpliceOptions.builder().aToB(limiter).build()).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read()); relay.endpoint.sendDatagram(4, ByteBuffer.wrap(new byte[] {1, 2}));
                assertTrue(dropped.await(GUARD_SECONDS, TimeUnit.SECONDS)); relay.endpoint.finish(); peer.finish();
                var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
                assertEquals(1, result.aToB().datagramsDropped()); assertEquals(0, result.aToB().datagramBytes());
                assertEquals(1, result.aToB().datagramsLimited()); assertEquals(0, result.aToB().datagramsTooLarge());
                assertEquals(result.aToB().bytes(), charged.get());
            }
        }
    }

    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    @Test void ownerLoopShutdownDoesNotLeaveObserversParked() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
            var blocked = new CompletableFuture<Long>(); var entered = new CountDownLatch(1);
            Limiter limiter = new Limiter() {
                @Override public long maxBurstBytes() { return Long.MAX_VALUE; }
                @Override public CompletionStage<Long> acquire(long min, long max) { entered.countDown(); return blocked; }
                @Override public long tryAcquire(long min, long max) { return 0; }
                @Override public void refund(long n) { fail("no byte allowed"); }
            };
            var splice = Zmux.splice(relay.a, relay.b, SpliceOptions.builder().aToB(limiter).build()).toCompletableFuture();
            assertTrue(entered.await(GUARD_SECONDS, TimeUnit.SECONDS)); loops.close();
            var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
            assertTrue(result.aToB().end() instanceof SpliceEnd.UpstreamLost || result.aToB().end() instanceof SpliceEnd.DownstreamLost,
                    String.valueOf(result.aToB().end())); awaitCancelled(blocked);
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
                    var chargedAll = new CompletableFuture<Void>();
                    Limiter limiter = new Limiter() {
                        @Override public long maxBurstBytes() { return Long.MAX_VALUE; }
                        @Override public CompletionStage<Long> acquire(long min, long max) {
                            if (charged.addAndGet(max) >= 100001) chargedAll.complete(null);
                            return CompletableFuture.completedFuture(max);
                        }
                        @Override public long tryAcquire(long min, long max) { charged.addAndGet(max); return max; }
                        @Override public void refund(long n) { charged.addAndGet(-n); }
                    };
                    var abort = new SpliceAbort();
                    var control = Zmux.spliceControlled(relay.a, relay.b,
                            SpliceOptions.builder().chunk(16384).aToB(limiter).abort(abort).build());
                    var splice = control.result().toCompletableFuture();
                    try (var peer = relay.destination()) {
                        assertEquals(7, peer.inputStream().read()); gate.paused = true;
                        relay.endpoint.write(new byte[100000]); relay.endpoint.markReliableBoundary(); relay.endpoint.reset(95);
                        await(chargedAll);
                        // Wait for the source reset decision before introducing a second terminal.
                        while (!(await(control.aToB().snapshot().toCompletableFuture()).end() instanceof SpliceEnd.Reset)) {
                            Thread.onSpinWait();
                        }
                        // Undelivered: still unsent, or in the batch the paused carrier holds (a batch can take all of it).
                        boolean unsent = (Boolean)relay.b.session.run(op -> relay.b.session.core.sending().holdsUnsent(relay.b.st));
                        synchronized (gate) { assertTrue(unsent || gate.pending != null, mode); }
                        assertNull(relay.b.session.run(op -> relay.b.session.core.sending().writeError(relay.b.st)), "fallback must not reset before draining");
                        if (mode.equals("drain")) {
                            gate.release(); var received = new java.io.ByteArrayOutputStream();
                            var error = assertThrows(StreamResetException.class, () -> {
                                for (Chunk chunk; (chunk = peer.readChunk(65536)) != null;) {
                                    try (Chunk owned = chunk) { received.write(new byte[owned.remaining()]); }
                                }
                            });
                            assertEquals(95, error.code());
                            // Plain RESET_STREAM may discard already handed-off bytes at its peer.
                            assertTrue(received.size() <= 100000); peer.finish();
                            var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
                            assertEquals(new SpliceEnd.Reset(95), result.aToB().end());
                            assertEquals(result.aToB().bytes(), charged.get(), "what was not forwarded came back");
                        } else {
                            if (mode.equals("cancel")) {
                                assertTrue(abort.abort(7), mode);
                                assertEquals(new SpliceEnd.Reset(95), await(splice).aToB().end());
                            } else {
                                if (mode.equals("session")) relay.left.client().abort(32, "gone");
                                if (mode.equals("stop")) { peer.stop(82); peer.finish(); }
                                var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
                                assertEquals(new SpliceEnd.Reset(95), result.aToB().end(), mode);
                            }
                            gate.release();
                        }
                    } finally { gate.release(); }
                }
                Leaks.none(loops, mode);
            }
        }
    }

    @Test void aLimiterFailingOnADatagramDropsItAndTheSpliceGoesOn() throws Exception {
        try (EventLoops loops = EventLoops.create(2);
             var relay = new Relay(ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).build())) {
            var thrown = new IllegalStateException("datagram limiter broke");
            Limiter limiter = new Limiter() {
                @Override public long maxBurstBytes() { return Long.MAX_VALUE; }
                @Override public CompletionStage<Long> acquire(long min, long max) { return CompletableFuture.completedFuture(max); }
                @Override public long tryAcquire(long min, long max) { throw thrown; }
                @Override public void refund(long n) { }
            };
            var splice = Zmux.splice(relay.a, relay.b, SpliceOptions.builder().aToB(limiter).build()).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read()); relay.endpoint.sendDatagram(4, ByteBuffer.wrap(new byte[] {1, 2}));
                // tryAcquire failing counts as nothing taken: the datagram is dropped as limited, the stream goes on.
                relay.endpoint.write(new byte[] {9}); relay.endpoint.finish();
                assertEquals(9, peer.inputStream().read());
                peer.finish();
                var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
                assertEquals(1, result.aToB().datagramsDropped());
            }
        }
    }

    /** A limiter failing in acquire ends its direction LimiterFailed; the other direction goes on and the stage completes. */
    @Test void aLimiterFailingInAcquireEndsOnlyItsDirection() throws Exception {
        for (String mode : List.of("invalid", "throws", "failed")) {
            try (EventLoops loops = EventLoops.create(2); var relay = new Relay(config(loops))) {
                Limiter limiter = new Limiter() {
                    @Override public long maxBurstBytes() { return Long.MAX_VALUE; }
                    @Override public CompletionStage<Long> acquire(long min, long max) {
                        return switch (mode) {
                            case "invalid" -> CompletableFuture.completedFuture(0L);
                            case "throws" -> throw new IllegalStateException("limiter broke");
                            default -> CompletableFuture.failedFuture(new IOException("budget exhausted"));
                        };
                    }
                    @Override public long tryAcquire(long min, long max) { return 0; }
                    @Override public void refund(long n) { fail("a failed acquire took nothing"); }
                };
                relay.b.flush(); // opened on the wire, so the peer sees the reset
                var splice = Zmux.splice(relay.a, relay.b, SpliceOptions.builder().aToB(limiter).build()).toCompletableFuture();
                try (var peer = relay.destination()) {
                    var reset = assertThrows(StreamResetException.class, () -> peer.inputStream().read(), mode);
                    assertEquals(io.zmux.internal.wire.TransportError.CANCELLED, reset.code(), mode);
                    peer.write(new byte[] {5}); peer.finish(); // the other direction goes on
                    assertEquals(5, relay.endpoint.inputStream().read(), mode);
                    assertEquals(-1, relay.endpoint.inputStream().read(), mode);
                    var stop = assertInstanceOf(StreamStoppedException.class, await(sendEnd(relay.endpoint)), mode);
                    assertEquals(io.zmux.internal.wire.TransportError.CANCELLED, stop.code(), mode);
                    var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
                    assertEquals(new SpliceEnd.LimiterFailed(), result.aToB().end(), mode);
                    assertEquals(new SpliceEnd.Finished(), result.bToA().end(), mode);
                    assertEquals(0, result.aToB().bytes(), mode); assertEquals(1, result.bToA().bytes(), mode);
                }
            }
        }
    }

    /** One abort handle serves several splices; the first code decides. */
    @Test void oneAbortHandleEndsEverySpliceItServes() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var one = new Relay(config(loops)); var two = new Relay(config(loops))) {
            var abort = new SpliceAbort();
            var s1 = Zmux.splice(one.a, one.b, SpliceOptions.builder().abort(abort).build()).toCompletableFuture();
            var s2 = Zmux.splice(two.a, two.b, SpliceOptions.builder().abort(abort).build()).toCompletableFuture();
            try (var p1 = one.destination(); var p2 = two.destination()) {
                assertEquals(7, p1.inputStream().read()); assertEquals(7, p2.inputStream().read());
                assertTrue(abort.abort(0x66)); assertFalse(abort.abort(0x67), "the first code decides");
                for (var r : List.of(s1.get(GUARD_SECONDS, TimeUnit.SECONDS), s2.get(GUARD_SECONDS, TimeUnit.SECONDS))) {
                    assertEquals(new SpliceEnd.Cancelled(0x66), r.aToB().end());
                    assertEquals(new SpliceEnd.Cancelled(0x66), r.bToA().end());
                }
                var reset = assertThrows(StreamResetException.class, () -> p2.inputStream().read());
                assertEquals(0x66, reset.code());
            }
            // A splice started with a handle that already aborted is aborted as it starts.
            try (var three = new Relay(config(loops))) {
                var r = Zmux.splice(three.a, three.b, SpliceOptions.builder().abort(abort).build()).toCompletableFuture().get(GUARD_SECONDS, TimeUnit.SECONDS);
                assertEquals(new SpliceEnd.Cancelled(0x66), r.aToB().end());
            }
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
                    splice.get(GUARD_SECONDS, TimeUnit.SECONDS); destination.get(GUARD_SECONDS, TimeUnit.SECONDS);
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

    /** Cancelling while the direction waits for data, or for the target's capacity, cancels that wait and releases what it holds. */
    @Test void cancellationEndsTheReadAndTheWriteWait() throws Exception {
        for (boolean write : new boolean[] {false, true}) {
            try (EventLoops loops = EventLoops.create(3)) {
                var cfg = ZmuxConfig.builder().eventLoops(loops).initialStreamReceiveWindow(4096).maxStreamReceiveWindow(4096).build();
                try (var relay = new Relay(cfg); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
                    var control = new SpliceAbort();
                    var limiter = new FixedBudget(4096);
                    var options = SpliceOptions.builder().abort(control);
                    if (write) options.aToB(limiter);
                    var splice = Zmux.splice(relay.a, relay.b, options.build()).toCompletableFuture();
                    try (var peer = relay.destination()) {
                        if (write) {
                            tasks.submit(() -> { try { relay.endpoint.write(new byte[32768]); } catch (IOException expected) {} return null; });
                            await(limiter.exhausted);
                            SessionTestSupport.await(relay.b.session, () -> relay.b.sendCapacity() == 0);
                        } else assertEquals(7, peer.inputStream().read());
                        assertTrue(control.abort(0x55));
                        var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
                        assertEquals(new SpliceEnd.Cancelled(0x55), result.aToB().end(), "write " + write);
                        assertEquals(new SpliceEnd.Cancelled(0x55), result.bToA().end(), "write " + write);
                        peer.setReadTimeout(Duration.ofSeconds(GUARD_SECONDS));
                        var reset = assertThrows(StreamResetException.class, () -> SessionTest.readAll(peer));
                        assertEquals(0x55, reset.code());
                    }
                }
                Leaks.none(loops);
            }
        }
    }

    /** Cancelling the stage while datagram and stream receives wait ends both without a result. */
    @Test void cancellationEndsTheDatagramWait() throws Exception {
        try (EventLoops loops = EventLoops.create(2);
             var relay = new Relay(ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).build())) {
            var splice = Zmux.splice(relay.a, relay.b).toCompletableFuture();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read());
                assertTrue(splice.cancel(true), () -> "the splice ended first: " + splice);
                assertThrows(CancellationException.class, () -> splice.get(GUARD_SECONDS, TimeUnit.SECONDS));
                var reset = assertThrows(StreamResetException.class, () -> SessionTest.readAll(relay.endpoint));
                assertEquals(io.zmux.internal.wire.TransportError.CANCELLED, reset.code());
            }
        }
    }

    /**
     * The cancellation itself ends the splice on another thread, and that can finish before the cancelling
     * thread completes the future. Forced here: the cancel hook returns only after the splice decided; the
     * future must still end cancelled, and cancel report true.
     */
    @Test void cancellationWinsWhenTheSpliceDecidesBeforeTheFutureCompletes() throws Exception {
        try (EventLoops loops = EventLoops.create(2);
             var relay = new Relay(ZmuxConfig.builder().eventLoops(loops).build())) {
            var splice = Zmux.splice(relay.a, relay.b);
            var operation = ZmuxFuture.class.getDeclaredField("operation");
            operation.setAccessible(true);
            var cancelOperation = (java.util.function.BooleanSupplier) operation.get(splice);
            operation.set(splice, (java.util.function.BooleanSupplier) () -> {
                boolean cancelling = cancelOperation.getAsBoolean();
                try {
                    var reset = assertThrows(StreamResetException.class, () -> SessionTest.readAll(relay.endpoint));
                    assertEquals(io.zmux.internal.wire.TransportError.CANCELLED, reset.code());
                    assertInstanceOf(StreamStoppedException.class, await(sendEnd(relay.endpoint)));
                } catch (Exception error) {
                    throw new AssertionError(error);
                }
                return cancelling;
            });
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read());
                assertTrue(splice.cancel(true), () -> "the splice published before the cancellation completed it: " + splice);
                assertTrue(splice.isCancelled());
                assertThrows(CancellationException.class, () -> splice.get(GUARD_SECONDS, TimeUnit.SECONDS));
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
                var result = splice.get(GUARD_SECONDS, TimeUnit.SECONDS);
                assertEquals(new SpliceEnd.DownstreamLost(), result.aToB().end());
                assertEquals(new SpliceEnd.UpstreamLost(), result.bToA().end());
                var reset = assertThrows(StreamResetException.class, () -> SessionTest.readAll(relay.endpoint));
                assertEquals(io.zmux.carrier.PipeIo.CARRIER_FAILED, reset.code());
            }
        }
    }
}
