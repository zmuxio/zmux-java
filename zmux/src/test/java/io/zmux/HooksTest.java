package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.helper.Limiter;
import io.zmux.helper.LimitedStreams;
import java.io.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class HooksTest {
    static class Budget implements Limiter {
        final AtomicInteger charged = new AtomicInteger();
        @Override public CompletionStage<Integer> ready(int min, int max) { return CompletableFuture.completedFuture(Math.min(max, 2)); }
        @Override public int tryReady(int max) { assertFalse(io.zmux.internal.loop.SessionHost.onHostThread()); return max <= 4 ? max : 0; }
        @Override public void charge(int bytes) { assertFalse(io.zmux.internal.loop.SessionHost.onHostThread()); charged.addAndGet(bytes); }
    }

    @Test void streamWrappersChargeOnlyForwardedBytesAndDoNotBypassLimits() throws Exception {
        Budget budget = new Budget();
        var out = new ByteArrayOutputStream();
        var limited = LimitedStreams.output(out, budget);
        limited.write(new byte[] {1,2,3,4,5});
        assertEquals(5, budget.charged.get());
        var input = LimitedStreams.input(new ByteArrayInputStream(out.toByteArray()), budget);
        assertEquals(2, input.skip(2));
        var forwarded = new ByteArrayOutputStream();
        assertEquals(3, input.transferTo(forwarded));
        assertArrayEquals(new byte[] {3,4,5}, forwarded.toByteArray());
        assertEquals(10, budget.charged.get());
        AtomicReference<CompletableFuture<Integer>> waiting = new AtomicReference<>();
        Limiter paused = new Limiter() {
            @Override public CompletionStage<Integer> ready(int min, int max) {
                var f = new CompletableFuture<Integer>(); waiting.set(f); return f;
            }
            @Override public int tryReady(int max) { return 0; }
            @Override public void charge(int n) { fail("cancelled readiness cannot charge"); }
        };
        Thread thread = Thread.ofVirtual().start(() -> assertThrows(InterruptedIOException.class,
                () -> LimitedStreams.input(new ByteArrayInputStream(new byte[] {1}), paused).read()));
        ListenerResourcesTest.until(() -> waiting.get() != null);
        thread.interrupt(); thread.join(5000);
        assertFalse(thread.isAlive()); assertTrue(waiting.get().isCancelled());
    }

    @Test void interruptedOutputReportsAllCompletedChunks() throws Exception {
        Budget budget = new Budget();
        AtomicInteger writes = new AtomicInteger();
        OutputStream partial = new OutputStream() {
            @Override public void write(int b) { }
            @Override public void write(byte[] bytes, int off, int len) throws IOException {
                if (writes.incrementAndGet() == 2) {
                    InterruptedIOException e = new InterruptedIOException("partial"); e.bytesTransferred = 1; throw e;
                }
            }
        };
        InterruptedIOException error = assertThrows(InterruptedIOException.class,
                () -> LimitedStreams.output(partial, budget).write(new byte[5]));
        assertEquals(3, error.bytesTransferred); assertEquals(3, budget.charged.get());
        AtomicInteger asks = new AtomicInteger();
        Budget cancelled = new Budget() {
            @Override public CompletionStage<Integer> ready(int min, int max) {
                if (asks.incrementAndGet() == 1) return CompletableFuture.completedFuture(2);
                Thread.currentThread().interrupt(); return new CompletableFuture<>();
            }
        };
        try {
            error = assertThrows(InterruptedIOException.class, () -> LimitedStreams.output(new ByteArrayOutputStream(), cancelled).write(new byte[5]));
            assertEquals(2, error.bytesTransferred); assertEquals(2, cancelled.charged.get());
        } finally { Thread.interrupted(); }
    }

    @Test void datagramLimitersRunOffLoopAndChargeAcceptedPayloads() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            Budget send = new Budget(), receive = new Budget();
            ZmuxConfig cc = ZmuxConfig.builder().eventLoops(loops).datagrams(true).datagramLimiters(send, null).build();
            ZmuxConfig sc = ZmuxConfig.builder().eventLoops(loops).datagrams(true).datagramLimiters(null, receive).build();
            try (ZmuxListener listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(sc))) {
                var accepted = ex.submit((Callable<ZmuxSession>)listener::accept);
                var c = Zmux.connect(listener.localAddress(), cc); var s = accepted.get();
                assertFalse(c.datagrams().send(ByteBuffer.wrap(new byte[5])), "a limiter drop is reported");
                assertTrue(c.datagrams().send(ByteBuffer.wrap(new byte[] {1,2,3})));
                assertArrayEquals(new byte[] {1,2,3}, s.datagrams().receive());
                ListenerResourcesTest.until(() -> receive.charged.get() == 3);
                assertEquals(3, send.charged.get());
                assertEquals(1, c.stats().datagramsDroppedSend());
                var stream = c.openStream();
                stream.write(new byte[] {1});
                var remote = s.acceptStream();
                stream.sendDatagram(1 << 20, ByteBuffer.wrap(new byte[] {9,8}));
                StreamDatagram datagram = remote.receiveDatagram();
                assertEquals(1 << 20, datagram.context());
                assertArrayEquals(new byte[] {9,8}, datagram.payload());
                ListenerResourcesTest.until(() -> receive.charged.get() == 5);
                assertEquals(5, send.charged.get());
                stream.close(); remote.close(); c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void tracerIsSerialBoundedAndCannotBlockItsOwnSession() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), closed = new CountDownLatch(1);
            AtomicLong dropped = new AtomicLong(); AtomicBoolean blocked = new AtomicBoolean();
            AtomicReference<ZmuxSession> ref = new AtomicReference<>();
            var cfg = ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(2).withAuth(PipeAuth.insecure())).observer(new SessionObserver() {
                        @Override public void established() {
                            entered.countDown();
                            try { assertTrue(release.await(5, TimeUnit.SECONDS)); } catch (InterruptedException e) { throw new AssertionError(e); }
                            assertThrows(IllegalStateException.class, () -> ref.get().ping()); blocked.set(true);
                        }
                        @Override public void dropped(long n) { dropped.addAndGet(n); throw new IllegalArgumentException("failed loss notification"); }
                        @Override public void closed(SessionError error) { closed.countDown(); }
                    }, 1).build();
            // Trace only the client, so both contexts cannot wait on the same gate.
            var serverConfig = ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(2).withAuth(PipeAuth.insecure())).build();
            try (ZmuxListener listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(serverConfig))) {
                var accepted = ex.submit((Callable<ZmuxSession>)listener::accept);
                var c = Zmux.connect(listener.localAddress(), cfg); ref.set(c); var s = accepted.get();
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                var pipe = c.paths().list().getFirst();
                for (int i=0; i<20; i++) pipe.setSendPriority(i);
                assertTrue(c.ping().toNanos() > 0);
                release.countDown();
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
                assertTrue(closed.await(5, TimeUnit.SECONDS));
                assertTrue(blocked.get()); assertTrue(dropped.get() > 0);
            } finally { release.countDown(); }
        }
    }
}
