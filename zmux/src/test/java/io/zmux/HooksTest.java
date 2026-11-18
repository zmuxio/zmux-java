package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.helper.Limiter;
import io.zmux.helper.LimitedStreams;
import java.io.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class HooksTest {
    static class Budget implements Limiter {
        final AtomicLong charged = new AtomicLong();
        /** Net bytes taken: allowances handed out minus what came back. */
        @Override public CompletionStage<Long> acquire(long min, long max) { long n = Math.min(max, 2); charged.addAndGet(n); return CompletableFuture.completedFuture(n); }
        @Override public long tryAcquire(long min, long max) { assertFalse(io.zmux.internal.loop.SessionHost.onHostThread()); long n = max <= 4 ? max : 0; charged.addAndGet(n); return n; }
        @Override public void refund(long bytes) { assertFalse(io.zmux.internal.loop.SessionHost.onHostThread()); charged.addAndGet(-bytes); }
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
        AtomicReference<CompletableFuture<Long>> waiting = new AtomicReference<>();
        Limiter paused = new Limiter() {
            @Override public CompletionStage<Long> acquire(long min, long max) {
                var f = new CompletableFuture<Long>(); waiting.set(f); return f;
            }
            @Override public long tryAcquire(long min, long max) { return 0; }
            @Override public void refund(long n) { fail("a cancelled wait took nothing"); }
        };
        Thread thread = Thread.ofVirtual().start(() -> assertThrows(InterruptedIOException.class,
                () -> LimitedStreams.input(new ByteArrayInputStream(new byte[] {1}), paused).read()));
        ListenerResourcesTest.until(() -> waiting.get() != null);
        thread.interrupt(); thread.join(5000);
        assertFalse(thread.isAlive()); assertTrue(waiting.get().isCancelled());
    }

    @Test void permitsRefundUnusedBytesAndAreCancelSafe() throws Exception {
        var budget = new SpliceTest.FixedBudget(10);
        try (var p = io.zmux.helper.Permit.tryAcquire(budget, 1, 2)) {
            assertEquals(2, p.size()); assertEquals(8, budget.left());
            p.consume(1);
        }
        assertEquals(9, budget.left());
        try (var p = io.zmux.helper.Permit.acquire(budget, 1, 4).toCompletableFuture().get()) { assertEquals(4, p.size()); }
        assertEquals(9, budget.left());
        assertFalse(io.zmux.helper.Permit.police(budget, 10)); assertEquals(9, budget.left());
        assertTrue(io.zmux.helper.Permit.police(budget, 9)); assertEquals(0, budget.left());
        var waiting = io.zmux.helper.Permit.acquire(budget, 5, 5).toCompletableFuture();
        assertTrue(waiting.cancel(true));
        budget.refund(9); // the cancelled wait takes nothing
        assertEquals(9, budget.left());
    }

    /** Records the arguments the limiter sees; grants what grant says. */
    static final class Recording implements Limiter {
        final java.util.List<long[]> calls = new java.util.concurrent.CopyOnWriteArrayList<>();
        final AtomicLong refunded = new AtomicLong();
        volatile java.util.function.LongUnaryOperator grant = max -> max;
        volatile RuntimeException fail;
        @Override public CompletionStage<Long> acquire(long min, long max) {
            calls.add(new long[] {min, max});
            if (fail != null) throw fail;
            return CompletableFuture.completedFuture(grant.applyAsLong(max));
        }
        @Override public long tryAcquire(long min, long max) {
            calls.add(new long[] {min, max});
            if (fail != null) throw fail;
            return grant.applyAsLong(max);
        }
        @Override public void refund(long n) {
            refunded.addAndGet(n);
            if (fail != null) throw fail;
        }
    }

    @Test void permitClampsArgumentsAndAppliesTheFailureRules() throws Exception {
        var l = new Recording();
        // max <= 0 takes nothing without a call; min < 1 is 1; min > max is max.
        assertTrue(io.zmux.helper.Permit.acquire(l, 5, 0).toCompletableFuture().get().isEmpty());
        assertTrue(io.zmux.helper.Permit.tryAcquire(l, 1, -3).isEmpty());
        assertTrue(l.calls.isEmpty());
        assertEquals(5, io.zmux.helper.Permit.acquire(l, 0, 5).toCompletableFuture().get().size());
        assertArrayEquals(new long[] {1, 5}, l.calls.get(0));
        assertEquals(4, io.zmux.helper.Permit.tryAcquire(l, 9, 4).size());
        assertArrayEquals(new long[] {4, 4}, l.calls.get(1));
        assertEquals(Long.MAX_VALUE, io.zmux.helper.Permit.tryAcquire(l, -1, Long.MAX_VALUE).size());
        assertArrayEquals(new long[] {1, Long.MAX_VALUE}, l.calls.get(2));
        // A unit granted (n, n) below n (a burst) passes whole and stays taken.
        l.grant = max -> 2;
        assertTrue(io.zmux.helper.Permit.police(l, 10));
        assertTrue(io.zmux.helper.Permit.police(l, 0), "an empty unit passes");
        assertEquals(4, l.calls.size(), "without a call");
        // An invalid grant: acquire fails the operation, tryAcquire takes nothing; a positive one is refunded.
        l.grant = max -> max + 1;
        var invalid = assertThrows(ExecutionException.class, () -> io.zmux.helper.Permit.acquire(l, 1, 3).toCompletableFuture().get());
        assertInstanceOf(CallbackFailedException.class, invalid.getCause());
        assertEquals(4, l.refunded.get());
        assertTrue(io.zmux.helper.Permit.tryAcquire(l, 3, 3).isEmpty());
        assertEquals(8, l.refunded.get());
        l.grant = max -> 0;
        assertInstanceOf(CallbackFailedException.class, assertThrows(ExecutionException.class,
                () -> io.zmux.helper.Permit.acquire(l, 1, 3).toCompletableFuture().get()).getCause());
        assertFalse(io.zmux.helper.Permit.police(l, 3));
        // A throwing limiter: acquire fails, tryAcquire counts as nothing taken, refund failures are ignored.
        l.grant = max -> max;
        l.fail = new IllegalStateException("broken");
        var thrown = assertThrows(ExecutionException.class, () -> io.zmux.helper.Permit.acquire(l, 1, 3).toCompletableFuture().get());
        assertSame(l.fail, thrown.getCause().getCause());
        assertTrue(io.zmux.helper.Permit.tryAcquire(l, 1, 3).isEmpty());
        assertFalse(io.zmux.helper.Permit.police(l, 3));
        io.zmux.helper.Permit.refund(l, 5);
        io.zmux.helper.Permit.refund(l, 0);
        assertEquals(13, l.refunded.get(), "refunds of 0 are not passed on");
    }

    @Test void permitsSharingALimiterNeverOverGrant() throws Exception {
        var budget = new SpliceTest.FixedBudget(10000);
        AtomicLong used = new AtomicLong();
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var all = new java.util.ArrayList<Future<?>>();
            for (int t = 0; t < 8; t++) {
                int id = t;
                all.add(tasks.submit(() -> {
                    for (int i = 0; i < 2000; i++) {
                        try (var p = id % 2 == 0 ? io.zmux.helper.Permit.tryAcquire(budget, 1, 7)
                                : io.zmux.helper.Permit.acquire(budget, 3, 3).toCompletableFuture().get(5, TimeUnit.SECONDS)) {
                            assertTrue(budget.left() >= 0);
                            // Waiters only borrow; pollers keep one byte each until 9000 are used, so waiters are always served.
                            long take = id % 2 == 0 && used.get() < 9000 ? Math.min(p.size(), 1) : 0;
                            p.consume(take); used.addAndGet(take);
                        }
                    }
                    return null;
                }));
            }
            for (var f : all) f.get(20, TimeUnit.SECONDS);
        }
        assertTrue(budget.lowest >= 0);
        assertEquals(10000, budget.left() + used.get(), "grants minus refunds equal what was used");
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
            @Override public CompletionStage<Long> acquire(long min, long max) {
                if (asks.incrementAndGet() == 1) { charged.addAndGet(2); return CompletableFuture.completedFuture(2L); }
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
            ZmuxConfig cc = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).datagramLimiter(new io.zmux.helper.DatagramLimiter(send, null)).build();
            ZmuxConfig sc = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).datagramLimiter(new io.zmux.helper.DatagramLimiter(null, receive)).build();
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
            Leaks.none(loops);
        }
    }

    @Test void aFailingDatagramLimiterDropsTheDatagramAndLeavesTheSessionUsable() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var failing = new io.zmux.helper.Limiter() {
                final AtomicInteger calls = new AtomicInteger();
                @Override public java.util.concurrent.CompletionStage<Long> acquire(long min, long max) { return java.util.concurrent.CompletableFuture.completedFuture(max); }
                @Override public long tryAcquire(long min, long max) {
                    // The first datagram of the direction throws, the second grants beyond max, the rest pass.
                    return switch (calls.incrementAndGet()) { case 1 -> throw new IllegalStateException("limiter broke"); case 2 -> max + 1; default -> max; };
                }
                @Override public void refund(long n) { }
            };
            var failingReceive = new io.zmux.helper.Limiter() {
                final AtomicInteger calls = new AtomicInteger();
                @Override public java.util.concurrent.CompletionStage<Long> acquire(long min, long max) { return java.util.concurrent.CompletableFuture.completedFuture(max); }
                @Override public long tryAcquire(long min, long max) { if (calls.incrementAndGet() == 1) throw new IllegalStateException("limiter broke"); return max; }
                @Override public void refund(long n) { }
            };
            ZmuxConfig cc = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).datagramLimiter(new io.zmux.helper.DatagramLimiter(failing, null)).build();
            ZmuxConfig sc = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).datagramLimiter(new io.zmux.helper.DatagramLimiter(null, failingReceive)).build();
            try (ZmuxListener listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(sc))) {
                var accepted = ex.submit((Callable<ZmuxSession>)listener::accept);
                var c = Zmux.connect(listener.localAddress(), cc); var s = accepted.get();
                assertFalse(c.datagrams().send(ByteBuffer.wrap(new byte[] {1})), "a throwing limiter drops the datagram");
                assertFalse(c.datagrams().send(ByteBuffer.wrap(new byte[] {2})), "an invalid grant drops the datagram");
                assertEquals(2, c.stats().datagramsDroppedSend());
                // The receiving limiter throws on the first datagram that arrives: it is dropped, the next is delivered.
                assertTrue(c.datagrams().send(ByteBuffer.wrap(new byte[] {3})));
                assertTrue(c.datagrams().send(ByteBuffer.wrap(new byte[] {4})));
                assertArrayEquals(new byte[] {4}, s.datagrams().receive());
                assertEquals(1, s.stats().datagramsDroppedLimit());
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
            Leaks.none(loops);
        }
    }

    /** A grant taken for a received datagram goes back when the session ended before it could take the answer. */
    @Test void aReceiveGrantIsRefundedWhenTheSessionEndedBeforeTheAnswerWasPosted() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            CountDownLatch entered = new CountDownLatch(1), go = new CountDownLatch(1);
            AtomicLong refunded = new AtomicLong();
            var receive = new io.zmux.helper.Limiter() {
                @Override public java.util.concurrent.CompletionStage<Long> acquire(long min, long max) { return CompletableFuture.completedFuture(max); }
                @Override public long tryAcquire(long min, long max) {
                    entered.countDown();
                    try { go.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    return max;
                }
                @Override public void refund(long n) { refunded.addAndGet(n); }
            };
            ZmuxConfig cc = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).build();
            ZmuxConfig sc = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).datagramLimiter(new io.zmux.helper.DatagramLimiter(null, receive)).build();
            try (ZmuxListener listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(sc))) {
                var accepted = ex.submit((Callable<ZmuxSession>)listener::accept);
                var c = Zmux.connect(listener.localAddress(), cc); var s = accepted.get();
                assertTrue(c.datagrams().send(ByteBuffer.wrap(new byte[] {1, 2, 3})));
                assertTrue(entered.await(5, TimeUnit.SECONDS), "the limiter was asked for the received datagram");
                s.close(); s.awaitClosed(); c.close(); c.awaitClosed();
                go.countDown(); // granted after the session is gone
                ListenerResourcesTest.until(() -> refunded.get() == 3);
            }
        }
    }

    @Test void tracerIsSerialBoundedAndCannotBlockItsOwnSession() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), closed = new CountDownLatch(1);
            AtomicLong dropped = new AtomicLong(); AtomicBoolean blocked = new AtomicBoolean();
            AtomicReference<ZmuxSession> ref = new AtomicReference<>(); AtomicReference<SessionId> seen = new AtomicReference<>();
            var cfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(2).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).tracer(new Tracer() {
                        @Override public void established(SessionId session) {
                            seen.set(session);
                            entered.countDown();
                            try { assertTrue(release.await(5, TimeUnit.SECONDS)); } catch (InterruptedException e) { throw new AssertionError(e); }
                            assertThrows(IllegalStateException.class, () -> ref.get().ping()); blocked.set(true);
                        }
                        @Override public void dropped(SessionId session, long n) { if (!session.equals(seen.get())) return; dropped.addAndGet(n); throw new IllegalArgumentException("failed loss notification"); }
                        @Override public void closed(SessionId session, SessionError error) { if (session.equals(seen.get())) closed.countDown(); }
                    }, 1).build();
            // Trace only the client, so both contexts cannot wait on the same gate.
            var serverConfig = ZmuxConfig.builder().eventLoops(loops).maxPipes(2).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).build();
            try (ZmuxListener listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(serverConfig))) {
                var accepted = ex.submit((Callable<ZmuxSession>)listener::accept);
                var c = Zmux.connect(listener.localAddress(), cfg); ref.set(c); var s = accepted.get();
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                var pipe = c.pipes().getFirst();
                for (int i=0; i<20; i++) pipe.update(PipeUpdate.builder().sendPriority(i).build());
                assertTrue(c.ping().toNanos() > 0);
                release.countDown();
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
                assertTrue(closed.await(5, TimeUnit.SECONDS));
                assertTrue(blocked.get()); assertTrue(dropped.get() > 0);
                assertEquals(c.id(), seen.get(), "the tracer names its session"); assertNotEquals(c.id(), s.id());
            } finally { release.countDown(); }
        }
    }

    /** A full backlog drops the event that arrives, counts it, and never drops established or closed: after a blocked established, a session end delivers nothing but the loss count and closed. */
    @Test void fullTracerBacklogDropsTheNewArrivalAndKeepsOnlyEstablishedAndClosed() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), closed = new CountDownLatch(1);
            AtomicReference<SessionId> seen = new AtomicReference<>();
            List<String> calls = Collections.synchronizedList(new ArrayList<>());
            var cfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(2).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).tracer(new Tracer() {
                        @Override public void established(SessionId session) {
                            seen.set(session); calls.add("established"); entered.countDown();
                            try { assertTrue(release.await(5, TimeUnit.SECONDS)); } catch (InterruptedException e) { throw new AssertionError(e); }
                        }
                        @Override public void event(SessionId session, SessionEvent event) { if (session.equals(seen.get())) calls.add("event"); }
                        @Override public void dropped(SessionId session, long n) { if (session.equals(seen.get()) && n > 0) calls.add("dropped"); }
                        @Override public void closed(SessionId session, SessionError error) { if (session.equals(seen.get())) { calls.add("closed"); closed.countDown(); } }
                    }, 1).build();
            var serverConfig = ZmuxConfig.builder().eventLoops(loops).maxPipes(2).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).build();
            try (ZmuxListener listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(serverConfig))) {
                var accepted = ex.submit((Callable<ZmuxSession>)listener::accept);
                var c = Zmux.connect(listener.localAddress(), cfg); var s = accepted.get();
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                // The backlog (capacity 1) holds the blocked established call: everything that follows arrives to a full one.
                // A pipe's end is no exception: its terminal event is dropped like any other arrival.
                ZmuxPipe added = c.addPipe(listener.localAddress(), PipeOptions.defaults());
                added.close();
                // The pipe is retired before its events are read: they are folded into the retirement summary, the only report of
                // its end (a PipeClosed read earlier would be one as well).
                added.awaitClosed(Duration.ofSeconds(10));
                long pipe = added.id().value();
                SessionEvent e;
                do {
                    e = c.nextEvent(Duration.ofSeconds(10));
                    assertNotNull(e, "the session's own event stream reports the pipe's end");
                } while (!(e instanceof SessionEvent.PipeClosed pc && pc.pipe().value() == pipe)
                        && !(e instanceof SessionEvent.PipesRetired pr && pr.last().stream().anyMatch(p -> p.value() == pipe)));
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
                release.countDown();
                assertTrue(closed.await(5, TimeUnit.SECONDS));
                assertEquals(List.of("established", "dropped", "closed"), calls);
            } finally { release.countDown(); }
        }
    }
}
