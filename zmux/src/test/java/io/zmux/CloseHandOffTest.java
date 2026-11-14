package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * close() returns once CONNECTION_CLOSE is handed to the carriers, within its bound;
 * adding a pipe waits for the peer's attachment unless returnOnReply.
 */
@Timeout(30)
class CloseHandOffTest {
    static final PipeOptions NO_WAIT = PipeOptions.builder().returnOnReply(true).build();

    /** A writer whose writes stop completing once held: the carrier takes nothing more. */
    static final class Stalling implements PipeWriter {
        final PipeWriter delegate; final AtomicBoolean held = new AtomicBoolean();
        Stalling(PipeWriter delegate) { this.delegate = delegate; }
        @Override public void write(OutboundBatch batch, Completion done) { if (!held.get()) delegate.write(batch, done); }
        @Override public void close() { delegate.close(); }
        @Override public void abort(Throwable error) { delegate.abort(error); }
    }

    static ZmuxConfig.Builder resuming(ZmuxConfig.Builder b) {
        return b.maxPipes(2).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                .resumeTimeout(Duration.ofSeconds(20)).noProbation();
    }

    /** The child process: connects, opens a stream, closes and halts as soon as close returned. */
    public static final class Child {
        public static void main(String[] args) throws Exception {
            var client = Zmux.connect(new java.net.InetSocketAddress("127.0.0.1", Integer.parseInt(args[0])),
                    resuming(ZmuxConfig.builder()).build());
            var s = client.openStream(); s.write(new byte[] {1});
            System.in.read(); // The parent accepted the stream.
            client.close();
            Runtime.getRuntime().halt(0);
        }
    }

    @Test void aProcessExitingRightAfterCloseStillTellsAResumingPeer() throws Exception {
        String launcher = java.nio.file.Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String cp = String.join(java.io.File.pathSeparator, System.getProperty("java.class.path"), System.getProperty("jdk.module.path", ""));
        try (EventLoops loops = EventLoops.create(1);
             var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(resuming(ZmuxConfig.builder().eventLoops(loops)).build()))) {
            Process child = new ProcessBuilder(launcher, "-cp", cp, Child.class.getName(), String.valueOf(listener.localAddress().getPort()))
                    .redirectError(ProcessBuilder.Redirect.INHERIT).start();
            try {
                var server = listener.accept(Duration.ofSeconds(10));
                server.acceptStream().close();
                child.getOutputStream().write('\n'); child.getOutputStream().flush();
                assertTrue(child.waitFor(10, TimeUnit.SECONDS));
                var why = server.closed().get(5, TimeUnit.SECONDS);
                var app = assertInstanceOf(SessionError.ApplicationClosed.class, why, "resume held the session: " + why);
                assertEquals(0, app.code());
                assertEquals(0, server.stats().suspensions());
            } finally {
                child.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test void aCarrierThatTakesNothingEndsTheCloseAtTheBound() throws Exception {
        long saved = io.zmux.internal.core.Session.closeLimit;
        io.zmux.internal.core.Session.closeLimit = TimeUnit.MILLISECONDS.toNanos(300);
        try (EventLoops loops = EventLoops.create(2); var listener = ZmuxListener.of(ListenerConfig.of(PipePoolTest.config(loops, 2)))) {
            var io = MemoryPipe.pair(Framing.BYTE_STREAM);
            listener.adapter().handle(io[1], JoinHint.inBand(TestAddr.peer("stall"))).closed();
            var writer = new Stalling(io[0].writer());
            var client = Zmux.open(PipeIo.of(io[0].reader(), writer), Role.CLIENT, PipePoolTest.config(loops, 2));
            var server = listener.accept(Duration.ofSeconds(3));
            client.awaitEstablished(System.nanoTime() + TimeUnit.SECONDS.toNanos(3));
            writer.held.set(true);
            var again = CompletableFuture.runAsync(() -> { sleep(50); client.close(); }, Executors.newVirtualThreadPerTaskExecutor());
            long t0 = System.nanoTime();
            client.close();
            long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            assertTrue(took >= 250 && took < 1_000, "close took " + took + " ms");
            again.get(200, TimeUnit.MILLISECONDS); // A concurrent call returns no later than the first.
            // The session ended locally at the bound; its future completes once the loop released the carriers.
            client.closed().get(200, TimeUnit.MILLISECONDS);
            long t1 = System.nanoTime();
            client.close();
            assertTrue(System.nanoTime() - t1 < TimeUnit.MILLISECONDS.toNanos(50), "an ended session's close returns at once");
            server.close(); server.awaitClosed();
        } finally {
            io.zmux.internal.core.Session.closeLimit = saved;
        }
    }

    /** Two sessions over a memory pipe whose write sides stall on demand. */
    private static ZmuxSession[] stalledPair(ZmuxConfig cfg, Stalling[] writers) throws Exception {
        var io = MemoryPipe.pair(Framing.BYTE_STREAM);
        writers[0] = new Stalling(io[0].writer());
        writers[1] = new Stalling(io[1].writer());
        var server = CompletableFuture.supplyAsync(() -> {
            try { return Zmux.open(PipeIo.of(io[1].reader(), writers[1]), Role.SERVER, cfg); }
            catch (Exception e) { throw new CompletionException(e); }
        }, r -> Thread.ofVirtual().start(r));
        var client = Zmux.open(PipeIo.of(io[0].reader(), writers[0]), Role.CLIENT, cfg);
        client.awaitEstablished(System.nanoTime() + TimeUnit.SECONDS.toNanos(3));
        return new ZmuxSession[] {client, server.get(3, TimeUnit.SECONDS)};
    }

    /** An abort whose CONNECTION_CLOSE a stuck carrier never takes still ends the session. */
    @Test void anAbortOnAStuckCarrierEndsTheSession() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var w = new Stalling[2];
            var s = stalledPair(ZmuxConfig.builder().eventLoops(loops).build(), w);
            w[0].held.set(true);
            s[0].abort(3, "stuck");
            assertInstanceOf(SessionError.ApplicationClosed.class, s[0].closed().get(5, TimeUnit.SECONDS));
            s[1].abort(0, "");
            s[1].closed().get(5, TimeUnit.SECONDS);
        }
    }

    /** An idle timeout whose CONNECTION_CLOSE a stuck carrier never takes still ends the session within the close bound. */
    @Test void anIdleTimeoutOnAStuckCarrierEndsTheSession() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var w = new Stalling[2];
            var s = stalledPair(ZmuxConfig.builder().eventLoops(loops).maxIdleTimeout(Duration.ofMillis(300)).build(), w);
            w[0].held.set(true);
            w[1].held.set(true);
            long t0 = System.nanoTime();
            for (var x : s) assertInstanceOf(SessionError.IdleTimeout.class, x.closed().get(5, TimeUnit.SECONDS));
            long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            assertTrue(took < 3_000, "the sessions ended after " + took + " ms");
        }
    }

    @Test void closeOnAStalledLoopStillReturnsAtTheBound() throws Exception {
        long saved = io.zmux.internal.core.Session.closeLimit;
        io.zmux.internal.core.Session.closeLimit = TimeUnit.MILLISECONDS.toNanos(300);
        CountDownLatch release = new CountDownLatch(1), entered = new CountDownLatch(1);
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 2))) {
            try {
                // The flush operation then stays queued: close must not wait for the loop past its bound.
                assertTrue(f.client.driver.executeCarrier(() -> {
                    entered.countDown();
                    try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }));
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                long t0 = System.nanoTime();
                f.client.close();
                long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                assertTrue(took < 1_500, "close took " + took + " ms on a stalled loop");
            } finally {
                release.countDown();
            }
            f.client.closed().get(3, TimeUnit.SECONDS);
        } finally {
            io.zmux.internal.core.Session.closeLimit = saved;
        }
    }

    /** close() on the session's loop returns at once, and still flushes written data before CONNECTION_CLOSE. */
    @Test void closeOnTheLoopStillFlushes() throws Exception {
        int n = 4 << 20;
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 2))) {
            var st = f.client.openStream();
            st.setSendBuffer(2L * n);
            st.write(new byte[n], 0, n); // mostly unsent: the peer's window holds it back until it reads
            st.finish();
            var in = f.server.acceptStream(Duration.ofSeconds(3));
            CountDownLatch called = new CountDownLatch(1);
            long[] took = new long[1];
            assertTrue(f.client.driver.executeCarrier(() -> {
                long t0 = System.nanoTime();
                f.client.close();
                took[0] = System.nanoTime() - t0;
                called.countDown();
            }));
            assertTrue(called.await(3, TimeUnit.SECONDS));
            assertTrue(took[0] < TimeUnit.MILLISECONDS.toNanos(100), "close blocked the loop");
            long got = 0;
            byte[] b = new byte[1 << 16];
            for (int r; (r = in.read(b, 0, b.length)) > 0; ) got += r;
            assertEquals(n, got, "the data written before close arrived");
            var why = assertInstanceOf(SessionError.ApplicationClosed.class, f.server.closed().get(5, TimeUnit.SECONDS));
            assertEquals(0, why.code());
        }
    }

    @Test void aHealthyCarrierReturnsCloseWellBeforeTheBound() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 2))) {
            long t0 = System.nanoTime();
            f.client.abort(3, "done");
            assertTrue(System.nanoTime() - t0 < TimeUnit.MILLISECONDS.toNanos(500));
            var app = assertInstanceOf(SessionError.ApplicationClosed.class, f.server.closed().get(3, TimeUnit.SECONDS));
            assertEquals(3, app.code());
        }
    }

    @Test void addingWaitsUntilThePeerAttachedThePipe() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 3))) {
            long t0 = System.nanoTime();
            ZmuxPipe p = PeerAttachedTest.lateJoin(f, 300, PipeOptions.defaults());
            assertTrue(System.nanoTime() - t0 >= TimeUnit.MILLISECONDS.toNanos(250), "returned before the Confirm reached the acceptor");
            assertTrue(PeerAttachedTest.held(f.server, p));
            p.awaitPeerAttached(Duration.ZERO);

            long t1 = System.nanoTime();
            ZmuxPipe early = PeerAttachedTest.lateJoin(f, 300, NO_WAIT);
            assertTrue(System.nanoTime() - t1 < TimeUnit.MILLISECONDS.toNanos(250), "returnOnReply waited");
            assertFalse(PeerAttachedTest.held(f.server, early));
            early.awaitPeerAttached(Duration.ofSeconds(3));
        }
    }

    @Test void anUnattachedPipeAtTheDeadlineIsAbortedAndLeavesNothing() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).joinTimeout(Duration.ofMillis(400))
                    .maxPipes(3).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation().build();
            try (var f = new PipePoolTest.Fixture(cfg)) {
                long t0 = System.nanoTime();
                var e = assertThrows(SocketTimeoutException.class, () -> PeerAttachedTest.lateJoin(f, 2_000, PipeOptions.defaults()));
                long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                assertTrue(took >= 350 && took < 1_500, "the wait took " + took + " ms: " + e);
                PipePoolTest.await(() -> f.client.pipes().stream().allMatch(q -> q.id().value() == 0));
                assertFalse(f.client.isClosed());
            }
        }
    }

    @Test void aPipeRemovedBeforeItsAttachmentFailsTheAddWithPipeGone() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 3));
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            // The Confirm is held past the test: only the removal can end the add.
            var add = workers.submit(() -> PeerAttachedTest.lateJoin(f, 60_000, PipeOptions.defaults()));
            // Joined (the Reply verified) but not attached: the acceptor has not read the Confirm.
            PipePoolTest.await(() -> f.client.pipeStats().stream().anyMatch(q -> q.pipeId().value() != 0 && q.outbound() == PipeState.Outbound.ACTIVE));
            ZmuxPipe removed = f.client.pipes().stream().filter(q -> q.id().value() != 0).findFirst().orElseThrow();
            removed.abort();
            var e = assertThrows(ExecutionException.class, () -> add.get(3, TimeUnit.SECONDS));
            assertInstanceOf(PipeGoneException.class, e.getCause());
            // The failure ended the wait, not the retirement: the peer never learnt of the pipe,
            // so it retires only unilaterally, seconds after its PIPE_ABANDON was acknowledged.
            assertTrue(removed.stats().isPresent(), "the add waited for the retirement");

            var ended = workers.submit(() -> PeerAttachedTest.lateJoin(f, 2_000, PipeOptions.defaults()));
            PipePoolTest.await(() -> f.client.pipeStats().stream().anyMatch(q -> q.pipeId().value() != 0 && q.outbound() == PipeState.Outbound.ACTIVE));
            f.client.close();
            var c = assertThrows(ExecutionException.class, () -> ended.get(3, TimeUnit.SECONDS));
            assertInstanceOf(java.io.IOException.class, c.getCause());
            assertFalse(c.getCause() instanceof SocketTimeoutException || c.getCause() instanceof PipeGoneException, c.getCause().toString());
        }
    }

    /**
     * A close while the peer keeps writing: the closer half-closes after CONNECTION_CLOSE and
     * drains what the peer still sends in the background, so the peer receives the close.
     */
    @Test void aCloseUnderThePeersBulkWritesDrainsBeforeItReturns() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg))) {
                var closer = Zmux.connect(l.localAddress(), cfg);
                var peer = l.accept(Duration.ofSeconds(5));
                var bulk = workers.submit(() -> {
                    try (var st = peer.openStream()) {
                        byte[] b = new byte[64 << 10];
                        while (true) st.write(b);
                    } catch (java.io.IOException ended) { return true; }
                });
                // The closer never reads the stream: the peer's data fills its windows and socket.
                var st = closer.acceptStream(Duration.ofSeconds(5));
                assertNotNull(st);
                Thread.sleep(200);
                closer.close();
                var app = assertInstanceOf(SessionError.ApplicationClosed.class, peer.closed().get(3, TimeUnit.SECONDS));
                assertEquals(0, app.code());
                closer.awaitClosed();
                assertTrue(bulk.get(3, TimeUnit.SECONDS));
            }
        }
    }

    /** A stuck event loop holds neither a failed join's abort nor a splice's cleanup longer than a close's bound. */
    @Test void loopWaitsOfAbortsAndSpliceCleanupAreBounded() throws Exception {
        long saved = io.zmux.internal.core.Session.closeLimit;
        io.zmux.internal.core.Session.closeLimit = TimeUnit.MILLISECONDS.toNanos(200);
        try (EventLoops loops = EventLoops.create(1); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 2))) {
            var stream = f.client.openStream();
            var entered = new CountDownLatch(1);
            var proceed = new CountDownLatch(1);
            assertTrue(f.client.driver.executeCarrier(() -> {
                entered.countDown();
                try { proceed.await(); } catch (InterruptedException ignored) { }
            }));
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                long t0 = System.nanoTime();
                assertFalse(f.client.cancelJoinedPipe(99), "the stuck loop did not run the abort");
                assertFalse(Splice.ownerCleanup(stream, () -> { }), "the stuck loop did not run the cleanup");
                long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                assertTrue(took >= 350 && took < 2_000, "two bounded waits took " + took + " ms");
            } finally {
                proceed.countDown();
            }
            assertTrue(f.client.cancelJoinedPipe(99), "a running loop answers");
        } finally {
            io.zmux.internal.core.Session.closeLimit = saved;
        }
    }

    /** close returns only once the carrier's write side is shut, not when its shutdown is merely requested. */
    @Test void closeReturnsAfterTheWriteSideIsShut() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var listener = ZmuxListener.of(ListenerConfig.of(PipePoolTest.config(loops, 2)))) {
            var io = MemoryPipe.pair(Framing.BYTE_STREAM);
            listener.adapter().handle(io[1], JoinHint.inBand(TestAddr.peer("slow-shut"))).closed();
            var shut = new java.util.concurrent.atomic.AtomicLong();
            PipeWriter w = io[0].writer();
            PipeWriter slowShut = new PipeWriter() {
                @Override public void write(OutboundBatch batch, Completion done) { w.write(batch, done); }
                @Override public void close() { sleep(200); w.close(); shut.set(System.nanoTime()); }
                @Override public void abort(Throwable error) { w.abort(error); }
            };
            var client = Zmux.open(PipeIo.of(io[0].reader(), slowShut), Role.CLIENT, PipePoolTest.config(loops, 2));
            var server = listener.accept(Duration.ofSeconds(3));
            client.awaitEstablished(System.nanoTime() + TimeUnit.SECONDS.toNanos(3));
            client.close();
            long returned = System.nanoTime();
            assertTrue(shut.get() != 0 && shut.get() - returned <= 0, "close returned before the write side was shut");
            var app = assertInstanceOf(SessionError.ApplicationClosed.class, server.closed().get(3, TimeUnit.SECONDS));
            assertEquals(0, app.code());
        }
    }

    /** Over one socket channel, the sending half's close sends FIN and the receiving half keeps reading. */
    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    @Test void aChannelPipesSendingHalfClosesWithFin() throws Exception {
        try (var server = java.nio.channels.ServerSocketChannel.open().bind(NetworkTest.any());
             var ch = java.nio.channels.SocketChannel.open(server.getLocalAddress());
             var far = server.accept()) {
            PipeIo io = PipeChannel.pipe(ch, ch, Framing.BYTE_STREAM, 1 << 16);
            var got = new LinkedBlockingQueue<Object>();
            io.reader().start(new PipeReader.Sink() {
                @Override public void onData(java.nio.ByteBuffer data, Runnable release) { got.add(data.remaining()); release.run(); }
                @Override public void onEnd() { got.add("end"); }
                @Override public void onError(Throwable cause) { got.add(cause); }
            });
            io.writer().close();
            assertEquals(-1, far.read(java.nio.ByteBuffer.allocate(16)), "FIN, not a reset");
            far.write(java.nio.ByteBuffer.wrap(new byte[5]));
            assertEquals(5, got.poll(5, TimeUnit.SECONDS), "the input still reads");
            far.close();
            assertEquals("end", got.poll(5, TimeUnit.SECONDS));
            io.reader().close();
            assertFalse(ch.isOpen(), "the socket closes with its second half");
        }
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
