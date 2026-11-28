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
    static final class Stalling implements PipeWriter, io.zmux.internal.spi.AdapterPorts.ReleasingHalf {
        final PipeWriter delegate;
        final AtomicBoolean held = new AtomicBoolean();
        final CompletableFuture<Completion> pending = new CompletableFuture<>();

        Stalling(PipeWriter delegate) { this.delegate = delegate; }
        @Override public void write(OutboundBatch batch, Completion done) {
            if (held.get()) pending.complete(done);
            else delegate.write(batch, done);
        }
        @Override public void close() {
            delegate.close();
            pending.thenAccept(done -> done.failed(new java.io.EOFException("fixture writer closed")));
        }
        @Override public void abort(Throwable error) {
            delegate.abort(error);
            pending.thenAccept(done -> done.failed(error));
        }
        @Override public CompletionStage<Void> released() {
            return io.zmux.internal.spi.AdapterPorts.releasedHalf(delegate);
        }
    }

    static ZmuxConfig.Builder resuming(ZmuxConfig.Builder b) {
        return b.sessionPolicy(new SessionPolicy(new CapacityLimits(2))).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                .resumeTimeout(Duration.ofSeconds(20)).noHandshakeTimeout().noProbation();
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

    @Test @Timeout(120)
    void aProcessExitingRightAfterCloseStillTellsAResumingPeer() throws Exception {
        String launcher = java.nio.file.Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String cp = String.join(java.io.File.pathSeparator, System.getProperty("java.class.path"), System.getProperty("jdk.module.path", ""));
        try (EventLoops loops = EventLoops.create(1);
             var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.builder()
                     .session(resuming(ZmuxConfig.builder().eventLoops(loops)).build())
                     .firstRecordTimeout(Duration.ofMinutes(2)).build())) {
            Process child = new ProcessBuilder(launcher, "-cp", cp, Child.class.getName(), String.valueOf(listener.localAddress().getPort()))
                    .redirectError(ProcessBuilder.Redirect.INHERIT).start();
            var accepted = listener.acceptAsync();
            child.onExit().thenAccept(exited -> accepted.completeExceptionally(new java.io.IOException(
                    "child exited before session establishment, exit=" + exited.exitValue() + ", " + listener.stats())));
            try (var server = accepted.get()) {
                server.acceptStream().close();
                child.getOutputStream().write('\n'); child.getOutputStream().flush();
                assertEquals(0, child.waitFor());
                var why = server.closed().get();
                var app = assertInstanceOf(SessionError.ApplicationClosed.class, why, "resume held the session: " + why);
                assertEquals(0, app.code());
                assertEquals(0, server.stats().suspensions());
            } finally {
                accepted.cancel(true);
                child.destroyForcibly().waitFor(30, TimeUnit.SECONDS);
            }
        }
    }

    @Test void aCarrierThatTakesNothingEndsTheCloseAtTheBound() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        try (var loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).clockDomain(clock.domain()).build();
            var writers = new Stalling[2];
            var sessions = stalledPair(cfg, writers);
            var client = sessions[0];
            var server = sessions[1];
            writers[0].held.set(true);
            long bound = io.zmux.internal.core.Session.closeLimit;
            CloseLingerTest.closeOnOwner(client, 0x2a, "stalled");
            writers[0].pending.get(20, TimeUnit.SECONDS);
            var settled = client.closed();
            clock.advance(Duration.ofNanos(bound - 1));
            CloseLingerTest.afterRound(client);
            assertFalse(settled.isDone(), "the close writer is still within its hand-off bound");
            clock.advance(Duration.ofNanos(1));
            assertEquals(new SessionError.ApplicationClosed(0x2a, "stalled", false), settled.get(20, TimeUnit.SECONDS));
            client.close();
            server.abort(0, "");
            server.awaitClosed();
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

    @Test void closeOnAStalledLoopStillReturnsAtTheBound() throws Exception {
        long saved = io.zmux.internal.core.Session.closeLimit;
        io.zmux.internal.core.Session.closeLimit = TimeUnit.MILLISECONDS.toNanos(300);
        CountDownLatch release = new CountDownLatch(1), entered = new CountDownLatch(1);
        try (EventLoops loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 2))) {
            try {
                // The flush operation then stays queued: close must not wait for the loop past its bound.
                assertTrue(f.client.driver.executeCarrier(() -> {
                    entered.countDown();
                    try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }));
                assertTrue(entered.await(3, TimeUnit.SECONDS));
                f.client.close();
                assertFalse(f.client.closed().isDone(), "the caller returned while its owner was still held");
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
        try (EventLoops loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 2))) {
            var st = f.client.openStream();
            st.setSendBuffer(2L * n);
            st.write(new byte[n], 0, n); // mostly unsent: the peer's window holds it back until it reads
            st.finish();
            var in = f.server.acceptStream(Duration.ofSeconds(3));
            CountDownLatch called = new CountDownLatch(1);
            assertTrue(f.client.driver.executeCarrier(() -> {
                f.client.close();
                called.countDown();
            }));
            assertTrue(called.await(3, TimeUnit.SECONDS));
            long got = 0;
            byte[] b = new byte[1 << 16];
            for (int r; (r = in.read(b, 0, b.length)) > 0; ) got += r;
            assertEquals(n, got, "the data written before close arrived");
            var why = assertInstanceOf(SessionError.ApplicationClosed.class, f.server.closed().get(5, TimeUnit.SECONDS));
            assertEquals(0, why.code());
        }
    }

    @Test void aHealthyCarrierDeliversTheAbort() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 2))) {
            f.client.abort(3, "done");
            var app = assertInstanceOf(SessionError.ApplicationClosed.class, f.server.closed().get(3, TimeUnit.SECONDS));
            assertEquals(3, app.code());
        }
    }

    /** Holds the Confirm after the joiner observes the Reply and before the acceptor takes the pipe. */
    private static final class HeldJoin implements AutoCloseable {
        final PipeIo io;
        final CompletableFuture<Runnable> confirm = new CompletableFuture<>();
        final AtomicBoolean released = new AtomicBoolean();

        HeldJoin(SessionTestSupport.Fixture fixture) {
            var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
            fixture.listener.adapter().handle(pair[1], JoinHint.inBand(TestAddr.peer("held-confirm")));
            io = pair[0].withHalves(pair[0].reader(), new SessionTestSupport.LateConfirm(pair[0].writer(), confirm::complete));
        }

        @Override public void close() {
            if (released.compareAndSet(false, true)) confirm.thenAccept(Runnable::run);
        }
    }

    @SuppressWarnings("try") // Opening the gate is part of the scenario.
    @Test void addingWaitsUntilThePeerAttachedThePipe() throws Exception {
        try (var loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 3));
             var workers = Executors.newVirtualThreadPerTaskExecutor(); var ordinary = new HeldJoin(f); var early = new HeldJoin(f)) {
            var waited = workers.submit(() -> f.client.addPipe(ordinary.io, PipeOptions.defaults()));
            ordinary.confirm.get(20, TimeUnit.SECONDS);
            assertFalse(waited.isDone());
            ordinary.close();
            var attached = waited.get(20, TimeUnit.SECONDS);
            assertTrue(PeerAttachedTest.held(f.server, attached));

            var replyOnly = f.client.addPipe(early.io, NO_WAIT);
            early.confirm.get(20, TimeUnit.SECONDS);
            assertFalse(PeerAttachedTest.held(f.server, replyOnly));
            early.close();
            replyOnly.attached().get(20, TimeUnit.SECONDS);
            assertTrue(PeerAttachedTest.held(f.server, replyOnly));
        }
    }

    @Test void anUnattachedPipeAtTheDeadlineIsAbortedAndLeavesNothing() throws Exception {
        try (var loops = EventLoops.create(2)) {
            var timeout = Duration.ofMinutes(1);
            var clock = new SpliceHooksTest.ManualClock();
            var cfg = SessionTestSupport.configBuilder(loops, 3).clockDomain(clock.domain())
                    .noMaxIdleTimeout().joinTimeout(timeout).build();
            try (var f = new SessionTestSupport.Fixture(cfg); var held = new HeldJoin(f)) {
                var attempt = f.client.driver.addPipe(held.io, NO_WAIT.join());
                var pipe = f.client.joinCarrier(attempt, NO_WAIT, ZmuxSession.deadline(timeout), null);
                held.confirm.get(20, TimeUnit.SECONDS);
                var attached = pipe.attached();
                assertFalse(attached.isDone());
                f.client.driver.loop().advanceTimersForTests(timeout.toNanos()).get(20, TimeUnit.SECONDS);
                var failure = assertThrows(ExecutionException.class, () -> attached.get(20, TimeUnit.SECONDS));
                assertInstanceOf(SocketTimeoutException.class, failure.getCause());
                assertFalse(f.client.isClosed());
                assertFalse(f.server.pipes().stream().anyMatch(peer -> peer.id().value() != 0));
            }
        }
    }

    @Test void aPipeRemovedBeforeItsAttachmentFailsTheAddWithPipeGone() throws Exception {
        try (var loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 3));
             var workers = Executors.newVirtualThreadPerTaskExecutor(); var held = new HeldJoin(f); var next = new HeldJoin(f)) {
            var add = workers.submit(() -> f.client.addPipe(held.io, PipeOptions.defaults()));
            held.confirm.get(20, TimeUnit.SECONDS);
            var removed = activeJoin(f.client);
            removed.abort();
            var e = assertThrows(ExecutionException.class, () -> add.get(20, TimeUnit.SECONDS));
            assertInstanceOf(PipeGoneException.class, e.getCause());
            assertTrue(removed.stats().isPresent(), "the add returned before the pipe retired");

            var ended = workers.submit(() -> f.client.addPipe(next.io, PipeOptions.defaults()));
            next.confirm.get(20, TimeUnit.SECONDS);
            assertNotEquals(removed.id(), activeJoin(f.client).id());
            f.client.close();
            var c = assertThrows(ExecutionException.class, () -> ended.get(20, TimeUnit.SECONDS));
            assertInstanceOf(java.io.IOException.class, c.getCause());
            assertFalse(c.getCause() instanceof SocketTimeoutException || c.getCause() instanceof PipeGoneException, c.getCause().toString());
        }
    }

    private static ZmuxPipe activeJoin(ZmuxSession session) throws Exception {
        SessionTestSupport.await(session, () -> session.pipeStats().stream().anyMatch(pipe ->
                pipe.pipe().value() != 0 && pipe.outbound() == PipeState.Outbound.ACTIVE));
        var active = session.pipeStats().stream().filter(pipe -> pipe.pipe().value() != 0
                && pipe.outbound() == PipeState.Outbound.ACTIVE).findFirst().orElseThrow().pipe();
        return session.pipes().stream().filter(pipe -> pipe.id().equals(active)).findFirst().orElseThrow();
    }

    /**
     * A close while the peer keeps writing: the closer half-closes after CONNECTION_CLOSE and
     * drains what the peer still sends in the background, so the peer receives the close.
     */
    @SuppressWarnings("try") // Close is the action under test.
    @Test void aCloseUnderThePeersBulkWritesDrainsBeforeItReturns() throws Exception {
        try (var loops = EventLoops.create(2)) {
            var cfg = NonBlockingCallsTest.smallBuilder(loops).sendBuffer(NonBlockingCallsTest.WINDOW)
                    .sendBufferMax(NonBlockingCallsTest.WINDOW).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var closer = Zmux.connect(l.localAddress(), cfg); var peer = l.accept();
                 var outgoing = peer.openStream()) {
                var bulk = outgoing.writeAsync(java.nio.ByteBuffer.allocate(4 * NonBlockingCallsTest.WINDOW));
                try (var incoming = closer.acceptStream()) {
                    assertEquals(outgoing.id(), incoming.id());
                    assertFalse(bulk.isDone(), "the peer is waiting for a write to drain");
                    closer.close();
                    var app = assertInstanceOf(SessionError.ApplicationClosed.class, peer.closed().get(20, TimeUnit.SECONDS));
                    assertEquals(0, app.code());
                    closer.awaitClosed();
                    var error = assertThrows(ExecutionException.class, () -> bulk.get(20, TimeUnit.SECONDS));
                    assertInstanceOf(java.io.IOException.class, error.getCause());
                }
            }
        }
    }

    /** A stuck event loop holds neither a failed join's abort nor a splice's cleanup longer than a close's bound. */
    @Test void loopWaitsOfAbortsAndSpliceCleanupAreBounded() throws Exception {
        long saved = io.zmux.internal.core.Session.closeLimit;
        io.zmux.internal.core.Session.closeLimit = TimeUnit.MILLISECONDS.toNanos(200);
        try (EventLoops loops = EventLoops.create(1); var f = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 2))) {
            var stream = f.client.openStream();
            var entered = new CountDownLatch(1);
            var proceed = new CountDownLatch(1);
            assertTrue(f.client.driver.executeCarrier(() -> {
                entered.countDown();
                try { proceed.await(); } catch (InterruptedException ignored) { }
            }));
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertFalse(f.client.cancelJoinedPipe(99), "the stuck loop did not run the abort");
                assertFalse(Splice.boundedCleanup(stream, () -> { }).join(), "the stuck loop did not run the cleanup");
            } finally {
                proceed.countDown();
            }
            assertTrue(f.client.cancelJoinedPipe(99), "a running loop answers");
        } finally {
            io.zmux.internal.core.Session.closeLimit = saved;
        }
    }

    /** The hand-off ends only when the carrier's write side actually shuts. */
    @Test void closeReturnsAfterTheWriteSideIsShut() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).clockDomain(clock.domain()).build();
            var io = MemoryPipe.pair(Framing.BYTE_STREAM);
            var requested = new CompletableFuture<Void>();
            var allow = new CompletableFuture<Void>();
            PipeWriter wire = io[0].writer();
            PipeWriter gated = new PipeWriter() {
                @Override public void write(OutboundBatch batch, Completion done) { wire.write(batch, done); }
                @Override public void close() {
                    requested.complete(null);
                    allow.join();
                    wire.close();
                }
                @Override public void abort(Throwable error) { wire.abort(error); }
            };
            io[0] = io[0].withHalves(io[0].reader(), gated);
            try (var pair = Pair.open(io, cfg, cfg)) {
                var returned = workers.submit(() -> { pair.client().close(); return null; });
                try {
                    requested.get(20, TimeUnit.SECONDS);
                    assertFalse(returned.isDone(), "shutdown was requested but the carrier is still open");
                } finally { allow.complete(null); }
                returned.get(20, TimeUnit.SECONDS);
                var app = assertInstanceOf(SessionError.ApplicationClosed.class, pair.server().closed().get(20, TimeUnit.SECONDS));
                assertEquals(0, app.code());
            }
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
                @Override public void onChunk(io.zmux.Chunk chunk, Runnable consumed) { Runnable release = () -> { chunk.close(); consumed.run(); }; got.add(chunk.remaining()); release.run(); }
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

}
