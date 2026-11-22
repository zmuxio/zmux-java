package io.zmux;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;

import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class JoinOutcomeTest {
    private record Result(ZmuxSession session, PipeId pipe, Throwable error) {}

    private static ZmuxConfig config(EventLoops loops, JoinOutcome hook, int cap) {
        return config(loops, hook, cap, null);
    }

    private static ZmuxConfig config(EventLoops loops, JoinOutcome hook, int cap, Tracer tracer) {
        return ZmuxConfig.builder().eventLoops(loops).maxPipes(cap).pipeHeadroom(0)
                .pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).tracer(tracer).joinOutcome(hook).build();
    }

    @Test void bothCallbacksHoldOnlyTheirPipeAndReturnOnReplyPrecedesConfirmation() throws Exception {
        // HS-04, HS-20, HS-21, HS-27.
        var entered = new LinkedBlockingQueue<Result>();
        var release = new CompletableFuture<Void>();
        var sendConfirm = new CompletableFuture<Runnable>();
        var traced = new LinkedBlockingQueue<PipeId>();
        var tracer = new Tracer() {
            @Override public void event(SessionId session, SessionEvent e) {
                if (e instanceof SessionEvent.PipeJoined joined) traced.add(joined.pipe());
            }
        };
        try (var loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(config(loops, (session, id, error) -> {
            entered.add(new Result(session, id, error));
            release.join();
            throw new IllegalStateException("host failed after observing join");
        }, 4, tracer))) {
            var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
            var incoming = f.listener.adapter().handle(pair[1], JoinHint.inBand(null));
            var writer = new PipePoolTest.LateConfirm(pair[0].writer(), sendConfirm::complete);
            try {
                var pipe = f.client.addPipe(PipeIo.of(pair[0].reader(), writer), PeerAttachedTest.NO_WAIT);
                assertTrue(entered.isEmpty());
                sendConfirm.get(5, TimeUnit.SECONDS).run();
                incoming.awaitSetup();
                var a = entered.poll(5, TimeUnit.SECONDS);
                var b = entered.poll(5, TimeUnit.SECONDS);
                assertNotNull(a);
                assertNotNull(b);
                assertEquals(java.util.Set.of(f.client, f.server), java.util.Set.of(a.session, b.session));
                assertEquals(pipe.id(), a.pipe);
                assertEquals(pipe.id(), b.pipe);
                assertNull(a.error);
                assertNull(b.error);
                for (var session : java.util.List.of(f.client, f.server)) {
                    var marker = new SessionEvent.CapacityRestored(23);
                    session.driver.executeCarrier(() -> session.driver.addApplicationEvent(marker, marker));
                    SessionEvent e;
                    while ((e = session.nextEvent(Duration.ofSeconds(3))) != marker) {
                        if (e instanceof SessionEvent.PipeJoined joined) assertNotEquals(pipe.id(), joined.pipe());
                        if (e instanceof SessionEvent.PipeAdded added) assertNotEquals(pipe.id(), added.pipe());
                    }
                }
                release.complete(null);
                for (var session : java.util.List.of(f.client, f.server)) {
                    SessionEvent e;
                    do { e = session.nextEvent(Duration.ofSeconds(3)); }
                    while (!(e instanceof SessionEvent.PipeJoined joined && pipe.id().equals(joined.pipe())));
                }
                assertEquals(pipe.id(), traced.poll(5, TimeUnit.SECONDS));
                assertEquals(pipe.id(), traced.poll(5, TimeUnit.SECONDS));
                assertTrue(entered.isEmpty());
            } finally { release.complete(null); }
        }
    }

    @Test void proofDeliveredAfterTimeoutCannotChangeAnyHandleOrRepeatTheOutcome() throws Exception {
        // HS-22.
        var release = new CompletableFuture<Runnable>();
        var ended = new LinkedBlockingQueue<Result>();
        try (var loops = EventLoops.create(2)) {
            var client = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                    .joinTimeout(Duration.ofMillis(750)).joinOutcome((s,p,e) -> ended.add(new Result(s,p,e))).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config(loops, null, 4)));
                 var c = Zmux.connect(listener.localAddress(), client); var s = listener.accept()) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.adapter().handle(pair[1], JoinHint.inBand(null));
                var pipe = c.addPipe(PipeIo.of(pair[0].reader(), new PipePoolTest.LateConfirm(pair[0].writer(), release::complete)),
                        PeerAttachedTest.NO_WAIT);
                var other = c.pipes().stream().filter(p -> p.id().equals(pipe.id())).findFirst().orElseThrow();
                var waiting = other.attached();
                var confirm = release.get(5, TimeUnit.SECONDS);
                try {
                    var reason = assertThrows(ExecutionException.class, () -> waiting.get(5, TimeUnit.SECONDS)).getCause();
                    assertInstanceOf(JoinTimeoutException.class, reason);
                    assertSame(reason, assertThrows(JoinTimeoutException.class, pipe::awaitAttached));
                    assertSame(reason, ended.take().error);
                } finally { confirm.run(); }
                assertThrows(JoinTimeoutException.class, other::awaitAttached);
                SealTest.echo(c, s);
                assertTrue(ended.isEmpty());
            }
        }
    }

    @Test void aWinningFailureFinishesRetirementBeforePublishingConfirmation() throws Exception {
        // HS-23.
        try (var loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(config(loops, null, 4));
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var attempt = f.client.driver.new JoinAttempt();
            var retiring = new CompletableFuture<Void>();
            var release = new CompletableFuture<Void>();
            var failure = new JoinTimeoutException();
            var reported = workers.submit(() -> attempt.report(failure, () -> {
                retiring.complete(null);
                release.join();
            }));
            var lateProof = new CompletableFuture<CompletableFuture<Throwable>>();
            try {
                retiring.get(3, TimeUnit.SECONDS);
                var nextCallback = new CompletableFuture<Void>();
                f.client.driver.publishCompletion(() -> lateProof.complete(attempt.report(null)));
                f.client.driver.publishCompletion(() -> nextCallback.complete(null));
                nextCallback.get(3, TimeUnit.SECONDS);
                assertFalse(lateProof.get(3, TimeUnit.SECONDS).isDone());
                assertFalse(attempt.confirmation().isDone());
            } finally {
                release.complete(null);
            }
            assertSame(failure, reported.get(3, TimeUnit.SECONDS).get(3, TimeUnit.SECONDS));
            assertSame(failure, lateProof.get(3, TimeUnit.SECONDS).get(3, TimeUnit.SECONDS));
            assertSame(failure, assertThrows(ExecutionException.class, () -> attempt.confirmation().get()).getCause());
        }
    }

    /** A send-only join's proof must travel on the bootstrap pipe. Hold that exact write in flight. */
    private static final class AckGate implements PipeWriter {
        private final PipeWriter inner;
        private final CompletableFuture<Runnable> delivery = new CompletableFuture<>();
        private volatile boolean armed;

        AckGate(PipeWriter inner) { this.inner = inner; }

        @Override public void write(OutboundBatch batch, Completion done) {
            if (armed) {
                var bytes = java.nio.ByteBuffer.allocate(batch.bytes());
                for (int i = 0; i < batch.segmentCount(); i++) bytes.put(batch.segment(i));
                if (ShapingTest.Recorder.carriesPipeAck(bytes.array(), ack -> ack.pipeId != 0 && ack.position > 0)) {
                    armed = false;
                    delivery.complete(() -> inner.write(batch, done));
                    return;
                }
            }
            inner.write(batch, done);
        }

        @Override public void close() { inner.close(); }
    }

    @Test void latePipeAckCannotReviveAJoinAfterItsCallerDeadline() throws Exception {
        // HS-23, HS-24.
        for (boolean early : new boolean[]{false, true}) {
            var ended = new LinkedBlockingQueue<Result>();
            try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor();
                 var listener = Zmux.listener(ListenerConfig.of(config(loops, null, 4)))) {
                var cfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).joinTimeout(Duration.ofMinutes(1))
                        .pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                        .joinOutcome((s, id, e) -> ended.add(new Result(s, id, e))).build();
                var bootstrap = MemoryPipe.pair(Framing.BYTE_STREAM);
                var ack = new AckGate(bootstrap[1].writer());
                listener.adapter().handle(PipeIo.of(bootstrap[1].reader(), ack), JoinHint.inBand(null));
                try (var c = Zmux.open(bootstrap[0], Role.CLIENT, cfg); var s = listener.accept()) {
                    ack.armed = true;
                    var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                    var incoming = listener.adapter().handle(pair[1], JoinHint.inBand(null));
                    var opts = PipeOptions.builder().direction(PipeOptions.Direction.SEND_ONLY).returnOnReply(early).build();
                    long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
                    var now = new java.util.concurrent.atomic.AtomicLong(deadline - 1);
                    var proofWaiting = new CompletableFuture<Void>();
                    var attempt = c.driver.addPipe(pair[0], opts.join());
                    var adding = workers.submit(() -> c.joinCarrier(attempt, opts, deadline, null, () -> {
                        proofWaiting.complete(null);
                        return now.get();
                    }));
                    incoming.awaitSetup();
                    Runnable deliver = ack.delivery.get(5, TimeUnit.SECONDS);
                    var observed = c.pipes().stream().filter(p -> !PipeId.BOOTSTRAP.equals(p.id())).findFirst().orElseThrow();
                    ZmuxPipe returned = early ? adding.get(1, TimeUnit.SECONDS) : observed;
                    Throwable reason;
                    try {
                        var attached = observed.attached();
                        assertFalse(attached.isDone(), "the peer's positive PIPE_ACK is still in flight");
                        proofWaiting.get(3, TimeUnit.SECONDS);
                        now.set(deadline);
                        c.driver.executeCarrier(() -> io.zmux.internal.core.ObserverFixture.wakeWaiters(c.core));
                        reason = early ? assertThrows(JoinTimeoutException.class, returned::awaitAttached)
                                : assertThrows(ExecutionException.class, () -> adding.get(5, TimeUnit.SECONDS)).getCause();
                        assertInstanceOf(JoinTimeoutException.class, reason);
                        PipePoolTest.inactive(observed);
                        assertFalse(PeerAttachedTest.held(c, observed));
                        assertTrue(c.pipeStats().stream().noneMatch(st -> st.pipe().equals(observed.id())));
                        assertSame(reason, assertThrows(ExecutionException.class, () -> attached.get(5, TimeUnit.SECONDS)).getCause());
                        var outcome = ended.poll(5, TimeUnit.SECONDS);
                        assertNotNull(outcome);
                        assertEquals(observed.id(), outcome.pipe);
                        assertSame(reason, outcome.error);
                    } finally {
                        deliver.run();
                    }
                    SealTest.echo(c, s); // The bootstrap consumed the released ACK before the echo response.
                    assertSame(reason, assertThrows(JoinTimeoutException.class, observed::awaitAttached));
                    assertSame(reason, assertThrows(JoinTimeoutException.class, returned::awaitAttached));
                    assertTrue(ended.isEmpty());
                    assertEquals(java.util.List.of(PipeId.BOOTSTRAP), c.pipes().stream().map(ZmuxPipe::id).toList());
                }
            }
        }
    }

    // Fix the proof clock on each side of the deadline while the real Confirm is held.
    @Test void confirmationBeforeAtAndAfterTheDeadlineUsesOneReceipt() throws Exception {
        // HS-22.
        for (boolean early : new boolean[]{false, true}) for (int offset : new int[]{-1, 0, 1}) {
            var outcomes = new LinkedBlockingQueue<Result>();
            try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor();
                 var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config(loops, null, 4)));
                 var c = Zmux.connect(l.localAddress(), config(loops, (s,p,e) -> outcomes.add(new Result(s,p,e)), 4));
                 var server = l.accept()) {
                var send = new CompletableFuture<Runnable>();
                var ready = new CompletableFuture<Void>();
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.adapter().handle(pair[1], JoinHint.inBand(null));
                var opts = PipeOptions.builder().returnOnReply(early).build();
                long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
                var now = new java.util.concurrent.atomic.AtomicLong(deadline - 1);
                var attempt = c.driver.addPipe(PipeIo.of(pair[0].reader(), new PipePoolTest.LateConfirm(pair[0].writer(), send::complete)), opts.join());
                var joining = workers.submit(() -> c.joinCarrier(attempt, opts, deadline, null, () -> {
                    ready.complete(null);
                    return now.get();
                }));
                ready.get(5, TimeUnit.SECONDS);
                var handle = early ? joining.get(5, TimeUnit.SECONDS)
                        : c.pipes().stream().filter(p -> !p.id().equals(PipeId.BOOTSTRAP)).findFirst().orElseThrow();
                var waiting = handle.attached();
                now.set(deadline + offset);
                send.get(5, TimeUnit.SECONDS).run();
                var outcome = outcomes.poll(5, TimeUnit.SECONDS);
                assertNotNull(outcome);
                assertEquals(handle.id(), outcome.pipe);
                if (offset < 0) {
                    assertNull(outcome.error);
                    assertEquals(handle.id(), joining.get(5, TimeUnit.SECONDS).id());
                    waiting.get(5, TimeUnit.SECONDS);
                } else {
                    assertInstanceOf(JoinTimeoutException.class, outcome.error);
                    assertSame(outcome.error, assertThrows(ExecutionException.class, () -> waiting.get(5, TimeUnit.SECONDS)).getCause());
                    if (!early) assertSame(outcome.error, assertThrows(ExecutionException.class, () -> joining.get(5, TimeUnit.SECONDS)).getCause());
                }
                SealTest.echo(c, server);
                assertTrue(outcomes.isEmpty());
            }
        }
    }

    @Test void outcomeMayWaitForItsOwnSessionToClose() throws Exception {
        // HS-26.
        var entered = new CompletableFuture<Void>();
        var returned = new CompletableFuture<SessionError>();
        try (var loops = EventLoops.create(2);
             var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config(loops, (s,p,e) -> {
                 s.abort(7, "session wait inside hook");
                 entered.complete(null);
                 try { returned.complete(s.awaitClosed()); }
                 catch (IOException failure) { returned.completeExceptionally(failure); }
             }, 4)));
             var c = Zmux.connect(listener.localAddress(), config(loops, null, 4)); var server = listener.accept()) {
            try { c.addPipe(listener.localAddress(), PeerAttachedTest.NO_WAIT); }
            catch (SessionClosedException ended) { /* The hook closed the session first. */ }
            entered.get(5, TimeUnit.SECONDS);
            assertEquals(server.awaitClosed(), returned.get(5, TimeUnit.SECONDS));
        }
    }

    @Test void aFailureBeforeAllocationReportsTheReservedId() throws Exception {
        // HS-25.
        var outcomes = new LinkedBlockingQueue<Result>();
        try (var loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(config(loops,
                (session, pipe, error) -> outcomes.add(new Result(session, pipe, error)), 4))) {
            var p = MemoryPipe.pair(Framing.BYTE_STREAM);
            var error = assertThrows(IllegalArgumentException.class, () ->
                    f.client.addPipe(PipeIo.of(p[0].reader(), null), PipeOptions.defaults()));
            var result = outcomes.poll(3, TimeUnit.SECONDS);
            assertNotNull(result);
            assertSame(f.client, result.session);
            assertSame(error, result.error);
            assertSame(PipeId.UNASSIGNED, result.pipe);
            assertEquals(1L << 62, result.pipe.value());
            assertNotEquals(PipeId.BOOTSTRAP, result.pipe);
            assertNotEquals(PipeId.of((1L << 62) - 1), result.pipe);
            assertThrows(IllegalArgumentException.class, () -> PipeId.of((1L << 62) + 1));
            assertTrue(outcomes.isEmpty());
            p[0].writer().close();
            p[1].reader().close();
            p[1].writer().close();
        }
    }

    @Test void aLostCarrierAndAHandshakeTimeoutHaveDifferentOutcomes() throws Exception {
        // HS-24.
        for (boolean timeout : new boolean[]{false, true}) {
            var ended = new CompletableFuture<Result>();
            try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                var config = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                        .noMaxIdleTimeout().joinTimeout(Duration.ofSeconds(10)).joinOutcome((s, id, error) -> ended.complete(new Result(s, id, error))).build();
                try (var f = new PipePoolTest.Fixture(config)) {
                    var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                    var sawHello = TakeoverTest.reading(p[1]);
                    var adding = workers.submit(() -> f.client.addPipe(p[0], PipeOptions.defaults()));
                    assertNotNull(sawHello.poll(3, TimeUnit.SECONDS));
                    PipeStats.Failure reportedFailure = null;
                    if (timeout) {
                        // The hello arrived, but the peer will never send a reply. Advance only the
                        // core timer on the real owner, before the driver's wall-clock gate can fire.
                        var driver = f.client.driver;
                        var advanced = new CompletableFuture<PipeStats.Failure>();
                        driver.loop().execute(() -> {
                            try {
                                var core = driver.core();
                                core.handleTimeout(core.now() + core.joinDeadline());
                                // Read the failed core attachment before the driver drains the abort.
                                var failed = f.client.pipeStats().stream().filter(stats -> stats.failure() != null)
                                        .findFirst().orElseThrow();
                                driver.roundEnd();
                                advanced.complete(failed.failure());
                            } catch (RuntimeException | AssertionError failure) {
                                advanced.completeExceptionally(failure);
                            }
                        });
                        reportedFailure = advanced.get(3, TimeUnit.SECONDS);
                    } else {
                        p[1].writer().close();
                    }
                    var error = assertThrows(ExecutionException.class, () -> adding.get(3, TimeUnit.SECONDS)).getCause();
                    var result = ended.get(3, TimeUnit.SECONDS);
                    assertSame(error, result.error);
                    assertSame(f.client, result.session);
                    assertNotNull(result.pipe);
                    if (timeout) {
                        assertInstanceOf(JoinTimeoutException.class, error);
                        assertNotNull(reportedFailure);
                        assertEquals(io.zmux.internal.wire.TransportError.PIPE_FAILED, reportedFailure.code());
                        assertSame(error, reportedFailure.cause());
                    } else {
                        assertInstanceOf(PipeGoneException.class, error);
                    }
                    p[1].reader().close();
                    p[1].writer().close();
                }
            }
        }
    }

    @Test void malformedReplyPreservesTheHandshakeErrorInTheOutcome() throws Exception {
        // HS-24.
        var ended = new LinkedBlockingQueue<Result>();
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor();
             var f = new PipePoolTest.Fixture(config(loops, (s,p,e) -> ended.add(new Result(s,p,e)), 4))) {
            var p = MemoryPipe.pair(Framing.MESSAGES);
            var hello = TakeoverTest.reading(p[1]);
            var adding = workers.submit(() -> f.client.addPipe(p[0], PipeOptions.defaults()));
            assertNotNull(hello.poll(3, TimeUnit.SECONDS));
            p[1].writer().write(OutboundBatch.of(java.nio.ByteBuffer.wrap(new byte[]{0})), new PipeWriter.Completion() {
                @Override public void completed() {}
                @Override public void failed(Throwable error) { throw new AssertionError(error); }
            });
            var error = assertThrows(ExecutionException.class, () -> adding.get(3, TimeUnit.SECONDS)).getCause();
            var handshake = assertInstanceOf(io.zmux.seal.SecureCarrierException.class, error);
            assertEquals(io.zmux.seal.SecureCarrierException.Kind.MALFORMED, handshake.kind());
            var result = ended.poll(3, TimeUnit.SECONDS);
            assertNotNull(result);
            assertSame(error, result.error);
            assertTrue(ended.isEmpty());
            p[1].reader().close();
            p[1].writer().close();
        }
    }

    @Test void poolReportsTheCarrierItJoins() throws Exception {
        // HS-21.
        var ended = new LinkedBlockingQueue<Result>();
        try (var loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(config(loops,
                (session, pipe, error) -> ended.add(new Result(session, pipe, error)), 4)); var cancel = new Cancellation()) {
            f.listener.drain("joins remain open");
            var pool = f.client.maintainPipes(PipePoolTest.fast(f.dial()).withCount(1), cancel);
            var first = ended.poll(5, TimeUnit.SECONDS);
            var second = ended.poll(5, TimeUnit.SECONDS);
            assertNotNull(first);
            assertNotNull(second);
            assertEquals(java.util.Set.of(f.client, f.server), java.util.Set.of(first.session, second.session));
            assertEquals(first.pipe, second.pipe);
            assertNull(first.error);
            assertNull(second.error);
            cancel.cancel();
            pool.completion().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertTrue(ended.isEmpty());
        }
    }

    @Test void aModeRefusalAfterAuthorizationReportsItsAssignedId() throws Exception {
        // HS-20.
        var authorized = new CompletableFuture<PipeId>();
        var results = new LinkedBlockingQueue<Result>();
        try (var loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                    .authorizeJoin((s, id) -> authorized.complete(id)).allowRecordMode((carrier, mode, peer) -> false)
                    .joinOutcome((s, id, error) -> results.add(new Result(s, id, error))).build();
            try (var f = new PipePoolTest.Fixture(config)) {
                var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                var handled = f.listener.adapter().handle(p[1], JoinHint.inBand(null));
                assertThrows(IOException.class, () -> f.client.addPipe(p[0], PipeOptions.defaults()));
                var inboundError = assertThrows(JoinRefusedException.class, handled::awaitSetup);
                var id = authorized.get(3, TimeUnit.SECONDS);
                assertNotEquals(PipeId.UNASSIGNED, id);
                var first = results.poll(3, TimeUnit.SECONDS);
                var second = results.poll(3, TimeUnit.SECONDS);
                assertNotNull(first);
                assertNotNull(second);
                assertEquals(java.util.Set.of(f.client, f.server), java.util.Set.of(first.session, second.session));
                var refused = first.session == f.server ? first : second;
                assertEquals(id, refused.pipe);
                assertSame(inboundError, refused.error);
                assertTrue(results.isEmpty());
            }
        }
    }

    @Test void peerLimitReportsOneRefusalOnEachEndpoint() throws Exception {
        // HS-24.
        var ended = new LinkedBlockingQueue<Result>();
        JoinOutcome hook = (s, p, e) -> ended.add(new Result(s, p, e));
        try (var loops = EventLoops.create(2);
             var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config(loops, hook, 2)));
             var client = Zmux.connect(listener.localAddress(), config(loops, hook, 6)); var server = listener.accept()) {
            client.addPipe(listener.localAddress(), PipeOptions.defaults());
            for (int i = 0; i < 2; i++) assertNull(ended.take().error);
            assertThrows(JoinRefusedException.class, () -> client.addPipe(listener.localAddress(), PipeOptions.defaults()));
            var a = ended.take();
            var b = ended.take();
            assertEquals(java.util.Set.of(client, server), java.util.Set.of(a.session, b.session));
            assertInstanceOf(JoinRefusedException.class, a.error);
            assertInstanceOf(JoinRefusedException.class, b.error);
            assertEquals(a.pipe, b.pipe);
            assertTrue(ended.isEmpty());
        }
    }

    @SuppressWarnings("try") // Close races a deliberately incomplete Reply.
    @Test void withdrawalAndListenerStopHaveDistinctInboundOutcomes() throws Exception {
        // HS-04, HS-24.
        for (boolean stop : new boolean[]{false, true}) {
            var ended = new CompletableFuture<Throwable>();
            try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor();
                 var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config(loops, (s,p,e) -> ended.complete(e), 4)));
                 var client = Zmux.connect(listener.localAddress(), config(loops, null, 4)); var server = listener.accept()) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var reply = new CompletableFuture<PipeWriter.Completion>();
                var writer = new PipeWriter() {
                    @Override public void write(OutboundBatch b, Completion done) { reply.complete(done); }
                    @Override public void close() { pair[1].writer().close(); }
                };
                var h = listener.adapter().handle(PipeIo.of(pair[1].reader(), writer), JoinHint.inBand(null));
                var join = workers.submit(() -> client.addPipe(pair[0], PipeOptions.defaults()));
                var completion = reply.get(5, TimeUnit.SECONDS);
                try {
                    if (stop) listener.close(); else io.zmux.internal.spi.AdapterPorts.setup(h).interrupt();
                    Throwable reason = ended.get(5, TimeUnit.SECONDS);
                    assertSame(reason, assertThrows(IOException.class, h::awaitSetup));
                    if (stop) assertInstanceOf(ListenerClosedException.class, reason);
                    else assertInstanceOf(java.io.InterruptedIOException.class, reason);
                    assertEquals(1, server.pipes().size());
                } finally { completion.failed(new IOException("fixture released")); }
                assertThrows(ExecutionException.class, () -> join.get(5, TimeUnit.SECONDS));
            }
        }
    }
}
