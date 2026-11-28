package io.zmux;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;

import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class JoinReceiptTest {
    private static ZmuxConfig config(EventLoops loops, int cap) {
        return ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(cap, 0)))
                .pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).build();
    }

    @Test void proofDeliveredAfterTimeoutCannotChangeAnyHandleOrRepeatTheOutcome() throws Exception {
        // HS-22.
        var release = new CompletableFuture<Runnable>();
        try (var loops = EventLoops.create(2)) {
            var client = ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                    .joinTimeout(Duration.ofMillis(750)).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config(loops, 4)));
                 var c = Zmux.connect(listener.localAddress(), client); var s = listener.accept()) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.adapter().handle(pair[1], JoinHint.inBand(null));
                var pipe = c.addPipe(PipeIo.of(pair[0].reader(), new SessionTestSupport.LateConfirm(pair[0].writer(), release::complete)),
                        PeerAttachedTest.NO_WAIT);
                var other = c.pipes().stream().filter(p -> p.id().equals(pipe.id())).findFirst().orElseThrow();
                var waiting = other.attached();
                var confirm = release.get(5, TimeUnit.SECONDS);
                try {
                    var reason = assertThrows(ExecutionException.class, () -> waiting.get(5, TimeUnit.SECONDS)).getCause();
                    assertInstanceOf(JoinTimeoutException.class, reason);
                    assertSame(reason, assertThrows(JoinTimeoutException.class, pipe::awaitAttached));
                } finally { confirm.run(); }
                assertThrows(JoinTimeoutException.class, other::awaitAttached);
                SealTest.echo(c, s);
            }
        }
    }

    @Test void aWinningFailureFinishesRetirementBeforePublishingConfirmation() throws Exception {
        // HS-23.
        try (var loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(config(loops, 4));
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var attempt = new io.zmux.internal.loop.SessionRuntime.JoinAttempt();
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
            try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor();
                 var listener = Zmux.listener(ListenerConfig.of(config(loops, 4)))) {
                var cfg = ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(4))).joinTimeout(Duration.ofMinutes(1))
                        .pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                        .build();
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
                        SessionTestSupport.inactive(observed);
                        assertFalse(PeerAttachedTest.held(c, observed));
                        assertTrue(c.pipeStats().stream().noneMatch(st -> st.pipe().equals(observed.id())));
                        assertSame(reason, assertThrows(ExecutionException.class, () -> attached.get(5, TimeUnit.SECONDS)).getCause());
                    } finally {
                        deliver.run();
                    }
                    SealTest.echo(c, s); // The bootstrap consumed the released ACK before the echo response.
                    assertSame(reason, assertThrows(JoinTimeoutException.class, observed::awaitAttached));
                    assertSame(reason, assertThrows(JoinTimeoutException.class, returned::awaitAttached));
                    assertEquals(java.util.List.of(PipeId.BOOTSTRAP), c.pipes().stream().map(ZmuxPipe::id).toList());
                }
            }
        }
    }

    // Fix the proof clock on each side of the deadline while the real Confirm is held.
    @Test void confirmationBeforeAtAndAfterTheDeadlineUsesOneReceipt() throws Exception {
        // HS-22.
        for (boolean early : new boolean[]{false, true}) for (int offset : new int[]{-1, 0, 1}) {
            try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor();
                 var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config(loops, 4)));
                 var c = Zmux.connect(l.localAddress(), config(loops, 4));
                 var server = l.accept()) {
                var send = new CompletableFuture<Runnable>();
                var ready = new CompletableFuture<Void>();
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.adapter().handle(pair[1], JoinHint.inBand(null));
                var opts = PipeOptions.builder().returnOnReply(early).build();
                long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
                var now = new java.util.concurrent.atomic.AtomicLong(deadline - 1);
                var attempt = c.driver.addPipe(PipeIo.of(pair[0].reader(), new SessionTestSupport.LateConfirm(pair[0].writer(), send::complete)), opts.join());
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
                if (offset < 0) {
                    assertEquals(handle.id(), joining.get(5, TimeUnit.SECONDS).id());
                    waiting.get(5, TimeUnit.SECONDS);
                } else {
                    var error = assertThrows(ExecutionException.class, () -> waiting.get(5, TimeUnit.SECONDS)).getCause();
                    assertInstanceOf(JoinTimeoutException.class, error);
                    if (!early) assertSame(error, assertThrows(ExecutionException.class, () -> joining.get(5, TimeUnit.SECONDS)).getCause());
                }
                SealTest.echo(c, server);
            }
        }
    }

    @Test void aLostCarrierAndAHandshakeTimeoutHaveDifferentOutcomes() throws Exception {
        // HS-24.
        for (boolean timeout : new boolean[]{false, true}) {
            try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                var config = ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                        .noMaxIdleTimeout().joinTimeout(Duration.ofSeconds(10)).build();
                try (var f = new SessionTestSupport.Fixture(config)) {
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
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor();
             var f = new SessionTestSupport.Fixture(config(loops, 4))) {
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
            p[1].reader().close();
            p[1].writer().close();
        }
    }

    @Test void capacityChangeReportsTheCarrierItJoins() throws Exception {
        try (var loops = EventLoops.create(2); var f = new SessionTestSupport.Fixture(config(loops, 4))) {
            f.listener.drain("joins remain open");
            var snapshot = f.client.capacity();
            var member = new MemberSpec("", List.of(new AttachmentSpec(Directions.BOTH, PipeOptions.defaults())));
            try (var change = f.client.beginChange(new ChangeRequest(snapshot.policyRevision(), snapshot.observed(),
                    Duration.ofSeconds(45), List.of(member), java.util.Set.of(), 0))) {
                var attempt = change.startMember(0, (setup, request) -> {
                    var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                    f.listener.adapter().handle(pair[1], JoinHint.inBand(null));
                    return new CarrierOpener.Opened(List.of(pair[0]));
                });
                assertEquals(Attempt.Disposition.CONFIRMED, attempt.completion().get(30, TimeUnit.SECONDS).disposition());
                assertEquals(CapacityChange.Status.COMMITTED, change.commit().status());
                PipeId id = attempt.completion().join().pipes().getFirst();
                assertTrue(f.server.pipes().stream().anyMatch(pipe -> pipe.id().equals(id)));
            }
        }
    }

    @Test void aModeRefusalAfterAuthorizationReportsItsAssignedId() throws Exception {
        // HS-20.
        var authorized = new CompletableFuture<PipeId>();
        try (var loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                    .sessionPolicy(new SessionPolicy(0, 0, request -> authorized.complete(request.pipe()), new CapacityLimits(4))).allowRecordMode((carrier, mode, peer) -> false)
                    .build();
            try (var f = new SessionTestSupport.Fixture(config)) {
                var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                var handled = f.listener.adapter().handle(p[1], JoinHint.inBand(null));
                assertThrows(IOException.class, () -> f.client.addPipe(p[0], PipeOptions.defaults()));
                var inboundError = assertThrows(JoinRefusedException.class, handled::awaitSetup);
                var id = authorized.get(3, TimeUnit.SECONDS);
                assertNotEquals(PipeId.UNASSIGNED, id);
                assertEquals(id, inboundError.pipeId());
            }
        }
    }

    @Test void peerLimitRefusesTheAdditionalAttachment() throws Exception {
        try (var loops = EventLoops.create(2);
             var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config(loops, 2)));
             var client = Zmux.connect(listener.localAddress(), config(loops, 6)); var server = listener.accept()) {
            client.addPipe(listener.localAddress(), PipeOptions.defaults());
            assertThrows(JoinRefusedException.class, () -> client.addPipe(listener.localAddress(), PipeOptions.defaults()));
            assertEquals(2, server.pipes().size());
        }
    }

    @SuppressWarnings("try") // Close races a deliberately incomplete Reply.
    @Test void withdrawalAndListenerStopHaveDistinctInboundOutcomes() throws Exception {
        // HS-04, HS-24.
        for (boolean stop : new boolean[]{false, true}) {
            try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor();
                 var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config(loops, 4)));
                 var client = Zmux.connect(listener.localAddress(), config(loops, 4)); var server = listener.accept()) {
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
                    Throwable reason = assertThrows(IOException.class, h::awaitSetup);
                    if (stop) assertInstanceOf(ListenerClosedException.class, reason);
                    else assertInstanceOf(java.io.InterruptedIOException.class, reason);
                    assertEquals(1, server.pipes().size());
                } finally { completion.failed(new IOException("fixture released")); }
                assertThrows(ExecutionException.class, () -> join.get(5, TimeUnit.SECONDS));
            }
        }
    }
}
