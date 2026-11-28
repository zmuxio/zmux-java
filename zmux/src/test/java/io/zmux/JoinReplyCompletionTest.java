package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Exercises the acceptor while its Reply has reached the peer and its write is still outstanding. */
@Timeout(60)
class JoinReplyCompletionTest {
    private static final class ReplyGate implements PipeWriter {
        private final PipeWriter delegate;
        private final CompletableFuture<Completion> replyDelivered = new CompletableFuture<>();
        private final CompletableFuture<Void> release = new CompletableFuture<>();
        private boolean first = true;
        private int replyBytes;

        ReplyGate(PipeWriter delegate) {
            this.delegate = delegate;
            replyDelivered.thenAccept(completion -> release.thenRun(completion::completed));
        }

        @Override public void write(OutboundBatch batch, Completion completion) {
            if (!first) {
                delegate.write(batch, completion);
                return;
            }
            first = false;
            replyBytes = batch.bytes();
            delegate.write(batch, new Completion() {
                @Override public void completed() { replyDelivered.complete(completion); }
                @Override public void failed(Throwable cause) {
                    replyDelivered.completeExceptionally(cause);
                    completion.failed(cause);
                }
            });
        }

        void awaitDelivery() throws Exception { replyDelivered.get(30, TimeUnit.SECONDS); }
        void release() { release.complete(null); }
        @Override public void close() { delegate.close(); }
        @Override public void abort(Throwable cause) { delegate.abort(cause); }
    }

    private static void join(RecordMode mode, Framing framing, boolean changeGeneration) throws Exception {
        try (var loops = EventLoops.create(2)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops)
                    .sessionPolicy(new SessionPolicy(new CapacityLimits(4)))
                    .pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                    .noProbation().joinTimeout(Duration.ofMinutes(1))
                    .allowRecordMode((carrier, recordMode, peer) -> true).build();
            try (var f = new SessionTestSupport.Fixture(cfg)) {
                PipeIo[] pair = MemoryPipe.pair(framing);
                var gate = new ReplyGate(pair[1].writer());
                PipeIo server = new PipeIo(pair[1].reader(), gate, framing, "memory", null);
                try {
                    var handled = f.listener.adapter().handle(bound(server, mode), JoinHint.inBand(null));
                    var pipe = f.client.addPipe(bound(pair[0], mode),
                            PipeOptions.builder().recordMode(mode).returnOnReply(true).build());
                    gate.awaitDelivery();

                    if (changeGeneration) {
                        var policy = f.server.sessionPolicy();
                        f.server.setSessionPolicy(new SessionPolicy(1, 1, policy.authorizeJoin(), policy.limits()));
                        gate.release();
                        assertThrows(JoinAuthorizationChangedException.class, handled::awaitSetup);
                        assertEquals(1, f.server.pipes().size());
                    } else {
                        assertEquals(1, f.server.pipes().size());
                        try (var stream = f.client.openStream()) {
                            byte[] payload = {1, 3, 5};
                            stream.write(payload);
                            stream.finish();
                            gate.release();
                            handled.awaitSetup();
                            try (var peer = f.server.acceptStream()) {
                                assertArrayEquals(payload, peer.readAll(16));
                            }
                        }
                        assertTrue(f.server.pipes().stream().anyMatch(p -> p.id().equals(pipe.id())));
                        assertEquals(0, f.server.stats().pipeFailures());
                        assertEquals(0, f.client.stats().pipeFailures());
                    }
                } finally {
                    gate.release();
                }
            }
        }
    }

    @Test
    @Timeout(15)
    void timedOutReplySettlesItsLateCarrierPrefixAgainstTheOriginalDispatch() throws Exception {
        var clock = new SpliceHooksTest.ManualClock();
        var deadline = Duration.ofMillis(5);
        try (var loops = EventLoops.create(2)) {
            var config = SessionTestSupport.configBuilder(loops, 4).clockDomain(clock.domain())
                    .joinTimeout(deadline).handshakeShaper(view -> view.message().equals("reply")
                            ? new io.zmux.carrier.HandshakeShaper.UnitPlan(700, 0, 0, null)
                            : io.zmux.carrier.HandshakeShaper.UnitPlan.NONE)
                    .allowRecordMode((carrier, mode, peer) -> true).build();
            try (var fixture = new SessionTestSupport.Fixture(config)) {
                PipeIo[] pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var gate = new ReplyGate(pair[1].writer());
                try {
                    var server = pair[1].withHalves(pair[1].reader(), gate);
                    var admission = fixture.listener.adapter().handle(server, JoinHint.inBand(null));
                    var joined = fixture.client.driver.addPipe(pair[0], PipeOptions.builder()
                            .recordMode(RecordMode.SEALED).returnOnReply(true).build().join());
                    gate.awaitDelivery();
                    joined.get(5, TimeUnit.SECONDS);

                    var dispatched = fixture.server.shapingMeters().overhead();
                    long padding = dispatched.handshakeReservedBytes();
                    assertTrue(padding > 0);
                    clock.advance(deadline);
                    assertThrows(JoinTimeoutException.class, admission::awaitSetup);
                    assertEquals(padding, fixture.server.shapingMeters().overhead().handshakeReservedBytes());
                    gate.release();
                    var completed = fixture.server.shapingMeters().overhead();
                    assertEquals(0, completed.handshakeReservedBytes());
                    assertEquals(0, completed.handshakeUnresolvedBytes());
                    assertEquals(dispatched.handshakePaddingBytes() + padding, completed.handshakePaddingBytes());
                    assertEquals(dispatched.policy().baselineBytes() + gate.replyBytes - padding,
                            completed.policy().baselineBytes());
                } finally {
                    gate.release();
                    fixture.client.abort(0x10, "test complete");
                    fixture.server.abort(0x10, "test complete");
                    clock.advance(Duration.ofMinutes(1));
                }
            }
        }
    }

    private static PipeIo bound(PipeIo carrier, RecordMode mode) {
        return mode == RecordMode.BOUND ? carrier.withExporter((label, context, length) -> new byte[length]).withCarrierSecure() : carrier;
    }

    @Test void sealedOnMessages() throws Exception { join(RecordMode.SEALED, Framing.MESSAGES, false); }
    @Test void boundOnMessages() throws Exception { join(RecordMode.BOUND, Framing.MESSAGES, false); }
    @Test void sealedOnAByteStream() throws Exception { join(RecordMode.SEALED, Framing.BYTE_STREAM, false); }
    @Test void sealedAuthorizationChangesAfterReply() throws Exception { join(RecordMode.SEALED, Framing.BYTE_STREAM, true); }
    @Test void boundAuthorizationChangesAfterReply() throws Exception { join(RecordMode.BOUND, Framing.MESSAGES, true); }
}
