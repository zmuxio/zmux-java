package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A listener that refuses a new session by its policy, while draining or with its backlog full,
 * sends CONNECTION_REFUSED after its own parameters, as under memory pressure. TCP, not a
 * synchronous memory pipe: the refusal must reach a client still writing its first records.
 */
@Timeout(30)
class ListenerRefuseTest {
    private static final InetSocketAddress LOOPBACK = new InetSocketAddress("127.0.0.1", 0);

    @Test void aDrainingListenerRefusesANewSession() throws Exception {
        // HS-30, HS-32.
        try (var l = Zmux.listen(LOOPBACK, ListenerConfig.builder().backlog(1).build())) {
            l.drain("emptying");
            assertEquals("emptying", assertRefused(l).reason());
            l.undrain();
            try (var peer = Zmux.connect(l.localAddress()); var accepted = l.accept()) {
                assertNotNull(accepted.ping());
                assertNotNull(peer.ping());
            }
            l.drain("new reason");
            l.drain("replacement");
            assertEquals("replacement", assertRefused(l).reason());
            assertEquals(2, l.stats().refused());
        }
    }

    @Test void classificationsInProgressKeepTheirDecisionAcrossDrainChanges() throws Exception {
        // HS-33.
        for (boolean drainFirst : new boolean[]{true, false}) {
            var classified = new java.util.concurrent.CompletableFuture<Void>();
            var resume = new java.util.concurrent.CompletableFuture<Void>();
            var cfg = ListenerConfig.builder().sessionConfigs((info, value) -> {
                classified.complete(null);
                resume.join();
                return null;
            }).build();
            try (var l = Zmux.listen(LOOPBACK, cfg); var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                if (drainFirst) l.drain("classified");
                var dial = workers.submit(() -> {
                    if (drainFirst) return assertRefused(l).reason();
                    try (var client = Zmux.connect(l.localAddress()); var server = l.accept()) {
                        assertNotNull(server.ping());
                        assertNotNull(client.ping());
                        return "accepted";
                    }
                });
                try {
                    classified.get(5, java.util.concurrent.TimeUnit.SECONDS);
                    if (drainFirst) l.undrain(); else l.drain("later");
                } finally { resume.complete(null); }
                assertEquals(drainFirst ? "classified" : "accepted", dial.get(5, java.util.concurrent.TimeUnit.SECONDS));
                assertEquals(drainFirst ? 1 : 0, l.stats().refused());
            }
        }
    }

    @Test void shutdownDrainCannotBeUndone() throws Exception {
        // HS-34.
        try (var l = Zmux.listen(LOOPBACK, ListenerConfig.builder().build());
             var client = Zmux.connect(l.localAddress()); var server = l.accept();
             var sending = client.openStream()) {
            sending.write(new byte[]{1});
            try (var receiving = server.acceptStream()) {
                assertEquals(1, receiving.read(java.nio.ByteBuffer.allocate(1)));
                l.shutdownAsync(0, "shutdown reason");
                l.undrain();
                l.drain("ignored");
                assertEquals("shutdown reason", assertRefused(l).reason());
            }
        }
    }

    /** Expose only the server parameters. The client can try to join before seeing CONNECTION_CLOSE. */
    @Test void drainedBootstrapHasNoJoinRouteEvenWhileItsRefusalIsInFlight() throws Exception {
        // HS-33.
        var outcome = new java.util.concurrent.LinkedBlockingQueue<Throwable>();
        var resume = new java.util.concurrent.CompletableFuture<Void>();
        try (var loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                    .joinOutcome((s,p,e) -> outcome.add(e)).build();
            try (var listener = Zmux.listener(ListenerConfig.of(cfg))) {
                listener.drain("emptying");
                var pair = io.zmux.carrier.MemoryPipe.pair(io.zmux.carrier.Framing.BYTE_STREAM);
                var gated = new io.zmux.carrier.PipeWriter() {
                    boolean first = true;
                    @Override public void write(io.zmux.carrier.OutboundBatch batch, Completion done) {
                        if (!first) { resume.thenRun(() -> pair[1].writer().write(batch, done)); return; }
                        first = false;
                        var bytes = java.nio.ByteBuffer.allocate(batch.bytes());
                        for (int i = 0; i < batch.segmentCount(); i++) bytes.put(batch.segment(i).duplicate());
                        bytes.flip();
                        int n = Math.toIntExact(io.zmux.internal.wire.VarInt.get(bytes, 0, bytes.limit()))
                                + io.zmux.internal.wire.VarInt.lengthOf(bytes.get(0));
                        pair[1].writer().write(io.zmux.carrier.OutboundBatch.of(bytes.slice(0, n)), new Completion() {
                            @Override public void completed() {
                                resume.thenRun(() -> {
                                    if (n == bytes.limit()) done.completed();
                                    else pair[1].writer().write(io.zmux.carrier.OutboundBatch.of(bytes.slice(n, bytes.limit() - n)), done);
                                });
                            }
                            @Override public void failed(Throwable t) { done.failed(t); }
                        });
                    }
                    @Override public void close() { pair[1].writer().close(); }
                };
                var bootstrap = listener.adapter().handle(io.zmux.carrier.PipeIo.of(pair[1].reader(), gated), JoinHint.inBand(null));
                try (var client = Zmux.open(pair[0], Role.CLIENT, cfg)) {
                    assertThrows(IOException.class, bootstrap::awaitSetup);
                    var extra = io.zmux.carrier.MemoryPipe.pair(io.zmux.carrier.Framing.BYTE_STREAM);
                    var join = listener.adapter().handle(extra[1], JoinHint.inBand(null));
                    IOException error = assertThrows(IOException.class, () -> client.addPipe(extra[0], PipeOptions.defaults()));
                    assertSame(error, outcome.poll(3, java.util.concurrent.TimeUnit.SECONDS));
                    assertThrows(IOException.class, join::awaitSetup);
                    assertEquals(0, listener.stats().joins());
                    resume.complete(null);
                    var refusal = assertInstanceOf(SessionError.TransportClosed.class, client.awaitClosed());
                    assertTrue(refusal.isConnectionRefused());
                    assertEquals("emptying", refusal.reason());
                    // HS-32. Undrain accepts the next arrival; the original dialer's terminal result is immutable.
                    listener.undrain();
                    assertEquals(refusal, client.awaitClosed());
                    assertEquals(refusal, assertThrows(SessionClosedException.class, client::openStream).error());
                } finally { resume.complete(null); }
            }
        }
    }

    // A paired bootstrap reads the refusal code and reason over its downlink.
    @Test void pairedBootstrapReceivesTheDrainReasonInTheProtocol() throws Exception {
        // HS-30.
        try (var loops = EventLoops.create(2); var l = Zmux.listener(ListenerConfig.of(ListenerEntryTest.config(loops)))) {
            l.drain("split drain");
            for (boolean upFirst : new boolean[]{false, true}) {
                var up = io.zmux.carrier.MemoryPipe.pair(io.zmux.carrier.Framing.BYTE_STREAM);
                var down = io.zmux.carrier.MemoryPipe.pair(io.zmux.carrier.Framing.BYTE_STREAM);
                for (boolean upload : new boolean[]{upFirst, !upFirst}) {
                    var half = upload ? io.zmux.carrier.PipeIo.of(up[1].reader(), null)
                            : io.zmux.carrier.PipeIo.of(null, down[1].writer());
                    l.adapter().handle(half, JoinHint.pair(SplitBootstrapTest.token(upFirst ? 1 : 2), upload, null));
                }
                try (var c = Zmux.open(io.zmux.carrier.PipeIo.of(down[0].reader(), up[0].writer()), Role.CLIENT, l.config())) {
                    var refusal = assertInstanceOf(SessionError.TransportClosed.class, c.awaitClosed());
                    assertTrue(refusal.isConnectionRefused());
                    assertEquals("split drain", refusal.reason());
                } catch (ConnectionRefusedException refused) {
                    assertEquals("split drain", refused.reason());
                }
            }
            assertEquals(2, l.stats().refused());
            assertEquals(0, l.queued.get());
        }
    }

    @Test void aFullBacklogRefusesANewSession() throws Exception {
        try (var l = Zmux.listen(LOOPBACK, ListenerConfig.builder().backlog(1).build());
             var _ = Zmux.connect(l.localAddress())) {
            // The one backlog slot is taken by a session nobody accepts.
            PipePoolTest.await(() -> l.queued.get() == 1);
            var before = l.stats();
            assertRefused(l);
            var after = l.stats();
            assertEquals(1, before.sessions());
            assertEquals(2, after.sessions());
            assertEquals(before.refused(), after.refused());
            assertEquals(before.rejected(), after.rejected());
        }
    }

    private static SessionError.TransportClosed assertRefused(ZmuxListener l) throws Exception {
        SessionError e;
        try (var c = Zmux.connect(l.localAddress())) {
            e = c.awaitClosed();
        } catch (SessionClosedException closed) {
            assertInstanceOf(ConnectionRefusedException.class, closed);
            e = closed.error();
        } catch (IOException other) {
            throw new AssertionError("the client ended without the peer's refusal", other);
        }
        var t = assertInstanceOf(SessionError.TransportClosed.class, e, String.valueOf(e));
        assertTrue(t.remote(), String.valueOf(e));
        assertEquals(0x02, t.code(), String.valueOf(e));
        assertTrue(t.isConnectionRefused());
        return t;
    }
}
