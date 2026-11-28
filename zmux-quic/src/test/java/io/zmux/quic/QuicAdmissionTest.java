package io.zmux.quic;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.*;
import io.zmux.carrier.PipeReader;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class QuicAdmissionTest {
    @BeforeAll static void tls() throws Exception { TestTls.init(); }

    static final PrefixStore.Id TICKET_ID = new PrefixStore.Id(3, 4);
    private static final Duration SLOT = Duration.ofSeconds(30);
    private static final PrefixStore.Generation GENERATION = new PrefixStore.Generation(new byte[32], 0, -1);

    static PrefixAuth presenter(boolean bound, byte[] ticketKey) {
        return new PrefixAuth(new PrefixAuth.CarrierOptions(bound, false), SLOT, TICKET_ID, ticketKey, java.util.List.of(GENERATION));
    }

    static PrefixAuth.Verifier verifier(boolean bound, byte[] ticketKey) {
        var store = new MemoryPrefixStore(new PrefixStore.Window(SLOT, 1), java.util.List.of(GENERATION));
        store.addTicket(new PrefixStore.Ticket(TICKET_ID, ticketKey, 0, 0, -1, false));
        return new PrefixAuth.Verifier(new PrefixAuth.CarrierOptions(bound, false), store);
    }

    static ListenerConfig admitting(ZmuxConfig config, java.util.function.BiFunction<Admission.CarrierInfo, ByteBuffer, Admission.Decision> decider) {
        return ListenerConfig.builder().session(config)
                .admission(Admission.builder(4, Admission.Decider.immediate(decider)).peekTimeout(Duration.ofSeconds(2)).build()).build();
    }

    @Test void existingConnectionsUseTheListenersSoleFallbackTarget() throws Exception {
        var serve = QuicServer.class.getMethod("serve", io.netty.handler.codec.quic.QuicChannel.class,
                QuicOptions.class, ZmuxListener.class);
        assertEquals(QuicConnectionService.class, serve.getReturnType());
        assertEquals(1, java.util.Arrays.stream(QuicServer.class.getMethods()).filter(m -> m.getName().equals("serve")).count());
        assertFalse(java.util.Arrays.stream(QuicServer.class.getDeclaredClasses()).anyMatch(c -> c.getSimpleName().equals("Fallback")));
    }

    @Test void anAcceptedFirstStreamOpensTheConnectionToTheListener() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var seen = new LinkedBlockingQueue<String>();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(admitting(config, (info, prefix) -> {
                     seen.add(info.source().getAddress().getHostAddress() + " " + info.applicationProtocol() + " " + prefix.remaining());
                     return Admission.Decision.accept(0);
                 }));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                ZmuxSession client = endpoint.connectAsync(server.address(), "localhost", config).get(10, TimeUnit.SECONDS);
                ZmuxSession accepted = listener.accept(Duration.ofSeconds(10));
                assertNotNull(accepted);
                assertEquals("127.0.0.1 qmux-02 4", seen.poll(1, TimeUnit.SECONDS));
                // Lanes, further streams of a connection, reach the listener once its first one is accepted.
                QuicTest.transfer(workers, client, accepted, 8, 128 << 10);
                assertTrue(client.pipeStats().getFirst().lanesOut() > 1, "outbound lanes were not opened");
                client.close();
                accepted.awaitClosed();
            }
        }
    }

    /**
     * A connection given back goes with its first stream to the listener's fallback: the takeover
     * carries the stream with exactly the bytes the decider saw replayed in front, and its QUIC
     * connection through zmux-quic; the connection's streams reach no session, and zmux closes the
     * connection once the fallback is done.
     */
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void aConnectionThatIsNotAcceptedGoesToTheListenersFallbackAsATakeover(boolean takeover) throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            var decided = new CompletableFuture<byte[]>();
            var got = new CompletableFuture<byte[]>();
            var connection = new CompletableFuture<io.netty.handler.codec.quic.QuicChannel>();
            var release = new CompletableFuture<Void>();
            Admission.Fallback fallback = t -> {
                QuicTakeover.connection(t).ifPresent(connection::complete);
                assertTrue(t.socket().isEmpty(), "a QUIC stream has no socket view");
                var all = new java.io.ByteArrayOutputStream();
                t.pipe().reader().start(new PipeReader.Sink() {
                    @Override public void onChunk(io.zmux.Chunk chunk, Runnable consumed) {
                        byte[] b = chunk.copy();
                        chunk.close();
                        consumed.run();
                        all.writeBytes(b);
                        if (all.size() >= 4) got.complete(all.toByteArray());
                    }
                    @Override public void onEnd() { got.complete(all.toByteArray()); }
                    @Override public void onError(Throwable cause) { got.complete(all.toByteArray()); }
                });
                return release; // the host holds the stream until the test lets it go
            };
            var lc = ListenerConfig.builder().session(config).fallback(fallback)
                    .admission(Admission.builder(4, Admission.Decider.immediate((info, prefix) -> {
                        byte[] b = new byte[prefix.remaining()];
                        prefix.duplicate().get(b);
                        decided.complete(b);
                        return takeover ? Admission.Decision.takeover(null) : Admission.Decision.FALLBACK;
                    })).peekTimeout(Duration.ofSeconds(2)).build()).build();
            try (var listener = Zmux.listener(lc);
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                var connecting = endpoint.connectAsync(server.address(), "localhost", config);
                byte[] prefix = decided.get(10, TimeUnit.SECONDS);
                byte[] first = got.get(10, TimeUnit.SECONDS);
                assertEquals(4, prefix.length);
                assertArrayEquals(prefix, java.util.Arrays.copyOf(first, 4), "the bytes read are replayed first");
                var quic = connection.get(5, TimeUnit.SECONDS);
                assertTrue(quic.isActive(), "the connection is the host's during the takeover");
                assertThrows(java.net.SocketTimeoutException.class, () -> listener.accept(Duration.ofMillis(200)));
                assertEquals(takeover ? 0 : 1, listener.stats().admission().fallback());
                assertEquals(takeover ? 1 : 0, listener.stats().admission().takeover());
                release.complete(null);
                quic.closeFuture().get(10, TimeUnit.SECONDS);
                assertThrows(Exception.class, () -> connecting.get(10, TimeUnit.SECONDS));
            }
        }
    }

    /**
     * A stream the host detached leaves its connection to the host: zmux closes neither the stream when the fallback
     * returns nor its connection (the client would end both on the stream's end); the host closes them itself.
     */
    @Test void aDetachedStreamLeavesItsConnectionToTheHost() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            var taken = new CompletableFuture<Takeover>();
            var lc = ListenerConfig.builder().session(config).fallback(t -> { assertTrue(t.detach()); taken.complete(t); return null; })
                    .admission(Admission.builder(4, Admission.Decider.immediate((info, prefix) -> Admission.Decision.fallback(7)))
                            .peekTimeout(Duration.ofSeconds(2)).build()).build();
            try (var listener = Zmux.listener(lc);
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                var connecting = endpoint.connectAsync(server.address(), "localhost", config);
                Takeover t = taken.get(10, TimeUnit.SECONDS);
                assertEquals(7, t.value());
                var quic = QuicTakeover.connection(t).orElseThrow();
                var service = quic.attr(ConnectionCarrier.KEY).get();
                if (service != null) service.whenDrained().toCompletableFuture().get(20, TimeUnit.SECONDS);
                assertTrue(quic.isActive(), "the detached stream and its connection are the host's");
                t.pipe().reader().close();
                t.pipe().writer().close();
                ConnectionCarrier.close(quic);
                quic.closeFuture().get(10, TimeUnit.SECONDS);
                assertThrows(Exception.class, () -> connecting.get(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test void aClosedVerdictClosesTheConnection() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(admitting(config, (info, prefix) -> Admission.Decision.CLOSE));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                var connecting = endpoint.connectAsync(server.address(), "localhost", config);
                assertThrows(Exception.class, () -> connecting.get(10, TimeUnit.SECONDS));
                assertThrows(java.net.SocketTimeoutException.class, () -> listener.accept(Duration.ofMillis(100)));
            }
        }
    }

    /**
     * A client presents its credential on the first stream of every connection it opens: the
     * session dialer from the listener's configuration, openPipe and the pool per call. The
     * server admits each connection by its stream 0, also when a pooled connection's second
     * stream is opened at once; a connection without the credential is refused.
     */
    @Test void credentialsAdmitEveryConnectionTheClientOpens() throws Exception {
        var auth = presenter(false, new byte[32]);
        var verifier = verifier(false, new byte[32]);
        var verdicts = new LinkedBlockingQueue<String>();
        Admission.Decider decider = (info, prefix, deadline) -> {
            int length = prefix.remaining();
            return verifier.decide(info, prefix, deadline).thenApply(d -> {
                verdicts.add(length + " " + d.verdict());
                return d;
            });
        };
        try (EventLoops loops = EventLoops.create(3); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = QuicEndpointTest.config(loops);
            var dialing = QuicEndpointTest.builder(loops).carrierCredential(auth).build();
            var options = QuicOptions.defaults();
            try (var local = Zmux.listener(ListenerConfig.of(dialing));
                 var remote = Zmux.listener(ListenerConfig.builder().session(config)
                         .admission(Admission.builder(PrefixAuth.TOKEN_LENGTH, decider).peekTimeout(Duration.ofSeconds(5)).build()).build());
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, remote);
                 var client = new QuicClient(TestTls.client, options, local)) {
                var a = client.connectAsync(server.address(), "localhost").get(10, TimeUnit.SECONDS);
                var b = remote.accept(Duration.ofSeconds(5));
                assertNotNull(b);
                assertEquals("64 ACCEPT", verdicts.poll(1, TimeUnit.SECONDS));

                var joined = client.dialAsync(server.address(), "localhost", a, auth).get(5, TimeUnit.SECONDS);
                a.addPipe(joined, PipeOptions.defaults());
                assertEquals("64 ACCEPT", verdicts.poll(5, TimeUnit.SECONDS));

                try (var pool = QuicPool.builder(client, server.address(), "localhost").limits(QuicPool.Limits.defaults().withMaxPipesPerConnection(2)).credential(auth).build()) {
                    var first = pool.openPipeAsync();
                    var second = pool.openPipeAsync();
                    var one = first.get(5, TimeUnit.SECONDS);
                    var two = second.get(5, TimeUnit.SECONDS);
                    assertSame(one.carrierConn(), two.carrierConn(), "one pooled connection");
                    a.addPipe(one, PipeOptions.defaults());
                    a.addPipe(two, PipeOptions.defaults());
                    assertEquals("64 ACCEPT", verdicts.poll(5, TimeUnit.SECONDS));
                    QuicTest.transfer(workers, a, b, 8, 128 << 10);
                    QuicTest.transfer(workers, b, a, 4, 64 << 10);
                }
                assertNull(verdicts.poll(100, TimeUnit.MILLISECONDS), "one admission per connection");

                var bare = client.dialAsync(server.address(), "localhost", a).get(5, TimeUnit.SECONDS);
                var refused = CompletableFuture.runAsync(() -> {
                    try { a.addPipe(bare, PipeOptions.defaults()); }
                    catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
                }, workers);
                String verdict = verdicts.poll(5, TimeUnit.SECONDS);
                assertNotNull(verdict, "a connection without the credential is decided");
                assertTrue(verdict.endsWith("CLOSE"), verdict);
                try { refused.get(10, TimeUnit.SECONDS); } catch (ExecutionException expected) { }
                a.close(); b.close(); a.awaitClosed(); b.awaitClosed();
            }
            assertEquals(0, io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
    }

    /** The session dialer with a configuration presents its credential; a bound one has the connection's exporter, and a listener that expects none refuses its prefix. */
    @Test void sessionDialersPresentTheConfiguredCredential() throws Exception {
        var auth = presenter(false, new byte[32]);
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(ListenerConfig.builder().session(config)
                         .admission(Admission.builder(PrefixAuth.TOKEN_LENGTH, verifier(false, new byte[32])).peekTimeout(Duration.ofSeconds(5)).build()).build());
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                var withCredential = ZmuxConfig.builder().eventLoops(loops).carrierCredential(auth).build();
                try (var _ = endpoint.connectAsync(server.address(), "localhost", withCredential).get(10, TimeUnit.SECONDS);
                     var accepted = listener.accept(Duration.ofSeconds(5))) {
                    assertNotNull(accepted);
                }
                var bound = ZmuxConfig.builder().eventLoops(loops)
                        .carrierCredential(presenter(true, new byte[32])).build();
                var failure = assertThrows(ExecutionException.class,
                        () -> endpoint.connectAsync(server.address(), "localhost", bound).get(30, TimeUnit.SECONDS));
                assertInstanceOf(java.io.IOException.class, failure.getCause());
            }
        }
    }
}
