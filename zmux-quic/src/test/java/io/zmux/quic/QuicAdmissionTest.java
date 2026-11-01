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

    static ListenerConfig admitting(ZmuxConfig config, java.util.function.BiFunction<Admission.CarrierInfo, java.nio.ByteBuffer, Admission.Decision> decider) {
        return ListenerConfig.builder().session(config)
                .admission(Admission.of(4, Admission.Decider.immediate(decider)).withPeekTimeout(Duration.ofSeconds(2))).build();
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
                ZmuxSession client = endpoint.connect(server.address(), "localhost", config).toCompletableFuture().get(10, TimeUnit.SECONDS);
                ZmuxSession accepted = listener.accept(Duration.ofSeconds(10));
                assertNotNull(accepted);
                assertEquals("127.0.0.1 qmux-02 4", seen.poll(1, TimeUnit.SECONDS));
                // Lanes, further streams of a connection, reach the listener once its first one is accepted.
                QuicTest.transfer(workers, client, accepted, 8, 128 << 10);
                assertTrue(client.paths().stats().getFirst().lanesOut() > 1, "outbound lanes were not opened");
                client.close();
                accepted.awaitClosed();
            }
        }
    }

    @Test void aConnectionThatIsNotAcceptedGoesToTheFallbackWithItsFirstStream() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            var got = new CompletableFuture<byte[]>();
            QuicServer.Fallback fallback = (connection, stream) -> {
                assertNotNull(connection);
                var all = new java.io.ByteArrayOutputStream();
                stream.reader().start(new PipeReader.Sink() {
                    @Override public void onData(ByteBuffer data, Runnable release) {
                        byte[] b = new byte[data.remaining()];
                        data.get(b);
                        all.writeBytes(b);
                        release.run();
                        if (all.size() >= 4) got.complete(all.toByteArray());
                    }
                    @Override public void onEnd() { got.complete(all.toByteArray()); }
                    @Override public void onError(Throwable cause) { got.complete(all.toByteArray()); }
                });
            };
            try (var listener = Zmux.listener(admitting(config, (info, prefix) -> Admission.Decision.FALLBACK));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, listener, fallback);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                var connecting = endpoint.connect(server.address(), "localhost", config).toCompletableFuture();
                byte[] first = got.get(10, TimeUnit.SECONDS);
                assertTrue(first.length >= 4, "the bytes read are replayed to the fallback");
                assertNull(listener.accept(Duration.ofMillis(200)));
                server.connections().forEach(c -> c.close());
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
                var connecting = endpoint.connect(server.address(), "localhost", config).toCompletableFuture();
                assertThrows(Exception.class, () -> connecting.get(10, TimeUnit.SECONDS));
                assertNull(listener.accept(Duration.ofMillis(100)));
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
        var auth = new PrefixAuth(new byte[32], Duration.ofSeconds(30), false, 1, 0);
        var verifier = auth.verifier();
        var verdicts = new LinkedBlockingQueue<String>();
        Admission.Decider decider = Admission.Decider.immediate((info, prefix) -> {
            var d = verifier.decide(info, prefix);
            verdicts.add(prefix.remaining() + " " + d.verdict());
            return d.verdict() == Admission.Verdict.ACCEPT ? d : Admission.Decision.CLOSE;
        });
        try (EventLoops loops = EventLoops.create(3); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = QuicEndpointTest.config(loops);
            var dialing = ZmuxConfig.builder().eventLoops(loops).multipath(config.multipathConfig()).credential(auth).build();
            var options = QuicOptions.defaults();
            try (var local = Zmux.listener(ListenerConfig.of(dialing));
                 var remote = Zmux.listener(ListenerConfig.builder().session(config)
                         .admission(Admission.of(PrefixAuth.TOKEN_LENGTH, decider).withPeekTimeout(Duration.ofSeconds(5))).build());
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, remote);
                 var client = new QuicClient(TestTls.client, options, local)) {
                var a = client.connect(server.address(), "localhost").toCompletableFuture().get(10, TimeUnit.SECONDS);
                var b = remote.accept(Duration.ofSeconds(5));
                assertNotNull(b);
                assertEquals("32 ACCEPT", verdicts.poll(1, TimeUnit.SECONDS));

                var joined = client.openPipe(server.address(), "localhost", a, auth).toCompletableFuture().get(5, TimeUnit.SECONDS);
                a.paths().add(joined, PipeOptions.defaults());
                assertEquals("32 ACCEPT", verdicts.poll(5, TimeUnit.SECONDS));

                try (var pool = new QuicPool(client, server.address(), "localhost", new QuicPool.Limits(2, 0, Duration.ZERO), auth)) {
                    var first = pool.openPipe().toCompletableFuture();
                    var second = pool.openPipe().toCompletableFuture();
                    var one = first.get(5, TimeUnit.SECONDS);
                    var two = second.get(5, TimeUnit.SECONDS);
                    assertSame(one.carrierConn(), two.carrierConn(), "one pooled connection");
                    a.paths().add(one, PipeOptions.defaults());
                    a.paths().add(two, PipeOptions.defaults());
                    assertEquals("32 ACCEPT", verdicts.poll(5, TimeUnit.SECONDS));
                    QuicTest.transfer(workers, a, b, 8, 128 << 10);
                    QuicTest.transfer(workers, b, a, 4, 64 << 10);
                }
                assertNull(verdicts.poll(100, TimeUnit.MILLISECONDS), "one admission per connection");

                var bare = client.openPipe(server.address(), "localhost", a).toCompletableFuture().get(5, TimeUnit.SECONDS);
                var refused = CompletableFuture.runAsync(() -> {
                    try { a.paths().add(bare, PipeOptions.defaults()); }
                    catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
                }, workers);
                String verdict = verdicts.poll(5, TimeUnit.SECONDS);
                assertNotNull(verdict, "a connection without the credential is decided");
                assertTrue(verdict.endsWith("FALLBACK"), verdict);
                try { refused.get(10, TimeUnit.SECONDS); } catch (ExecutionException expected) { }
                a.close(); b.close(); a.awaitClosed(); b.awaitClosed();
            }
            assertEquals(0, io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
    }

    /** The session dialer with a configuration presents its credential; a bound one fails without an exporter. */
    @Test void sessionDialersPresentTheConfiguredCredential() throws Exception {
        var auth = new PrefixAuth(new byte[32], Duration.ofSeconds(30), false, 1, 0);
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(ListenerConfig.builder().session(config)
                         .admission(Admission.of(PrefixAuth.TOKEN_LENGTH, auth.verifier()).withPeekTimeout(Duration.ofSeconds(5))).build());
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                var withCredential = ZmuxConfig.builder().eventLoops(loops).credential(auth).build();
                try (var client = endpoint.connect(server.address(), "localhost", withCredential).toCompletableFuture().get(10, TimeUnit.SECONDS);
                     var accepted = listener.accept(Duration.ofSeconds(5))) {
                    assertNotNull(accepted);
                }
                var bound = ZmuxConfig.builder().eventLoops(loops)
                        .credential(new PrefixAuth(new byte[32], Duration.ofSeconds(30), true, 1, 0)).build();
                var failure = assertThrows(ExecutionException.class,
                        () -> endpoint.connect(server.address(), "localhost", bound).toCompletableFuture().get(10, TimeUnit.SECONDS));
                assertInstanceOf(java.io.IOException.class, failure.getCause());
            }
        }
    }
}
