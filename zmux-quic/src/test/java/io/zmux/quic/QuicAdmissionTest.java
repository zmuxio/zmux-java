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
            try (var listener = ZmuxListener.of(admitting(config, (info, prefix) -> {
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
            try (var listener = ZmuxListener.of(admitting(config, (info, prefix) -> Admission.Decision.FALLBACK));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, listener, fallback);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                var connecting = endpoint.connectAsync(server.address(), "localhost", config);
                byte[] first = got.get(10, TimeUnit.SECONDS);
                assertTrue(first.length >= 4, "the bytes read are replayed to the fallback");
                assertThrows(java.net.SocketTimeoutException.class, () -> listener.accept(Duration.ofMillis(200)));
                server.connections().forEach(c -> c.close());
                assertThrows(Exception.class, () -> connecting.get(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test void aClosedVerdictClosesTheConnection() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            try (var listener = ZmuxListener.of(admitting(config, (info, prefix) -> Admission.Decision.CLOSE));
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
        var auth = new PrefixAuth(new byte[32], Duration.ofSeconds(30), false, 1, 0);
        var verifier = auth.verifier();
        var verdicts = new LinkedBlockingQueue<String>();
        Admission.Decider decider = Admission.Decider.immediate((info, prefix) -> {
            var d = verifier.decide(info, prefix);
            verdicts.add(prefix.remaining() + " " + d.verdict());
            return d.verdict() == AdmissionVerdict.ACCEPT ? d : Admission.Decision.CLOSE;
        });
        try (EventLoops loops = EventLoops.create(3); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = QuicEndpointTest.config(loops);
            var dialing = QuicEndpointTest.builder(loops).carrierCredential(auth).build();
            var options = QuicOptions.defaults();
            try (var local = ZmuxListener.of(ListenerConfig.of(dialing));
                 var remote = ZmuxListener.of(ListenerConfig.builder().session(config)
                         .admission(Admission.of(PrefixAuth.TOKEN_LENGTH, decider).withPeekTimeout(Duration.ofSeconds(5))).build());
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, remote);
                 var client = new QuicClient(TestTls.client, options, local)) {
                var a = client.connectAsync(server.address(), "localhost").get(10, TimeUnit.SECONDS);
                var b = remote.accept(Duration.ofSeconds(5));
                assertNotNull(b);
                assertEquals("32 ACCEPT", verdicts.poll(1, TimeUnit.SECONDS));

                var joined = client.dialAsync(server.address(), "localhost", a, auth).get(5, TimeUnit.SECONDS);
                a.addPipe(joined, PipeOptions.defaults());
                assertEquals("32 ACCEPT", verdicts.poll(5, TimeUnit.SECONDS));

                try (var pool = new QuicPool(client, server.address(), "localhost", QuicPool.Limits.defaults().withMaxPipesPerConnection(2), auth)) {
                    var first = pool.openPipeAsync();
                    var second = pool.openPipeAsync();
                    var one = first.get(5, TimeUnit.SECONDS);
                    var two = second.get(5, TimeUnit.SECONDS);
                    assertSame(one.carrierConn(), two.carrierConn(), "one pooled connection");
                    a.addPipe(one, PipeOptions.defaults());
                    a.addPipe(two, PipeOptions.defaults());
                    assertEquals("32 ACCEPT", verdicts.poll(5, TimeUnit.SECONDS));
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
                assertTrue(verdict.endsWith("FALLBACK"), verdict);
                try { refused.get(10, TimeUnit.SECONDS); } catch (ExecutionException expected) { }
                a.close(); b.close(); a.awaitClosed(); b.awaitClosed();
            }
            assertEquals(0, io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
    }

    /** The session dialer with a configuration presents its credential; a bound one has the connection's exporter, and a listener that expects none refuses its prefix. */
    @Test void sessionDialersPresentTheConfiguredCredential() throws Exception {
        var auth = new PrefixAuth(new byte[32], Duration.ofSeconds(30), false, 1, 0);
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            try (var listener = ZmuxListener.of(ListenerConfig.builder().session(config)
                         .admission(Admission.of(PrefixAuth.TOKEN_LENGTH, auth.verifier()).withPeekTimeout(Duration.ofSeconds(5))).build());
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                var withCredential = ZmuxConfig.builder().eventLoops(loops).carrierCredential(auth).build();
                try (var _ = endpoint.connectAsync(server.address(), "localhost", withCredential).get(10, TimeUnit.SECONDS);
                     var accepted = listener.accept(Duration.ofSeconds(5))) {
                    assertNotNull(accepted);
                }
                var bound = ZmuxConfig.builder().eventLoops(loops)
                        .carrierCredential(new PrefixAuth(new byte[32], Duration.ofSeconds(30), true, 1, 0)).build();
                var failure = assertThrows(ExecutionException.class,
                        () -> endpoint.connectAsync(server.address(), "localhost", bound).get(30, TimeUnit.SECONDS));
                assertInstanceOf(java.io.IOException.class, failure.getCause());
            }
        }
    }
}
