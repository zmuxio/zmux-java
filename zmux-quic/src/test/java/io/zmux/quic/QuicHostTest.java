package io.zmux.quic;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Sessions over QUIC live on their connection's loop and parse its reads in place. */
@Timeout(60)
class QuicHostTest {
    @org.junit.jupiter.api.BeforeAll static void tls() throws Exception { TestTls.init(); }

    @Test void sessionsRunOnTheConnectionLoopWithoutAnInbox() throws Exception {
        byte[] data = new byte[4 << 20];
        new Random(7).nextBytes(data);
        try (EventLoops loops = EventLoops.create(1)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build(); var options = QuicOptions.defaults();
            NettyHost serverHost, clientHost;
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                serverHost = server.host(); clientHost = endpoint.host();
                var session = endpoint.connect(server.address(), "localhost", config).toCompletableFuture().get(10, TimeUnit.SECONDS);
                var accepted = listener.accept(Duration.ofSeconds(5));
                assertNotNull(accepted);
                // The peer echoes the stream; both directions carry data.length bytes.
                var echo = CompletableFuture.runAsync(() -> {
                    try (ZmuxStream s = accepted.acceptStream()) {
                        ByteBuffer b = ByteBuffer.allocate(64 << 10);
                        while (s.read(b.clear()) >= 0) s.write(b.flip());
                        s.finish();
                    } catch (Exception e) { throw new CompletionException(e); }
                }, Executors.newVirtualThreadPerTaskExecutor());
                byte[] back = new byte[data.length];
                try (ZmuxStream s = session.openStream()) {
                    var reader = CompletableFuture.runAsync(() -> {
                        try {
                            ByteBuffer b = ByteBuffer.wrap(back);
                            while (b.hasRemaining() && s.read(b) >= 0) { }
                        } catch (Exception e) { throw new CompletionException(e); }
                    }, Executors.newVirtualThreadPerTaskExecutor());
                    for (int off = 0; off < data.length; off += 256 << 10) s.write(ByteBuffer.wrap(data, off, 256 << 10));
                    s.finish();
                    reader.get(30, TimeUnit.SECONDS);
                }
                echo.get(30, TimeUnit.SECONDS);
                assertArrayEquals(data, back);
                for (NettyHost host : new NettyHost[] {serverHost, clientHost}) {
                    assertEquals(0, host.externalTransports(), "a QUIC stream went through an inbox");
                    // Every byte reached its session on the connection's loop. Netty reads a stream
                    // per loop iteration, a few KiB at a time, so most records span reads and are
                    // carried (copied once); the copy is not asserted away.
                    long inPlace = host.inPlaceInputBytes(), copied = host.copiedInputBytes();
                    assertTrue(inPlace + copied >= data.length, Arrays.toString(new long[] {inPlace, copied}));
                }
                session.close(); accepted.close(); session.awaitClosed(); accepted.awaitClosed();
            }
            assertEquals(0, serverHost.outstandingBuffers());
            assertEquals(0, clientHost.outstandingBuffers());
            assertEquals(0, io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
    }

    @Test void quicPipesJoinASessionHostedOnAnotherLoop() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(3)
                    .withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO)).build();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listen(QuicTest.any(), ListenerConfig.of(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                // A TCP session lives on a NIO loop; its QUIC pipes reach it through their halves.
                var client = Zmux.connect(listener.localAddress(), config);
                var accepted = listener.accept(Duration.ofSeconds(10));
                assertNotNull(accepted);
                var pipe = endpoint.openPipe(server.address(), "localhost", client).toCompletableFuture().get(10, TimeUnit.SECONDS);
                ZmuxPipe joined = client.paths().add(pipe, PipeOptions.defaults());
                assertTrue(joined.id() > 0);
                QuicTest.transfer(workers, client, accepted, 8, 256 << 10);
                QuicTest.transfer(workers, accepted, client, 4, 64 << 10);
                client.close(); accepted.close(); client.awaitClosed(); accepted.awaitClosed();
            }
            assertEquals(0, io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops));
        }
    }

    @Test void closingTheEndpointEndsTheSessionsItHosts() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build(); var options = QuicOptions.defaults();
            NettyHost host;
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, options, listener)) {
                ZmuxSession client, accepted;
                try (var endpoint = new QuicClient(TestTls.client, options)) {
                    host = endpoint.host();
                    client = endpoint.connect(server.address(), "localhost", config).toCompletableFuture().get(10, TimeUnit.SECONDS);
                    accepted = listener.accept(Duration.ofSeconds(5));
                    assertNotNull(accepted);
                }
                assertNotNull(client.awaitClosed(), "the endpoint's session outlived its loop");
                assertNotNull(accepted.awaitClosed());
            }
            assertEquals(0, host.outstandingBuffers());
        }
    }
}
