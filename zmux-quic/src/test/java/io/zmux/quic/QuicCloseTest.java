package io.zmux.quic;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicConnectionCloseEvent;
import io.zmux.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Close delivery over QUIC: the zmux close reaches the peer first, then the QUIC connection closes with the same code. */
@Timeout(60)
class QuicCloseTest {
    @BeforeAll static void tls() throws Exception { TestTls.init(); }
    static InetSocketAddress any() { return new InetSocketAddress("127.0.0.1", 0); }

    /** The QUIC close the peer of connection received, once it arrives. */
    static CompletableFuture<QuicConnectionCloseEvent> peerClose(QuicChannel connection) {
        var got = new CompletableFuture<QuicConnectionCloseEvent>();
        connection.pipeline().addFirst(new ChannelInboundHandlerAdapter() {
            @Override public void userEventTriggered(ChannelHandlerContext context, Object event) {
                if (event instanceof QuicConnectionCloseEvent close) got.complete(close);
                context.fireUserEventTriggered(event);
            }
        });
        connection.closeFuture().addListener(done -> got.completeExceptionally(new IOException("closed without the peer's close")));
        return got;
    }

    /** As the core close-delivery check: 1 MiB, then close with code and reason; the peer reads it all, then sees the code. */
    static void closeDelivers(ZmuxSession client, ZmuxSession server, long code, String reason) throws Exception {
        byte[] data = new byte[1 << 20];
        new java.util.Random(code).nextBytes(data);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var st = client.openStream();
            st.write(data);
            var second = client.openStream();
            second.write(new byte[] {1, 2, 3});
            var in = server.acceptStream(Duration.ofSeconds(5));
            assertNotNull(in);
            Future<byte[]> read = workers.submit(() -> {
                var got = new java.io.ByteArrayOutputStream();
                byte[] b = new byte[1 << 16];
                try {
                    for (int n; (n = in.read(b, 0, b.length)) > 0; ) got.write(b, 0, n);
                } catch (IOException ended) { /* The session's end. */ }
                return got.toByteArray();
            });
            // With a linger of an hour, an end within the guard means the peer read the close and ended its side.
            io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ofHours(1));
            SessionError.ApplicationClosed mine;
            try {
                if (code == 0 && reason.isEmpty()) client.close(); else client.close(code, reason);
                mine = assertInstanceOf(SessionError.ApplicationClosed.class, client.closed().get(30, TimeUnit.SECONDS));
                assertEquals(new SessionError.ApplicationClosed(code, reason, true), server.closed().get(30, TimeUnit.SECONDS));
            } finally {
                io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ZERO);
            }
            assertEquals(code, mine.code());
            assertFalse(mine.remote());
            var e = assertThrows(SessionClosedException.class, () -> second.write(new byte[] {4}));
            assertEquals(mine, e.error());
            byte[] got = read.get(5, TimeUnit.SECONDS);
            assertTrue(Arrays.equals(data, got), "the peer read " + got.length + " bytes, not the whole 1 MiB in order");
        }
    }

    @Test void theQuicConnectionClosesAfterTheZmuxCloseWithItsCode() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                for (long code : new long[] {0x2a, 0}) {
                    ZmuxSession client = endpoint.connect(server.address(), "localhost", config);
                    ZmuxSession accepted = listener.accept(Duration.ofSeconds(10));
                    assertNotNull(accepted);
                    QuicChannel serverSide = server.connections().stream().filter(QuicChannel::isActive).findFirst().orElseThrow();
                    var closeSeen = peerClose(serverSide);
                    QuicChannel clientSide = endpoint.connections().getFirst();
                    closeDelivers(client, accepted, code, code == 0 ? "" : "bye");
                    var close = closeSeen.get(5, TimeUnit.SECONDS);
                    assertTrue(close.isApplicationClose(), close.toString());
                    assertEquals(code, close.error(), "the QUIC close carries the zmux close's code");
                    assertTrue(clientSide.closeFuture().await(5, TimeUnit.SECONDS));
                    assertTrue(serverSide.closeFuture().await(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    /** An abort keeps the order: the QUIC close follows the zmux close's delivery, with the abort's code. */
    @Test void anAbortStillClosesTheConnectionAfterTheZmuxClose() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                ZmuxSession client = endpoint.connect(server.address(), "localhost", config);
                ZmuxSession accepted = listener.accept(Duration.ofSeconds(10));
                assertNotNull(accepted);
                var closeSeen = peerClose(server.connections().getFirst());
                client.abort(0x33, "gone");
                assertEquals(new SessionError.ApplicationClosed(0x33, "gone", true), accepted.closed().get(5, TimeUnit.SECONDS),
                        "the peer read the zmux close, not a lost connection");
                var close = closeSeen.get(5, TimeUnit.SECONDS);
                assertTrue(close.isApplicationClose());
                assertEquals(0x33, close.error());
            }
        }
    }

    /**
     * A server whose session ends by its own close closes its accepted connection, with that
     * code; the client, whose session the peer ended, waits for that close.
     */
    @Test void aServersOwnCloseClosesTheAcceptedConnectionWithItsCode() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = QuicOptions.defaults();
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(any(), TestTls.server, options, listener);
                 var endpoint = new QuicClient(TestTls.client, options)) {
                ZmuxSession client = endpoint.connect(server.address(), "localhost", config);
                ZmuxSession accepted = listener.accept(Duration.ofSeconds(10));
                assertNotNull(accepted);
                var atClient = peerClose(endpoint.connections().getFirst());
                accepted.close(0x44, "server");
                assertEquals(new SessionError.ApplicationClosed(0x44, "server", true), client.closed().get(5, TimeUnit.SECONDS));
                var close = atClient.get(5, TimeUnit.SECONDS);
                assertTrue(close.isApplicationClose());
                assertEquals(0x44, close.error());
            }
        }
    }
}
