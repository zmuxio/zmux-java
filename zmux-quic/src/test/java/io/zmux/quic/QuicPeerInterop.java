package io.zmux.quic;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.zmux.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.stream.Stream;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * QUIC sessions against the scripted peer of another implementation, named by {@code ZMUX_INTEROP_PEER} (zmux-rs's
 * {@code zmux-interop-peer}): its {@code quic-server 2} and {@code quic-client addr}, with or without the last
 * argument {@code exporter} (the join secret from the exporter alone, and {@code KEYS <hex>} per session). This side
 * drives: bulk echo, the {@code close} command (QUIC replies {@code closing} first), and a second, resumed session
 * whose first stream is {@code early}. No 0-RTT is offered by this adapter, so the early stream goes after the
 * handshake and the peer's report says it was not sent early. Explicit selection only: a missing peer fails.
 */
@Timeout(120)
class QuicPeerInterop {
    private static final byte[] PSK = "zmux interop pre-shared key 0123".getBytes(StandardCharsets.US_ASCII);
    private static final QuicOptions OPTIONS = QuicOptions.builder().streamWindow(2 << 20).lanes(16).build();

    private static String binary() {
        String p = System.getenv("ZMUX_INTEROP_PEER");
        if (p == null || p.isBlank() || !Files.isExecutable(Path.of(p)))
            throw new IllegalStateException("ZMUX_INTEROP_PEER must name a built executable (no skipped interop cases)");
        return p;
    }

    private static ZmuxConfig config(EventLoops loops, boolean exporter) {
        var b = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).earlyData(EarlyData.ZMUX_PEER).maxPipes(4).maxIncomingUniStreams(16)
                .allowRecordMode((carrier, mode, peer) -> true);
        return (exporter ? b.pipeAuth(PipeAuth.exporter().withEndToEnd()) : b.pipeAuth(PipeAuth.psk(PSK).withEndToEnd())).build();
    }

    private static InetSocketAddress address(String text) {
        int split = text.lastIndexOf(':');
        return new InetSocketAddress(text.substring(0, split), Integer.parseInt(text.substring(split + 1)));
    }

    private static String command(ZmuxSession session, String command, byte[] body) throws Exception {
        try (var stream = session.openStream(Preamble.builder().openInfo(command.getBytes(StandardCharsets.UTF_8)).build())) {
            if (body.length > 0) stream.write(body, 0, body.length);
            stream.finish();
            return new String(stream.readAll(1 << 20), StandardCharsets.UTF_8);
        }
    }

    private static void echo(ZmuxSession session, int size) throws Exception {
        byte[] bytes = new byte[size];
        new java.util.Random(size).nextBytes(bytes);
        try (var stream = session.openStream(Preamble.builder().openInfo("echo".getBytes(StandardCharsets.UTF_8)).build());
             var jobs = Executors.newVirtualThreadPerTaskExecutor()) {
            var writing = jobs.submit(() -> { stream.write(bytes, 0, bytes.length); stream.finish(); return null; });
            assertArrayEquals(bytes, stream.readAll(size + 1L));
            writing.get(20, TimeUnit.SECONDS);
        }
    }

    /** The peer's close command: QUIC replies "closing", then closes the session with code 7. */
    private static void closeByPeer(ZmuxSession session, QuicChannelInteropTest.Peer peer) throws Exception {
        assertEquals("closing", command(session, "close:7:done", new byte[0]));
        var closed = assertInstanceOf(SessionError.ApplicationClosed.class, session.awaitClosed());
        assertEquals(7, closed.code());
        assertEquals("done", closed.reason());
        peer.expect("CLOSED");
    }

    /** One line per prefix, in any order, by prefix without it; other lines are ignored. */
    private static java.util.Map<String, String> reports(QuicChannelInteropTest.Peer peer, String... prefixes) throws Exception {
        var got = new java.util.HashMap<String, String>();
        for (long end = System.nanoTime() + 20_000_000_000L; got.size() < prefixes.length && System.nanoTime() < end; ) {
            String line = peer.lines.poll(1, TimeUnit.SECONDS);
            if (line != null) for (String prefix : prefixes) if (line.startsWith(prefix)) got.put(prefix, line.substring(prefix.length()).trim());
        }
        assertEquals(prefixes.length, got.size(), "peer reports " + got + " of " + Arrays.toString(prefixes));
        return got;
    }

    private static void exporterKeys(ZmuxSession session, String theirs) throws Exception {
        assertEquals(theirs, HexFormat.of().formatHex(session.exportKeyingMaterial("EXPORTER-interop-app", new byte[0], 16)));
    }

    private static void client(boolean exporter) throws Exception {
        TestTls.init();
        try (var peer = new QuicChannelInteropTest.Peer(Stream.of(binary(), "quic-server", "2", exporter ? "exporter" : "").filter(a -> !a.isEmpty()).toArray(String[]::new));
             var loops = EventLoops.create(2)) {
            var remote = address(peer.expect("LISTEN "));
            var tls = QuicSslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).applicationProtocols("qmux-02");
            try (var client = new QuicClient(tls, OPTIONS)) {
                ZmuxSession first = client.connectAsync(remote, "localhost", config(loops, exporter)).get(10, TimeUnit.SECONDS);
                var firstReports = exporter ? reports(peer, "ESTABLISHED", "KEYS ") : reports(peer, "ESTABLISHED");
                echo(first, 4 << 20);
                if (exporter) exporterKeys(first, firstReports.get("KEYS "));
                closeByPeer(first, peer);
                ZmuxSession second = client.connectAsync(remote, "localhost", config(loops, exporter)).get(10, TimeUnit.SECONDS);
                assertTrue(command(second, "early", "early-data".getBytes(StandardCharsets.UTF_8)).startsWith("early=early-data;replayable="));
                var secondReports = exporter ? reports(peer, "ESTABLISHED", "KEYS ") : reports(peer, "ESTABLISHED");
                echo(second, 1 << 20);
                if (exporter) exporterKeys(second, secondReports.get("KEYS "));
                closeByPeer(second, peer);
                Thread.sleep(300);
            }
        }
    }

    private static void server(boolean exporter) throws Exception {
        TestTls.init();
        try (var loops = EventLoops.create(2);
             var listener = Zmux.listener(ListenerConfig.of(config(loops, exporter)));
             var server = new QuicServer(new InetSocketAddress("127.0.0.1", 0), TestTls.server, OPTIONS, listener)) {
            String address = "127.0.0.1:" + server.address().getPort();
            try (var peer = new QuicChannelInteropTest.Peer(Stream.of(binary(), "quic-client", address, exporter ? "exporter" : "").filter(a -> !a.isEmpty()).toArray(String[]::new))) {
                ZmuxSession first = listener.accept(Duration.ofSeconds(20));
                assertNotNull(first);
                var firstReports = exporter ? reports(peer, "ESTABLISHED", "KEYS ") : reports(peer, "ESTABLISHED");
                echo(first, 4 << 20);
                if (exporter) exporterKeys(first, firstReports.get("KEYS "));
                closeByPeer(first, peer);
                ZmuxSession second = listener.accept(Duration.ofSeconds(20));
                assertNotNull(second);
                try (var stream = second.acceptStream(Duration.ofSeconds(10))) {
                    assertEquals("early", new String(stream.preamble().openInfo(), StandardCharsets.UTF_8));
                    assertEquals("early-data", new String(stream.readAll(1 << 16), StandardCharsets.UTF_8));
                    stream.write("ok".getBytes(StandardCharsets.UTF_8));
                    stream.finish();
                }
                var secondReports = exporter ? reports(peer, "EARLY ", "ESTABLISHED", "KEYS ") : reports(peer, "EARLY ", "ESTABLISHED");
                assertTrue(secondReports.get("EARLY ").endsWith(" reply=ok"));
                echo(second, 1 << 20);
                if (exporter) exporterKeys(second, secondReports.get("KEYS "));
                closeByPeer(second, peer);
                Thread.sleep(300);
            }
        }
    }

    @Test void javaClient() throws Exception { client(false); }
    @Test void javaServer() throws Exception { server(false); }
    @Test void javaClientExporter() throws Exception { client(true); }
    @Test void javaServerExporter() throws Exception { server(true); }
}
