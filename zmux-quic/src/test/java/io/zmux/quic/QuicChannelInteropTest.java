package io.zmux.quic;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.zmux.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * QUIC datagram channels against another implementation, both directions, plain and through a
 * lossy relay. The peer is an external program named by ZMUX_GO_QUIC_CHANNEL_PEER or
 * ZMUX_RS_QUIC_CHANNEL_PEER (the test is skipped when it is not set):
 *
 * <ul>
 *   <li>{@code server CERT KEY}: listens, prints {@code LISTEN addr}, echoes every session
 *       datagram and stream, prints {@code STATS sent=.. sentch=.. recv=.. recvch=..} when a
 *       session ends;
 *   <li>{@code client ADDR N}: sends N datagrams, expects each echoed, prints {@code STATS ...}.
 * </ul>
 */
@Timeout(120)
class QuicChannelInteropTest {
    @BeforeAll static void tls() throws Exception { TestTls.init(); }

    static String peer(String env) {
        String p = System.getenv(env);
        assumeTrue(p != null && !p.isEmpty(), env + " not set");
        assertTrue(Files.isExecutable(Path.of(p)), env + " is not executable: " + p);
        return p;
    }

    /** The test certificate and key as PEM files, for the peer's server. */
    static Path[] pem() throws Exception {
        Path dir = Path.of("target", "tls");
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(dir.resolve("localhost.p12"))) { store.load(in, "changeit".toCharArray()); }
        var mime = Base64.getMimeEncoder(64, "\n".getBytes());
        Path cert = dir.resolve("localhost.crt"), key = dir.resolve("localhost.key");
        Files.writeString(cert, "-----BEGIN CERTIFICATE-----\n" + mime.encodeToString(store.getCertificate("localhost").getEncoded())
                + "\n-----END CERTIFICATE-----\n");
        Files.writeString(key, "-----BEGIN PRIVATE KEY-----\n"
                + mime.encodeToString(store.getKey("localhost", "changeit".toCharArray()).getEncoded()) + "\n-----END PRIVATE KEY-----\n");
        return new Path[] {cert, key};
    }

    /** A running peer and its output lines. */
    static final class Peer implements AutoCloseable {
        final Process process;
        final BlockingQueue<String> lines = new LinkedBlockingQueue<>();
        final Thread reader;

        Peer(String... command) throws Exception {
            process = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
            reader = Thread.ofVirtual().start(() -> {
                try (var r = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    for (String l; (l = r.readLine()) != null; ) lines.add(l);
                } catch (Exception ignored) { }
            });
        }

        String expect(String prefix) throws Exception {
            for (long end = System.nanoTime() + 20_000_000_000L; System.nanoTime() < end; ) {
                String l = lines.poll(1, TimeUnit.SECONDS);
                if (l != null && l.startsWith(prefix)) return l.substring(prefix.length()).trim();
            }
            throw new AssertionError("peer: no " + prefix);
        }

        @Override public void close() throws Exception {
            if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
            reader.join(5000);
        }
    }

    static InetSocketAddress address(String text) {
        int i = text.lastIndexOf(':');
        return new InetSocketAddress(text.substring(0, i), Integer.parseInt(text.substring(i + 1)));
    }

    static void assertStats(String stats, int n) {
        assertTrue(stats.contains("sentch=" + n + " ") && stats.endsWith("recvch=" + n), stats);
    }

    /** This side dials the peer's server, plain or through a lossy relay. */
    void javaClient(String env, boolean lossy) throws Exception {
        Path[] pem = pem();
        try (var peer = new Peer(peer(env), "server", pem[0].toString(), pem[1].toString());
             var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            InetSocketAddress server = address(peer.expect("LISTEN "));
            var config = QuicChannelTest.config(loops);
            Duration d = Duration.ofMillis(40);
            try (var relay = lossy ? new QuicChannelTest.LossyRelay(server, d) : null;
                 var endpoint = new QuicClient(TestTls.client, QuicOptions.defaults())) {
                ZmuxSession c = endpoint.connect(lossy ? relay.address() : server, "localhost", config).toCompletableFuture().get(10, TimeUnit.SECONDS);
                try {
                    int n = 20;
                    for (int i = 0; i < n; i++) {
                        c.datagrams().send(ByteBuffer.wrap(QuicChannelTest.stamp(i)));
                        assertEquals(i, c.datagrams().receive(Duration.ofSeconds(5))[8]);
                    }
                    if (lossy) {
                        ZmuxStream st = c.openStream();
                        Future<byte[]> echo = workers.submit(() -> QuicTest.read(st));
                        st.write(ByteBuffer.wrap(new byte[] {1}));
                        Thread.sleep(4 * d.toMillis());
                        relay.drop.set(2);
                        st.write(ByteBuffer.wrap(new byte[QuicChannelTest.BURST]));
                        Thread.sleep(5);
                        List<Long> lat = new ArrayList<>();
                        for (int i = 0; i < 10; i++) c.datagrams().send(ByteBuffer.wrap(QuicChannelTest.stamp(i)));
                        for (int i = 0; i < 10; i++) {
                            try { lat.add(QuicChannelTest.latencyMillis(c.datagrams().receive(Duration.ofSeconds(2)))); }
                            catch (java.net.SocketTimeoutException e) { break; }
                        }
                        st.finish();
                        assertEquals(QuicChannelTest.BURST + 1, echo.get(20, TimeUnit.SECONDS).length);
                        st.close();
                        assertTrue(relay.dropped.get() > 0, "no packet lost");
                        assertTrue(lat.size() >= 5, lat.toString());
                        // A round trip through the relay is 2d; the lost stream packets delay none of them.
                        for (long l : lat) assertTrue(l < 3 * d.toMillis(), "echoed channel datagram took " + l + " ms: " + lat);
                        n += lat.size();
                        System.out.println(env + " lossy round trips " + lat);
                    }
                    var s = c.stats();
                    assertEquals(n, s.datagramsSentChannel(), s.toString());
                    assertEquals(n, s.datagramsReceivedChannel(), s.toString());
                    c.close();
                    c.awaitClosed();
                    assertStats(peer.expect("STATS "), n);
                } finally {
                    c.close();
                }
            }
            peer.process.destroy();
        }
    }

    /** The peer dials this side's server, which echoes its datagrams. */
    void javaServer(String env) throws Exception {
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = QuicChannelTest.config(loops);
            try (var listener = Zmux.listener(ListenerConfig.of(config));
                 var server = new QuicServer(QuicTest.any(), TestTls.server, QuicOptions.defaults(), listener);
                 var peer = new Peer(peer(env), "client", "127.0.0.1:" + server.address().getPort(), "20")) {
                ZmuxSession s = listener.accept(Duration.ofSeconds(20));
                assertNotNull(s);
                Future<?> echo = workers.submit(() -> {
                    for (;;) s.datagrams().send(ByteBuffer.wrap(s.datagrams().receive()));
                });
                try {
                    assertStats(peer.expect("STATS "), 20);
                    assertEquals(0, peer.process.waitFor());
                    s.awaitClosed();
                    assertEquals(20, s.stats().datagramsReceivedChannel(), s.stats().toString());
                    assertEquals(20, s.stats().datagramsSentChannel(), s.stats().toString());
                } finally {
                    echo.cancel(true);
                    s.close();
                }
            }
        }
    }

    @Test void javaClientGoServer() throws Exception { javaClient("ZMUX_GO_QUIC_CHANNEL_PEER", false); }
    @Test void javaClientGoServerLossy() throws Exception { javaClient("ZMUX_GO_QUIC_CHANNEL_PEER", true); }
    @Test void goClientJavaServer() throws Exception { javaServer("ZMUX_GO_QUIC_CHANNEL_PEER"); }
    @Test void javaClientRustServer() throws Exception { javaClient("ZMUX_RS_QUIC_CHANNEL_PEER", false); }
    @Test void javaClientRustServerLossy() throws Exception { javaClient("ZMUX_RS_QUIC_CHANNEL_PEER", true); }
    @Test void rustClientJavaServer() throws Exception { javaServer("ZMUX_RS_QUIC_CHANNEL_PEER"); }
}
