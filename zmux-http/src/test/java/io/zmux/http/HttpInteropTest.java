package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import io.zmux.Role;
import io.zmux.Zmux;
import io.zmux.ZmuxListener;
import io.zmux.ZmuxSession;
import io.zmux.ZmuxStream;
import io.zmux.Admission;
import io.zmux.HttpStatusException;
import io.zmux.PrefixAuth;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * HTTP carriers against the Go and Rust implementations, both directions. The peers are
 * external programs named by environment variables; a test whose peer is not set is skipped:
 *
 * <ul>
 *   <li>ZMUX_GO_EXCHANGE_PEER: zmux-go's test binary ({@code go test -c}), run as
 *       {@code TestHTTPExchangeInterop};
 *   <li>ZMUX_RS_EXCHANGE_PEER: zmux-rs's {@code exchange_interop} example;
 *   <li>ZMUX_GO_HTTP_PEER, ZMUX_RS_HTTP_PEER: streaming peers ({@code server|client|bad KIND ADDR}, KIND stream, split, stream2 or split2 for h2c): PrefixAuth "right" as the body prefix or header form, a fallback that answers 418;
 *   <li>ZMUX_GO_WS_PEER, ZMUX_RS_WS_PEER: WebSocket peers ({@code server|client|bad ADDR}):
 *       PrefixAuth "ws-interop" (30 s slots, skew 1) in the upgrade's header field, a fallback
 *       that answers 418 and prints FALLBACK, a session that echoes one stream;
 * </ul>
 *
 * The exchange peers are configured by ZMUX_EXCHANGE* variables: a server echoes every
 * stream, a client sends random bytes on one stream and checks the echo.
 */
@Timeout(600)
class HttpInteropTest {
    static final String PSK = "000102030405060708090a0b0c0d0e0f";

    /** The command of a peer, or null when it is not set. */
    static List<String> peer(String env) {
        String p = System.getenv(env);
        if (p == null || p.isEmpty()) return null;
        assertTrue(Files.isExecutable(Path.of(p)), env + " is not executable: " + p);
        return env.contains("_GO_") ? List.of(p, "-test.run", "^TestHTTPExchangeInterop$", "-test.count=1", "-test.v") : List.of(p);
    }

    @Test void exchangeWithGo() throws Exception { exchangeMatrix("ZMUX_GO_EXCHANGE_PEER"); }

    @Test void exchangeWithRust() throws Exception { exchangeMatrix("ZMUX_RS_EXCHANGE_PEER"); }

    private void exchangeMatrix(String env) throws Exception {
        List<String> cmd = peer(env);
        assumeTrue(cmd != null, env + " not set");
        List<String> failed = new ArrayList<>();
        for (boolean javaServer : new boolean[] {true, false}) {
            for (String psk : new String[] {PSK, ""}) {
                for (boolean b64 : new boolean[] {false, true}) {
                    for (boolean split : new boolean[] {false, true}) {
                        String tag = (javaServer ? "java-server" : "java-client") + " psk=" + !psk.isEmpty() + " b64=" + b64 + " split=" + split;
                        try {
                            if (javaServer) javaServer(cmd, psk, b64, split);
                            else javaClient(cmd, psk, b64, split);
                            System.out.println("PASS " + env + " " + tag);
                        } catch (Exception | AssertionError e) {
                            System.out.println("FAIL " + env + " " + tag + ": " + e);
                            failed.add(tag + ": " + e);
                        }
                    }
                }
            }
        }
        assertEquals(List.of(), failed);
    }

    // ---- WebSocket ----

    static final PrefixAuth WS_AUTH = new PrefixAuth("ws-interop".getBytes(StandardCharsets.US_ASCII), Duration.ofSeconds(30), false, 1, 0);

    @Test void webSocketWithGo() throws Exception { webSocket("ZMUX_GO_WS_PEER"); }

    @Test void webSocketWithRust() throws Exception { webSocket("ZMUX_RS_WS_PEER"); }

    private void webSocket(String env) throws Exception {
        String bin = System.getenv(env);
        assumeTrue(bin != null && !bin.isEmpty(), env + " not set");
        // The peer's clients against a Java server.
        var seen = new LinkedBlockingQueue<String>();
        var verifier = WS_AUTH.verifier();
        try (var f = new Fixture(b -> b, l -> l.admission(Admission.of(PrefixAuth.TOKEN_LENGTH, verifier::decide)), e -> e.webSocket(),
                false, () -> new Teapot(seen))) {
            Thread echo = echoSessions(f.listener);
            String addr = "127.0.0.1:" + f.base.getPort();
            assertEquals("OK ping", lastLine(run(List.of(bin, "client", addr))));
            assertTrue(seen.isEmpty(), "an admitted client reached the fallback");
            String bad = lastLine(run(List.of(bin, "bad", addr)));
            assertTrue(bad.startsWith("REFUSED") && bad.contains("418"), bad);
            assertEquals("/zmux", seen.poll(5, TimeUnit.SECONDS));
            f.listener.close();
            echo.join(5000);
        }
        // Java clients against the peer's server.
        int port = freePort();
        String addr = "127.0.0.1:" + port;
        Process p = startReady(List.of(bin, "server", addr));
        try (var f = Fixture.plain(b -> b); var hc = f.client(b -> b)) {
            URI u = URI.create("ws://" + addr + "/zmux");
            try (ZmuxSession s = Zmux.open(hc.webSocketPipe(u, Map.of(), List.of(), WS_AUTH), Role.CLIENT, f.config)) {
                assertEquals("ping", new String(pingPong(s), StandardCharsets.US_ASCII));
            }
            var e = assertThrows(HttpStatusException.class, () -> hc.webSocketPipe(u, Map.of(), List.of(), null));
            assertEquals(418, e.status());
            awaitOutput(p, "SESSION ping");
            awaitOutput(p, "FALLBACK /zmux");
        } finally {
            stop(p);
        }
    }

    // ---- streaming (bidirectional and split, HTTP/1.1 and h2c, prefix authentication) ----

    @Test void streamingWithGo() throws Exception { streaming("ZMUX_GO_HTTP_PEER"); }

    @Test void streamingWithRust() throws Exception { streaming("ZMUX_RS_HTTP_PEER"); }

    static final io.zmux.PipeOptions.Direction BIDI = io.zmux.PipeOptions.Direction.BIDIRECTIONAL;

    static PrefixAuth streamAuth(String key) {
        return new PrefixAuth(key.getBytes(StandardCharsets.US_ASCII), Duration.ofSeconds(30), false, 1, 0);
    }

    private void streaming(String env) throws Exception {
        String bin = System.getenv(env);
        assumeTrue(bin != null && !bin.isEmpty(), env + " not set");
        for (String kind : new String[] {"stream", "split", "stream2", "split2"}) {
            boolean split = kind.startsWith("split"), h2 = kind.endsWith("2");
            // The peer's client against a Java server.
            var seen = new LinkedBlockingQueue<String>();
            var verifier = streamAuth("right").verifier();
            try (var f = new Fixture(b -> b, l -> l.admission(Admission.of(PrefixAuth.TOKEN_LENGTH, verifier::decide)), e -> e.streaming(),
                    false, () -> new Teapot(seen))) {
                Thread echo = echoSessions(f.listener);
                String addr = "127.0.0.1:" + f.base.getPort();
                assertEquals("OK ping", lastLine(run(List.of(bin, "client", kind, addr))), kind);
                assertTrue(seen.isEmpty(), kind + ": an admitted client reached the fallback");
                assertFalse(lastLine(run(List.of(bin, "bad", kind, addr))).startsWith("OK"), kind);
                assertNotNull(seen.poll(5, TimeUnit.SECONDS), kind + ": a wrong key reaches the fallback");
                f.listener.close();
                echo.join(5000);
            }
            // A Java client against the peer's server.
            String addr = "127.0.0.1:" + freePort();
            Process p = startReady(List.of(bin, "server", kind, addr));
            try (var f = Fixture.plain(b -> b); var hc = f.client(b -> h2 ? b.http2() : b.http1())) {
                URI u = URI.create("http://" + addr + "/zmux");
                var pipe = split ? hc.splitPipe(u, Map.of(), streamAuth("right"))
                        : hc.pipe(u, Map.of(), BIDI, streamAuth("right"));
                try (ZmuxSession s = Zmux.open(pipe, Role.CLIENT, f.config)) {
                    assertEquals("ping", new String(pingPong(s), StandardCharsets.US_ASCII), kind);
                }
                var bad = split ? hc.splitPipe(u, Map.of(), streamAuth("wrong"))
                        : hc.pipe(u, Map.of(), BIDI, streamAuth("wrong"));
                assertThrows(IOException.class, () -> { try (ZmuxSession s = Zmux.open(bad, Role.CLIENT, f.config)) { pingPong(s); } }, kind);
                awaitOutput(p, "SESSION ping");
                awaitOutput(p, "FALLBACK");
            } finally {
                stop(p);
            }
        }
    }

    /** Opens a stream, sends "ping", finishes it and reads the echo. */
    static byte[] pingPong(ZmuxSession s) throws IOException {
        try (ZmuxStream st = s.openStream()) {
            byte[] ping = "ping".getBytes(StandardCharsets.US_ASCII);
            st.write(ping, 0, ping.length);
            st.finish();
            return st.readAll(1 << 16);
        }
    }

    /** A host's handler that answers 418 "fallback TARGET" and records the target. */
    static final class Teapot extends io.netty.channel.ChannelInboundHandlerAdapter {
        private final LinkedBlockingQueue<String> seen;

        Teapot(LinkedBlockingQueue<String> seen) { this.seen = seen; }

        @Override public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg) {
            try {
                if (msg instanceof io.netty.handler.codec.http.HttpRequest r) {
                    seen.add(r.uri());
                    byte[] body = ("fallback " + r.uri()).getBytes(StandardCharsets.US_ASCII);
                    var resp = new io.netty.handler.codec.http.DefaultFullHttpResponse(r.protocolVersion(),
                            io.netty.handler.codec.http.HttpResponseStatus.valueOf(418), io.netty.buffer.Unpooled.wrappedBuffer(body));
                    io.netty.handler.codec.http.HttpUtil.setContentLength(resp, body.length);
                    resp.headers().set("Connection", "close");
                    ctx.writeAndFlush(resp).addListener(io.netty.channel.ChannelFutureListener.CLOSE);
                }
            } finally {
                io.netty.util.ReferenceCountUtil.release(msg);
            }
        }
    }

    // ---- processes ----

    static int freePort() throws IOException {
        try (var s = new java.net.ServerSocket(0)) { return s.getLocalPort(); }
    }

    /** Runs a peer to its end (at most 30 s) and returns its output. */
    static String run(List<String> cmd) throws Exception {
        Process p = start(cmd, Map.of());
        try {
            assertTrue(p.waitFor(30, TimeUnit.SECONDS), () -> "the peer did not finish: " + output(p));
            return output(p);
        } finally {
            stop(p);
        }
    }

    static String lastLine(String out) {
        String[] l = out.strip().split("\\R");
        return l[l.length - 1];
    }

    /** Starts a peer server and waits for its READY line. */
    static Process startReady(List<String> cmd) throws Exception {
        Process p = start(cmd, Map.of());
        try {
            awaitOutput(p, "READY");
        } catch (AssertionError | Exception e) {
            stop(p);
            throw e;
        }
        return p;
    }

    static void awaitOutput(Process p, String line) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!output(p).lines().anyMatch(l -> l.startsWith(line))) {
            if (System.nanoTime() - end > 0) fail("no " + line + " from the peer: " + output(p));
            Thread.sleep(20);
        }
    }

    static ExchangeConfig config(String psk, boolean b64, String downUrl) {
        var b = ExchangeConfig.builder().base64(b64).downUrl(downUrl);
        return (psk.isEmpty() ? b.allowInsecure(true) : b.psk(HexFormat.of().parseHex(psk))).build();
    }

    /** Echoes every stream of every session the listener accepts, until it closes. */
    static Thread echoSessions(ZmuxListener l) {
        return Thread.ofVirtual().start(() -> {
            while (true) {
                ZmuxSession s;
                try {
                    s = l.accept();
                } catch (IOException e) {
                    return;
                }
                if (s == null) return;
                Thread.ofVirtual().start(() -> {
                    try (s) {
                        while (true) {
                            ZmuxStream st = s.acceptStream(Duration.ofSeconds(30));
                            if (st == null) return;
                            Thread.ofVirtual().start(() -> {
                                try (st) {
                                    byte[] b = st.readAll(64 << 20);
                                    st.write(b, 0, b.length);
                                    st.finish();
                                } catch (IOException ignored) {
                                    // The peer checks the echo.
                                }
                            });
                        }
                    } catch (IOException ignored) {
                        // The session ended.
                    }
                });
            }
        });
    }

    private void javaServer(List<String> cmd, String psk, boolean b64, boolean split) throws Exception {
        try (var f = Fixture.plain(b -> b.exchange(config(psk, b64, "")))) {
            Thread echo = echoSessions(f.listener);
            String origin = "http://127.0.0.1:" + f.base.getPort();
            var env = Map.of("ZMUX_EXCHANGE", "client", "ZMUX_EXCHANGE_URL", origin + "/up/{nonce}",
                    "ZMUX_EXCHANGE_DOWN_URL", split ? origin + "/down/{nonce}" : "", "ZMUX_EXCHANGE_PSK", psk,
                    "ZMUX_EXCHANGE_BASE64", b64 ? "1" : "0", "ZMUX_EXCHANGE_BYTES", Integer.toString(4 << 20));
            Process p = start(cmd, env);
            try {
                assertTrue(p.waitFor(90, TimeUnit.SECONDS), "the peer client did not finish");
                assertEquals(0, p.exitValue(), () -> output(p));
                assertTrue(output(p).contains("echoed " + (4 << 20) + " bytes"), () -> "the peer did not run its client: " + output(p));
            } finally {
                stop(p);
                f.listener.close();
                echo.join(5000);
            }
        }
    }

    private void javaClient(List<String> cmd, String psk, boolean b64, boolean split) throws Exception {
        int port;
        try (var s = new java.net.ServerSocket(0)) { port = s.getLocalPort(); }
        var env = Map.of("ZMUX_EXCHANGE", "server", "ZMUX_EXCHANGE_ADDR", "127.0.0.1:" + port, "ZMUX_EXCHANGE_FOR", "60s",
                "ZMUX_EXCHANGE_PSK", psk, "ZMUX_EXCHANGE_BASE64", b64 ? "1" : "0");
        Process p = start(cmd, env);
        try (var f = Fixture.plain(b -> b); var hc = f.client(b -> b)) {
            awaitPort(port, p);
            String origin = "http://127.0.0.1:" + port;
            var c = config(psk, b64, split ? origin + "/down/{nonce}" : "");
            try (ZmuxSession s = Zmux.open(hc.exchangePipe(origin + "/up/{nonce}", c), Role.CLIENT, f.config)) {
                byte[] data = new byte[4 << 20];
                new java.util.Random(port).nextBytes(data);
                ZmuxStream st = s.openStream();
                Thread w = Thread.ofVirtual().start(() -> {
                    try { st.write(data, 0, data.length); st.finish(); } catch (IOException ignored) { /* The read fails. */ }
                });
                assertArrayEquals(data, st.readAll(8 << 20));
                w.join(10_000);
                st.close();
            }
        } finally {
            stop(p);
        }
    }

    static Process start(List<String> cmd, Map<String, String> env) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        pb.environment().putAll(env);
        // The Go peer's test binary reads its own source directory for nothing; run it where it is.
        Path dir = Path.of(cmd.getFirst()).getParent();
        if (dir != null) pb.directory(dir.toFile());
        Path out = Files.createTempFile("peer", ".log");
        pb.redirectOutput(out.toFile());
        Process p = pb.start();
        logs.put(p, out);
        return p;
    }

    private static final Map<Process, Path> logs = new java.util.concurrent.ConcurrentHashMap<>();

    static String output(Process p) {
        try {
            return Files.readString(logs.get(p));
        } catch (IOException e) {
            return e.toString();
        }
    }

    static void stop(Process p) throws InterruptedException {
        p.destroy();
        if (!p.waitFor(5, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            p.waitFor(5, TimeUnit.SECONDS);
        }
        Path log = logs.remove(p);
        if (log != null) { try { Files.deleteIfExists(log); } catch (IOException ignored) { /* Left in target. */ } }
    }

    static void awaitPort(int port, Process p) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("127.0.0.1", port), 200);
                return;
            } catch (IOException e) {
                if (!p.isAlive()) fail("the peer server exited: " + output(p));
                if (System.nanoTime() - end > 0) fail("the peer server did not listen");
                Thread.sleep(50);
            }
        }
    }
}
