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
 *       KIND tls1 and tls2 (HTTP/1.1 and HTTP/2 over TLS 1.3, multipath, the client joins a second carrier
 *       in Bound, else Sealed, and prints MODE), behind (that server in cleartext, behind a TLS front)
 *       and, for the Rust client, wsh2 (a WebSocket over an HTTP/2 extended CONNECT joined in Bound);
 *   <li>ZMUX_GO_TLS_FRONT: a reverse proxy that terminates TLS ({@code ADDR UPSTREAM}, h2c upstream);
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

    /** The command of a peer, or an empty list when it is not set. */
    static List<String> peer(String env) {
        String p = System.getenv(env);
        if (p == null || p.isEmpty()) return List.of();
        assertTrue(Files.isExecutable(Path.of(p)), env + " is not executable: " + p);
        return env.contains("_GO_") ? List.of(p, "-test.run", "^TestHTTPExchangeInterop$", "-test.count=1", "-test.v") : List.of(p);
    }

    @Test void exchangeWithGo() throws Exception { exchangeMatrix("ZMUX_GO_EXCHANGE_PEER"); }

    @Test void exchangeWithRust() throws Exception { exchangeMatrix("ZMUX_RS_EXCHANGE_PEER"); }

    private static void exchangeMatrix(String env) throws Exception {
        List<String> cmd = peer(env);
        assumeTrue(!cmd.isEmpty(), env + " not set");
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

    private static void webSocket(String env) throws Exception {
        String bin = System.getenv(env);
        assumeTrue(bin != null && !bin.isEmpty(), env + " not set");
        // The peer's clients against a Java server.
        var seen = new LinkedBlockingQueue<String>();
        var verifier = WS_AUTH.verifier();
        try (var f = new Fixture(b -> b, l -> l.admission(Admission.of(PrefixAuth.TOKEN_LENGTH, verifier)), e -> e.webSocket(),
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

    private static void streaming(String env) throws Exception {
        String bin = System.getenv(env);
        assumeTrue(bin != null && !bin.isEmpty(), env + " not set");
        for (String kind : new String[] {"stream", "split", "stream2", "split2"}) {
            boolean split = kind.startsWith("split"), h2 = kind.endsWith("2");
            // The peer's client against a Java server.
            var seen = new LinkedBlockingQueue<String>();
            var verifier = streamAuth("right").verifier();
            try (var f = new Fixture(b -> b, l -> l.admission(Admission.of(PrefixAuth.TOKEN_LENGTH, verifier)), e -> e.streaming(),
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

    // ---- Bound over TLS (bidirectional carriers on TLS 1.3 both ends terminate, and behind a TLS front) ----

    static final byte[] JOIN_PSK = filled(32, (byte) 7);

    static byte[] filled(int n, byte b) {
        byte[] a = new byte[n];
        java.util.Arrays.fill(a, b);
        return a;
    }

    /** The peers' session configuration: multipath, joins in Bound only on a carrier that declares the contract. */
    static io.zmux.ZmuxConfig.Builder mp(io.zmux.ZmuxConfig.Builder b) {
        return b.pipeAuth(io.zmux.PipeAuth.psk(JOIN_PSK).withEndToEnd()).maxPipes(4)
                .allowRecordMode((c, m, p) -> m == io.zmux.RecordMode.SEALED || (m == io.zmux.RecordMode.BOUND && c.isSecure() && c.isExporterPresent()));
    }

    @Test void boundOverTls() throws Exception {
        String go = System.getenv("ZMUX_GO_HTTP_PEER"), rs = System.getenv("ZMUX_RS_HTTP_PEER");
        assumeTrue(go != null && !go.isEmpty() && rs != null && !rs.isEmpty(), "ZMUX_GO_HTTP_PEER and ZMUX_RS_HTTP_PEER not set");
        for (String kind : new String[] {"tls2", "tls1"}) {
            javaClientTls(go, kind, null, "bound");
            javaClientTls(rs, kind, null, "bound");
            javaServerTls(go, kind, null, "bound");
            javaServerTls(rs, kind, null, "bound");
            peerPair(rs, go, kind, kind, null, "bound");
            peerPair(go, rs, kind, kind, null, "bound");
        }
    }

    @Test void boundWebSocketOverHttp2() throws Exception {
        String go = System.getenv("ZMUX_GO_HTTP_PEER"), rs = System.getenv("ZMUX_RS_HTTP_PEER");
        assumeTrue(go != null && !go.isEmpty() && rs != null && !rs.isEmpty(), "ZMUX_GO_HTTP_PEER and ZMUX_RS_HTTP_PEER not set");
        // Only the Rust peer's client opens a WebSocket over HTTP/2.
        peerPair(go, rs, "tls2", "wsh2", null, "bound");
        peerPair(rs, rs, "tls2", "wsh2", null, "bound");
    }

    /** Behind a front that terminates TLS the server has no TLS of its own: a Bound join is refused, a Sealed one joins. */
    @Test void sealedBehindTlsFront() throws Exception {
        String go = System.getenv("ZMUX_GO_HTTP_PEER"), rs = System.getenv("ZMUX_RS_HTTP_PEER"), front = System.getenv("ZMUX_GO_TLS_FRONT");
        assumeTrue(go != null && !go.isEmpty() && rs != null && !rs.isEmpty() && front != null && !front.isEmpty(),
                "ZMUX_GO_HTTP_PEER, ZMUX_RS_HTTP_PEER and ZMUX_GO_TLS_FRONT not set");
        peerPair(go, rs, "behind", "tls2", front, "sealed");
        javaClientTls(rs, "behind", front, "sealed");
        javaServerTls(go, "behind", front, "sealed");
    }

    /** Starts the TLS front on a free port forwarding to upstream, or returns null without a front. */
    static Process startFront(String front, String upstream, String[] addr) throws Exception {
        if (front == null) return null;
        addr[0] = "127.0.0.1:" + freePort();
        return startReady(List.of(front, addr[0], upstream));
    }

    /** The peer's client against the peer's server, directly or through the front; both report the join's mode. */
    private static void peerPair(String server, String client, String serverKind, String clientKind, String front, String want) throws Exception {
        String addr = "127.0.0.1:" + freePort();
        String what = client + " -> " + server + " " + clientKind + (front != null ? " via front" : "");
        // The Go server serves WebSocket over HTTP/2 only with extended CONNECT turned on.
        Process p = startReady(List.of(server, "server", serverKind, addr), Map.of("GODEBUG", "http2xconnect=1"));
        String[] via = {addr};
        Process fp = null;
        try {
            fp = startFront(front, addr, via);
            String out = run(List.of(client, "client", clientKind, via[0]));
            assertEquals("OK ping", lastLine(out), () -> what + ": " + out);
            assertTrue(out.lines().anyMatch(l -> l.equals("MODE " + want)), () -> what + ": " + out);
            awaitOutput(p, "MODE " + want);
        } finally {
            if (fp != null) stop(fp);
            stop(p);
        }
    }

    /** A Java client against the peer's server: the join's mode at both ends, then data on the session. */
    private static void javaClientTls(String server, String kind, String front, String want) throws Exception {
        String addr = "127.0.0.1:" + freePort();
        Process p = startReady(List.of(server, "server", kind, addr));
        String[] via = {addr};
        Process fp = null;
        try (var f = new Fixture(HttpInteropTest::mp, b -> b, e -> e.streaming(), false, null)) {
            fp = startFront(front, addr, via);
            boolean h1 = kind.equals("tls1");
            var ssl = io.netty.handler.ssl.SslContextBuilder.forClient().sslProvider(io.netty.handler.ssl.SslProvider.JDK)
                    .trustManager(io.netty.handler.ssl.util.InsecureTrustManagerFactory.INSTANCE).protocols("TLSv1.3");
            try (var hc = f.client(b -> (h1 ? b.http1() : b.http2()).tls(ssl))) {
                URI u = URI.create("https://" + via[0] + "/zmux");
                try (ZmuxSession s = Zmux.open(hc.pipe(u, Map.of(), BIDI, null), Role.CLIENT, f.config)) {
                    assertEquals("ping", new String(pingPong(s), StandardCharsets.US_ASCII));
                    assertEquals(want, joinMode(s, () -> hc.pipe(u, Map.of(), BIDI, null)), server + " " + kind);
                    assertEquals("ping", new String(pingPong(s), StandardCharsets.US_ASCII));
                }
            }
            awaitOutput(p, "MODE " + want);
        } finally {
            if (fp != null) stop(fp);
            stop(p);
        }
    }

    /** The peer's client against a Java server over TLS, or in cleartext behind the front. */
    private static void javaServerTls(String client, String kind, String front, String want) throws Exception {
        var sessions = new LinkedBlockingQueue<ZmuxSession>();
        try (var f = new Fixture(HttpInteropTest::mp, b -> b, e -> e.streaming(), front == null, null)) {
            Thread echo = echoSessions(f.listener, sessions);
            String addr = "127.0.0.1:" + f.base.getPort();
            String[] via = {addr};
            Process fp = startFront(front, addr, via);
            try {
                String clientKind = front == null ? kind : "tls2";
                String out = run(List.of(client, "client", clientKind, via[0]));
                assertEquals("OK ping", lastLine(out), () -> client + " " + kind + ": " + out);
                assertTrue(out.lines().anyMatch(l -> l.equals("MODE " + want)), () -> client + " " + kind + ": " + out);
                ZmuxSession s = sessions.poll(5, TimeUnit.SECONDS);
                assertNotNull(s);
                assertTrue(s.pipeStats().stream().anyMatch(st -> st.pipeId().value() != 0 && st.recordMode().toString().equalsIgnoreCase(want)),
                        () -> client + " " + kind + ": " + s.pipeStats());
            } finally {
                if (fp != null) stop(fp);
                f.listener.close();
                echo.join(5000);
            }
        }
    }

    interface Dial { io.zmux.carrier.PipeIo dial() throws IOException; }

    /** Joins a carrier from dial in Bound and, when that fails, a new one in Sealed; returns the joined pipe's mode. */
    static String joinMode(ZmuxSession s, Dial dial) throws Exception {
        try {
            var p = s.addPipe(dial.dial(), io.zmux.PipeOptions.builder().recordMode(io.zmux.RecordMode.BOUND).build());
            return p.stats().orElseThrow().recordMode().toString().toLowerCase(java.util.Locale.ROOT);
        } catch (IOException bound) {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (true) {
                try {
                    var p = s.addPipe(dial.dial(), io.zmux.PipeOptions.builder().recordMode(io.zmux.RecordMode.SEALED).build());
                    return p.stats().orElseThrow().recordMode().toString().toLowerCase(java.util.Locale.ROOT);
                } catch (IOException e) {
                    if (System.nanoTime() - end > 0) throw e;
                    Thread.sleep(100);
                }
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
        return startReady(cmd, Map.of());
    }

    static Process startReady(List<String> cmd, Map<String, String> env) throws Exception {
        Process p = start(cmd, env);
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
        return ExchangeConfig.builder(psk.isEmpty() ? new byte[32] : HexFormat.of().parseHex(psk)).base64(b64).downUrl(downUrl).build();
    }

    /** Echoes every stream of every session the listener accepts, until it closes. */
    static Thread echoSessions(ZmuxListener l) {
        return echoSessions(l, null);
    }

    /** Echoes as {@link #echoSessions(ZmuxListener)} and hands each session to accepted. */
    static Thread echoSessions(ZmuxListener l, LinkedBlockingQueue<ZmuxSession> accepted) {
        return Thread.ofVirtual().start(() -> {
            while (true) {
                ZmuxSession s;
                try {
                    s = l.accept();
                } catch (IOException e) {
                    return;
                }
                if (s == null) return;
                if (accepted != null) accepted.add(s);
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

    private static void javaServer(List<String> cmd, String psk, boolean b64, boolean split) throws Exception {
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

    private static void javaClient(List<String> cmd, String psk, boolean b64, boolean split) throws Exception {
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
