package io.zmux.http;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import io.zmux.CarrierCache;
import io.zmux.CarrierChoice;
import io.zmux.CarrierChoiceException;
import io.zmux.CarrierChoicePolicy;
import io.zmux.HttpRedirectException;
import io.zmux.PipeOptions;
import io.zmux.Role;
import io.zmux.Zmux;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Redirects of the HTTP and WebSocket carriers: same host, scheme upgrade, bounded, uplink replayed. */
@Timeout(60)
class RedirectTest {
    /**
     * A front on its own port: a request whose target starts with /old is answered with status
     * and the Location location gives for the target, and the connection closed; any other
     * connection is passed through to upstream as it is.
     */
    static final class Front implements AutoCloseable {
        final java.net.ServerSocket server;
        final URI base;
        final ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<>();
        /** The bytes after the head that a redirect waits for. */
        volatile int answerAfter;
        private final java.util.Set<java.net.Socket> open = java.util.concurrent.ConcurrentHashMap.newKeySet();
        /** The pass-through threads, by name, for a stall dump. */
        private final java.util.Map<String, Thread> passing = new java.util.concurrent.ConcurrentHashMap<>();
        /** The bytes passed through: from the client up, and from upstream down. */
        final java.util.concurrent.atomic.AtomicLong upBytes = new java.util.concurrent.atomic.AtomicLong();
        final java.util.concurrent.atomic.AtomicLong downBytes = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicInteger conns = new java.util.concurrent.atomic.AtomicInteger();

        Front(URI upstream, int status, Function<String, String> location) throws Exception {
            server = new java.net.ServerSocket(0, 64, java.net.InetAddress.getLoopbackAddress());
            base = URI.create("http://localhost:" + server.getLocalPort() + "/old");
            Thread.ofVirtual().start(() -> {
                while (true) {
                    java.net.Socket s;
                    try { s = server.accept(); } catch (java.io.IOException e) { return; }
                    open.add(s);
                    Thread.ofVirtual().start(() -> serve(s, upstream, status, location));
                }
            });
        }

        private void serve(java.net.Socket s, URI upstream, int status, Function<String, String> location) {
            try (s) {
                var in = s.getInputStream();
                var head = new java.io.ByteArrayOutputStream();
                while (!head.toString(java.nio.charset.StandardCharsets.ISO_8859_1).endsWith("\r\n\r\n")) {
                    int b = in.read();
                    if (b < 0) return;
                    head.write(b);
                }
                String line = head.toString(java.nio.charset.StandardCharsets.ISO_8859_1).lines().findFirst().orElse("");
                String[] parts = line.split(" ");
                if (parts.length > 1 && parts[1].startsWith("/old")) {
                    if (in.readNBytes(answerAfter).length < answerAfter) return;
                    seen.add(parts[0] + " " + parts[1]);
                    String resp = "HTTP/1.1 " + status + " Moved\r\nLocation: " + location.apply(parts[1])
                            + "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                    s.getOutputStream().write(resp.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1));
                    s.getOutputStream().flush();
                    return;
                }
                try (var up = new java.net.Socket(upstream.getHost(), upstream.getPort())) {
                    open.add(up);
                    up.getOutputStream().write(head.toByteArray());
                    int id = conns.incrementAndGet();
                    var t = Thread.ofVirtual().name("front-down-" + id).unstarted(() -> { try { copy(up.getInputStream(), s.getOutputStream(), downBytes); } catch (java.io.IOException ignored) { } finally { try { s.close(); } catch (java.io.IOException ignored) { } } });
                    passing.put(t.getName(), t);
                    Thread.currentThread().setName("front-up-" + id);
                    passing.put(Thread.currentThread().getName(), Thread.currentThread());
                    t.start();
                    try { copy(in, up.getOutputStream(), upBytes); } catch (java.io.IOException ignored) { }
                    try { up.shutdownOutput(); } catch (java.io.IOException ignored) { }
                    t.join(10_000);
                }
            } catch (java.io.IOException | InterruptedException ignored) {
                // Gone.
            }
        }

        /** Like transferTo, counting what it passed in n as it goes. */
        private static void copy(java.io.InputStream in, java.io.OutputStream out, java.util.concurrent.atomic.AtomicLong n) throws java.io.IOException {
            byte[] b = new byte[16384];
            for (int r; (r = in.read(b, 0, b.length)) >= 0; ) {
                out.write(b, 0, r);
                n.addAndGet(r);
            }
        }

        /** The front's counters and its pass-through threads' stacks, for a stall dump. */
        String describe() {
            var b = new StringBuilder("front ").append(base).append(" seen=").append(seen).append(" connections=").append(conns.get())
                    .append(" upBytes=").append(upBytes.get()).append(" downBytes=").append(downBytes.get()).append('\n');
            for (var t : passing.values()) b.append(Stall.stack(t));
            return b.toString();
        }

        @Override public void close() throws IOException {
            server.close();
            for (var s : open) s.close();
        }
    }

    @Test void checkFollowsTheSameHostAndRefusesTheRest() throws Exception {
        URI from = URI.create("http://h.example:8080/a?x=1");
        assertEquals(URI.create("http://h.example:8080/b"), Redirects.check(from, "POST", 307, "/b"));
        assertEquals(URI.create("https://h.example/a?x=1"), Redirects.check(from, "POST", 301, "https://h.example/a?x=1"), "a pure scheme upgrade");
        assertEquals(URI.create("wss://h.example:9443/z"),
                Redirects.check(URI.create("ws://h.example/z"), "GET", 302, "https://h.example:9443/z"), "keeps the WebSocket family");
        assertEquals(URI.create("http://h.example:8080/c"), Redirects.check(from, "GET", 302, "/c#frag"), "no fragment");
        for (String[] bad : new String[][] {
                {"POST", "302", "/b"}, {"GET", "307", "http://other.example:8080/a"}, {"GET", "307", "http://h.example:9090/a"},
                {"GET", "307", "http://u:p@h.example:8080/a"}, {"GET", "307", ""}, {"GET", "307", "ftp://h.example/a"}}) {
            assertThrows(HttpRedirectException.class, () -> Redirects.check(from, bad[0], Integer.parseInt(bad[1]), bad[2]), String.join(" ", bad));
        }
        assertThrows(HttpRedirectException.class,
                () -> Redirects.check(URI.create("https://h.example/a"), "GET", 307, "http://h.example/a"), "no downgrade");
        assertTrue(Redirects.schemeUpgradeOf(from, URI.create("https://h.example/a?x=1")));
    }

    @Test void streamingFollowsAndReplaysTheUplink() throws Exception {
        try (var f = Fixture.plain(b -> b.streaming()); var c = f.client(b -> b.http1());
             var front = new Front(f.base, 307, u -> "/z")) {
            // The session's first record goes up before the front answers: it is replayed at the new URL.
            try (var s = Zmux.open(c.pipe(front.base, Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null), Role.CLIENT, f.config)) {
                f.echo(s);
            }
            assertEquals(List.of("POST /old"), List.copyOf(front.seen));
        }
    }

    /**
     * A write made while a redirect is under way, after the old request was given up and before
     * the new one is made, goes up on the new request after the bytes written before it. The
     * client's second connection gets a loop of its own, held until that write was made.
     */
    @Test void aWriteDuringTheRedirectIsReplayedInOrder() throws Exception {
        var loops = new io.netty.channel.MultiThreadIoEventLoopGroup(2, (java.util.concurrent.Executor) null,
                executors -> {
                    var n = new java.util.concurrent.atomic.AtomicInteger();
                    return () -> executors[Math.min(n.getAndIncrement(), 1)];
                }, io.netty.channel.nio.NioIoHandler.newFactory());
        var up = new java.net.ServerSocket(0, 8, java.net.InetAddress.getLoopbackAddress());
        var body = new java.io.ByteArrayOutputStream();
        Thread.ofVirtual().start(() -> {
            try (var s = up.accept(); var in = new java.io.DataInputStream(s.getInputStream())) {
                for (String line = readLine(in); !line.isEmpty(); line = readLine(in)) { /* the head */ }
                while (true) {
                    int n = Integer.parseInt(readLine(in).trim(), 16);
                    if (n == 0) return;
                    byte[] b = in.readNBytes(n);
                    readLine(in);
                    synchronized (body) { body.write(b); body.notifyAll(); }
                }
            } catch (java.io.IOException | RuntimeException ignored) {
                // Gone.
            }
        });
        var hold = new java.util.concurrent.CountDownLatch(1);
        try (up; var c = HttpCarrierClient.builder().group(loops).http1().build();
             var front = new Front(URI.create("http://localhost:" + up.getLocalPort()), 307, u -> "/z")) {
            front.answerAfter = "7\r\nbefore \r\n".length(); // the first write, as one chunk
            var all = loops.iterator();
            all.next(); // the first connection's loop
            var second = all.next();
            second.execute(() -> { try { hold.await(); } catch (InterruptedException ignored) { } });
            var pipe = c.pipe(front.base, Map.of(), PipeOptions.Direction.SEND_ONLY, null);
            write(pipe, "before ");
            // The front answers once it read the head; the redirect's new connection then waits
            // for the held loop to register it.
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
            while (((io.netty.util.concurrent.SingleThreadEventExecutor) second).pendingTasks() == 0) {
                assertTrue(System.nanoTime() < deadline, "no redirect");
                Thread.sleep(1);
            }
            var during = new java.util.concurrent.CompletableFuture<Void>();
            pipe.writer().write(io.zmux.carrier.OutboundBatch.of(java.nio.ByteBuffer.wrap("during".getBytes(UTF_8))), completion(during));
            hold.countDown();
            during.get(10, java.util.concurrent.TimeUnit.SECONDS);
            String want = "before during";
            synchronized (body) {
                while (body.size() < want.length() && System.nanoTime() < deadline) body.wait(100);
                assertEquals(want, body.toString(java.nio.charset.StandardCharsets.ISO_8859_1));
            }
            pipe.writer().close();
        } finally {
            hold.countDown();
            loops.shutdownGracefully(0, 1, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private static String readLine(java.io.DataInputStream in) throws java.io.IOException {
        var b = new StringBuilder();
        for (int c; (c = in.read()) != '\n'; ) {
            if (c < 0) throw new java.io.EOFException();
            if (c != '\r') b.append((char) c);
        }
        return b.toString();
    }

    private static io.zmux.carrier.PipeWriter.Completion completion(java.util.concurrent.CompletableFuture<Void> f) {
        return new io.zmux.carrier.PipeWriter.Completion() {
            @Override public void completed() { f.complete(null); }
            @Override public void failed(Throwable cause) { f.completeExceptionally(cause); }
        };
    }

    private static void write(io.zmux.carrier.PipeIo pipe, String s) throws Exception {
        var f = new java.util.concurrent.CompletableFuture<Void>();
        pipe.writer().write(io.zmux.carrier.OutboundBatch.of(java.nio.ByteBuffer.wrap(s.getBytes(UTF_8))), completion(f));
        f.get(10, java.util.concurrent.TimeUnit.SECONDS);
    }

    @Test void webSocketFollowsAScheme302() throws Exception {
        try (var f = new Fixture(b -> b, b -> b, b -> b.webSocket(), false, null); var c = f.client(b -> b);
             var front = new Front(f.base, 302, u -> "/z")) {
            try (var s = Zmux.open(c.webSocketPipe(WebSocketTest.ws(front.base), Map.of(), List.of(), null), Role.CLIENT, f.config)) {
                f.echo(s);
            }
        }
    }

    @Test void anotherHostAndEndlessRedirectsAreRefused() throws Exception {
        try (var f = new Fixture(b -> b, b -> b, b -> b.webSocket().streaming(), false, null); var c = f.client(b -> b.http1());
             var away = new Front(f.base, 302, u -> "http://other.example/z");
             var loop = new Front(f.base, 307, u -> u + "x")) {
            var e = assertThrows(HttpRedirectException.class, () -> c.webSocketPipe(WebSocketTest.ws(away.base), Map.of(), List.of(), null));
            assertEquals("another host", e.reason());
            e = assertThrows(HttpRedirectException.class, () -> c.webSocketPipe(WebSocketTest.ws(loop.base), Map.of(), List.of(), null));
            assertEquals("too many redirects", e.reason());
            assertEquals(Redirects.DEFAULT_MAX + 1, loop.seen.size());
            // A streaming carrier opens at once and fails with the refusal: the dial reports it as refused.
            var mode = CarrierChoice.explicit(c.streamingCandidate(away.base, Map.of()));
            var me = assertThrows(CarrierChoiceException.class, () -> Zmux.dial(mode, f.config, null));
            assertEquals(io.zmux.CandidateOutcome.REFUSED, me.results().getFirst().outcome(), me.toString());
        }
    }

    @Test void theModeCacheKeepsTheFinalUrl() throws Exception {
        try (var f = Fixture.plain(b -> b.streaming()); var c = f.client(b -> b.http1());
             var front = new Front(f.base, 308, u -> "/z")) {
            f.diagnostics = front::describe;
            CarrierCache cache = new CarrierCache();
            var cand = c.streamingCandidate(front.base, Map.of());
            try (var s = Zmux.dial(CarrierChoice.auto(cand), f.config, CarrierChoicePolicy.of(cache, "k"))) {
                f.echo(s);
            }
            String fin = front.base.resolve("/z").toString();
            assertEquals(fin, cand.target().url());
            var entry = cache.snapshot().entries().getFirst();
            assertEquals(fin, entry.url());
            // A later carrier starts at the final URL: the front sees no more requests.
            int before = front.seen.size();
            try (var s = Zmux.dial(CarrierChoice.auto(cand), f.config, CarrierChoicePolicy.of(cache, "k"))) {
                f.echo(s);
            }
            assertEquals(before, front.seen.size());
        }
    }

    @Test void everyExchangeRequestFollowsA307WithItsBody() throws Exception {
        var c = ExchangeTest.cfg().build();
        try (var f = Fixture.plain(b -> b.exchange(c)); var hc = f.client(b -> b);
             var front = new Front(f.base, 307, u -> u.replaceFirst("^/old", "/z"))) {
            try (var s = Zmux.open(hc.exchangePipe(front.base.toString(), c), Role.CLIENT, f.config)) {
                ExchangeTest.echo(f, s, 64 << 10);
            }
            // Followed per request: the next one starts at the configured URL again.
            assertTrue(front.seen.size() > 1, front.seen.toString());
            assertTrue(front.seen.stream().allMatch(r -> r.startsWith("POST /old")), front.seen.toString());
        }
    }

    @Test void aRefusedExchangeRedirectFailsTheCarrier() throws Exception {
        var c = ExchangeTest.cfg().build();
        try (var f = Fixture.plain(b -> b.exchange(c)); var hc = f.client(b -> b);
             var moved = new Front(f.base, 301, u -> u.replaceFirst("^/old", "/z"));
             var away = new Front(f.base, 308, u -> "http://other.example/z");
             var loop = new Front(f.base, 307, u -> u.replaceFirst("^/old", "/oldx"))) {
            assertEquals("would change POST to GET", refusal(f, hc, c, moved).reason());
            assertEquals("another host", refusal(f, hc, c, away).reason());
            assertEquals("too many redirects", refusal(f, hc, c, loop).reason());
        }
    }

    private static HttpRedirectException refusal(Fixture f, HttpCarrierClient hc, ExchangeConfig c, Front front) {
        var e = assertThrows(java.io.IOException.class, () -> {
            try (var s = Zmux.open(hc.exchangePipe(front.base.toString(), c), Role.CLIENT, f.config)) {
                s.awaitClosed();
            }
        });
        for (Throwable t = e; t != null; t = t.getCause()) if (t instanceof HttpRedirectException r) return r;
        throw new AssertionError("no redirect refusal", e);
    }
}
