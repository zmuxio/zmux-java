package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.Admission;
import io.zmux.AdmissionVerdict;
import io.zmux.FirstBytes;
import io.zmux.Role;
import io.zmux.Takeover;
import io.zmux.Zmux;
import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The reverse-tunnel flow over WebSocket: the admission defers on the upgrade's head, then
 * decides on the carrier's first message, counting carriers per source. A zmux client's first
 * record is accepted into a working session; any other first message hands the carrier to the
 * listener's fallback, which gets that exact message first, then those that follow, answers
 * over the same carrier, and is followed by the carrier's close.
 */
@Timeout(60)
class WebSocketTakeoverTest {
    private static final byte[] TUNNEL = "TUN1".getBytes(StandardCharsets.US_ASCII);

    /** A fallback answering each message with "echo:" and it, one write at a time; done after "bye". */
    private static Admission.Fallback echo(BlockingQueue<String> got, BlockingQueue<Takeover> taken) {
        return t -> {
            taken.add(t);
            PipeIo p = t.pipe();
            var done = new CompletableFuture<Void>();
            p.reader().start(new PipeReader.Sink() {
                @Override public void onChunk(io.zmux.Chunk chunk, Runnable consumed) {
                    ByteBuffer b = chunk.buffer();
                    byte[] m = new byte[b.remaining()];
                    b.get(m);
                    chunk.close();
                    consumed.run();
                    String s = new String(m, StandardCharsets.US_ASCII);
                    got.add(s);
                    if (s.equals("bye")) { done.complete(null); return; }
                    p.writer().write(OutboundBatch.of(ByteBuffer.wrap(("echo:" + s).getBytes(StandardCharsets.US_ASCII))), new PipeWriter.Completion() {
                        @Override public void completed() { }
                        @Override public void failed(Throwable cause) { done.completeExceptionally(cause); }
                    });
                }
                @Override public void onEnd() { done.complete(null); }
                @Override public void onError(Throwable cause) { done.completeExceptionally(cause); }
            });
            return done;
        };
    }

    /** A minimal WebSocket client on a socket: masked binary messages out, unmasked messages in, the close. */
    private record Client(java.net.Socket socket, java.io.InputStream in, java.io.OutputStream out) {
        static Client open(java.net.URI uri) throws Exception {
            var socket = new java.net.Socket(uri.getHost(), uri.getPort());
            socket.setSoTimeout(10_000);
            var out = socket.getOutputStream();
            out.write(("GET " + uri.getPath() + " HTTP/1.1\r\nHost: " + uri.getHost() + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                    + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: qmux-02\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            out.flush();
            var in = socket.getInputStream();
            var head = new StringBuilder();
            while (!head.toString().endsWith("\r\n\r\n")) {
                int b = in.read();
                if (b < 0) throw new java.io.EOFException("no response");
                head.append((char) b);
            }
            assertTrue(head.toString().startsWith("HTTP/1.1 101"), head.toString());
            return new Client(socket, in, out);
        }

        void sendOnly(String s) throws Exception {
            byte[] m = s.getBytes(StandardCharsets.US_ASCII);
            byte[] mask = {1, 2, 3, 4};
            var f = new java.io.ByteArrayOutputStream();
            f.write(0x82);
            f.write(0x80 | m.length);
            f.write(mask);
            for (int i = 0; i < m.length; i++) f.write(m[i] ^ mask[i & 3]);
            out.write(f.toByteArray());
            out.flush();
        }

        /** The next message: its opcode and payload ("close" for a close frame). */
        String receive() throws Exception {
            int op = in.read() & 0x0f, len = in.read() & 0x7f;
            byte[] b = in.readNBytes(len);
            return op == 0x8 ? "close" : new String(b, StandardCharsets.US_ASCII);
        }

        String send(String s) throws Exception {
            sendOnly(s);
            return receive();
        }
    }

    @Test void takeoverOnTheHeadUpgradesAndServesTheFirstMessage() throws Exception {
        var got = new LinkedBlockingQueue<String>();
        var taken = new LinkedBlockingQueue<Takeover>();
        var admission = Admission.builder(0, Admission.Decider.immediate((info, prefix) -> Admission.Decision.takeover("tunnel"))).build();
        try (var f = new Fixture(b -> b, b -> b.admission(admission).fallback(echo(got, taken)), b -> b.webSocket(), false, null)) {
            var client = Client.open(WebSocketTest.ws(f.base)); // Waits for the 101 response.
            try (var _ = client.socket()) {
                var carrier = taken.poll(5, TimeUnit.SECONDS);
                assertNotNull(carrier);
                assertEquals(Admission.Stage.HEAD, carrier.info().stage());
                assertEquals("tunnel", carrier.value());
                assertEquals("echo:first", client.send("first"));
                client.sendOnly("bye");
                assertEquals("close", client.receive());
            }
        }
    }

    @Test void deferredDecisionsAcceptSessionsAndHandTunnelsToTheFallback() throws Exception {
        var perSource = new ConcurrentHashMap<String, AtomicInteger>();
        var stages = new LinkedBlockingQueue<String>();
        Admission.Decider decider = Admission.Decider.immediate((info, prefix) -> {
            stages.add(info.stage() + " " + info.upgrade());
            if (info.stage() == Admission.Stage.HEAD) return new Admission.Decision(AdmissionVerdict.DEFER, 0);
            assertEquals(Framing.MESSAGES, info.framing());
            perSource.computeIfAbsent(info.source().getAddress().getHostAddress(), k -> new AtomicInteger()).incrementAndGet();
            byte[] head = new byte[Math.min(prefix.remaining(), TUNNEL.length)];
            prefix.duplicate().get(head);
            if (java.util.Arrays.equals(head, TUNNEL)) return Admission.Decision.FALLBACK.withVariant("tunnel");
            return FirstBytes.classify(prefix, info.framing()).kind() == FirstBytes.Kind.RECORD
                    ? Admission.Decision.accept(0).withVariant("zmux") : Admission.Decision.CLOSE;
        });
        var got = new LinkedBlockingQueue<String>();
        var taken = new LinkedBlockingQueue<Takeover>();
        try (var f = new Fixture(b -> b, b -> b.admission(Admission.builder(0, decider).peekTimeout(java.time.Duration.ofSeconds(10)).build())
                .fallback(echo(got, taken)), b -> b.webSocket(), false, null); var c = f.client(b -> b)) {
            var uri = WebSocketTest.ws(f.base);
            // Accepted on its first record: a working session.
            PipeIo p = c.webSocketPipe(uri, Map.of(), List.of(), null);
            try (var s = Zmux.open(p, Role.CLIENT, f.config)) {
                f.echo(s);
            }
            assertEquals("HEAD true", stages.poll(5, TimeUnit.SECONDS));
            assertEquals("CARRIER true", stages.poll(5, TimeUnit.SECONDS));

            // Taken over on its first message: the fallback gets exactly it, then what follows, and answers.
            var t = Client.open(uri);
            assertEquals("echo:TUN1 open", t.send("TUN1 open"));
            assertEquals("TUN1 open", got.poll(5, TimeUnit.SECONDS), "the first message, replayed whole");
            assertEquals("echo:second", t.send("second"));
            assertEquals("second", got.poll(5, TimeUnit.SECONDS));
            Takeover tk = taken.poll(5, TimeUnit.SECONDS);
            assertNotNull(tk);
            assertEquals(Admission.Stage.CARRIER, tk.info().stage());
            assertTrue(tk.info().upgrade());
            assertEquals("127.0.0.1", tk.info().source().getAddress().getHostAddress());
            assertTrue(tk.socket().isEmpty(), "a WebSocket carrier has no socket view");
            t.sendOnly("bye");
            assertEquals("bye", got.poll(5, TimeUnit.SECONDS));
            assertEquals("close", t.receive(), "zmux closed the carrier once the fallback was done");
            t.socket().close();

            assertEquals(2, perSource.get("127.0.0.1").get(), "both carriers counted on their first message");
            var st = f.listener.stats().admission();
            assertEquals(1, st.accepted());
            assertEquals(1, st.fallback());
            assertEquals(2, st.accepted() + st.fallback() + st.takeover() + st.stalled() + st.closed() + st.stallOverflow() + st.tooLarge()
                    + st.peekTimedOut() + st.handshakeFailed());
        }
    }

    @Test void deferredFallbackWithNoTargetCountsAsStall() throws Exception {
        var admission = Admission.builder(0, Admission.Decider.immediate((info, prefix) ->
                info.stage() == Admission.Stage.HEAD ? new Admission.Decision(AdmissionVerdict.DEFER, 0)
                        : Admission.Decision.FALLBACK.withVariant("missing-target"))).build();
        try (var f = new Fixture(b -> b, b -> b.admission(admission), b -> b.webSocket(), false, null)) {
            var client = Client.open(WebSocketTest.ws(f.base));
            try (var _ = client.socket()) {
                client.sendOnly("first");
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (f.listener.stats().admission().stalled() == 0 && System.nanoTime() - deadline < 0) {
                    Thread.sleep(10);
                }
                var stats = f.listener.stats().admission();
                assertEquals(1, stats.stalled());
                assertEquals(0, stats.fallback());
                var variant = f.listener.variantStats().getFirst();
                assertEquals(1, variant.stalled());
                assertEquals(0, variant.fallback());
            }
        }
    }

    /**
     * A WebSocket carrier the host detached outlives the fallback: the host answers on it after the fallback returned,
     * through its typed message halves, and the connection closes only once the host closes the carrier.
     */
    @Test void aDetachedWebSocketIsServedAfterTheFallbackReturned() throws Exception {
        Admission.Decider decider = Admission.Decider.immediate((info, prefix) -> info.stage() == Admission.Stage.HEAD
                ? new Admission.Decision(AdmissionVerdict.DEFER, 0) : Admission.Decision.fallback("tunnel"));
        var taken = new LinkedBlockingQueue<Takeover>();
        try (var f = new Fixture(b -> b, b -> b.admission(Admission.builder(0, decider).peekTimeout(java.time.Duration.ofSeconds(10)).build())
                .fallback(t -> { assertTrue(t.detach()); taken.add(t); return null; }), b -> b.webSocket(), false, null)) {
            var c = Client.open(WebSocketTest.ws(f.base));
            c.sendOnly("TUN1 open");
            Takeover t = taken.poll(5, TimeUnit.SECONDS);
            assertNotNull(t);
            assertEquals("tunnel", t.value());
            Takeover.Messages m = t.messages().orElseThrow();
            Thread.sleep(200); // the fallback has returned: a carrier it did not detach would be closed by now
            var got = new LinkedBlockingQueue<String>();
            m.reader().start(new PipeReader.Sink() {
                @Override public void onChunk(io.zmux.Chunk chunk, Runnable consumed) {
                    ByteBuffer b = chunk.buffer();
                    byte[] x = new byte[b.remaining()];
                    b.get(x);
                    chunk.close();
                    consumed.run();
                    got.add(new String(x, StandardCharsets.US_ASCII));
                }
                @Override public void onEnd() { got.add("end"); }
                @Override public void onError(Throwable cause) { got.add("error"); }
            });
            assertEquals("TUN1 open", got.poll(5, TimeUnit.SECONDS), "the first message, replayed");
            var wrote = new CompletableFuture<Void>();
            m.writer().write(OutboundBatch.of(ByteBuffer.wrap("late".getBytes(StandardCharsets.US_ASCII))), new PipeWriter.Completion() {
                @Override public void completed() { wrote.complete(null); }
                @Override public void failed(Throwable cause) { wrote.completeExceptionally(cause); }
            });
            wrote.get(5, TimeUnit.SECONDS);
            assertEquals("late", c.receive(), "answered after the fallback returned");
            c.sendOnly("x");
            assertEquals("x", got.poll(5, TimeUnit.SECONDS), "the host reads on");
            m.reader().close();
            m.writer().close();
            assertEquals("close", c.receive(), "closed once the host closed it");
            c.socket().close();
        }
    }
}
