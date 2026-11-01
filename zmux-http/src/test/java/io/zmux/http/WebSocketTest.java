package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.zmux.Admission;
import io.zmux.HttpStatusException;
import io.zmux.PrefixAuth;
import io.zmux.Role;
import io.zmux.Zmux;
import io.zmux.carrier.Framing;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class WebSocketTest {
    static URI ws(URI base) { return URI.create(base.toString().replaceFirst("^http", "ws")); }

    @Test void echoOverWsAndWss() throws Exception {
        for (boolean tls : new boolean[] {false, true}) {
            try (var f = new Fixture(b -> b, b -> b, b -> b.webSocket(), tls, null); var c = f.client(b -> b)) {
                PipeIo p = c.webSocketPipe(ws(f.base), Map.of(), List.of(), null);
                assertEquals(Framing.MESSAGES, p.framing());
                assertEquals("qmux-02", p.capabilities().applicationProtocol());
                try (var s = Zmux.open(p, Role.CLIENT, f.config)) {
                    f.echo(s);
                }
            }
        }
    }

    @Test void subprotocolsByTheServersPreferenceWithAliases() {
        var accepted = List.of("b-02", "qmux-02.app", "a-02", "plain");
        assertEquals("b-02", WebSocketServer.select(accepted, List.of("a-02", "b-02")));
        assertEquals("b-01", WebSocketServer.select(accepted, List.of("a-02", "b-01")));
        assertEquals("qmux-01.app", WebSocketServer.select(accepted, List.of("a-01", "qmux-01.app")));
        assertEquals("a-02", WebSocketServer.select(accepted, List.of("a-02", "qmux-00.app")));
        assertEquals("plain", WebSocketServer.select(accepted, List.of("plain")));
        assertNull(WebSocketServer.select(accepted, List.of("plain-01", "B-02", "qmux-01")));
        assertNull(WebSocketServer.select(List.of("qmux-02"), List.of("qmux-00", "qmux-01.x", "qmux")));
    }

    @Test void theSelectedSubprotocolIsThePipesProtocol() throws Exception {
        try (var f = new Fixture(b -> b.alpn(List.of("b-02", "a-02")), b -> b, b -> b.webSocket(), false, null); var c = f.client(b -> b)) {
            PipeIo p = c.webSocketPipe(ws(f.base), Map.of(), List.of("a-02", "b-02"), null);
            assertEquals("b-02", p.capabilities().applicationProtocol());
            try (var s = Zmux.open(p, Role.CLIENT, f.config)) { f.echo(s); }
            // Only an alias offered: the server selects it, and the session runs.
            p = c.webSocketPipe(ws(f.base), Map.of(), List.of("b-01"), null);
            assertEquals("b-01", p.capabilities().applicationProtocol());
            try (var s = Zmux.open(p, Role.CLIENT, f.config)) { f.echo(s); }
        }
    }

    @Test void requestsTheServerDoesNotTakeGoToTheHostWithNothingWritten() throws Exception {
        var seen = new LinkedBlockingQueue<HostHandler.Seen>();
        try (var f = new Fixture(b -> b, b -> b, b -> b.webSocket(), false, () -> new HostHandler(seen)); var c = f.client(b -> b)) {
            String key = "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n";
            // No accepted subprotocol.
            String got = RawHttp.send(f.base, "GET /z HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" + key
                    + "Sec-WebSocket-Protocol: other\r\n\r\n", null);
            assertTrue(got.startsWith("HTTP/1.1 200") && got.endsWith("host"), got);
            // Not a valid upgrade: no key.
            got = RawHttp.send(f.base, "GET /z HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n"
                    + "Sec-WebSocket-Protocol: qmux-02\r\n\r\n", null);
            assertTrue(got.endsWith("host"), got);
            assertEquals(2, seen.size());
            var e = assertThrows(HttpStatusException.class, () -> c.webSocketPipe(ws(f.base), Map.of(), List.of("zz-02"), null));
            assertEquals(200, e.status());
        }
    }

    @Test void admissionOnTheUpgradeHead() throws Exception {
        PrefixAuth auth = AdmissionHttpTest.AUTH;
        var verifier = auth.verifier();
        var seen = new LinkedBlockingQueue<HostHandler.Seen>();
        try (var f = new Fixture(b -> b, l -> l.admission(Admission.of(PrefixAuth.TOKEN_LENGTH, verifier::decide)),
                e -> e.webSocket(), false, () -> new HostHandler(seen)); var c = f.client(b -> b)) {
            try (var s = Zmux.open(c.webSocketPipe(ws(f.base), Map.of(), List.of(), auth), Role.CLIENT, f.config)) {
                f.echo(s);
            }
            assertTrue(seen.isEmpty());
            // Without a credential, the upgrade request goes to the host, unanswered before.
            var e = assertThrows(HttpStatusException.class, () -> c.webSocketPipe(ws(f.base), Map.of(), List.of(), null));
            assertEquals(200, e.status());
            assertEquals("GET", seen.poll(2, TimeUnit.SECONDS).method());
        }
    }

    /** The messages of a carrier: binary (fragments joined), close, ping, text. */
    @Test void messages() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel();
        BodyCarrier c = new BodyCarrier(ch, Framing.MESSAGES, true, true, WebSocketFrames.OUT, () -> {}, x -> { ch.close(); return null; });
        WebSocketFrames frames = new WebSocketFrames(c);
        List<String> got = new ArrayList<>();
        Throwable[] err = new Throwable[1];
        boolean[] end = new boolean[1];
        c.reader().start(new PipeReader.Sink() {
            @Override public void onData(ByteBuffer data, Runnable release) { got.add(StandardCharsets.US_ASCII.decode(data).toString()); release.run(); }
            @Override public void onEnd() { end[0] = true; }
            @Override public void onError(Throwable cause) { err[0] = cause; }
        });
        ch.runPendingTasks();
        frames.read(new BinaryWebSocketFrame(Unpooled.copiedBuffer("ab", StandardCharsets.US_ASCII)));
        frames.read(new BinaryWebSocketFrame(false, 0, Unpooled.copiedBuffer("cd", StandardCharsets.US_ASCII)));
        frames.read(new ContinuationWebSocketFrame(false, 0, Unpooled.copiedBuffer("ef", StandardCharsets.US_ASCII)));
        frames.read(new ContinuationWebSocketFrame(true, 0, Unpooled.copiedBuffer("g", StandardCharsets.US_ASCII)));
        frames.read(new PingWebSocketFrame(Unpooled.copiedBuffer("p", StandardCharsets.US_ASCII)));
        ch.runPendingTasks();
        assertEquals(List.of("ab", "cdefg"), got);
        PongWebSocketFrame pong = ch.readOutbound();
        assertEquals("p", pong.content().toString(StandardCharsets.US_ASCII));
        pong.release();
        frames.read(new CloseWebSocketFrame(1000, ""));
        ch.runPendingTasks();
        assertTrue(end[0]);
        assertNull(err[0]);

        EmbeddedChannel ch2 = new EmbeddedChannel();
        BodyCarrier c2 = new BodyCarrier(ch2, Framing.MESSAGES, true, true, WebSocketFrames.OUT, () -> {}, x -> null);
        WebSocketFrames f2 = new WebSocketFrames(c2);
        c2.reader().start(new PipeReader.Sink() {
            @Override public void onData(ByteBuffer data, Runnable release) { release.run(); }
            @Override public void onEnd() { }
            @Override public void onError(Throwable cause) { err[0] = cause; }
        });
        ch2.runPendingTasks();
        f2.read(new TextWebSocketFrame("hello"));
        ch2.runPendingTasks();
        assertInstanceOf(java.net.ProtocolException.class, err[0]);
        assertFalse(ch2.isOpen());
        ch.finishAndReleaseAll();
        ch2.finishAndReleaseAll();
    }
}
