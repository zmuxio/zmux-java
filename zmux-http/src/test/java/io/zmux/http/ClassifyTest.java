package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.RequestHead;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ClassifyTest {
    static HttpClass.Head head(String method, int version, String protocol, long cl, String... fields) {
        Map<String, List<String>> m = new HashMap<>();
        for (int i = 0; i < fields.length; i += 2) m.computeIfAbsent(fields[i], k -> new java.util.ArrayList<>()).add(fields[i + 1]);
        return new HttpClass.Head(method, version, protocol, cl, RequestHead.of(m, false));
    }

    /** The classification rules of HTTP requests, each case with the first rule that matches. */
    @Test void requestsAreClassifiedByTheFirstRuleThatMatches() {
        Object[][] cases = {
                // 1. WebSocket opening handshakes.
                {head("GET", 1, "", -1, "Upgrade", "websocket", "Connection", "Upgrade"), HttpClass.WEBSOCKET},
                {head("GET", 1, "", -1, "Upgrade", "h2c, WebSocket"), HttpClass.WEBSOCKET},
                {head("GET", 1, "", -1, "Upgrade", "websocket", "Zmux-Pipe-Join", "x"), HttpClass.WEBSOCKET},
                {head("GET", 2, "", -1, "Upgrade", "websocket"), HttpClass.NONE},
                {head("CONNECT", 2, "websocket", -1), HttpClass.WEBSOCKET},
                {head("CONNECT", 3, "WebSocket", -1), HttpClass.WEBSOCKET},
                // 2. WebTransport.
                {head("CONNECT", 3, "webtransport", -1), HttpClass.WEBTRANSPORT},
                {head("CONNECT", 3, "webtransport-h3", -1), HttpClass.WEBTRANSPORT},
                {head("CONNECT", 3, "webtransport-", -1), HttpClass.NONE},
                {head("CONNECT", 3, "webtransport-h4", -1), HttpClass.NONE},
                {head("CONNECT", 3, "WebTransport-H3", -1), HttpClass.WEBTRANSPORT},
                {head("CONNECT", 1, "", -1), HttpClass.NONE},
                // 3. Downlinks.
                {head("GET", 1, "", -1, "Zmux-Pipe-Pair", "t"), HttpClass.DOWNLINK},
                {head("GET", 2, "", -1, "zmux-pipe-join", "j"), HttpClass.DOWNLINK},
                {head("GET", 1, "", -1, "Zmux-Pipe-Join", ""), HttpClass.NONE},
                // 4. A split bootstrap's uplink, with or without a length.
                {head("POST", 1, "", -1, "Zmux-Pipe-Pair", "t"), HttpClass.UPLINK},
                {head("POST", 1, "", 10, "Zmux-Pipe-Pair", "t"), HttpClass.UPLINK},
                // 5. Exchange requests.
                {head("POST", 1, "", 0), HttpClass.EXCHANGE},
                {head("POST", 2, "", 100, "Zmux-Pipe-Join", "j"), HttpClass.EXCHANGE},
                // 6. Bidirectional carriers, or an uplink with a join.
                {head("POST", 2, "", -1), HttpClass.BIDIRECTIONAL},
                {head("POST", 1, "", -1, "Zmux-Pipe-Join", "j"), HttpClass.UPLINK},
                // 7. Everything else.
                {head("GET", 1, "", -1), HttpClass.NONE},
                {head("PUT", 1, "", -1), HttpClass.NONE},
                {head("HEAD", 1, "", -1, "Zmux-Pipe-Pair", "t"), HttpClass.NONE},
        };
        for (Object[] c : cases) assertEquals(c[1], HttpClass.classify((HttpClass.Head) c[0]), c[0].toString());
        assertEquals("bidirectional", HttpClass.BIDIRECTIONAL.toString());
    }

    static RequestHead fields(String name, String... values) {
        return RequestHead.of(Map.of(name, List.of(values)), false);
    }

    static InetSocketAddress peer(String ip) {
        return new InetSocketAddress(InetAddress.ofLiteral(ip), 4000);
    }

    static String ip(InetSocketAddress a) {
        return TrustedProxies.text(a.getAddress());
    }

    @Test void aTrustedIntermediaryReportsTheSource() {
        var t = TrustedProxies.of(List.of("10.0.0.0/8", "2001:db8::/32", "192.0.2.1")).withHeader("X-Forwarded-For");
        assertEquals("203.0.113.9", ip(t.source(peer("10.1.2.3"), fields("X-Forwarded-For", "198.51.100.1, 203.0.113.9"))));
        // Trusted hops are skipped from the right; lines in order.
        assertEquals("203.0.113.9", ip(t.source(peer("10.1.2.3"), fields("x-forwarded-for", "203.0.113.9", "10.9.9.9, 192.0.2.1"))));
        // A report from an untrusted peer is ignored.
        assertEquals("198.51.100.7", ip(t.source(peer("198.51.100.7"), fields("X-Forwarded-For", "203.0.113.9"))));
        // Every entry trusted: the leftmost.
        assertEquals("10.2.2.2", ip(t.source(peer("10.1.2.3"), fields("X-Forwarded-For", "10.2.2.2"))));
        // An entry that is not an address ends the walk at the last one parsed.
        assertEquals("10.1.2.3", ip(t.source(peer("10.1.2.3"), fields("X-Forwarded-For", "unknown"))));
        assertEquals("10.4.4.4", ip(t.source(peer("10.1.2.3"), fields("X-Forwarded-For", "bogus, 10.4.4.4"))));
        assertEquals("10.1.2.3", ip(t.source(peer("10.1.2.3"), RequestHead.NONE)));
        // IPv6, ports and IPv4-mapped peers.
        assertEquals("2001:db9:0:0:0:0:0:1", ip(t.source(peer("2001:db8::5"), fields("X-Forwarded-For", "[2001:db9::1]:443"))));
        assertEquals("203.0.113.9", ip(t.source(peer("::ffff:10.0.0.1"), fields("X-Forwarded-For", "203.0.113.9:1234"))));

        var f = TrustedProxies.of(List.of("10.0.0.0/8")).withHeader("Forwarded");
        assertEquals("2001:db8:0:0:0:0:0:cafe", ip(f.source(peer("10.0.0.1"),
                fields("Forwarded", "for=192.0.2.43, for=\"[2001:db8:cafe::17]\";proto=https".replace("cafe::17", "0:0:0:0:0:cafe")))));
        assertEquals("192.0.2.60", ip(f.source(peer("10.0.0.1"), fields("Forwarded", "for=192.0.2.60;proto=http;by=203.0.113.43"))));
        assertEquals("10.0.0.1", ip(f.source(peer("10.0.0.1"), fields("Forwarded", "for=unknown"))));
        assertEquals("192.0.2.60", ip(f.source(peer("10.0.0.1"), fields("Forwarded", "for=\"192.0.2.60:80\", for=10.3.3.3"))));

        var r = TrustedProxies.of(List.of("127.0.0.1")).withHeader("X-Real-IP");
        assertEquals("203.0.113.5", ip(r.source(peer("127.0.0.1"), fields("X-Real-Ip", "203.0.113.5"))));
        var c = TrustedProxies.of(List.of("127.0.0.1")).withHeader("CF-Connecting-IP");
        assertEquals("203.0.113.6", ip(c.source(peer("127.0.0.1"), fields("cf-connecting-ip", " 203.0.113.6 "))));
        // No header configured: never read.
        assertEquals("127.0.0.1", ip(TrustedProxies.of(List.of("127.0.0.1")).source(peer("127.0.0.1"), fields("X-Forwarded-For", "203.0.113.9"))));

        assertThrows(IllegalArgumentException.class, () -> TrustedProxies.of(List.of("10.0.0.0/33")));
        assertThrows(IllegalArgumentException.class, () -> TrustedProxies.of(List.of("example.com")));
        assertThrows(IllegalArgumentException.class, () -> TrustedProxies.of(List.of()).withHeader("Bad Name"));
        assertTrue(TrustedProxies.of(List.of("::ffff:10.0.0.0/104")).trusts(InetAddress.ofLiteral("10.200.0.1")));
    }

    @Test void theSourceCarriesThePortTheIntermediaryReports() {
        var t = TrustedProxies.of(List.of("10.0.0.0/8")).withHeader("X-Forwarded-For");
        assertEquals(new InetSocketAddress(InetAddress.ofLiteral("203.0.113.9"), 1234),
                t.source(peer("10.1.2.3"), fields("X-Forwarded-For", "203.0.113.9:1234")));
        assertEquals(new InetSocketAddress(InetAddress.ofLiteral("2001:db9::1"), 443),
                t.source(peer("10.1.2.3"), fields("X-Forwarded-For", "[2001:db9::1]:443")));
        assertEquals(0, t.source(peer("10.1.2.3"), fields("X-Forwarded-For", "203.0.113.9")).getPort(), "no port reported");
        // Not trusted, or no client named: the peer itself, its port included.
        assertEquals(peer("198.51.100.7"), t.source(peer("198.51.100.7"), fields("X-Forwarded-For", "203.0.113.9:1")));
        assertEquals(peer("10.1.2.3"), t.source(peer("10.1.2.3"), RequestHead.NONE));
        var f = TrustedProxies.of(List.of("10.0.0.0/8")).withHeader("Forwarded");
        assertEquals(new InetSocketAddress(InetAddress.ofLiteral("192.0.2.60"), 80),
                f.source(peer("10.0.0.1"), fields("Forwarded", "for=\"192.0.2.60:80\", for=10.3.3.3")));
    }

    static ByteBuffer bytes(String s) { return ByteBuffer.wrap(s.getBytes(StandardCharsets.US_ASCII)); }

    @Test void proxyProtocolHeadersOfBothVersions() throws Exception {
        var h = ProxyHeader.parse(bytes("PROXY TCP4 203.0.113.9 10.0.0.1 51000 443\r\nGET /"));
        assertEquals(new ProxyHeader(43, new InetSocketAddress(InetAddress.ofLiteral("203.0.113.9"), 51000)), h);
        assertEquals("2001:db8:0:0:0:0:0:1", ProxyHeader.parse(bytes("PROXY TCP6 2001:db8::1 ::1 1 2\r\n")).source().getAddress().getHostAddress());
        assertNull(ProxyHeader.parse(bytes("PROXY UNKNOWN\r\n")).source());
        assertNull(ProxyHeader.parse(bytes("PROXY TCP4 203.0.113.9 10.0.0.1 5")), "incomplete");
        assertNull(ProxyHeader.parse(bytes("PRO")), "incomplete");
        for (String bad : new String[] {"GET / HTTP/1.1\r\n", "PROXY TCP4 203.0.113.9 10.0.0.1 51000\r\n", "PROXY TCP4 2001:db8::1 ::1 1 2\r\n",
                "PROXY TCP4 203.0.113.9 10.0.0.1 51000 443\n", "PROXY TCP4 1.2.3.4 1.2.3.4 65536 1\r\n", "PROXY " + "x".repeat(110)}) {
            assertThrows(ProtocolException.class, () -> ProxyHeader.parse(bytes(bad)), bad);
        }
        ByteBuffer v2 = ByteBuffer.allocate(16 + 12 + 3 + 4);
        v2.put(new byte[] {0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a, 0x21, 0x11}).putShort((short) 15);
        v2.put(new byte[] {(byte) 203, 0, 113, 9, 10, 0, 0, 1}).putShort((short) 51000).putShort((short) 443).put(new byte[] {4, 0, 0}).put("rest".getBytes());
        v2.flip();
        var p2 = ProxyHeader.parse(v2);
        assertEquals(31, p2.length());
        assertEquals(new InetSocketAddress(InetAddress.ofLiteral("203.0.113.9"), 51000), p2.source());
        assertEquals(0, v2.position());
        assertNull(ProxyHeader.parse(v2.duplicate().limit(20)));
        ByteBuffer local = ByteBuffer.wrap(new byte[] {0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a, 0x20, 0x00, 0, 0});
        assertEquals(new ProxyHeader(16, null), ProxyHeader.parse(local));
        local.put(12, (byte) 0x13);
        assertThrows(ProtocolException.class, () -> ProxyHeader.parse(local));
    }
}
