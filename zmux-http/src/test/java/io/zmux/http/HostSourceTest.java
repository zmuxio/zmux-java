package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.jupiter.api.Test;

/** A host that resolves a connection's source itself gives it through {@link HttpEndpoint#SOURCE}. */
class HostSourceTest {
    static InetSocketAddress ip(String literal, int port) { return new InetSocketAddress(InetAddress.ofLiteral(literal), port); }

    @Test void theHostsSourceIsTheRequestsSourceAddress() {
        var ch = new EmbeddedChannel();
        var r = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/z");
        r.headers().set("X-Forwarded-For", "203.0.113.7");
        assertNull(Requests.source(ch, r, null), "no address known");
        ch.attr(HttpEndpoint.SOURCE).set(ip("198.51.100.4", 4444));
        assertEquals(ip("198.51.100.4", 4444), Requests.source(ch, r, null), "used as given without trusted intermediaries");
        var trusted = TrustedProxies.of(List.of("198.51.100.0/24")).withHeader("X-Forwarded-For");
        assertEquals(ip("203.0.113.7", 0), Requests.source(ch, r, trusted), "the start of the trusted resolution");
        assertNull(Requests.peer(ch), "the peer stays the connection's");
    }
}
