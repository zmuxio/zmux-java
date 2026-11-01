package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.Admission;
import io.zmux.PipeOptions;
import io.zmux.PrefixAuth;
import io.zmux.Role;
import io.zmux.Zmux;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class AdmissionHttpTest {
    static final PrefixAuth AUTH = new PrefixAuth(new byte[32], Duration.ofSeconds(30), false, 1, 0);

    static Fixture fixture(LinkedBlockingQueue<HostHandler.Seen> seen, LinkedBlockingQueue<String> sources) throws Exception {
        var verifier = AUTH.verifier();
        return new Fixture(b -> b, l -> l.admission(Admission.of(PrefixAuth.TOKEN_LENGTH, Admission.Decider.immediate((info, prefix) -> {
            sources.add(info.source().getAddress().getHostAddress());
            return verifier.decide(info, prefix);
        }))), e -> e.streaming().trusted(TrustedProxies.of(List.of("127.0.0.1")).withHeader("X-Forwarded-For")), false,
                () -> new HostHandler(seen));
    }

    @Test void tokensAdmitBodyAndHeadCarriersAndTheRestGoesToTheHost() throws Exception {
        var seen = new LinkedBlockingQueue<HostHandler.Seen>();
        var sources = new LinkedBlockingQueue<String>();
        try (var f = fixture(seen, sources)) {
            for (boolean h1 : new boolean[] {true, false}) {
                try (var c = f.client(b -> h1 ? b.http1() : b.http2())) {
                    // A bidirectional carrier with the token as its body's prefix.
                    try (var s = Zmux.open(c.pipe(f.base, Map.of("X-Forwarded-For", List.of("203.0.113.7")),
                            PipeOptions.Direction.BIDIRECTIONAL, AUTH), Role.CLIENT, f.config)) {
                        f.echo(s);
                    }
                    assertEquals("203.0.113.7", sources.poll(1, TimeUnit.SECONDS));
                    // A split bootstrap: the POST with the prefix, the GET with the header form.
                    try (var s = Zmux.open(c.splitPipe(f.base, Map.of(), AUTH), Role.CLIENT, f.config)) {
                        f.echo(s);
                    }
                    sources.clear();
                }
            }
            // Without a token the request goes to the host, its body replayed.
            byte[] body = "not a token, but more than thirty-two bytes of a body for the host".getBytes();
            String chunked = Integer.toHexString(body.length) + "\r\n" + new String(body) + "\r\n0\r\n\r\n";
            String got = RawHttp.send(f.base, "POST /z HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n", chunked.getBytes());
            assertTrue(got.startsWith("HTTP/1.1 200") && got.endsWith("host"), got);
            var s = seen.poll(2, TimeUnit.SECONDS);
            assertNotNull(s);
            assertEquals("POST", s.method());
            assertArrayEquals(body, s.body());
            // A downlink GET without the header form, likewise.
            got = RawHttp.send(f.base, "GET /z HTTP/1.1\r\nHost: x\r\nZmux-Pipe-Pair: AAAAAAAAAAAAAAAAAAAAAA\r\n\r\n", null);
            assertTrue(got.endsWith("host"), got);
            assertEquals("GET", seen.poll(2, TimeUnit.SECONDS).method());
            // Not a carrier at all.
            got = RawHttp.send(f.base, "GET /other HTTP/1.1\r\nHost: x\r\n\r\n", null);
            assertTrue(got.endsWith("host"), got);
            assertEquals("/other", seen.poll(2, TimeUnit.SECONDS).uri());
        }
    }

    @Test void earlyDataIsAnsweredTooEarlyBeforeAnyAdmission() throws Exception {
        var seen = new LinkedBlockingQueue<HostHandler.Seen>();
        var sources = new LinkedBlockingQueue<String>();
        try (var f = fixture(seen, sources)) {
            String got = RawHttp.send(f.base, "POST /z HTTP/1.1\r\nHost: x\r\nEarly-Data: 1\r\nTransfer-Encoding: chunked\r\n\r\n", "3\r\nabc\r\n0\r\n\r\n".getBytes());
            assertTrue(got.startsWith("HTTP/1.1 425"), got);
            // Every field counts, not only the first.
            got = RawHttp.send(f.base, "POST /z HTTP/1.1\r\nHost: x\r\nEarly-Data: 0\r\nEarly-Data:  1 \r\nTransfer-Encoding: chunked\r\n\r\n", "3\r\nabc\r\n0\r\n\r\n".getBytes());
            assertTrue(got.startsWith("HTTP/1.1 425"), got);
            assertTrue(sources.isEmpty(), "no admission");
            assertTrue(seen.isEmpty(), "no host");
            // Failed upgrades: setup failures, counted nowhere else.
            var st = f.listener.stats();
            assertEquals(2, st.setupFailures());
            assertEquals(0, st.sessions() + st.joins() + st.rejected() + st.refused() + st.timedOut());
        }
    }
}
