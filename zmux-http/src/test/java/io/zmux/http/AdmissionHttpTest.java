package io.zmux.http;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import io.zmux.Admission;
import io.zmux.PipeOptions;
import io.zmux.PrefixAuth;
import io.zmux.PrefixStore;
import io.zmux.MemoryPrefixStore;
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
    static final TicketFixture TICKET = new TicketFixture("http-admission");
    static final PrefixAuth AUTH = TICKET.presenter();

    record TicketFixture(String name) {
        private static final Duration SLOT = Duration.ofSeconds(30);
        private static final PrefixAuth.CarrierOptions OPTIONS = new PrefixAuth.CarrierOptions(false, true);

        private byte[] digest(String field) {
            try {
                return java.security.MessageDigest.getInstance("SHA-256")
                        .digest(("interop " + field + " " + name).getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            } catch (java.security.NoSuchAlgorithmException e) {
                throw new AssertionError(e);
            }
        }

        private PrefixStore.Id id() { return PrefixStore.Id.of(java.util.Arrays.copyOf(digest("ticket id"), 16)); }
        private List<PrefixStore.Generation> generations() {
            return List.of(new PrefixStore.Generation(digest("outer key"), 0, -1));
        }
        PrefixAuth presenter() { return new PrefixAuth(OPTIONS, SLOT, id(), digest("ticket key"), generations()); }
        PrefixAuth.Verifier verifier() {
            var store = new MemoryPrefixStore(new PrefixStore.Window(SLOT, 1), generations());
            store.addTicket(new PrefixStore.Ticket(id(), digest("ticket key"), 0, 0, -1, false));
            return new PrefixAuth.Verifier(OPTIONS, store);
        }
    }

    static Fixture fixture(LinkedBlockingQueue<HostHandler.Seen> seen, LinkedBlockingQueue<String> sources,
                           boolean tls) throws Exception {
        var verifier = TICKET.verifier();
        return new Fixture(b -> b, l -> l.fallback(t -> { throw new AssertionError("HTTP fallback reached the byte handler"); })
                .admission(Admission.builder(PrefixAuth.TOKEN_LENGTH, (info, prefix, deadline) -> {
                    sources.add(info.source().getAddress().getHostAddress());
                    return verifier.decide(info, prefix, deadline);
                }).refusal(io.zmux.AdmissionVerdict.FALLBACK).build()),
                e -> e.streaming().trusted(TrustedProxies.of(List.of("127.0.0.1")).withHeader("X-Forwarded-For")), tls,
                () -> new HostHandler(seen));
    }

    @Test void tokensAdmitBodyAndHeadCarriersAndTheRestGoesToTheHost() throws Exception {
        var seen = new LinkedBlockingQueue<HostHandler.Seen>();
        var sources = new LinkedBlockingQueue<String>();
        try (var f = fixture(seen, sources, true)) {
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
        }
        try (var f = fixture(seen, sources, false)) {
            // Without a token the request goes to the host, its body replayed.
            byte[] body = "not a token, but more than sixty-four bytes of a request body to replay to the host".getBytes(UTF_8);
            String chunked = Integer.toHexString(body.length) + "\r\n" + new String(body, UTF_8) + "\r\n0\r\n\r\n";
            String got = RawHttp.send(f.base, "POST /z HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n", chunked.getBytes(UTF_8));
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

    @Test void anAuthenticatedExchangeNeedsOnlyItsOwnEnvelope() throws Exception {
        var exchange = ExchangeTest.cfg().build();
        try (var f = new Fixture(b -> b, l -> l.admission(TICKET.verifier().admission().build()),
                e -> e.exchange(exchange), false, null);
             var client = f.client(b -> b);
             var session = Zmux.open(client.exchangePipe(f.base.toString(), exchange), Role.CLIENT, f.config)) {
            f.echo(session);
        }
    }

    @Test void earlyDataIsAnsweredTooEarlyBeforeAnyAdmission() throws Exception {
        var seen = new LinkedBlockingQueue<HostHandler.Seen>();
        var sources = new LinkedBlockingQueue<String>();
        try (var f = fixture(seen, sources, false)) {
            String got = RawHttp.send(f.base, "POST /z HTTP/1.1\r\nHost: x\r\nEarly-Data: 1\r\nTransfer-Encoding: chunked\r\n\r\n", "3\r\nabc\r\n0\r\n\r\n".getBytes(UTF_8));
            assertTrue(got.startsWith("HTTP/1.1 425"), got);
            // Every field counts, not only the first.
            got = RawHttp.send(f.base, "POST /z HTTP/1.1\r\nHost: x\r\nEarly-Data: 0\r\nEarly-Data:  1 \r\nTransfer-Encoding: chunked\r\n\r\n", "3\r\nabc\r\n0\r\n\r\n".getBytes(UTF_8));
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
