package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.Role;
import io.zmux.Zmux;
import io.zmux.ZmuxSession;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The exchange carrier's server and client. */
@Timeout(120)
class ExchangeTest {
    static final HexFormat HEX = HexFormat.of();
    static final SecureRandom RANDOM = new SecureRandom();
    static final byte[] PSK = new byte[32];
    static { Arrays.fill(PSK, (byte) 9); }

    static ExchangeConfig.Builder cfg() { return ExchangeConfig.builder(PSK); }

    record Resp(int status, byte[] body) {
        int flags() { return body[Exchange.MAC] & 0xff; }
    }

    /** POSTs body on a connection of its own and reads the response. */
    static Resp post(URI u, byte[] body) throws IOException {
        try (Socket s = new Socket(u.getHost(), u.getPort())) {
            s.setSoTimeout(10_000);
            String head = "POST " + u.getRawPath() + " HTTP/1.1\r\nHost: " + u.getHost() + "\r\nContent-Length: " + body.length
                    + "\r\nConnection: close\r\n\r\n";
            s.getOutputStream().write(head.getBytes(StandardCharsets.ISO_8859_1));
            s.getOutputStream().write(body);
            s.getOutputStream().flush();
            InputStream in = new BufferedInputStream(s.getInputStream());
            TestProxy.Head h = TestProxy.head(in);
            return new Resp(h.status(), TestProxy.body(in, h, true, 0));
        }
    }

    static int now() { return (int) (System.currentTimeMillis() / 1000); }

    static byte[] request(byte[] psk, byte[] cid, int kind, int flags, long seq, long ack, int time, byte[] nonce, byte[] data) {
        var key = Exchange.key(psk, cid);
        var sealed = Exchange.seal(key, cid, kind, flags, seq, ack, time, nonce, ByteBuffer.wrap(data));
        byte[] out = new byte[Exchange.REQ_OVERHEAD + data.length];
        sealed.header().get(out, 0, Exchange.HDR);
        System.arraycopy(data, 0, out, Exchange.HDR, data.length);
        sealed.dataMac().get(out, Exchange.HDR + data.length, Exchange.MAC);
        return out;
    }

    /** A client of one carrier sending raw requests. */
    static final class Raw {
        final URI u;
        final byte[] psk;
        final byte[] cid = new byte[Exchange.CID];

        Raw(URI u, byte[] psk) { this.u = u; this.psk = psk; RANDOM.nextBytes(cid); }

        Resp send(int kind, int flags, long seq, long ack, byte[] data, boolean corrupt) throws IOException {
            byte[] nonce = new byte[Exchange.NONCE];
            RANDOM.nextBytes(nonce);
            byte[] b = request(psk, cid, kind, flags, seq, ack, now(), nonce, data);
            if (corrupt) b[Exchange.HDR - 1] ^= 1; // the Header MAC
            Resp r = post(u, b);
            if (r.status == 200) {
                // The response MAC binds it to this request.
                var key = Exchange.key(psk, cid);
                ByteBuffer d = ByteBuffer.wrap(r.body, Exchange.RESP_HDR, r.body.length - Exchange.RESP_HDR);
                assertArrayEquals(key.responseMac(cid, kind, seq, nonce, r.flags(), d), Arrays.copyOf(r.body, Exchange.MAC));
            }
            return r;
        }

        int flags(int kind, int flags, long seq, long ack, byte[] data) throws IOException {
            Resp r = send(kind, flags, seq, ack, data, false);
            assertEquals(200, r.status);
            return r.flags();
        }
    }

    static final byte[] NONE = new byte[0];

    @Test void vectorsThroughTheServer() throws Exception {
        String[][] vecs = {
            {"", ExchangeWireTest.UP_PLAIN, ExchangeWireTest.UP_RESP_PLAIN},
            {"6b".repeat(32), ExchangeWireTest.UP_PSK, ExchangeWireTest.UP_RESP_PSK},
        };
        for (String[] v : vecs) {
            byte[] psk = HEX.parseHex(v[0]);
            var c = psk.length == 0 ? ExchangeConfig.builder(new byte[32]) : ExchangeConfig.builder(psk);
            try (var f = Fixture.plain(b -> b.exchange(c.build()))) {
                f.endpoint.exchange.seconds = () -> 1767225600L;
                Resp r = post(f.base, HEX.parseHex(v[1]));
                assertEquals(200, r.status);
                assertEquals(v[2], HEX.formatHex(r.body));
                // The same request again repeats its nonce: not authenticated, so the host's.
                assertEquals(404, post(f.base, HEX.parseHex(v[1])).status);
                // Outside the time window: the host's.
                f.endpoint.exchange.seconds = () -> 1767225600L + 61;
                byte[] cid = HEX.parseHex("000102030405060708090a0b0c0d0e0f"), nonce = new byte[Exchange.NONCE];
                RANDOM.nextBytes(nonce);
                byte[] late = request(psk, cid, Exchange.KIND_UP, 0, 1, 0, 1767225600, nonce, NONE);
                assertEquals(404, post(f.base, late).status);
                f.endpoint.exchange.seconds = () -> 1767225600L + 60;
                assertEquals(200, post(f.base, late).status, "within the window");
            }
        }
    }

    /** Opens a request of contentLength on a connection of its own and writes its head and first bytes. */
    static Socket begin(URI u, int contentLength, byte[] first) throws IOException {
        Socket s = new Socket(u.getHost(), u.getPort());
        s.setSoTimeout(10_000);
        String head = "POST " + u.getRawPath() + " HTTP/1.1\r\nHost: " + u.getHost() + "\r\nContent-Length: " + contentLength
                + "\r\nConnection: close\r\n\r\n";
        s.getOutputStream().write(head.getBytes(StandardCharsets.ISO_8859_1));
        s.getOutputStream().write(first);
        s.getOutputStream().flush();
        return s;
    }

    /** Writes the rest of a request begun with {@link #begin} and reads its response. */
    static Resp finish(Socket s, byte[] rest) throws IOException {
        try (s) {
            s.getOutputStream().write(rest);
            s.getOutputStream().flush();
            InputStream in = new BufferedInputStream(s.getInputStream());
            TestProxy.Head h = TestProxy.head(in);
            return new Resp(h.status(), TestProxy.body(in, h, true, 0));
        }
    }

    static byte[] fresh(byte[] cid, int kind, long seq, byte[] data) {
        byte[] nonce = new byte[Exchange.NONCE];
        RANDOM.nextBytes(nonce);
        return request(PSK, cid, kind, 0, seq, 0, now(), nonce, data);
    }

    static byte[] newCid() { byte[] cid = new byte[Exchange.CID]; RANDOM.nextBytes(cid); return cid; }

    /**
     * Section 7.1: the authentication header within the first-record timeout, else the host's
     * with every byte replayed; the body after it within the handshake timeout; a failure after
     * the header is a status other than 200 without a body, and changes nothing.
     */
    @Test void theHeaderIsAuthenticatedBeforeTheBody() throws Exception {
        var seen = new java.util.concurrent.LinkedBlockingQueue<HostHandler.Seen>();
        var c = cfg().holdTime(Duration.ofMillis(50)).build();
        try (var f = new Fixture(b -> b.handshakeTimeout(Duration.ofMillis(600)), b -> b.firstRecordTimeout(Duration.ofMillis(300)),
                b -> b.exchange(c), false, () -> new HostHandler(seen))) {
            // A header that is late: the host's, its bytes replayed in front of the rest.
            byte[] q = fresh(newCid(), Exchange.KIND_UP, 0, new byte[100]);
            Socket s = begin(f.base, q.length, Arrays.copyOf(q, 30));
            Thread.sleep(700);
            Resp r = finish(s, Arrays.copyOfRange(q, 30, q.length));
            assertEquals(200, r.status);
            assertEquals("host", new String(r.body, StandardCharsets.US_ASCII));
            assertArrayEquals(q, seen.poll(5, TimeUnit.SECONDS).body());
            assertEquals(0, f.endpoint.exchange.live());
            // A header that does not verify: the host's, every byte replayed.
            byte[] bad = fresh(newCid(), Exchange.KIND_UP, 0, new byte[10]);
            bad[3] ^= 1;
            assertEquals(200, post(f.base, bad).status);
            assertArrayEquals(bad, seen.poll(5, TimeUnit.SECONDS).body());
            // A Data Length that disagrees with Content-Length: the host's.
            byte[] longer = Arrays.copyOf(fresh(newCid(), Exchange.KIND_UP, 0, new byte[10]), Exchange.REQ_OVERHEAD + 11);
            assertEquals(200, post(f.base, longer).status);
            assertArrayEquals(longer, seen.poll(5, TimeUnit.SECONDS).body());
            // An authenticated header whose Data MAC does not verify: refused without a body, nothing created.
            byte[] data = fresh(newCid(), Exchange.KIND_UP, 0, new byte[10]);
            data[Exchange.HDR + 2] ^= 1;
            r = post(f.base, data);
            assertEquals(400, r.status);
            assertEquals(0, r.body.length);
            assertEquals(0, f.endpoint.exchange.live());
            // An authenticated header whose body does not arrive within the handshake timeout.
            byte[] slow = fresh(newCid(), Exchange.KIND_UP, 0, new byte[100]);
            try (Socket t = begin(f.base, slow.length, Arrays.copyOf(slow, Exchange.HDR + 10))) {
                InputStream in = new BufferedInputStream(t.getInputStream());
                TestProxy.Head h = TestProxy.head(in);
                assertEquals(400, h.status(), "a timeout is 400, not 408");
                assertEquals(0, TestProxy.body(in, h, true, 0).length);
            }
            assertEquals(0, f.endpoint.exchange.live());
            assertTrue(seen.isEmpty(), "authenticated requests never reach the host");
        }
    }

    /** Every request holds a slot of its source and of the listener from its arrival until its header is authenticated. */
    @Test void aRequestHoldsItsSlotsUntilItsHeaderIsAuthenticated() throws Exception {
        var c = cfg().holdTime(Duration.ofMillis(50)).build();
        for (boolean global : new boolean[] {false, true}) {
            try (var f = new Fixture(b -> b, b -> (global ? b.maxPending(1) : b.maxPendingPerSource(1)).firstRecordTimeout(Duration.ofSeconds(5)),
                    b -> b.exchange(c), false, null)) {
                byte[] cid = newCid();
                // A request whose header is still arriving holds its slots.
                byte[] first = fresh(cid, Exchange.KIND_CLOSE, 0, NONE);
                Socket s = begin(f.base, first.length, Arrays.copyOf(first, 10));
                Thread.sleep(100);
                long rejected = f.listener.stats().rejected(), refused = f.listener.stats().refused();
                Resp busy = post(f.base, fresh(newCid(), Exchange.KIND_CLOSE, 0, NONE));
                assertEquals(503, busy.status, "no slot left: 503, never the host's");
                assertEquals(0, busy.body.length);
                assertEquals(global ? rejected + 1 : rejected, f.listener.stats().rejected());
                assertEquals(global ? refused : refused + 1, f.listener.stats().refused());
                // Its header authenticated, a CLOSE creates nothing and gives its slots back.
                Resp r = finish(s, Arrays.copyOfRange(first, 10, first.length));
                assertEquals(200, r.status);
                assertEquals(200, post(f.base, fresh(newCid(), Exchange.KIND_CLOSE, 0, NONE)).status, "served once the slot is back");
            }
        }
    }

    static void echo(Fixture f, ZmuxSession client, int n) throws Exception {
        try (ZmuxSession s = f.listener.accept(Duration.ofSeconds(10))) {
            assertNotNull(s);
            Fixture.exchange(client, s, n);
            Fixture.exchange(s, client, n);
        }
    }

    @Test void echoBinaryAndBase64() throws Exception {
        for (boolean b64 : new boolean[] {false, true}) {
            var c = cfg().base64(b64).upPieceSize(256 << 10).downPieceSize(256 << 10).holdTime(Duration.ofSeconds(2)).build();
            try (var f = Fixture.plain(b -> b.exchange(c)); var hc = f.client(b -> b)) {
                try (var s = Zmux.open(hc.exchangePipe(f.base.toString(), c), Role.CLIENT, f.config)) {
                    echo(f, s, 4 << 20);
                }
                waitFor(() -> f.endpoint.exchange.live() == 0, "the carrier ends with its session");
            }
        }
    }

    @Test void theAdmissionValueReachesTheCarriersSession() throws Exception {
        var c = cfg().holdTime(Duration.ofSeconds(2)).build();
        var adm = io.zmux.Admission.of(0, io.zmux.Admission.Decider.immediate((info, prefix) -> io.zmux.Admission.Decision.accept(0, "tenant-7")));
        try (var f = new Fixture(b -> b, b -> b.admission(adm), b -> b.exchange(c), false, null); var hc = f.client(b -> b)) {
            try (var _ = Zmux.open(hc.exchangePipe(f.base.toString(), c), Role.CLIENT, f.config);
                 ZmuxSession s = f.listener.accept(Duration.ofSeconds(10))) {
                assertNotNull(s);
                assertEquals(java.util.Optional.of("tenant-7"), s.admitted());
            }
        }
    }

    @Test void echoOverHttp2() throws Exception {
        var c = cfg().holdTime(Duration.ofSeconds(2)).build();
        try (var f = new Fixture(b -> b, b -> b, b -> b.exchange(c), true, null); var hc = f.client(b -> b)) {
            try (var s = Zmux.open(hc.exchangePipe(f.base.toString(), c), Role.CLIENT, f.config)) {
                echo(f, s, 1 << 20);
            }
        }
    }

    /** The server's answers that the end-to-end runs do not reach. */
    @Test void serverRules() throws Exception {
        byte[] k = "k".getBytes(StandardCharsets.US_ASCII);
        var c = ExchangeConfig.builder(k).downWindow(4).holdTime(Duration.ofMillis(50)).build();
        try (var f = new Fixture(b -> b, b -> b.firstRecordTimeout(Duration.ofSeconds(10)), b -> b.exchange(c), false, null)) {
            var x = new Raw(f.base, k);
            assertEquals(404, x.send(Exchange.KIND_DOWN, 0, 0, 0, NONE, true).status, "a bad MAC goes to the host");
            assertEquals(0, f.endpoint.exchange.live(), "a request with a bad MAC created a carrier");
            assertEquals(0, x.flags(Exchange.KIND_DOWN, 0, 0, 0, NONE), "a held DOWN gets an empty piece");
            assertEquals(Exchange.FLAG_BUSY, x.flags(Exchange.KIND_DOWN, 0, 4, 0, NONE), "DOWN beyond the window");
            assertEquals(Exchange.FLAG_GONE, x.flags(Exchange.KIND_DOWN, 0, 1, 5, NONE), "Down Ack above the assigned pieces");
            assertEquals(Exchange.FLAG_GONE, x.flags(Exchange.KIND_UP, 0, 0, 0, NONE), "after the failure");
            // A valid MAC over a request that breaks the request rules (flags in a DOWN) fails a live carrier.
            var y = new Raw(f.base, k);
            assertEquals(0, y.flags(Exchange.KIND_DOWN, 0, 0, 0, NONE));
            assertEquals(Exchange.FLAG_GONE, y.flags(Exchange.KIND_DOWN, Exchange.FLAG_FIN, 1, 0, NONE));
            assertEquals(Exchange.FLAG_GONE, y.flags(Exchange.KIND_DOWN, 0, 1, 0, NONE));
            // A CLOSE for a carrier without state is processed; the carrier is gone afterwards.
            var z = new Raw(f.base, k);
            assertEquals(0, z.flags(Exchange.KIND_CLOSE, 0, 0, 0, "bye".getBytes(StandardCharsets.US_ASCII)));
            assertEquals(Exchange.FLAG_GONE, z.flags(Exchange.KIND_UP, 0, 0, 0, NONE));
            assertEquals(0, f.endpoint.exchange.live());
        }
    }

    /** Duplicated, reordered and out-of-room UP pieces. */
    @Test void uplinkOrderAndRoom() throws Exception {
        var c = cfg().upStoreLimit(2 << 20).holdTime(Duration.ofMillis(50)).build(); // 2 MiB: twice the 1 MiB limit
        try (var f = new Fixture(b -> b, b -> b.firstRecordTimeout(Duration.ofSeconds(10)), b -> b.exchange(c), false, null)) {
            var x = new Raw(f.base, PSK);
            byte[] big = new byte[600_000];
            assertEquals(0, x.flags(Exchange.KIND_UP, 0, 1, 0, big), "out of order, within the room");
            assertEquals(0, x.flags(Exchange.KIND_UP, 0, 1, 0, big), "a repetition");
            assertEquals(Exchange.FLAG_BUSY, x.flags(Exchange.KIND_UP, 0, 2, 0, big), "out of order beyond the room");
            assertEquals(Exchange.FLAG_GONE, x.flags(Exchange.KIND_UP, Exchange.FLAG_FIN, 0, 0, NONE), "a FIN below a received piece");
        }
    }

    /** Out-of-order UP pieces count a fixed overhead each, so empty ones cannot be held without bound. */
    @Test void emptyOutOfOrderPiecesAreBounded() throws Exception {
        var c = cfg().upStoreLimit(2 << 20).holdTime(Duration.ofMillis(50)).build(); // out-of-order room: 1 MiB
        try (var f = new Fixture(b -> b, b -> b.firstRecordTimeout(Duration.ofSeconds(10)), b -> b.exchange(c), false, null)) {
            var x = new Raw(f.base, PSK);
            int room = c.upStoreLimit() - Exchange.LIMIT;
            assertEquals(0, x.flags(Exchange.KIND_UP, 0, 1, 0, new byte[room - 4 * ExchangeServer.PIECE_COST]));
            for (int n = 2; n <= 4; n++) assertEquals(0, x.flags(Exchange.KIND_UP, 0, n, 0, NONE), "empty piece " + n);
            assertEquals(Exchange.FLAG_BUSY, x.flags(Exchange.KIND_UP, 0, 5, 0, NONE), "an empty piece beyond the room");
            assertEquals(0, x.flags(Exchange.KIND_UP, 0, 0, 0, NONE), "the piece zmux waits for still fits");
            assertEquals(0, x.flags(Exchange.KIND_UP, 0, 5, 0, NONE), "room again once the pieces are delivered");
        }
    }

    /**
     * Every request takes a slot of its source on arrival, and a request that creates a carrier
     * keeps it until the carrier enters a session: a request beyond the share is answered 503
     * and holds nothing; once the carrier gave its slot back, the next one is served.
     */
    @Test void requestsTakeTheSourcesPendingShare() throws Exception {
        var c = cfg().holdTime(Duration.ofMillis(50)).build();
        try (var f = new Fixture(b -> b, b -> b.maxPendingPerSource(1).firstRecordTimeout(Duration.ofSeconds(1)), b -> b.exchange(c), false, null)) {
            var x = new Raw(f.base, PSK);
            var y = new Raw(f.base, PSK);
            assertEquals(0, x.flags(Exchange.KIND_UP, 0, 1, 0, NONE));
            assertEquals(1, f.endpoint.exchange.live());
            assertEquals(503, y.send(Exchange.KIND_UP, 0, 1, 0, NONE, false).status, "beyond the source's share");
            assertEquals(503, x.send(Exchange.KIND_DOWN, 0, 0, 0, NONE, false).status, "the new carrier holds the share");
            assertEquals(1, f.endpoint.exchange.live());
            // The carrier sends no first record: the listener closes it, and its slot goes.
            waitFor(() -> {
                try { return y.send(Exchange.KIND_UP, 0, 1, 0, NONE, false).status == 200; } catch (Exception e) { throw new RuntimeException(e); }
            }, "the second carrier served");
        }
    }

    /** HTTP/1.1 requests pipelined in one write are answered in their order, though the first is held. */
    @Test void pipelinedRequestsAreAnsweredInOrder() throws Exception {
        var c = cfg().holdTime(Duration.ofMillis(400)).build();
        try (var f = Fixture.plain(b -> b.exchange(c))) {
            var x = new Raw(f.base, PSK);
            assertEquals(0, x.flags(Exchange.KIND_UP, 0, 1, 0, NONE));
            byte[] nonce = new byte[Exchange.NONCE];
            RANDOM.nextBytes(nonce);
            byte[] down = request(PSK, x.cid, Exchange.KIND_DOWN, 0, 0, 0, now(), nonce, NONE);
            byte[] junk = new byte[Exchange.HDR]; // not authenticated: the host answers it at once
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            for (byte[] b : new byte[][] {down, junk}) {
                out.writeBytes(("POST " + f.base.getRawPath() + " HTTP/1.1\r\nHost: " + f.base.getHost() + "\r\nContent-Length: " + b.length
                        + "\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1));
                out.writeBytes(b);
            }
            try (Socket s = new Socket(f.base.getHost(), f.base.getPort())) {
                s.setSoTimeout(10_000);
                s.getOutputStream().write(out.toByteArray());
                s.getOutputStream().flush();
                InputStream in = new BufferedInputStream(s.getInputStream());
                TestProxy.Head first = TestProxy.head(in);
                TestProxy.body(in, first, false, 0);
                assertEquals(200, first.status(), "the held DOWN answers first");
                assertNotEquals(200, TestProxy.head(in).status());
            }
        }
    }

    /** Closing the endpoint after a carrier's event loop shut down neither throws nor skips the other carriers. */
    @Test void closeAfterTheCarriersLoopShutDown() throws Exception {
        var c = cfg().holdTime(Duration.ofSeconds(1)).build();
        var f = Fixture.plain(b -> b.exchange(c));
        try {
            var x = new Raw(f.base, PSK);
            assertEquals(0, x.flags(Exchange.KIND_UP, 0, 1, 0, NONE));
            assertEquals(1, f.endpoint.exchange.live());
            f.group.shutdownGracefully(0, 0, java.util.concurrent.TimeUnit.MILLISECONDS).syncUninterruptibly();
            assertDoesNotThrow(f.endpoint::close);
            assertEquals(0, f.endpoint.exchange.live(), "the carrier of a gone loop is dropped");
        } finally {
            // The fixture's own close needs the group this test shut down.
            f.listener.close();
            f.loops.close();
        }
    }

    /** A carrier the listener rejects ends its downlink; the final CLOSE completes it. */
    @Test void completionAndClose() throws Exception {
        var c = ExchangeConfig.builder(new byte[32]).holdTime(Duration.ofSeconds(1)).build();
        try (var f = Fixture.plain(b -> b.exchange(c))) {
            var x = new Raw(f.base, NONE);
            assertEquals(0, x.flags(Exchange.KIND_UP, Exchange.FLAG_FIN, 0, 0, new byte[] {(byte) 0xff}));
            int down = x.flags(Exchange.KIND_DOWN, 0, 0, 0, NONE);
            assertTrue(down == Exchange.FLAG_FIN || down == Exchange.FLAG_GONE, "flags " + down);
            if (down == Exchange.FLAG_FIN) assertEquals(0, x.flags(Exchange.KIND_CLOSE, 0, 0, 1, NONE));
            waitFor(() -> f.endpoint.exchange.live() == 0, "the carrier ends");
            assertEquals(Exchange.FLAG_GONE, x.flags(Exchange.KIND_DOWN, 0, 1, 1, NONE));
        }
    }

    @Test void closedListenerRefusesNewCarriers() throws Exception {
        var c = cfg().holdTime(Duration.ofSeconds(1)).build();
        try (var f = new Fixture(b -> b, b -> b.firstRecordTimeout(Duration.ofSeconds(10)), b -> b.exchange(c), false, null)) {
            // A request that arrived before the listener closed, authenticated after: BUSY, nothing created.
            byte[] early = fresh(newCid(), Exchange.KIND_UP, 0, new byte[] {1});
            Socket s = begin(f.base, early.length, Arrays.copyOf(early, 10));
            Thread.sleep(100);
            f.listener.close();
            Resp r = finish(s, Arrays.copyOfRange(early, 10, early.length));
            assertEquals(200, r.status);
            assertEquals(Exchange.FLAG_BUSY, r.flags());
            assertEquals(0, f.endpoint.exchange.live(), "no carrier stays");
            var x = new Raw(f.base, PSK);
            // A request to a closed listener is still authenticated, and answered GONE.
            assertEquals(Exchange.FLAG_GONE, x.flags(Exchange.KIND_UP, 0, 0, 0, new byte[] {1}));
            assertEquals(0, f.endpoint.exchange.live(), "no carrier stays");
            // One that is not authenticated is the host's.
            assertEquals(404, x.send(Exchange.KIND_UP, 0, 0, 0, NONE, true).status);
        }
    }

    /** An unknown Kind: Data limited to 0, then a malformed request that fails the carrier with GONE. */
    @Test void unknownKindFailsTheCarrier() throws Exception {
        var c = cfg().holdTime(Duration.ofMillis(50)).build();
        try (var f = new Fixture(b -> b, b -> b.firstRecordTimeout(Duration.ofSeconds(10)), b -> b.exchange(c), false, null)) {
            var x = new Raw(f.base, PSK);
            assertEquals(0, x.flags(Exchange.KIND_UP, 0, 1, 0, NONE));
            assertEquals(1, f.endpoint.exchange.live());
            assertEquals(404, x.send(7, 0, 0, 0, new byte[] {1}, false).status, "Data above the limit 0: not authenticated");
            assertEquals(1, f.endpoint.exchange.live(), "and nothing changed");
            assertEquals(Exchange.FLAG_GONE, x.flags(7, 0, 0, 0, NONE));
            waitFor(() -> f.endpoint.exchange.live() == 0, "the carrier fails");
            assertEquals(Exchange.FLAG_GONE, x.flags(Exchange.KIND_UP, 0, 2, 0, NONE));
            assertEquals(Exchange.FLAG_GONE, new Raw(f.base, PSK).flags(7, 0, 0, 0, NONE), "and creates none");
        }
    }

    /**
     * A request after a new carrier's first takes a slot of the same source while the carrier
     * still holds its own until it enters a session, so the default share must leave room for it.
     */
    @Test void theDefaultSourceShareLeavesRoomForACarriersNextRequest() {
        assertTrue(io.zmux.ListenerConfig.builder().build().maxPendingPerSource() >= 2);
    }

    /** An authenticated request whose body ends early: 400, no body, nothing created. */
    @Test void aTruncatedBodyIs400() throws Exception {
        var c = cfg().holdTime(Duration.ofMillis(50)).build();
        try (var f = new Fixture(b -> b, b -> b.firstRecordTimeout(Duration.ofSeconds(10)), b -> b.exchange(c), false, null)) {
            var ch = new io.netty.channel.embedded.EmbeddedChannel();
            ch.attr(HttpEndpoint.SOURCE).set(new java.net.InetSocketAddress("192.0.2.1", 1));
            ch.pipeline().addLast(new Router(f.endpoint));
            byte[] q = fresh(newCid(), Exchange.KIND_UP, 0, new byte[20]);
            var req = new io.netty.handler.codec.http.DefaultHttpRequest(io.netty.handler.codec.http.HttpVersion.HTTP_1_1,
                    io.netty.handler.codec.http.HttpMethod.POST, f.base.getRawPath());
            io.netty.handler.codec.http.HttpUtil.setContentLength(req, q.length);
            ch.writeInbound(req);
            ch.writeInbound(new io.netty.handler.codec.http.DefaultLastHttpContent(
                    io.netty.buffer.Unpooled.wrappedBuffer(Arrays.copyOf(q, q.length - 5))));
            ch.runPendingTasks();
            io.netty.handler.codec.http.FullHttpResponse resp = ch.readOutbound();
            assertNotNull(resp);
            assertEquals(400, resp.status().code());
            assertEquals(0, resp.content().readableBytes());
            resp.release();
            assertEquals(0, f.endpoint.exchange.live());
            ch.finishAndReleaseAll();
        }
    }

    /** Lost, repeated, delayed and replayed requests and responses through a proxy. */
    @Test void faults() throws Exception {
        var c = cfg().holdTime(Duration.ofMillis(500)).backoff(new io.zmux.Backoff(Duration.ofMillis(5), Duration.ofMillis(100), 0.5))
                .requestTimeout(Duration.ofSeconds(3)).upPieceSize(64 << 10).downPieceSize(64 << 10).build();
        try (var f = Fixture.plain(b -> b.exchange(c));
             var px = new TestProxy(f.base, TestProxy.Profile.bufferingCdn(0), new TestProxy.Faults(0.05, 0.05, 0.1, 0.2, 30, 0.05, null), 7);
             var hc = f.client(b -> b)) {
            try (var s = Zmux.open(hc.exchangePipe(px.base.toString(), c), Role.CLIENT, f.config)) {
                echo(f, s, 1 << 20);
            }
        }
    }

    /** BUSY makes the client repeat; GONE fails the carrier. */
    @Test void busyAndGone() throws Exception {
        var c = ExchangeConfig.builder(new byte[32]).holdTime(Duration.ofMillis(500))
                .backoff(new io.zmux.Backoff(Duration.ofMillis(5), Duration.ofMillis(50), 0.5)).build();
        AtomicInteger busy = new AtomicInteger();
        try (var f = Fixture.plain(b -> b.exchange(c));
             var px = new TestProxy(f.base, TestProxy.Profile.bufferingCdn(0),
                     new TestProxy.Faults(0, 0, 0, 0, 0, 0, body -> busy.getAndIncrement() % 3 == 0 ? answer(body, Exchange.FLAG_BUSY) : null), 1);
             var hc = f.client(b -> b)) {
            try (var s = Zmux.open(hc.exchangePipe(px.base.toString(), c), Role.CLIENT, f.config)) {
                echo(f, s, 256 << 10);
            }
            assertTrue(busy.get() > 3);
        }
        try (var f = Fixture.plain(b -> b.exchange(c));
             var px = new TestProxy(f.base, TestProxy.Profile.bufferingCdn(0),
                     new TestProxy.Faults(0, 0, 0, 0, 0, 0, body -> answer(body, Exchange.FLAG_GONE)), 1);
             var hc = f.client(b -> b)) {
            Throwable e = readFailure(hc.exchangePipe(px.base.toString(), c));
            assertEquals(ExchangeClient.GONE, e.getMessage());
        }
    }

    /** A response to body with flags, computed without a PSK. */
    static byte[] answer(byte[] body, int flags) {
        if (body.length < Exchange.HDR) return null;
        var q = Exchange.parseHeader(ByteBuffer.wrap(body));
        if (q.kind() == Exchange.KIND_CLOSE) return null;
        var key = Exchange.key(NONE, q.cid());
        ByteBuffer h = Exchange.responseHeader(key, q, flags, ByteBuffer.allocate(0));
        byte[] out = new byte[h.remaining()];
        h.get(out);
        return out;
    }

    /** Starts p's downlink and waits for its failure (null: none within 10 s). */
    static Throwable readFailure(PipeIo p) throws Exception {
        CompletableFuture<Throwable> err = new CompletableFuture<>();
        p.reader().start(new PipeReader.Sink() {
            @Override public void onData(ByteBuffer data, Runnable release) { release.run(); }
            @Override public void onEnd() { err.complete(null); }
            @Override public void onError(Throwable cause) { err.complete(cause); }
        });
        try {
            return err.get(10, TimeUnit.SECONDS);
        } finally {
            p.reader().close();
            p.writer().close();
        }
    }

    /** An UP over the server's limit is not authenticated, and the carrier fails at its deadline; a DOWN over the client's fails it at once. */
    @Test void bodyLimits() throws Exception {
        var srv = ExchangeConfig.builder(new byte[32]).upPieceSize(16 << 10).holdTime(Duration.ofMillis(100)).build();
        try (var f = Fixture.plain(b -> b.exchange(srv)); var hc = f.client(b -> b)) {
            var cl = ExchangeConfig.builder(new byte[32]).upPieceSize(2 << 20).coalesce(Duration.ZERO)
                    .deadline(Duration.ofSeconds(1)).build();
            PipeIo p = hc.exchangePipe(f.base.toString(), cl);
            CompletableFuture<Throwable> wrote = new CompletableFuture<>();
            p.writer().write(io.zmux.carrier.OutboundBatch.of(ByteBuffer.allocate(2 << 20)), new io.zmux.carrier.PipeWriter.Completion() {
                @Override public void completed() { wrote.complete(null); }
                @Override public void failed(Throwable cause) { wrote.complete(cause); }
            });
            assertEquals(ExchangeClient.DEADLINE, readFailure(p).getMessage());
        }
        try (var f = Fixture.plain(b -> b.exchange(srv));
             var px = new TestProxy(f.base, TestProxy.Profile.bufferingCdn(0), new TestProxy.Faults(0, 0, 0, 0, 0, 0, body -> new byte[2 << 20]), 1);
             var hc = f.client(b -> b)) {
            var cl = ExchangeConfig.builder(new byte[32]).downPieceSize(16 << 10).build();
            assertEquals(ExchangeClient.PROTOCOL, readFailure(hc.exchangePipe(px.base.toString(), cl)).getMessage());
        }
    }

    /** UP and DOWN requests to different fronts and paths reach one server. */
    @Test void splitTargets() throws Exception {
        var c0 = ExchangeConfig.builder(new byte[32]).holdTime(Duration.ofSeconds(1)).build();
        try (var f = Fixture.plain(b -> b.exchange(c0));
             var up = new TestProxy(f.base, TestProxy.Profile.PASS); var down = new TestProxy(f.base, TestProxy.Profile.PASS);
             var hc = f.client(b -> b)) {
            var c = ExchangeConfig.builder(new byte[32]).downUrl(down.base + "/down/{instance}/{nonce}").build();
            try (var s = Zmux.open(hc.exchangePipe(up.base + "/up/{instance}", c), Role.CLIENT, f.config)) {
                echo(f, s, 1 << 20);
            }
            assertTrue(up.requests.get() > 0 && down.requests.get() > 0, up.requests + " " + down.requests);
        }
    }

    /** The default piece size keeps every body, after the encoding, at most 1,000,000 bytes. */
    @Test void defaultBodySize() throws Exception {
        for (boolean b64 : new boolean[] {false, true}) {
            int piece = ExchangeConfig.defaultPieceSize(b64);
            java.util.function.IntUnaryOperator body = n -> b64 ? Exchange.base64Length(Exchange.REQ_OVERHEAD + n) : Exchange.REQ_OVERHEAD + n;
            assertTrue(body.applyAsInt(piece) <= 1_000_000 && body.applyAsInt(piece + 3) > 1_000_000, "piece " + piece);
            var c = cfg().base64(b64).holdTime(Duration.ofSeconds(2)).coalesce(Duration.ofMillis(200)).build();
            assertEquals(piece, c.upPieceSize());
            assertEquals(piece, c.downPieceSize());
            assertEquals(1 << 20, c.upLimit());
            try (var f = Fixture.plain(b -> b.exchange(c)); var px = new TestProxy(f.base, TestProxy.Profile.bufferingCdn(0));
                 var hc = f.client(b -> b)) {
                try (var s = Zmux.open(hc.exchangePipe(px.base.toString(), c), Role.CLIENT, f.config)) {
                    echo(f, s, 3 << 20);
                }
                long req = px.maxRequestBody.get(), resp = px.maxResponseBody.get();
                assertTrue(req > 0 && req <= 1_000_000 && resp > 0 && resp <= 1_000_000, "largest bodies " + req + " " + resp);
            }
        }
    }

    @Test void configNeedsAPsk() {
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder(new byte[0]));
        try (var hc = HttpCarrierClient.builder().build()) {
            assertThrows(IllegalArgumentException.class, () -> hc.exchangePipe("ftp://x/{nonce}", cfg().build()));
        } catch (IOException e) {
            fail(e);
        }
    }

    /** Both halves closed by zmux against a server that answers nothing usable: the final pieces are not repeated, the carrier ends within a second. */
    @Test void aClosedCarrierEndsWhileItsServerIsGone() throws Exception {
        try (var f = Fixture.plain(b -> b.exchange(cfg().build()));
             var px = new TestProxy(f.base, TestProxy.Profile.bufferingCdn(0), new TestProxy.Faults(0, 0, 0, 0, 0, 0, body -> new byte[3]), 1);
             var hc = f.client(b -> b)) {
            var cl = cfg().backoff(new io.zmux.Backoff(Duration.ofMillis(20), Duration.ofMillis(100), 0.5)).build();
            PipeIo p = hc.exchangePipe(px.base.toString(), cl);
            p.writer().write(io.zmux.carrier.OutboundBatch.of(ByteBuffer.wrap(new byte[] {'x'})), new io.zmux.carrier.PipeWriter.Completion() {
                @Override public void completed() { }
                @Override public void failed(Throwable cause) { }
            });
            Thread.sleep(100); // the pieces fail and wait to be repeated
            assertFalse(hc.exchanges.isEmpty());
            p.writer().close();
            p.reader().close();
            long t0 = System.nanoTime();
            waitFor(hc.exchanges::isEmpty, "the closed carrier keeps repeating its final pieces");
            assertTrue(System.nanoTime() - t0 < TimeUnit.SECONDS.toNanos(1));
        }
    }

    static void waitFor(java.util.function.BooleanSupplier ok, String what) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!ok.getAsBoolean()) {
            if (System.nanoTime() - end > 0) fail(what);
            Thread.sleep(10);
        }
    }
}
