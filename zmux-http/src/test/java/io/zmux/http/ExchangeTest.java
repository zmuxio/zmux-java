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

    static ExchangeConfig.Builder cfg() { return ExchangeConfig.builder().psk(PSK); }

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
        ByteBuffer hdr = Exchange.requestHeader(key, cid, kind, flags, seq, ack, time, nonce, ByteBuffer.wrap(data));
        byte[] out = new byte[hdr.remaining() + data.length];
        hdr.get(out, 0, hdr.remaining());
        System.arraycopy(data, 0, out, out.length - data.length, data.length);
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
            if (corrupt) b[b.length - data.length - 1] ^= 1;
            Resp r = post(u, b);
            if (r.status == 200) {
                // The response MAC binds it to this request.
                var q = Exchange.parseRequest(ByteBuffer.wrap(b));
                var key = Exchange.key(psk, cid);
                ByteBuffer d = ByteBuffer.wrap(r.body, Exchange.RESP_HDR, r.body.length - Exchange.RESP_HDR);
                assertArrayEquals(key.responseMac(cid, kind, q.seqRaw(), nonce, r.flags(), d), Arrays.copyOf(r.body, Exchange.MAC));
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
            {"", "000102030405060708090a0b0c0d0e0f010000006955b9000102030405060708507a0b4c9f35406fc018ecb298ac332068656c6c6f",
             "477e5b04a29e7fc80fd53eb36fd0715400"},
            {"6b".repeat(32), "000102030405060708090a0b0c0d0e0f010000006955b90001020304050607089b83951cb9d54b75d8d0785f3eb2d3d668656c6c6f",
             "c5b18d1988c3d46dbfee70883026026000"},
        };
        for (String[] v : vecs) {
            byte[] psk = HEX.parseHex(v[0]);
            var c = psk.length == 0 ? ExchangeConfig.builder().allowInsecure(true) : ExchangeConfig.builder().psk(psk);
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
            try (var cs = Zmux.open(hc.exchangePipe(f.base.toString(), c), Role.CLIENT, f.config);
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
        var c = ExchangeConfig.builder().psk(k).downWindow(4).holdTime(Duration.ofMillis(50)).build();
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

    /** A new carrier takes its source's pending share first: one beyond it is answered BUSY and holds nothing. */
    @Test void newCarriersTakeTheSourcesPendingShare() throws Exception {
        var c = cfg().holdTime(Duration.ofMillis(50)).build();
        try (var f = new Fixture(b -> b, b -> b.maxPendingPerSource(1).firstRecordTimeout(Duration.ofSeconds(10)), b -> b.exchange(c), false, null)) {
            var x = new Raw(f.base, PSK);
            var y = new Raw(f.base, PSK);
            assertEquals(0, x.flags(Exchange.KIND_UP, 0, 1, 0, NONE));
            assertEquals(1, f.endpoint.exchange.live());
            assertEquals(Exchange.FLAG_BUSY, y.flags(Exchange.KIND_UP, 0, 1, 0, NONE), "beyond the source's share");
            assertEquals(1, f.endpoint.exchange.live());
            // Not remembered as gone: once the share is free it is served.
            assertEquals(0, x.flags(Exchange.KIND_CLOSE, 0, 0, 0, NONE));
            waitFor(() -> {
                try { return y.flags(Exchange.KIND_UP, 0, 1, 0, NONE) == 0; } catch (Exception e) { throw new RuntimeException(e); }
            }, "the second carrier served");
        }
    }

    /** A carrier the listener rejects ends its downlink; the final CLOSE completes it. */
    @Test void completionAndClose() throws Exception {
        var c = ExchangeConfig.builder().allowInsecure(true).holdTime(Duration.ofSeconds(1)).build();
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
        var c = ExchangeConfig.builder().allowInsecure(true).holdTime(Duration.ofSeconds(1)).build();
        try (var f = Fixture.plain(b -> b.exchange(c))) {
            f.listener.close();
            var x = new Raw(f.base, NONE);
            assertEquals(Exchange.FLAG_GONE, x.flags(Exchange.KIND_UP, 0, 0, 0, new byte[] {1}));
            waitFor(() -> f.endpoint.exchange.live() == 0, "no carrier stays");
        }
    }

    /** Lost, repeated, delayed and replayed requests and responses through a proxy. */
    @Test void faults() throws Exception {
        var c = cfg().holdTime(Duration.ofMillis(500)).backoff(Duration.ofMillis(5), Duration.ofMillis(100))
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
        var c = ExchangeConfig.builder().allowInsecure(true).holdTime(Duration.ofMillis(500))
                .backoff(Duration.ofMillis(5), Duration.ofMillis(50)).build();
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
        var q = Exchange.parseRequest(ByteBuffer.wrap(body));
        if (q == null || q.kind() == Exchange.KIND_CLOSE) return null;
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
        var srv = ExchangeConfig.builder().allowInsecure(true).upPieceSize(16 << 10).holdTime(Duration.ofMillis(100)).build();
        try (var f = Fixture.plain(b -> b.exchange(srv)); var hc = f.client(b -> b)) {
            var cl = ExchangeConfig.builder().allowInsecure(true).upPieceSize(2 << 20).coalesce(Duration.ZERO)
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
            var cl = ExchangeConfig.builder().allowInsecure(true).downPieceSize(16 << 10).build();
            assertEquals(ExchangeClient.PROTOCOL, readFailure(hc.exchangePipe(px.base.toString(), cl)).getMessage());
        }
    }

    /** UP and DOWN requests to different fronts and paths reach one server. */
    @Test void splitTargets() throws Exception {
        var c0 = ExchangeConfig.builder().allowInsecure(true).holdTime(Duration.ofSeconds(1)).build();
        try (var f = Fixture.plain(b -> b.exchange(c0));
             var up = new TestProxy(f.base, TestProxy.Profile.PASS); var down = new TestProxy(f.base, TestProxy.Profile.PASS);
             var hc = f.client(b -> b)) {
            var c = ExchangeConfig.builder().allowInsecure(true).downUrl(down.base + "/down/{instance}/{nonce}").build();
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
            java.util.function.IntUnaryOperator body = n -> b64 ? Exchange.base64Length(Exchange.REQ_HDR_MAX + n) : Exchange.REQ_HDR_MAX + n;
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

    @Test void configNeedsAPskOrInsecure() {
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().build());
        assertThrows(IllegalArgumentException.class, () -> ExchangeConfig.builder().psk(PSK).allowInsecure(true).build());
        try (var hc = HttpCarrierClient.builder().build()) {
            assertThrows(IllegalArgumentException.class, () -> hc.exchangePipe("ftp://x/{nonce}", cfg().build()));
        } catch (IOException e) {
            fail(e);
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
