package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class AdmissionTest {
    static final HexFormat HEX = HexFormat.of();
    static final byte[] PSK = filled(32, 0x6b);
    static final byte[] NONCE = HEX.parseHex("a0a1a2a3a4a5a6a7a8a9aaabacadaeaf");

    static byte[] filled(int n, int v) { byte[] b = new byte[n]; Arrays.fill(b, (byte) v); return b; }

    @Test void prefixTokensMatchTheTestVectors() {
        var a = new PrefixAuth(PSK, Duration.ofSeconds(30), false, 0, 0);
        assertEquals("2eaed9a5e8ca0729884488be9c349871b69823f01876ad48062a8b15c6c1839c", HEX.formatHex(a.derive()));
        assertEquals("a0a1a2a3a4a5a6a7a8a9aaabacadaeaf3fed5e50dc8fc8fce2e962ddb13dbfae",
                HEX.formatHex(a.token(null, 1767225600L, NONCE)));
        byte[] binding = new byte[32];
        for (int i = 0; i < 32; i++) binding[i] = (byte) i;
        assertEquals("a0a1a2a3a4a5a6a7a8a9aaabacadaeaf36747d163099e42de649d7159d462334",
                HEX.formatHex(a.token(binding, 1767225600L, NONCE)));
        var b64 = java.util.Base64.getUrlEncoder().withoutPadding();
        assertEquals(UNBOUND_HEADER, b64.encodeToString(a.token(null, 1767225600L, NONCE)));
        assertEquals(BOUND_HEADER, b64.encodeToString(a.token(binding, 1767225600L, NONCE)));
        assertArrayEquals(a.token(null, 1767225600L, NONCE), PrefixAuth.decodeHeader(UNBOUND_HEADER));
        assertArrayEquals(a.token(binding, 1767225600L, NONCE), PrefixAuth.decodeHeader(BOUND_HEADER));
    }

    static final String UNBOUND_HEADER = "oKGio6SlpqeoqaqrrK2urz_tXlDcj8j84uli3bE9v64";
    static final String BOUND_HEADER = "oKGio6SlpqeoqaqrrK2urzZ0fRYwmeQt5knXFZ1GIzQ";
    static final byte[] VECTOR_BINDING = new byte[32];
    static { for (int i = 0; i < 32; i++) VECTOR_BINDING[i] = (byte) i; }

    /** The metadata of a carrier decided on its request head, with these header fields (name, value, ...). */
    static Admission.CarrierInfo headInfo(PipeIo.Exporter exporter, String... fields) {
        var headers = new java.util.LinkedHashMap<String, java.util.List<String>>();
        for (int i = 0; i < fields.length; i += 2)
            headers.computeIfAbsent(fields[i], k -> new java.util.ArrayList<>()).add(fields[i + 1]);
        return new Admission.CarrierInfo(TestAddr.peer("peer"), TestAddr.peer("peer"), "", "", exporter, RequestHead.of(headers, true));
    }

    static PipeIo.Exporter exporting(byte[] binding) {
        return (label, context, length) -> PrefixAuth.EXPORTER_LABEL.equals(label) && context.length == 0 && length == 32 ? binding.clone() : null;
    }

    /**
     * The header form of the token: the vectors verify on a carrier decided on
     * its request head, with no prefix and a skip of 0; a missing, repeated (also in another
     * case) or malformed field, a replay, and another connection's binding fail like a wrong
     * token; the field name is configurable.
     */
    @Test void headerFormVerifiesTheVectorsAndRejectsMalformedValues() {
        var unbound = new PrefixAuth(PSK, Duration.ofSeconds(30), false, 0, 0);
        var empty = ByteBuffer.allocate(0);
        var v = unbound.verifier();
        v.clock = () -> 1767225600L;
        var accept0 = Admission.Decision.accept(0);
        assertEquals(accept0, v.decide(headInfo(null, "zmux-prefix-auth", UNBOUND_HEADER), empty), "field names ignore case");
        assertEquals(Admission.Decision.FALLBACK, v.decide(headInfo(null, "Zmux-Prefix-Auth", UNBOUND_HEADER), empty), "replay");

        String canonical = UNBOUND_HEADER;
        String[][] malformed = {
                {},
                {"Zmux-Prefix-Auth", canonical, "Zmux-Prefix-Auth", canonical},
                {"Zmux-Prefix-Auth", canonical, "ZMUX-PREFIX-AUTH", canonical},
                {"Zmux-Prefix-Auth", canonical + "="},
                {"Zmux-Prefix-Auth", canonical.substring(0, 42)},
                {"Zmux-Prefix-Auth", canonical.substring(0, 42) + "+"},
                {"Zmux-Prefix-Auth", "/" + canonical.substring(1)},
                {"Zmux-Prefix-Auth", canonical.substring(0, 42) + "5"},
                {"Zmux-Prefix-Auth", canonical.substring(0, 41) + "=="},
                {"Zmux-Prefix-Auth", " " + canonical.substring(1)},
                {"Other", canonical},
        };
        String[] names = {"missing", "duplicate", "duplicate in another case", "padded", "42 chars", "bad char +",
                "bad char /", "non-zero trailing bits", "padding inside", "space", "other field"};
        for (int i = 0; i < malformed.length; i++) {
            var fresh = unbound.verifier();
            fresh.clock = () -> 1767225600L;
            assertEquals(Admission.Decision.FALLBACK, fresh.decide(headInfo(null, malformed[i]), empty), names[i]);
            assertEquals(accept0, fresh.decide(headInfo(null, "Zmux-Prefix-Auth", canonical), empty),
                    names[i] + ": a rejected value records no Nonce");
        }
        assertNull(PrefixAuth.decodeHeader(canonical.substring(0, 42) + "5"), "non-canonical trailing bits");
        assertNull(PrefixAuth.decodeHeader(canonical.substring(0, 42) + "9"), "non-canonical trailing bits");
        assertNotNull(PrefixAuth.decodeHeader(canonical.substring(0, 42) + "8"));

        var bound = new PrefixAuth(PSK, Duration.ofSeconds(30), true, 0, 0);
        var bv = bound.verifier();
        bv.clock = () -> 1767225600L;
        byte[] other = VECTOR_BINDING.clone();
        other[0] ^= 1;
        assertEquals(Admission.Decision.FALLBACK, bv.decide(headInfo(exporting(other), "Zmux-Prefix-Auth", BOUND_HEADER), empty),
                "another connection's binding");
        assertEquals(Admission.Decision.FALLBACK, bv.decide(headInfo(null, "Zmux-Prefix-Auth", BOUND_HEADER), empty),
                "a bound token on a carrier without an exporter");
        assertEquals(Admission.Decision.FALLBACK, bv.decide(headInfo(exporting(VECTOR_BINDING), "Zmux-Prefix-Auth", UNBOUND_HEADER), empty),
                "an unbound token where tokens are bound");
        assertEquals(accept0, bv.decide(headInfo(exporting(VECTOR_BINDING), "Zmux-Prefix-Auth", BOUND_HEADER), empty));

        var renamed = unbound.withHeaderField("X-Auth");
        var rv = renamed.verifier();
        rv.clock = () -> 1767225600L;
        assertEquals(Admission.Decision.FALLBACK, rv.decide(headInfo(null, "Zmux-Prefix-Auth", canonical), empty), "default name not agreed");
        assertEquals(accept0, rv.decide(headInfo(null, "x-auth", canonical), empty));
        assertThrows(IllegalArgumentException.class, () -> unbound.withHeaderField("bad name"));
    }

    /** As a credential, PrefixAuth presents fresh tokens that its verifier accepts once, in either form. */
    @Test void prefixAuthIsACredential() throws Exception {
        var auth = new PrefixAuth(PSK, Duration.ofSeconds(30), true, 1, 0);
        var stream = new Admission.CarrierInfo(TestAddr.peer("server"), TestAddr.peer("server"), "qmux-02", "example.com", exporting(VECTOR_BINDING), RequestHead.NONE);
        byte[] a = auth.prefix(stream), b = auth.prefix(stream);
        assertEquals(PrefixAuth.TOKEN_LENGTH, a.length);
        assertFalse(Arrays.equals(a, b), "a fresh Nonce per carrier");
        var v = auth.verifier();
        assertEquals(Admission.Decision.accept(PrefixAuth.TOKEN_LENGTH), v.decide(stream, ByteBuffer.wrap(a)));
        assertEquals(Admission.Decision.FALLBACK, v.decide(stream, ByteBuffer.wrap(a)), "replay");
        Credential.HeaderField h = auth.header(stream);
        assertEquals(PrefixAuth.HEADER_FIELD, h.name());
        assertEquals(PrefixAuth.HEADER_LENGTH, h.value().length());
        assertEquals(Admission.Decision.accept(0), v.decide(headInfo(exporting(VECTOR_BINDING), h.name(), h.value()), ByteBuffer.allocate(0)));
        var plain = new Admission.CarrierInfo(TestAddr.peer("server"), TestAddr.peer("server"), "", "", null, RequestHead.NONE);
        assertThrows(IOException.class, () -> auth.prefix(plain), "a bound token needs an exporter");
        assertEquals("X-Auth", auth.withHeaderField("X-Auth").header(stream).name());
    }

    @Test void verifierAcceptsWithinTheSkewOnceAndRejectsReplays() {
        var a = new PrefixAuth(PSK, Duration.ofSeconds(30), false, 1, 0);
        var v = a.verifier();
        AtomicLong now = new AtomicLong(1767225600L);
        v.clock = now::get;
        byte[] t = a.token(null, 1767225600L, NONCE);
        assertTrue(v.verify(t, null));
        assertFalse(v.verify(t, null), "replayed Nonce");
        byte[] old = a.token(null, 1767225600L - 30, HEX.parseHex("b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"));
        assertTrue(v.verify(old, null), "previous slot within the skew");
        byte[] older = a.token(null, 1767225600L - 60, HEX.parseHex("c0c1c2c3c4c5c6c7c8c9cacbcccdcecf"));
        assertFalse(v.verify(older, null), "outside the skew");
        byte[] bad = t.clone();
        bad[31] ^= 1;
        assertFalse(v.verify(bad, null));
        assertFalse(v.verify(a.token(new byte[32], 1767225600L, HEX.parseHex("d0d1d2d3d4d5d6d7d8d9dadbdcdddedf")), null),
                "binding mismatch");
        var full = new PrefixAuth(PSK, Duration.ofSeconds(30), false, 0, 1).verifier();
        full.clock = now::get;
        assertTrue(full.verify(a.token(null, 1767225600L, filled(16, 1)), null));
        assertFalse(full.verify(a.token(null, 1767225600L, filled(16, 2)), null), "record full");
    }

    /** A carrier over a blocking socket, for a client that writes bytes before the session. */
    static PipeIo socketPipe(SocketChannel ch) {
        PipeReader r = new PipeReader() {
            volatile boolean closed;
            @Override public void start(Sink sink) {
                Thread.ofVirtual().start(() -> {
                    try {
                        while (!closed) {
                            ByteBuffer b = ByteBuffer.allocate(16384);
                            if (ch.read(b) < 0) { sink.onEnd(); return; }
                            b.flip();
                            CountDownLatch done = new CountDownLatch(1);
                            sink.onData(b, done::countDown);
                            done.await();
                        }
                    } catch (Exception e) { if (!closed) sink.onError(e); }
                });
            }
            @Override public void close() { closed = true; }
        };
        PipeWriter w = new PipeWriter() {
            @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) {
                try { for (ByteBuffer b : data.segments(new ByteBuffer[data.segmentCount()])) while (b.hasRemaining()) ch.write(b); done.completed(); }
                catch (IOException e) { done.failed(e); }
            }
            @Override public void close() { try { ch.shutdownOutput(); } catch (IOException ignored) { } }
            @Override public void abort(Throwable cause) { try { ch.close(); } catch (IOException ignored) { } }
        };
        return PipeIo.of(r, w);
    }

    @Test void prefixAuthAdmitsTokenCarriersAndStallsOthers() throws Exception {
        var auth = new PrefixAuth(PSK, Duration.ofSeconds(30), false, 1, 0);
        var stallFor = Duration.ofMillis(300);
        try (EventLoops loops = EventLoops.create(2)) {
            var multipath = MultipathConfig.defaults().withMaxPipes(4).withAuth(PipeAuth.insecure());
            var cfg = ZmuxConfig.builder().eventLoops(loops).multipath(multipath).build();
            var dialer = ZmuxConfig.builder().eventLoops(loops).multipath(multipath).credential(auth).build();
            var lc = ListenerConfig.builder().session(cfg)
                    .admission(Admission.of(PrefixAuth.TOKEN_LENGTH, auth.verifier()).withPeekTimeout(Duration.ofSeconds(2))
                            .withStall(stallFor, stallFor)).build();
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), lc)) {
                assertEquals(stallFor, l.stallDuration(new java.net.InetSocketAddress("127.0.0.1", 0)));
                try (var c = Zmux.connect(l.localAddress(), dialer); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s);
                    assertTrue(c.paths().add(l.localAddress(), PipeOptions.defaults()).id() > 0, "a session's own TCP pipe presents the credential");
                    try (var st = c.openStream()) {
                        st.write(new byte[] {1, 2, 3});
                        st.finish();
                        try (var in = s.acceptStream(Duration.ofSeconds(5))) { assertArrayEquals(new byte[] {1, 2, 3}, in.readAll(16)); }
                    }
                }
                // Without a token: nothing is sent, and the carrier closes after the stall.
                try (SocketChannel raw = SocketChannel.open(l.localAddress())) {
                    raw.write(ByteBuffer.wrap(new byte[40]));
                    long t0 = System.nanoTime();
                    raw.socket().setSoTimeout(5000);
                    int n = raw.socket().getInputStream().read(new byte[16]);
                    long ms = (System.nanoTime() - t0) / 1_000_000;
                    assertEquals(-1, n, "a stalled carrier sends nothing");
                    assertTrue(ms >= 200, "closed after " + ms + " ms");
                }
                assertNull(l.accept(Duration.ofMillis(100)));
            }
        }
    }

    @Test void verdictsCloseAcceptWithSkipAndBoundStallsPerSource() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            AtomicReference<Admission.Decision> verdict = new AtomicReference<>(Admission.Decision.CLOSE);
            var seen = new AtomicReference<String>();
            var lc = ListenerConfig.builder().session(cfg).maxPendingPerSource(2)
                    .admission(Admission.of(4, Admission.Decider.immediate((info, prefix) -> {
                        seen.set(info.source().getHostString() + " " + info.tls() + " " + prefix.remaining());
                        return verdict.get();
                    })).withPeekTimeout(Duration.ofSeconds(2)).withStall(Duration.ofSeconds(5), Duration.ofSeconds(5))).build();
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), lc)) {
                try (SocketChannel raw = SocketChannel.open(l.localAddress())) {
                    raw.write(ByteBuffer.wrap(new byte[] {9, 9, 9, 9}));
                    raw.socket().setSoTimeout(3000);
                    long t0 = System.nanoTime();
                    assertEquals(-1, raw.socket().getInputStream().read());
                    assertTrue(System.nanoTime() - t0 < TimeUnit.SECONDS.toNanos(2), "closed at once");
                }
                assertEquals("127.0.0.1 false 4", seen.get());
                verdict.set(Admission.Decision.accept(4));
                SocketChannel ch = SocketChannel.open(l.localAddress());
                ch.write(ByteBuffer.wrap(new byte[] {7, 7, 7, 7}));
                try (var c = Zmux.open(socketPipe(ch), Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s, "accepted after the skipped prefix");
                }
                verdict.set(Admission.Decision.STALL);
                try (SocketChannel first = SocketChannel.open(l.localAddress());
                     SocketChannel second = SocketChannel.open(l.localAddress())) {
                    first.write(ByteBuffer.wrap(new byte[4]));
                    Thread.sleep(200);
                    second.write(ByteBuffer.wrap(new byte[4]));
                    second.socket().setSoTimeout(3000);
                    long t0 = System.nanoTime();
                    assertEquals(-1, second.socket().getInputStream().read(), "beyond the per-source limit");
                    assertTrue(System.nanoTime() - t0 < TimeUnit.SECONDS.toNanos(2));
                }
            }
        }
    }

    @Test void closingTheListenerEndsItsStalls() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            // An explicit stall and a fallback without a target, both far longer than the test.
            for (var d : java.util.List.of(Admission.Decision.STALL, Admission.Decision.FALLBACK)) {
                var lc = ListenerConfig.builder().session(cfg)
                        .admission(Admission.of(4, Admission.Decider.immediate((info, prefix) -> d))
                                .withStall(Duration.ofMinutes(5), Duration.ofMinutes(5))).build();
                var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), lc);
                try (SocketChannel raw = SocketChannel.open(l.localAddress())) {
                    raw.write(ByteBuffer.wrap(new byte[4]));
                    PipePoolTest.await(() -> l.stats().admission().stalled() + l.stats().admission().fallback() == 1);
                    // A fallback without a target stalls but counts as a fallback.
                    assertEquals(d == Admission.Decision.STALL ? 1 : 0, l.stats().admission().stalled());
                    long t0 = System.nanoTime();
                    l.close();
                    raw.socket().setSoTimeout(5000);
                    assertEquals(-1, raw.socket().getInputStream().read(), d + ": ended gracefully, nothing sent");
                    assertTrue(System.nanoTime() - t0 < TimeUnit.SECONDS.toNanos(2), d + ": ended at the close");
                } finally { l.close(); }
            }
        }
    }

    /** A fallback target that collects the input until it ends, then writes it back and closes both halves. */
    static Admission.Fallback echo(BlockingQueue<String> sources) {
        return (carrier, info) -> {
            sources.add(info.source().getHostString());
            var got = new java.io.ByteArrayOutputStream();
            carrier.reader().start(new PipeReader.Sink() {
                @Override public void onData(ByteBuffer data, Runnable release) {
                    byte[] b = new byte[data.remaining()];
                    data.get(b);
                    got.writeBytes(b);
                    release.run();
                }
                @Override public void onEnd() {
                    carrier.reader().close();
                    carrier.writer().write(io.zmux.carrier.OutboundBatch.of(ByteBuffer.wrap(got.toByteArray())), new PipeWriter.Completion() {
                        @Override public void completed() { carrier.writer().close(); }
                        @Override public void failed(Throwable cause) { carrier.writer().abort(cause); }
                    });
                }
                @Override public void onError(Throwable cause) { carrier.reader().close(); carrier.writer().abort(cause); }
            });
        };
    }

    static String readAll(SocketChannel ch) throws IOException {
        ch.socket().setSoTimeout(5000);
        return new String(ch.socket().getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.US_ASCII);
    }

    @Test void fallbackTakesTheCarrierWithEveryByteReadReplayed() throws Exception {
        var sources = new LinkedBlockingQueue<String>();
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var lc = ListenerConfig.builder().session(cfg).fallback(echo(sources))
                    .admission(Admission.of(4, Admission.Decider.immediate((info, prefix) -> Admission.Decision.FALLBACK))
                            .withPeekTimeout(Duration.ofMillis(200))).build();
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), lc)) {
                // The whole prefix at once: decided on live input.
                try (SocketChannel ch = SocketChannel.open(l.localAddress())) {
                    ch.write(ByteBuffer.wrap("abcdefgh".getBytes()));
                    ch.shutdownOutput();
                    assertEquals("abcdefgh", readAll(ch));
                }
                // Fewer bytes than the peek: decided when the peek timeout passes, on the buffered prefix.
                try (SocketChannel ch = SocketChannel.open(l.localAddress())) {
                    ch.write(ByteBuffer.wrap("ab".getBytes()));
                    Thread.sleep(500);
                    ch.write(ByteBuffer.wrap("cdefgh".getBytes()));
                    ch.shutdownOutput();
                    assertEquals("abcdefgh", readAll(ch));
                }
                // The input ends within the peek: decided at its end, the bytes still replayed.
                try (SocketChannel ch = SocketChannel.open(l.localAddress())) {
                    ch.write(ByteBuffer.wrap("xy".getBytes()));
                    ch.shutdownOutput();
                    assertEquals("xy", readAll(ch));
                }
                assertEquals("127.0.0.1", sources.poll(1, TimeUnit.SECONDS));
                assertNull(l.accept(Duration.ofMillis(50)));
            }
        }
    }

    @Test void anAcceptAfterThePeekTimeoutSkipsTheBufferedPrefix() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var lc = ListenerConfig.builder().session(cfg)
                    .admission(Admission.of(8, Admission.Decider.immediate((info, prefix) -> prefix.remaining() == 4 ? Admission.Decision.accept(4) : Admission.Decision.CLOSE))
                            .withPeekTimeout(Duration.ofMillis(200))).build();
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), lc)) {
                SocketChannel ch = SocketChannel.open(l.localAddress());
                ch.write(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
                Thread.sleep(400);
                try (var c = Zmux.open(socketPipe(ch), Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s, "the session starts after the skipped bytes");
                }
            }
        }
    }

    @Test void carriersGivenToTheListenerPassTheAdmission() throws Exception {
        var sources = new LinkedBlockingQueue<String>();
        try (EventLoops loops = EventLoops.create(2);
             var server = java.nio.channels.ServerSocketChannel.open().bind(new InetSocketAddress("127.0.0.1", 0))) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var lc = ListenerConfig.builder().session(cfg)
                    .admission(Admission.of(4, Admission.Decider.immediate((info, prefix) -> {
                        byte b = prefix.get(0);
                        return b == 7 ? Admission.Decision.accept(4) : b == 8 ? Admission.Decision.FALLBACK : Admission.Decision.CLOSE;
                    })).withPeekTimeout(Duration.ofSeconds(2))).build();
            try (var l = Zmux.listener(lc)) {
                SocketChannel ch = SocketChannel.open(server.getLocalAddress());
                ch.write(ByteBuffer.wrap(new byte[] {7, 7, 7, 7}));
                var admitted = l.handle(socketPipe(server.accept()), JoinHint.inBand(TestAddr.peer("peer")), echo(sources));
                try (var c = Zmux.open(socketPipe(ch), Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s);
                    assertEquals(Admission.Verdict.ACCEPT, admitted.verdict().toCompletableFuture().get(5, TimeUnit.SECONDS));
                }
                try (SocketChannel raw = SocketChannel.open(server.getLocalAddress())) {
                    raw.write(ByteBuffer.wrap(new byte[] {8, 'a', 'b', 'c', 'd'}));
                    raw.shutdownOutput();
                    var fb = l.handle(socketPipe(server.accept()), JoinHint.inBand(TestAddr.peer("peer")), echo(sources));
                    assertEquals(Admission.Verdict.FALLBACK, fb.verdict().toCompletableFuture().get(5, TimeUnit.SECONDS));
                    assertEquals(new String(new byte[] {8, 'a', 'b', 'c', 'd'}, java.nio.charset.StandardCharsets.US_ASCII), readAll(raw));
                    assertEquals("peer", sources.poll(1, TimeUnit.SECONDS));
                    fb.closed().toCompletableFuture().get(5, TimeUnit.SECONDS);
                }
                try (SocketChannel raw = SocketChannel.open(server.getLocalAddress())) {
                    raw.write(ByteBuffer.wrap(new byte[] {8, 0, 0, 0}));
                    var none = l.handle(socketPipe(server.accept()), JoinHint.inBand(TestAddr.peer("peer")), null);
                    assertEquals(Admission.Verdict.STALL, none.verdict().toCompletableFuture().get(5, TimeUnit.SECONDS),
                            "a fallback without a target stalls");
                }
            }
        }
    }

    /** A carrier over a connected blocking socket's streams. */
    static PipeIo streamPipe(java.net.Socket sock) throws IOException {
        var in = sock.getInputStream();
        var out = sock.getOutputStream();
        PipeReader r = new PipeReader() {
            volatile boolean closed;
            @Override public void start(Sink sink) {
                Thread.ofVirtual().start(() -> {
                    try {
                        byte[] b = new byte[16384];
                        for (int n; !closed && (n = in.read(b)) >= 0;) {
                            CountDownLatch done = new CountDownLatch(1);
                            sink.onData(ByteBuffer.wrap(b.clone(), 0, n), done::countDown);
                            done.await();
                        }
                        if (!closed) sink.onEnd();
                    } catch (Exception e) { if (!closed) sink.onError(e); }
                });
            }
            @Override public void close() { closed = true; }
        };
        PipeWriter w = new PipeWriter() {
            @Override public synchronized void write(io.zmux.carrier.OutboundBatch data, Completion done) {
                try {
                    for (ByteBuffer b : data.segments(new ByteBuffer[data.segmentCount()])) { byte[] a = new byte[b.remaining()]; b.duplicate().get(a); out.write(a); }
                    out.flush();
                    done.completed();
                } catch (IOException e) { done.failed(e); }
            }
            @Override public void close() { try { sock.close(); } catch (IOException ignored) { } }
        };
        return PipeIo.of(r, w);
    }

    /** The bytes a client session writes first. */
    static byte[] firstRecord(ZmuxConfig cfg) throws Exception {
        var first = new CompletableFuture<byte[]>();
        PipeWriter w = new PipeWriter() {
            @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) {
                var all = new java.io.ByteArrayOutputStream();
                for (ByteBuffer b : data.segments(new ByteBuffer[data.segmentCount()])) { byte[] a = new byte[b.remaining()]; b.duplicate().get(a); all.writeBytes(a); }
                first.complete(all.toByteArray());
                done.completed();
            }
            @Override public void close() { }
        };
        PipeReader r = new PipeReader() {
            @Override public void start(Sink sink) { }
            @Override public void close() { }
        };
        var opening = Thread.ofVirtual().start(() -> {
            try (var s = Zmux.open(PipeIo.of(r, w), Role.CLIENT, cfg)) { } catch (Exception ignored) { }
        });
        byte[] b = first.get(5, TimeUnit.SECONDS);
        opening.interrupt();
        return b;
    }

    static javax.net.ssl.SSLSocket dialTls(InetSocketAddress addr) throws Exception {
        var sock = (javax.net.ssl.SSLSocket) TestTls.client().getSocketFactory().createSocket(addr.getAddress(), addr.getPort());
        sock.startHandshake();
        sock.setSoTimeout(5000);
        return sock;
    }

    static byte[] binding(javax.net.ssl.SSLSocket sock) throws Exception {
        return ((javax.net.ssl.ExtendedSSLSession) sock.getSession()).exportKeyingMaterialData(PrefixAuth.EXPORTER_LABEL, new byte[0], 32);
    }

    /**
     * A TLS listener with bound prefix tokens: a valid carrier is accepted; a replayed token,
     * another carrier's token, a truncated one, random bytes and a first record without a
     * token all reach the fallback with exactly the bytes sent, and nothing else is written;
     * without a fallback they are stalled: nothing is written, and the carrier ends with an
     * orderly close.
     */
    @Test void invalidPrefixesTakeTheFallbackPathAlike() throws Exception {
        var auth = new PrefixAuth(PSK, Duration.ofSeconds(30), true, 1, 0);
        var got = new LinkedBlockingQueue<byte[]>();
        Admission.Fallback fallback = (carrier, info) -> {
            var all = new java.io.ByteArrayOutputStream();
            carrier.reader().start(new PipeReader.Sink() {
                @Override public void onData(ByteBuffer data, Runnable release) {
                    byte[] b = new byte[data.remaining()]; data.get(b); all.writeBytes(b); release.run();
                }
                @Override public void onEnd() {
                    got.add(all.toByteArray());
                    carrier.reader().close();
                    carrier.writer().write(io.zmux.carrier.OutboundBatch.of(ByteBuffer.wrap("fallback".getBytes())), new PipeWriter.Completion() {
                        @Override public void completed() { carrier.writer().close(); }
                        @Override public void failed(Throwable cause) { carrier.writer().abort(cause); }
                    });
                }
                @Override public void onError(Throwable cause) { got.add(all.toByteArray()); carrier.reader().close(); carrier.writer().abort(cause); }
            });
        };
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var adm = Admission.of(PrefixAuth.TOKEN_LENGTH, auth.verifier()).withPeekTimeout(Duration.ofMillis(300))
                    .withStall(Duration.ofMillis(50), Duration.ofMillis(100));
            var base = ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server())).admission(adm);
            byte[] rec = firstRecord(cfg);
            byte[] random = new byte[64];
            new java.security.SecureRandom().nextBytes(random);
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), base.fallback(fallback).build())) {
                var sock = dialTls(l.localAddress());
                byte[] tok = auth.token(binding(sock));
                sock.getOutputStream().write(tok);
                try (var c = Zmux.open(streamPipe(sock), Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s, "a valid carrier is accepted");
                }
                byte[] valid;
                try (var other = dialTls(l.localAddress())) { valid = auth.token(binding(other)); }
                assertArrayEquals(new byte[0], got.poll(5, TimeUnit.SECONDS), "a carrier closed without bytes");
                byte[][] cases = {tok, valid, Arrays.copyOf(valid, PrefixAuth.TOKEN_LENGTH / 2), random, rec};
                String[] names = {"replayed", "other carrier", "truncated", "random", "record"};
                for (int i = 0; i < cases.length; i++) {
                    try (var c = dialTls(l.localAddress())) {
                        c.getOutputStream().write(cases[i]);
                        c.shutdownOutput();
                        assertEquals("fallback", new String(c.getInputStream().readAllBytes()), names[i]);
                    }
                    assertArrayEquals(cases[i], got.poll(5, TimeUnit.SECONDS), names[i]);
                }
            }
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), base.fallback(null).build())) {
                byte[] tok;
                try (var other = dialTls(l.localAddress())) { tok = auth.token(binding(other)); }
                byte[][] cases = {tok, Arrays.copyOf(tok, PrefixAuth.TOKEN_LENGTH / 2), random, rec};
                for (byte[] x : cases) {
                    try (var c = dialTls(l.localAddress())) {
                        c.getOutputStream().write(x);
                        long t0 = System.nanoTime();
                        assertEquals(-1, c.getInputStream().read(), "a stalled carrier sends nothing");
                        assertTrue(System.nanoTime() - t0 >= TimeUnit.MILLISECONDS.toNanos(40), "closed after the stall");
                    }
                }
            }
        }
    }

    /**
     * A TLS client presents a bound credential after the handshake and before its first
     * record (Zmux.connectTls and ZmuxListener.connectTls, from the configuration); the
     * admission sees the server name the client sent.
     */
    @Test void tlsDialersPresentTheConfiguredCredential() throws Exception {
        var auth = new PrefixAuth(PSK, Duration.ofSeconds(30), true, 1, 0);
        var verifier = auth.verifier();
        var seen = new LinkedBlockingQueue<String>();
        try (EventLoops loops = EventLoops.create(2)) {
            var multipath = MultipathConfig.defaults().withMaxPipes(4).withAuth(PipeAuth.insecure());
            var cfg = ZmuxConfig.builder().eventLoops(loops).multipath(multipath).build();
            var dialer = ZmuxConfig.builder().eventLoops(loops).multipath(multipath).credential(auth).build();
            var lc = ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server()))
                    .admission(Admission.of(PrefixAuth.TOKEN_LENGTH, Admission.Decider.immediate((info, prefix) -> {
                        seen.add(info.serverName() + " " + info.tls() + " " + info.head() + " " + info.header(PrefixAuth.HEADER_FIELD));
                        return verifier.decide(info, prefix);
                    })).withPeekTimeout(Duration.ofSeconds(5))).build();
            var tls = TlsOptions.client(TestTls.client(), "localhost");
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), lc);
                 var endpoint = Zmux.listener(ListenerConfig.builder().session(dialer).build())) {
                try (var c = Zmux.connectTls(l.localAddress(), tls, dialer); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s, "accepted with the credential");
                    assertEquals("localhost true false []", seen.poll(1, TimeUnit.SECONDS));
                    assertTrue(c.paths().addTls(l.localAddress(), tls, PipeOptions.defaults()).id() > 0,
                            "a session's own TLS pipe presents the credential too");
                    assertEquals("localhost true false []", seen.poll(1, TimeUnit.SECONDS));
                }
                try (var c = endpoint.connectTls(l.localAddress(), tls); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s, "a listener's dialer presents its sessions' credential");
                    assertEquals("localhost true false []", seen.poll(1, TimeUnit.SECONDS));
                }
                try (var c = endpoint.connect(new InetSocketAddress("127.0.0.1", l.localAddress().getPort()))) {
                    fail("a bound credential on a carrier without TLS fails the dial");
                } catch (IOException expected) {
                    assertTrue(expected.getMessage().contains("exporter"), expected.toString());
                }
            }
        }
    }

    /**
     * A carrier given to the listener with an HTTP request decided on its head: the admission
     * runs at once, without reading a prefix, sees the request's header fields and the
     * carrier's server name, and the header form of the token admits it.
     */
    @Test void carriersDecidedOnTheirRequestHeadCarryTheTokenInAHeader() throws Exception {
        var auth = new PrefixAuth(PSK, Duration.ofSeconds(30), false, 1, 0);
        var verifier = auth.verifier();
        var seen = new LinkedBlockingQueue<String>();
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var lc = ListenerConfig.builder().session(cfg)
                    .admission(Admission.of(PrefixAuth.TOKEN_LENGTH, Admission.Decider.immediate((info, prefix) -> {
                        seen.add(info.source().getHostString() + " " + info.serverName() + " " + info.head() + " " + prefix.remaining()
                                + " " + info.header("x-app"));
                        return verifier.decide(info, prefix);
                    })).withPeekTimeout(Duration.ofSeconds(30))).build();
            var client = new Admission.CarrierInfo(TestAddr.peer("server"), TestAddr.peer("server"), "", "", null, RequestHead.NONE);
            try (var l = Zmux.listener(lc)) {
                var header = auth.header(client);
                var request = RequestHead.of(java.util.Map.of(header.name(), java.util.List.of(header.value()),
                        "X-App", java.util.List.of("1")), true);
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var admitted = l.handle(pair[1].withServerName("example.com"), JoinHint.of(request, false, TestAddr.peer("peer")), null);
                // Decided before the client writes anything: a prefix read would wait out the peek timeout.
                assertEquals(Admission.Verdict.ACCEPT, admitted.verdict().toCompletableFuture().get(5, TimeUnit.SECONDS));
                assertEquals("peer example.com true 0 [1]", seen.poll(1, TimeUnit.SECONDS));
                try (var c = Zmux.open(pair[0], Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s);
                }
                // The same token again, and none at all: both stall at once.
                var again = MemoryPipe.pair(Framing.BYTE_STREAM);
                assertEquals(Admission.Verdict.STALL, l.handle(again[1], JoinHint.of(request, false, TestAddr.peer("peer")), null)
                        .verdict().toCompletableFuture().get(5, TimeUnit.SECONDS));
                var none = MemoryPipe.pair(Framing.BYTE_STREAM);
                assertEquals(Admission.Verdict.STALL, l.handle(none[1], JoinHint.of(RequestHead.of(java.util.Map.of(), true), false, TestAddr.peer("peer")), null)
                        .verdict().toCompletableFuture().get(5, TimeUnit.SECONDS));
                for (var p : new PipeIo[] {again[0], none[0]}) { p.reader().close(); p.writer().abort(new IOException("done")); }
            }
        }
    }

    @Test void joinHintsOfARequestTakeTheJoinAndPairingFields() {
        var r = RequestHead.of(java.util.Map.of("zmux-pipe-join", java.util.List.of("j"), "Zmux-Pipe-Pair", java.util.List.of("p")), false);
        var h = JoinHint.of(r, true, TestAddr.peer("s"));
        assertEquals("j", h.join());
        assertEquals("p", h.pairing());
        assertTrue(h.uplink());
        assertSame(r, h.request());
        var twice = RequestHead.of(java.util.Map.of("Zmux-Pipe-Join", java.util.List.of("a", "b")), false);
        assertNull(io.zmux.internal.core.CarrierAdmission.decodeJoinHeader(JoinHint.of(twice, false, TestAddr.peer("s")).join()),
                "a repeated join field does not decode");
        assertEquals(RequestHead.NONE, JoinHint.inBand(TestAddr.peer("s")).request());
    }

    /** A pipe whose first write starts with prefix: the token and the first record arrive in one chunk. */
    static PipeIo prefixed(PipeIo io, byte[] prefix) {
        var first = new AtomicBoolean(true);
        PipeWriter w = io.writer();
        PipeWriter out = new PipeWriter() {
            @Override public void write(OutboundBatch batch, Completion done) {
                if (!first.getAndSet(false)) { w.write(batch, done); return; }
                var parts = new java.util.ArrayList<ByteBuffer>();
                parts.add(ByteBuffer.wrap(prefix));
                for (int i = 0; i < batch.segmentCount(); i++) parts.add(batch.segment(i));
                int n = 0;
                for (ByteBuffer b : parts) n += b.remaining();
                ByteBuffer all = ByteBuffer.allocate(n);
                for (ByteBuffer b : parts) all.put(b.duplicate());
                w.write(OutboundBatch.of(all.flip()), done);
            }
            @Override public void close() { w.close(); }
            @Override public void abort(Throwable cause) { w.abort(cause); }
        };
        return new PipeIo(io.reader(), out, io.framing(), io.label(), io.carrierConn(), io.capabilities());
    }

    /**
     * An accept that skips a token whose carrier's first record arrives in the same delivery:
     * the join that record starts moves the carrier to its session without the skipped bytes.
     */
    @Test void aJoinRightAfterTheSkippedTokenReachesItsSessionWithoutTheToken() throws Exception {
        var auth = new PrefixAuth(PSK, Duration.ofSeconds(30), false, 1, 0);
        for (int loopsCount : new int[] {1, 4}) {
            try (EventLoops loops = EventLoops.create(loopsCount)) {
                var cfg = ZmuxConfig.builder().eventLoops(loops)
                        .multipath(MultipathConfig.defaults().withMaxPipes(8).withAuth(PipeAuth.insecure())).build();
                var lc = ListenerConfig.builder().session(cfg)
                        .admission(Admission.of(PrefixAuth.TOKEN_LENGTH, auth.verifier()).withPeekTimeout(Duration.ofSeconds(5))).build();
                var plain = new Admission.CarrierInfo(TestAddr.peer("server"), TestAddr.peer("server"), "", "", null, RequestHead.NONE);
                try (var l = Zmux.listener(lc)) {
                    var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
                    l.handle(boot[1], JoinHint.inBand(TestAddr.peer("peer"))).closed();
                    try (var c = Zmux.open(prefixed(boot[0], auth.prefix(plain)), Role.CLIENT, cfg);
                         var s = l.accept(Duration.ofSeconds(5))) {
                        assertNotNull(s);
                        for (int i = 0; i < 6; i++) {
                            var pipe = MemoryPipe.pair(Framing.BYTE_STREAM);
                            l.handle(pipe[1], JoinHint.inBand(TestAddr.peer("peer"))).closed();
                            assertTrue(c.paths().add(prefixed(pipe[0], auth.prefix(plain)), PipeOptions.defaults()).id() > 0);
                        }
                        try (var st = c.openStream()) {
                            st.write(new byte[] {1, 2, 3});
                            st.finish();
                            try (var in = s.acceptStream(Duration.ofSeconds(5))) { assertArrayEquals(new byte[] {1, 2, 3}, in.readAll(16)); }
                        }
                    }
                }
            }
        }
    }

    /** A pipe input given to l from source after writing prefix; ended completes when l closes it. */
    static CompletableFuture<Void> silentCarrier(ZmuxListener l, String source, byte[] prefix) {
        var pipe = MemoryPipe.pair(Framing.BYTE_STREAM);
        var ended = new CompletableFuture<Void>();
        pipe[0].reader().start(new PipeReader.Sink() {
            @Override public void onData(ByteBuffer data, Runnable release) { release.run(); }
            @Override public void onEnd() { ended.complete(null); }
            @Override public void onError(Throwable cause) { ended.complete(null); }
        });
        pipe[0].writer().write(io.zmux.carrier.OutboundBatch.of(ByteBuffer.wrap(prefix)), new PipeWriter.Completion() {
            @Override public void completed() {}
            @Override public void failed(Throwable cause) {}
        });
        l.handle(pipe[1], JoinHint.inBand(TestAddr.literal(source)));
        return ended;
    }

    /**
     * A carrier accepted with prefix bytes left over waits for its first record counted against
     * its own source: under one pending carrier per source, silent carriers from two hosts both
     * wait, a second from the same host (an IPv6 one: from the same /64) is closed.
     */
    @Test void anAcceptedCarrierWithBytesLeftCountsAgainstItsOwnSource() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var lc = ListenerConfig.builder().session(cfg).maxPendingPerSource(1).firstRecordTimeout(Duration.ofSeconds(10))
                    .admission(Admission.of(2, Admission.Decider.immediate((info, prefix) -> Admission.Decision.accept(1))).withPeekTimeout(Duration.ofSeconds(5))).build();
            try (var l = Zmux.listener(lc)) {
                byte[] prefix = {0, 0x40}; // the leftover byte starts a record
                var one = silentCarrier(l, "192.0.2.1", prefix);
                var two = silentCarrier(l, "192.0.2.2", prefix);
                Thread.sleep(300);
                assertFalse(one.isDone(), "the first host's carrier waits");
                assertFalse(two.isDone(), "the second host's carrier waits");
                var three = silentCarrier(l, "192.0.2.1", prefix);
                three.get(5, TimeUnit.SECONDS); // beyond the first host's limit
                assertFalse(one.isDone());
                // An IPv6 client counts by its /64.
                var v6 = silentCarrier(l, "2001:db8::1", prefix);
                Thread.sleep(300);
                assertFalse(v6.isDone());
                silentCarrier(l, "2001:db8::2", prefix).get(5, TimeUnit.SECONDS);
                assertFalse(v6.isDone());
            }
        }
    }
}
