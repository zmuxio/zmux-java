package io.zmux;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.*;
import io.zmux.adapter.Handled;
import io.zmux.internal.loop.AdmissionCarrier;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class AdmissionTest {
    private static final Duration DEADLINE = Duration.ofMinutes(5);
    private static final long WATCHDOG_SECONDS = 20;
    static final Duration SLOT = Duration.ofSeconds(30);
    static final PrefixStore.Id TICKET_ID = new PrefixStore.Id(1, 2);
    static final byte[] TICKET_KEY = filled(32, 0x6b);
    static final PrefixStore.Generation GENERATION = new PrefixStore.Generation(filled(32, 0x3c), 0, -1);

    static byte[] filled(int n, int v) { byte[] b = new byte[n]; Arrays.fill(b, (byte) v); return b; }

    private static PrefixAuth.CarrierOptions options(boolean bound) {
        return new PrefixAuth.CarrierOptions(bound, !bound);
    }

    private static PrefixAuth presenter(boolean bound) {
        return new PrefixAuth(options(bound), SLOT, TICKET_ID, TICKET_KEY, java.util.List.of(GENERATION));
    }

    private static PrefixAuth.Verifier verifier(boolean bound) {
        return verifier(bound, java.time.Clock.systemUTC());
    }

    private static PrefixAuth.Verifier verifier(boolean bound, java.time.Clock clock) {
        var store = new MemoryPrefixStore(new PrefixStore.Window(SLOT, 1), java.util.List.of(GENERATION));
        store.addTicket(new PrefixStore.Ticket(TICKET_ID, TICKET_KEY, 0, 0, -1, false));
        return new PrefixAuth.Verifier(options(bound), store, 2, clock);
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
                            sink.onLent(b, done::countDown);
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
        var auth = presenter(false);
        var stallFor = DEADLINE;
        try (EventLoops loops = EventLoops.create(1)) {
            var multipath = PipeAuth.psk(new byte[32]).withEndToEnd();
            var cfg = ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(multipath).build();
            var dialer = ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(multipath).carrierCredential(auth).build();
            var lc = ListenerConfig.builder().session(cfg)
                    .admission(verifier(false).admission().refusal(AdmissionVerdict.STALL).peekTimeout(DEADLINE)
                            .stall(stallFor, stallFor).build()).build();
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), lc)) {
                assertEquals(stallFor, l.adapter().stallDuration(new java.net.InetSocketAddress("127.0.0.1", 0)));
                try (var c = Zmux.connect(l.localAddress(), dialer); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s);
                    assertTrue(PrefixAuth.boundPrincipal(s).isEmpty());
                    assertTrue(c.addPipe(l.localAddress(), PipeOptions.defaults()).id().value() > 0, "a session's own TCP pipe presents the credential");
                    try (var st = c.openStream()) {
                        st.write(new byte[] {1, 2, 3});
                        st.finish();
                        try (var in = s.acceptStream(Duration.ofSeconds(5))) { assertArrayEquals(new byte[] {1, 2, 3}, in.readAll(16)); }
                    }
                }
                var denied = GlobalBoundTest.handle(l, "no-token");
                denied.sendZeros(PrefixAuth.TOKEN_LENGTH);
                assertEquals(AdmissionVerdict.STALL, denied.handled.verdict().get(WATCHDOG_SECONDS, TimeUnit.SECONDS));
                elapse(loops, stallFor);
                denied.handled.closed().get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
                assertEquals(0, denied.written.get(), "a stalled carrier sends nothing");
                assertThrows(java.net.SocketTimeoutException.class, () -> l.accept(Duration.ZERO));
            }
        }
    }

    @Test void verdictsCloseAcceptWithSkipAndBoundStallsPerSource() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            AtomicReference<Admission.Decision> verdict = new AtomicReference<>(Admission.Decision.CLOSE);
            var seen = new AtomicReference<String>();
            var lc = ListenerConfig.builder().session(cfg).maxPendingPerSource(2)
                    .admission(Admission.builder(4, Admission.Decider.immediate((info, prefix) -> {
                        seen.set(info.source().getHostString() + " " + info.tls() + " " + prefix.remaining());
                        return verdict.get();
                    })).peekTimeout(DEADLINE).stall(DEADLINE, DEADLINE).build()).build();
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), lc)) {
                try (SocketChannel raw = SocketChannel.open(l.localAddress())) {
                    raw.write(ByteBuffer.wrap(new byte[] {9, 9, 9, 9}));
                    raw.socket().setSoTimeout(3000);
                    assertEquals(-1, raw.socket().getInputStream().read());
                }
                assertEquals("127.0.0.1 false 4", seen.get());
                verdict.set(Admission.Decision.accept(4));
                SocketChannel ch = SocketChannel.open(l.localAddress());
                ch.write(ByteBuffer.wrap(new byte[] {7, 7, 7, 7}));
                try (var _ = Zmux.open(socketPipe(ch), Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s, "accepted after the skipped prefix");
                }
                verdict.set(Admission.Decision.STALL);
                var first = GlobalBoundTest.handle(l, "stall-source");
                first.sendZeros(4);
                assertEquals(AdmissionVerdict.STALL, first.handled.verdict().get(WATCHDOG_SECONDS, TimeUnit.SECONDS));
                var second = GlobalBoundTest.handle(l, "stall-source");
                second.sendZeros(4);
                second.handled.closed().get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
                assertFalse(first.isClosed(), "the incumbent keeps its per-source stall");
                assertEquals(1, l.stats().admission().stallOverflow());
            }
        }
    }

    @Test void closingTheListenerEndsItsStalls() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            // An explicit stall and a fallback without a target, both far longer than the test.
            for (var d : java.util.List.of(Admission.Decision.STALL, Admission.Decision.FALLBACK)) {
                var lc = ListenerConfig.builder().session(cfg)
                        .admission(Admission.builder(4, Admission.Decider.immediate((info, prefix) -> d))
                                .stall(Duration.ofMinutes(5), Duration.ofMinutes(5)).build()).build();
                var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), lc);
                GlobalBoundTest.Fake stalled;
                try (l) {
                    stalled = GlobalBoundTest.handle(l, "stall-source");
                    stalled.sendZeros(4);
                    assertEquals(AdmissionVerdict.STALL, stalled.handled.verdict().get(WATCHDOG_SECONDS, TimeUnit.SECONDS));
                    assertEquals(0, l.stats().admission().fallback());
                    assertFalse(stalled.isClosed());
                }
                stalled.handled.closed().get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
                assertEquals(0, stalled.written.get(), "the stalled carrier sent nothing");
            }
        }
    }

    /** A fallback target that collects the input until it ends, then writes it back and closes both halves; done then. */
    static Admission.Fallback echo(BlockingQueue<String> sources) {
        return t -> {
            var carrier = t.pipe();
            var done = new java.util.concurrent.CompletableFuture<Void>();
            sources.add(t.info().source().getHostString());
            var got = new java.io.ByteArrayOutputStream();
            carrier.reader().start(new PipeReader.Sink() {
                @Override public void onChunk(io.zmux.Chunk chunk, Runnable consumed) { Runnable release = () -> { chunk.close(); consumed.run(); };
                    got.writeBytes(chunk.copy());
                    release.run();
                }
                @Override public void onEnd() {
                    carrier.reader().close();
                    carrier.writer().write(io.zmux.carrier.OutboundBatch.of(ByteBuffer.wrap(got.toByteArray())), new PipeWriter.Completion() {
                        @Override public void completed() { carrier.writer().close(); done.complete(null); }
                        @Override public void failed(Throwable cause) { done.completeExceptionally(cause); }
                    });
                }
                @Override public void onError(Throwable cause) { done.completeExceptionally(cause); }
            });
            return done;
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
                    .admission(Admission.builder(4, Admission.Decider.immediate((info, prefix) -> Admission.Decision.FALLBACK))
                            .peekTimeout(DEADLINE).build()).build();
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), lc)) {
                // The whole prefix at once: decided on live input.
                try (SocketChannel ch = SocketChannel.open(l.localAddress())) {
                    ch.write(ByteBuffer.wrap("abcdefgh".getBytes(UTF_8)));
                    ch.shutdownOutput();
                    assertEquals("abcdefgh", readAll(ch));
                }
                // The input ends within the peek: decided at its end, the bytes still replayed.
                try (SocketChannel ch = SocketChannel.open(l.localAddress())) {
                    ch.write(ByteBuffer.wrap("xy".getBytes(UTF_8)));
                    ch.shutdownOutput();
                    assertEquals("xy", readAll(ch));
                }
                assertEquals("127.0.0.1", sources.poll(1, TimeUnit.SECONDS));
                assertThrows(java.net.SocketTimeoutException.class, () -> l.accept(Duration.ZERO));
            }
        }
    }

    private record Peek(AdmissionCarrier carrier, Handled handled) {
        static Peek open(ZmuxListener listener) {
            var carrier = new AdmissionCarrier(listener.config().eventLoops().next());
            return new Peek(carrier, listener.adapter().handle(carrier.pipe(), JoinHint.inBand(TestAddr.peer("peer"))));
        }

        void feed(String bytes) throws Exception { carrier.feed(bytes.getBytes(UTF_8)); }

        void closedAs(AdmissionVerdict verdict) throws Exception {
            assertEquals(verdict, handled.verdict().get(WATCHDOG_SECONDS, TimeUnit.SECONDS));
            handled.closed().get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
        }
    }

    private static void elapse(EventLoops loops, Duration interval) throws Exception {
        loops.next().advanceTimersForTests(interval.toNanos()).get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
    }

    private static void afterFallback(Peek input, BlockingQueue<String> started, String rest) throws Exception {
        assertNotNull(started.poll(WATCHDOG_SECONDS, TimeUnit.SECONDS));
        input.carrier().awaitResume();
        input.feed(rest);
        input.carrier().eof();
        input.closedAs(AdmissionVerdict.FALLBACK);
        assertEquals("abcdefgh", new String(input.carrier().written(), UTF_8));
    }

    @Test void aLatePrefixTakesTheFallbackWithItsBytesWhenAskedTo() throws Exception {
        var sources = new LinkedBlockingQueue<String>();
        var decided = new AtomicInteger();
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            Admission.Decider closing = Admission.Decider.immediate((info, prefix) -> {
                decided.incrementAndGet();
                return Admission.Decision.CLOSE;
            });
            var on = Admission.builder(4, closing).peekTimeout(DEADLINE).fallbackOnPeekTimeout(true).build();
            try (var l = Zmux.listener(ListenerConfig.builder().session(cfg).fallback(echo(sources)).admission(on).build())) {
                var input = Peek.open(l);
                input.feed("ab");
                elapse(loops, DEADLINE);
                afterFallback(input, sources, "cdefgh");
                assertEquals(0, decided.get(), "the decider never saw the late prefix");
                assertEquals(new AdmissionStats(0, 1, 0, 0, 0, 0, 0, 0, 0), l.stats().admission());
            }
            try (var l = Zmux.listener(ListenerConfig.builder().session(cfg).fallback(echo(sources)).admission(on).build())) {
                var input = Peek.open(l);
                input.feed("abcd");
                input.closedAs(AdmissionVerdict.CLOSE);
                assertEquals(1, decided.get());
                assertEquals(1, l.stats().admission().closed());
            }
            var off = Admission.builder(4, closing).peekTimeout(DEADLINE).build();
            try (var l = Zmux.listener(ListenerConfig.builder().session(cfg).fallback(echo(sources)).admission(off).build())) {
                var input = Peek.open(l);
                input.feed("ab");
                elapse(loops, DEADLINE);
                input.closedAs(AdmissionVerdict.CLOSE);
                assertEquals(1, decided.get());
                assertEquals(1, l.stats().admission().peekTimedOut());
                assertEquals(0, l.stats().admission().fallback());
            }
        }
    }

    @Test void aLateMoreRoundTakesTheFallbackToo() throws Exception {
        var sources = new LinkedBlockingQueue<String>();
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var adm = Admission.builder(4, Admission.Decider.immediate((info, prefix) ->
                            new Admission.Decision(AdmissionVerdict.MORE, 0, null, 8, "partial")))
                    .maxPeek(8).peekTimeout(DEADLINE).fallbackOnPeekTimeout(true).build();
            try (var l = Zmux.listener(ListenerConfig.builder().session(cfg).fallback(echo(sources)).admission(adm).build())) {
                var input = Peek.open(l);
                input.feed("abcd");
                input.carrier().awaitResume();
                elapse(loops, DEADLINE);
                afterFallback(input, sources, "efgh");
                assertEquals(java.util.List.of(new Admission.VariantStat("partial", false, 0, 1, 0, 0, 0)), l.variantStats());
                assertEquals(new AdmissionStats(0, 1, 0, 0, 0, 0, 0, 0, 0), l.stats().admission());
            }
        }
    }

    @Test void aLatePrefixWithoutATargetUsesOnlyTheStallOutcome() throws Exception {
        try (var loops = EventLoops.create(1)) {
            var adm = Admission.builder(1, Admission.Decider.immediate((info, prefix) -> { throw new AssertionError("no input"); }))
                    .peekTimeout(DEADLINE).fallbackOnPeekTimeout(true).build();
            var cfg = ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).build())
                    .admission(adm).maxPending(1).build();
            try (var l = Zmux.listener(cfg)) {
                var input = Peek.open(l);
                elapse(loops, DEADLINE);
                input.closedAs(AdmissionVerdict.CLOSE);
                assertEquals(new AdmissionStats(0, 0, 0, 0, 0, 1, 0, 0, 0), l.stats().admission());
                assertTrue(l.variantStats().isEmpty());
            }
        }
    }

    @Test void knownJoinTellsAHelloLocatorFromItsHead() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ListenerEndpointTest.config(loops);
            try (var a = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var b = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var _ = a.connect(b.localAddress()); var server = b.accept(Duration.ofSeconds(3))) {
                byte[] known = (byte[]) server.run(op -> server.core.locator().clone());
                byte[] other = known.clone();
                other[0] ^= 1;
                byte[] head = helloHead(known, 1);
                int total = head.length;
                assertEquals(new io.zmux.adapter.JoinLookup(true, 0), b.adapter().knownJoin(ByteBuffer.wrap(head)));
                assertEquals(new io.zmux.adapter.JoinLookup(true, 0), b.adapter().knownJoin(ByteBuffer.wrap(Arrays.copyOf(head, total + 5)).limit(total + 5)), "bytes after the head");
                assertEquals(new io.zmux.adapter.JoinLookup(false, 0), a.adapter().knownJoin(ByteBuffer.wrap(head)), "another listener's session");
                assertEquals(new io.zmux.adapter.JoinLookup(false, 0), b.adapter().knownJoin(ByteBuffer.wrap(helloHead(other, 1))));
                assertEquals(new io.zmux.adapter.JoinLookup(false, 0), b.adapter().knownJoin(ByteBuffer.wrap(helloHead(known, 99))), "unknown suite");
                assertEquals(new io.zmux.adapter.JoinLookup(false, 0), b.adapter().knownJoin(ByteBuffer.allocate(0)));
                assertEquals(new io.zmux.adapter.JoinLookup(false, 0), b.adapter().knownJoin(ByteBuffer.wrap(new byte[] {(byte) 0x80, 1, 2})), "not a join");
                // A short prefix names the length that can make progress, and that prefix then answers.
                var cut = b.adapter().knownJoin(ByteBuffer.wrap(head, 0, 3));
                assertFalse(cut.known());
                assertTrue(cut.need() > 3 && cut.need() <= total);
                var more = b.adapter().knownJoin(ByteBuffer.wrap(head, 0, total - 1));
                assertEquals(new io.zmux.adapter.JoinLookup(false, total), more);
            }
        }
    }

    /** The clear head of a Join Hello: profile, suite, mode, locator and a nonce of 32 bytes. */
    private static byte[] helloHead(byte[] locator, int suite) {
        byte[] b = new byte[3 + 1 + locator.length + 32];
        b[0] = 2;
        b[1] = (byte) suite;
        b[2] = 0;
        b[3] = (byte) locator.length;
        System.arraycopy(locator, 0, b, 4, locator.length);
        return b;
    }

    @Test void anIncompleteExternalOrRequestReaderClosesOnPeekTimeout() throws Exception {
        var calls = new AtomicInteger();
        var routed = new AtomicInteger();
        try (var loops = EventLoops.create(1)) {
            var admission = Admission.builder(8, Admission.Decider.immediate((i, p) -> {
                calls.incrementAndGet();
                return Admission.Decision.accept(0);
            })).fallbackOnPeekTimeout(true).peekTimeout(DEADLINE).build();
            var cfg = ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).build())
                    .admission(admission).fallback(t -> { routed.incrementAndGet(); return null; }).build();
            try (var listener = Zmux.listener(cfg)) {
                var port = listener.adapter();
                var source = TestAddr.peer("input");
                for (boolean request : new boolean[] {false, true}) {
                    var pipe = io.zmux.carrier.MemoryPipe.pair(io.zmux.carrier.Framing.BYTE_STREAM)[1];
                    var h = request ? port.handleRequest(pipe, JoinHint.of(RequestHead.of(java.util.Map.of()), false, source),
                            false, t -> { routed.incrementAndGet(); return null; }, () -> CompletableFuture.completedFuture(null))
                            : port.handle(pipe, JoinHint.inBand(source));
                    elapse(loops, DEADLINE);
                    h.closed().get(WATCHDOG_SECONDS, TimeUnit.SECONDS);
                }
                assertEquals(2, listener.stats().admission().peekTimedOut());
                assertEquals(0, calls.get());
                assertEquals(0, routed.get());
            }
        }
    }

    @Test void carriersGivenToTheListenerPassTheAdmission() throws Exception {
        var sources = new LinkedBlockingQueue<String>();
        try (EventLoops loops = EventLoops.create(2);
             var server = java.nio.channels.ServerSocketChannel.open().bind(new InetSocketAddress("127.0.0.1", 0))) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var adm = Admission.builder(4, Admission.Decider.immediate((info, prefix) -> {
                byte b = prefix.get(0);
                return b == 7 ? Admission.Decision.accept(4) : b == 8 ? Admission.Decision.FALLBACK : Admission.Decision.CLOSE;
            })).peekTimeout(Duration.ofSeconds(2)).build();
            var lc = ListenerConfig.builder().session(cfg).admission(adm).fallback(echo(sources)).build();
            try (var l = Zmux.listener(lc); var bare = Zmux.listener(ListenerConfig.builder().session(cfg).admission(adm).build())) {
                SocketChannel ch = SocketChannel.open(server.getLocalAddress());
                ch.write(ByteBuffer.wrap(new byte[] {7, 7, 7, 7}));
                var admitted = l.adapter().handle(socketPipe(server.accept()), JoinHint.inBand(TestAddr.peer("peer")));
                try (var _ = Zmux.open(socketPipe(ch), Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s);
                    assertEquals(AdmissionVerdict.ACCEPT, admitted.verdict().toCompletableFuture().get(5, TimeUnit.SECONDS));
                }
                try (SocketChannel raw = SocketChannel.open(server.getLocalAddress())) {
                    raw.write(ByteBuffer.wrap(new byte[] {8, 'a', 'b', 'c', 'd'}));
                    raw.shutdownOutput();
                    var fb = l.adapter().handle(socketPipe(server.accept()), JoinHint.inBand(TestAddr.peer("peer")));
                    assertEquals(AdmissionVerdict.FALLBACK, fb.verdict().toCompletableFuture().get(5, TimeUnit.SECONDS));
                    assertEquals(new String(new byte[] {8, 'a', 'b', 'c', 'd'}, java.nio.charset.StandardCharsets.US_ASCII), readAll(raw));
                    assertEquals("peer", sources.poll(1, TimeUnit.SECONDS));
                    fb.closed().toCompletableFuture().get(5, TimeUnit.SECONDS);
                }
                try (SocketChannel raw = SocketChannel.open(server.getLocalAddress())) {
                    raw.write(ByteBuffer.wrap(new byte[] {8, 0, 0, 0}));
                    var none = bare.adapter().handle(socketPipe(server.accept()), JoinHint.inBand(TestAddr.peer("peer")));
                    assertEquals(AdmissionVerdict.STALL, none.verdict().toCompletableFuture().get(5, TimeUnit.SECONDS),
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
                            sink.onLent(ByteBuffer.wrap(b.clone(), 0, n), done::countDown);
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
            try (var _ = Zmux.open(PipeIo.of(r, w), Role.CLIENT, cfg)) { } catch (Exception ignored) { }
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

    static byte[] boundToken(PrefixAuth auth, javax.net.ssl.SSLSocket sock) throws Exception {
        var session = (javax.net.ssl.ExtendedSSLSession) sock.getSession();
        var info = new Admission.CarrierInfo(null, null, "", "", (label, context, length) -> {
            try { return session.exportKeyingMaterialData(label, context, length); }
            catch (javax.net.ssl.SSLException e) { return null; }
        }, RequestHead.NONE).withOuterSecurity(Admission.OuterSecurity.tls(session.getProtocol()));
        return auth.prefix(info);
    }

    /**
     * A TLS listener with bound prefix tokens: a valid carrier is accepted; a replayed token,
     * another carrier's token, a truncated one, random bytes and a first record without a
     * token all reach the fallback with exactly the bytes sent, and nothing else is written;
     * without a fallback handler they close silently.
     */
    @Test void invalidPrefixesTakeTheFallbackPathAlike() throws Exception {
        var auth = presenter(true);
        var got = new LinkedBlockingQueue<byte[]>();
        Admission.Fallback fallback = t -> {
            var carrier = t.pipe();
            var done = new java.util.concurrent.CompletableFuture<Void>();
            var all = new java.io.ByteArrayOutputStream();
            carrier.reader().start(new PipeReader.Sink() {
                @Override public void onChunk(io.zmux.Chunk chunk, Runnable consumed) { Runnable release = () -> { chunk.close(); consumed.run(); };
                    all.writeBytes(chunk.copy()); release.run();
                }
                @Override public void onEnd() {
                    got.add(all.toByteArray());
                    carrier.reader().close();
                    carrier.writer().write(io.zmux.carrier.OutboundBatch.of(ByteBuffer.wrap("fallback".getBytes(UTF_8))), new PipeWriter.Completion() {
                        @Override public void completed() { carrier.writer().close(); done.complete(null); }
                        @Override public void failed(Throwable cause) { done.completeExceptionally(cause); }
                    });
                }
                @Override public void onError(Throwable cause) { got.add(all.toByteArray()); done.completeExceptionally(cause); }
            });
            return done;
        };
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var adm = verifier(true).admission().refusal(AdmissionVerdict.FALLBACK).peekTimeout(DEADLINE).build();
            var base = ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server())).admission(adm);
            byte[] rec = firstRecord(cfg);
            byte[] random = filled(PrefixAuth.TOKEN_LENGTH, 0x93);
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), base.fallback(fallback).build())) {
                var sock = dialTls(l.localAddress());
                byte[] tok = boundToken(auth, sock);
                sock.getOutputStream().write(tok);
                try (var _ = Zmux.open(streamPipe(sock), Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s, "a valid carrier is accepted");
                }
                byte[] valid;
                try (var other = dialTls(l.localAddress())) { valid = boundToken(auth, other); }
                assertArrayEquals(new byte[0], got.poll(5, TimeUnit.SECONDS), "a carrier closed without bytes");
                byte[][] cases = {tok, valid, Arrays.copyOf(valid, PrefixAuth.TOKEN_LENGTH / 2), random, rec};
                String[] names = {"replayed", "other carrier", "truncated", "random", "record"};
                for (int i = 0; i < cases.length; i++) {
                    try (var c = dialTls(l.localAddress())) {
                        c.getOutputStream().write(cases[i]);
                        c.shutdownOutput();
                        assertEquals("fallback", new String(c.getInputStream().readAllBytes(), UTF_8), names[i]);
                    }
                    assertArrayEquals(cases[i], got.poll(5, TimeUnit.SECONDS), names[i]);
                }
            }
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), base.fallback(null).build())) {
                byte[] tok;
                try (var other = dialTls(l.localAddress())) { tok = boundToken(auth, other); }
                byte[][] cases = {tok, Arrays.copyOf(tok, PrefixAuth.TOKEN_LENGTH / 2), random, rec};
                for (byte[] x : cases) {
                    try (var c = dialTls(l.localAddress())) {
                        c.getOutputStream().write(x);
                        c.shutdownOutput();
                        assertSendsNothing(c);
                    }
                }
            }
        }
    }

    /**
     * A refused carrier is closed without a byte toward the client. The close can find bytes of the client unread
     * (its close_notify after the prefix), and the host then resets the connection instead of ending it: either is
     * nothing sent.
     */
    private static void assertSendsNothing(java.net.Socket c) {
        try {
            assertEquals(-1, c.getInputStream().read(), "a refused carrier sends nothing");
        } catch (java.net.SocketException reset) {
            // a reset carries no data either
        } catch (java.io.IOException e) {
            fail("a refused carrier: " + e);
        }
    }

    /**
     * A TLS client presents a bound credential after the handshake and before its first
     * record (Zmux.connectTls and ZmuxListener.connectTls, from the configuration); the
     * admission sees the server name the client sent.
     */
    @Test void tlsDialersPresentTheConfiguredCredential() throws Exception {
        var auth = presenter(true);
        var verifier = verifier(true);
        var seen = new LinkedBlockingQueue<String>();
        try (EventLoops loops = EventLoops.create(2)) {
            var multipath = PipeAuth.psk(new byte[32]).withEndToEnd();
            var cfg = ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(multipath).build();
            var dialer = ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(multipath).carrierCredential(auth).build();
            var lc = ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server()))
                    .admission(Admission.builder(PrefixAuth.TOKEN_LENGTH, (info, prefix, deadline) -> {
                        seen.add(info.serverName() + " " + info.tls() + " " + (info.stage() == Admission.Stage.HEAD) + " " + info.header(PrefixAuth.HEADER_FIELD));
                        return verifier.decide(info, prefix, deadline);
                    }).peekTimeout(Duration.ofSeconds(5)).build()).build();
            var tls = TlsOptions.client(TestTls.client(), "localhost");
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), lc);
                 var endpoint = Zmux.listener(ListenerConfig.builder().session(dialer).build())) {
                try (var c = Zmux.connectTls(l.localAddress(), tls, dialer); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s, "accepted with the credential");
                    assertEquals(TICKET_ID, PrefixAuth.boundPrincipal(s).orElseThrow());
                    assertEquals("localhost true false []", seen.poll(1, TimeUnit.SECONDS));
                    assertTrue(c.addTlsPipe(l.localAddress(), tls, PipeOptions.defaults()).id().value() > 0,
                            "a session's own TLS pipe presents the credential too");
                    assertEquals("localhost true false []", seen.poll(1, TimeUnit.SECONDS));
                }
                try (var _ = endpoint.connectTls(l.localAddress(), tls); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s, "a listener's dialer presents its sessions' credential");
                    assertEquals("localhost true false []", seen.poll(1, TimeUnit.SECONDS));
                }
                try (var _ = endpoint.connect(new InetSocketAddress("127.0.0.1", l.localAddress().getPort()))) {
                    fail("a bound credential on a carrier without TLS fails the dial");
                } catch (IOException expected) {
                    assertTrue(expected.getMessage().contains("prefix authentication"), expected.toString());
                }
            }
        }
    }

    @Test void carrierKindCannotAuthorizeAnExchangeEnvelope() throws Exception {
        var cfg = ListenerConfig.builder().admission(verifier(false).admission().build()).build();
        try (var listener = Zmux.listener(cfg)) {
            var carriers = MemoryPipe.pair(Framing.BYTE_STREAM);
            var tagged = carriers[1].withCarrier(CarrierKind.EXCHANGE)
                    .withOuterSecurity(Admission.OuterSecurity.ENCRYPTED);
            var hint = JoinHint.inBand(TestAddr.peer("client"));
            var handled = listener.adapter().handleRequest(tagged, hint, true, null,
                    () -> { throw new AssertionError("unexpected takeover"); });
            assertEquals(AdmissionVerdict.CLOSE, handled.verdict().get(5, TimeUnit.SECONDS));
            carriers[0].reader().close();
            carriers[0].writer().close();
        }
    }

    @Test void boundTicketAndInnerPeerSurviveEstablishTogether() throws Exception {
        var auth = presenter(true);
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var server = io.zmux.seal.ServerConfig.builder(SealTest.gate(null), SealTest.NODE).build();
            var options = ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server()))
                    .admission(verifier(true).admission().build()).inboundTransform(io.zmux.seal.Seal.server(server)).build();
            try (var listener = Zmux.listen(NetworkTest.any(), options); var socket = dialTls(listener.localAddress())) {
                socket.getOutputStream().write(boundToken(auth, socket));
                var established = SealTest.establish(streamPipe(socket), SealTest.client().allowedProfiles(io.zmux.seal.Profile.XXHFS_PSK0_CHACHA).build());
                try (var _ = Zmux.open(established, Role.CLIENT, cfg); var accepted = listener.accept(Duration.ofSeconds(5))) {
                    assertEquals(TICKET_ID, PrefixAuth.boundPrincipal(accepted).orElseThrow());
                    assertEquals(io.zmux.seal.Profile.XXHFS_PSK0_CHACHA, accepted.admitted(io.zmux.seal.Peer.class).orElseThrow().profile());
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
        var auth = presenter(false);
        var verifier = verifier(false);
        var seen = new LinkedBlockingQueue<String>();
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var lc = ListenerConfig.builder().session(cfg)
                    .admission(Admission.builder(PrefixAuth.TOKEN_LENGTH, (info, prefix, deadline) -> {
                        seen.add(info.source().getHostString() + " " + info.serverName() + " " + (info.stage() == Admission.Stage.HEAD) + " " + prefix.remaining()
                                + " " + info.header("x-app"));
                        return verifier.decide(info, prefix, deadline);
                    }).peekTimeout(Duration.ofSeconds(30)).build()).build();
            var client = new Admission.CarrierInfo(TestAddr.peer("server"), TestAddr.peer("server"), "", "", null, RequestHead.NONE)
                    .withOuterSecurity(Admission.OuterSecurity.ENCRYPTED);
            try (var l = Zmux.listener(lc)) {
                var header = auth.header(client);
                var request = RequestHead.of(java.util.Map.of(header.name(), java.util.List.of(header.value()),
                        "X-App", java.util.List.of("1")));
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var admitted = l.adapter().handleRequest(pair[1].withOuterSecurity(Admission.OuterSecurity.ENCRYPTED).withServerName("example.com"), JoinHint.of(request, false, TestAddr.peer("peer")), true, null, () -> { throw new AssertionError("unexpected acceptance head"); });
                // Decided before the client writes anything: a prefix read would wait out the peek timeout.
                assertEquals(AdmissionVerdict.ACCEPT, admitted.verdict().toCompletableFuture().get(5, TimeUnit.SECONDS));
                assertEquals("peer example.com true 0 [1]", seen.poll(1, TimeUnit.SECONDS));
                try (var _ = Zmux.open(pair[0], Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(5))) {
                    assertNotNull(s);
                }
                // The same token again, and none at all: both close.
                var again = MemoryPipe.pair(Framing.BYTE_STREAM);
                assertEquals(AdmissionVerdict.CLOSE, l.adapter().handleRequest(again[1].withOuterSecurity(Admission.OuterSecurity.ENCRYPTED), JoinHint.of(request, false, TestAddr.peer("peer")), true, null, () -> { throw new AssertionError("unexpected acceptance head"); })
                        .verdict().toCompletableFuture().get(5, TimeUnit.SECONDS));
                var none = MemoryPipe.pair(Framing.BYTE_STREAM);
                assertEquals(AdmissionVerdict.CLOSE, l.adapter().handleRequest(none[1], JoinHint.of(RequestHead.of(java.util.Map.of()), false, TestAddr.peer("peer")), true, null, () -> { throw new AssertionError("unexpected acceptance head"); })
                        .verdict().toCompletableFuture().get(5, TimeUnit.SECONDS));
                for (var p : new PipeIo[] {again[0], none[0]}) { p.reader().close(); p.writer().abort(new IOException("done")); }
            }
        }
    }

    @Test void joinHintsOfARequestTakeThePairingField() {
        var r = RequestHead.of(java.util.Map.of("Zmux-Pipe-Pair", java.util.List.of("p")));
        var h = JoinHint.of(r, true, TestAddr.peer("s"));
        assertEquals("p", h.pairing());
        assertTrue(h.uplink());
        assertSame(r, h.request());
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
        return new PipeIo(io.reader(), out, io.framing(), io.label(), io.carrierConn()).withCapabilities(io.capabilities());
    }

    /**
     * An accept that skips a token whose carrier's first record arrives in the same delivery:
     * the join that record starts moves the carrier to its session without the skipped bytes.
     */
    @Test void aJoinRightAfterTheSkippedTokenReachesItsSessionWithoutTheToken() throws Exception {
        long nowSeconds = 120L;
        // The class timeout guards the test. Carrier timeouts must not race token and record delivery.
        var carrierTimeout = Duration.ofMinutes(5);
        for (int loopsCount : new int[] {1, 4}) {
            try (EventLoops loops = EventLoops.create(loopsCount)) {
                var clock = java.time.Clock.fixed(java.time.Instant.ofEpochSecond(nowSeconds), java.time.ZoneOffset.UTC);
                var verifier = verifier(false, clock);
                var cfg = ZmuxConfig.builder().eventLoops(loops)
                        .noHandshakeTimeout().joinTimeout(Duration.ofMinutes(1))
                        .sessionPolicy(new SessionPolicy(new CapacityLimits(8))).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).build();
                var lc = ListenerConfig.builder().session(cfg).firstRecordTimeout(carrierTimeout)
                        .admission(Admission.builder(PrefixAuth.TOKEN_LENGTH, verifier).peekTimeout(carrierTimeout).build()).build();
                try (var l = Zmux.listener(lc)) {
                    var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
                    byte[] token = PrefixAuth.token(GENERATION, TICKET_ID, TICKET_KEY, nowSeconds / SLOT.toSeconds(), new byte[0], new byte[16]);
                    l.adapter().handle(boot[1], JoinHint.inBand(TestAddr.peer("peer")));
                    try (var c = assertDoesNotThrow(() -> Zmux.open(prefixed(boot[0], token), Role.CLIENT, cfg),
                            () -> l.stats().toString()); var s = l.accept()) {
                        for (int i = 0; i < 6; i++) {
                            var pipe = MemoryPipe.pair(Framing.BYTE_STREAM);
                            byte[] nonce = ByteBuffer.allocate(16).putInt(i + 1).array();
                            byte[] joinToken = PrefixAuth.token(GENERATION, TICKET_ID, TICKET_KEY, nowSeconds / SLOT.toSeconds(), new byte[0], nonce);
                            l.adapter().handle(pipe[1], JoinHint.inBand(TestAddr.peer("peer")));
                            assertTrue(c.addPipe(prefixed(pipe[0], joinToken), PipeOptions.defaults()).id().value() > 0);
                        }
                        try (var st = c.openStream()) {
                            st.write(new byte[] {1, 2, 3});
                            st.finish();
                            try (var in = s.acceptStream()) { assertArrayEquals(new byte[] {1, 2, 3}, in.readAll(16)); }
                        }
                    }
                }
            }
        }
    }

    /** A pipe input with no further input after the supplied prefix. */
    static CompletableFuture<Void> silentCarrier(ZmuxListener listener, String source, byte[] prefix) {
        return submitPrefix(listener, source, prefix).closed();
    }

    private static Handled submitPrefix(ZmuxListener listener, String source, byte[] prefix) {
        var pipe = MemoryPipe.pair(Framing.BYTE_STREAM);
        TakeoverTest.reading(pipe[0]);
        pipe[0].writer().write(OutboundBatch.of(ByteBuffer.wrap(prefix)), new PipeWriter.Completion() {
            @Override public void completed() {}
            @Override public void failed(Throwable cause) {}
        });
        return listener.adapter().handle(pipe[1], JoinHint.inBand(TestAddr.literal(source)));
    }

    /**
     * A carrier accepted with prefix bytes left over waits for its first record counted against
     * its own source: silent carriers from different hosts can both wait. A source with two slots
     * occupied refuses another carrier, including from the same IPv6 /64.
     */
    @Test void anAcceptedCarrierWithBytesLeftCountsAgainstItsOwnSource() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var lc = ListenerConfig.builder().session(cfg).maxPendingPerSource(2).firstRecordTimeout(Duration.ofSeconds(10))
                    .admission(Admission.builder(2, Admission.Decider.immediate((info, prefix) -> Admission.Decision.accept(1))).peekTimeout(Duration.ofSeconds(5)).build()).build();
            try (var l = Zmux.listener(lc)) {
                byte[] prefix = {0, 0x40}; // the leftover byte starts a record
                var one = acceptedPrefix(l, "192.0.2.1", prefix);
                var two = acceptedPrefix(l, "192.0.2.2", prefix);
                assertFalse(one.isDone(), "the first host's carrier waits");
                assertFalse(two.isDone(), "the second host's carrier waits");
                var same = l.adapter().reserve(new java.net.InetSocketAddress("192.0.2.1", 1), null);
                assertNotNull(same);
                try { silentCarrier(l, "192.0.2.1", prefix).get(5, TimeUnit.SECONDS); }
                finally { same.release(); }
                assertFalse(one.isDone());
                // An IPv6 client counts by its /64.
                var v6 = acceptedPrefix(l, "2001:db8::1", prefix);
                assertFalse(v6.isDone());
                var samePrefix = l.adapter().reserve(new java.net.InetSocketAddress("2001:db8::3", 1), null);
                assertNotNull(samePrefix);
                try { silentCarrier(l, "2001:db8::2", prefix).get(5, TimeUnit.SECONDS); }
                finally { samePrefix.release(); }
                assertFalse(v6.isDone());
            }
        }
    }
    private static CompletableFuture<Void> acceptedPrefix(ZmuxListener listener, String source, byte[] prefix) throws Exception {
        var handled = submitPrefix(listener, source, prefix);
        assertEquals(AdmissionVerdict.ACCEPT, handled.verdict().get(WATCHDOG_SECONDS, TimeUnit.SECONDS));
        return handled.closed();
    }
}
