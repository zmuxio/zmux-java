package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import io.zmux.shaping.ShapingLimits;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The carrier hooks and session settings of the 1102 contract, over the public API. */
@Timeout(30)
class CarrierHooksTest {
    private static final byte[] PSK = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);
    private static final Duration WAIT = Duration.ofSeconds(5);

    private static String resource(String name) throws IOException {
        try (var in = CarrierHooksTest.class.getResourceAsStream("/testdata/" + name)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String str(SnapshotJson.Obj o, String k) { return ((SnapshotJson.Str) o.get(k)).value(); }
    private static long num(SnapshotJson.Obj o, String k) { return ((SnapshotJson.Int) o.get(k)).value(); }
    private static List<SnapshotJson.Value> arr(SnapshotJson.Obj o, String k) { return ((SnapshotJson.Arr) o.get(k)).values(); }

    @Test void firstBytesVectors() throws Exception {
        var top = (SnapshotJson.Obj) SnapshotJson.parse(resource("firstbytes.json"));
        for (var v : arr(top, "vectors")) {
            var o = (SnapshotJson.Obj) v;
            var got = FirstBytes.classify(HexFormat.of().parseHex(str(o, "hex")),
                    str(o, "framing").equals("records") ? Framing.MESSAGES : Framing.BYTE_STREAM);
            String want = str(o, "class");
            assertEquals(want, switch (got.kind()) { case NEED_MORE -> "need_more"; case RECORD -> "record"; case OTHER -> "other"; }, str(o, "note"));
            assertEquals(num(o, "need"), got.need(), str(o, "note"));
        }
    }

    private static final class SetClock extends Clock {
        volatile long ms;
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId z) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(ms); }
    }

    @Test void replayVectors() throws Exception {
        var top = (SnapshotJson.Obj) SnapshotJson.parse(resource("replay.json"));
        for (var v : arr(top, "cases")) {
            var o = (SnapshotJson.Obj) v;
            var clock = new SetClock();
            long nb = num(o, "not_before_ms");
            var f = new ReplayFilter(Duration.ofMillis(num(o, "window_ms")), (int) num(o, "max"), nb == 0 ? null : Instant.ofEpochMilli(nb), clock);
            for (var sv : arr(o, "steps")) {
                var s = (SnapshotJson.Obj) sv;
                clock.ms = num(s, "now_ms");
                var got = f.check(HexFormat.of().parseHex(str(s, "key_hex")), num(s, "t_ms"));
                assertEquals(str(s, "verdict"), got.name().toLowerCase(java.util.Locale.ROOT), str(o, "name"));
            }
        }
    }

    @Test void cacheReportAndVariantsAreKeysOfTheirOwn() {
        var c = new CarrierCache();
        c.report("k", CarrierKind.WEBSOCKET, "x", CarrierCache.Outcome.REJECTED);
        assertTrue(c.backingOff("k", CarrierKind.WEBSOCKET, "x"));
        assertFalse(c.backingOff("k", CarrierKind.WEBSOCKET));
        assertEquals(1, c.snapshot().entries().size());
        var e = c.snapshot().entries().getFirst();
        assertEquals(c.backoffMax(), e.backoff(), "a definite verdict skips for the maximum backoff");
        assertEquals("x", CarrierCache.Snapshot.fromJson(c.snapshot().toJson()).entries().getFirst().variant());
        c.report("k", CarrierKind.WEBSOCKET, "x", CarrierCache.Outcome.USABLE);
        assertTrue(c.usable("k", CarrierKind.WEBSOCKET, "x"));
        c.report("k", CarrierKind.WEBSOCKET, "x", CarrierCache.Outcome.FAILED);
        assertTrue(c.backingOff("k", CarrierKind.WEBSOCKET, "x"));
    }

    private record Pair(ZmuxSession client, ZmuxSession server, ZmuxListener listener) implements AutoCloseable {
        @Override public void close() throws IOException { client.close(); server.close(); listener.close(); }
    }

    private static Pair pair(EventLoops loops, PipeAuth clientM, PipeAuth serverM, java.util.function.UnaryOperator<PipeIo> side) throws IOException {
        var s = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(serverM).build();
        var c = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(clientM).build();
        var l = ZmuxListener.of(ListenerConfig.of(s));
        var p = MemoryPipe.pair(Framing.BYTE_STREAM);
        l.adapter().handle(side.apply(p[1]), JoinHint.inBand(TestAddr.peer("peer")));
        var client = Zmux.open(side.apply(p[0]), Role.CLIENT, c);
        var server = l.accept(WAIT);
        assertNotNull(server);
        return new Pair(client, server, l);
    }

    private static PipeAuth mp(PipeAuth a) { return a; }

    private static ZmuxPipe join(Pair p) throws IOException {
        var j = MemoryPipe.pair(Framing.BYTE_STREAM);
        p.listener.adapter().handle(j[1], JoinHint.inBand(TestAddr.peer("peer")));
        return p.client.addPipe(j[0], PipeOptions.defaults());
    }

    @Test void deferredPskOpensJoinsOnceSupplied() throws Exception {
        try (EventLoops loops = EventLoops.create(1);
             var p = pair(loops, mp(PipeAuth.deferred().withEndToEnd()), mp(PipeAuth.deferred().withEndToEnd()), x -> x)) {
            assertTrue(p.client.info().joinPending() || waitPending(p.client));
            assertThrows(JoinPendingException.class, () -> join(p));
            assertThrows(IllegalArgumentException.class, () -> p.client.setJoinSecret(new byte[4]));
            p.client.setJoinSecret(PSK);
            assertThrows(JoinSecretSetException.class, () -> p.client.setJoinSecret(PSK));
            // The peer's join is held until its own PSK arrives, then goes on.
            var j = MemoryPipe.pair(Framing.BYTE_STREAM);
            p.listener.adapter().handle(j[1], JoinHint.inBand(TestAddr.peer("peer")));
            var pipe = Thread.ofVirtual().start(() -> {
                try { p.client.addPipe(j[0], PipeOptions.defaults()); } catch (IOException e) { throw new AssertionError(e); }
            });
            long t0 = System.nanoTime();
            while (p.server.stats().joinPending() == 0 && System.nanoTime() - t0 < WAIT.toNanos()) Thread.sleep(5);
            assertEquals(1, p.server.stats().joinPending(), "held, not failed");
            assertEquals(0, p.server.stats().joinAuthFailures());
            p.server.setJoinSecret(PSK);
            pipe.join(WAIT);
            t0 = System.nanoTime();
            while (p.server.pipes().size() < 2 && System.nanoTime() - t0 < WAIT.toNanos()) Thread.sleep(5);
            assertEquals(2, p.server.pipes().size());
            assertFalse(p.server.info().joinPending());
        }
    }

    private static boolean waitPending(ZmuxSession s) throws InterruptedException {
        long t0 = System.nanoTime();
        while (!s.info().joinPending() && System.nanoTime() - t0 < WAIT.toNanos()) Thread.sleep(2);
        return s.info().joinPending();
    }

    private static PipeIo.Exporter counting(AtomicInteger reads, byte tag) {
        return (label, context, length) -> {
            reads.incrementAndGet();
            byte[] b = new byte[length];
            java.util.Arrays.fill(b, tag);
            return b;
        };
    }

    @Test void exporterIsReadLateAndOnce() throws Exception {
        var reads = new AtomicInteger();
        try (EventLoops loops = EventLoops.create(1);
             var p = pair(loops, mp(PipeAuth.exporter().withEndToEnd()), mp(PipeAuth.exporter().withEndToEnd()), x -> x.withExporter(counting(reads, (byte) 7)))) {
            assertEquals(2, reads.get(), "each session reads its exporter once, at the peer's parameters");
            assertArrayEquals(new byte[] {7, 7, 7}, p.client.exportKeyingMaterial("app", null, 3));
            assertEquals(0, p.client.exportKeyingMaterial("app", null, 0).length);
            assertThrows(IllegalArgumentException.class, () -> p.client.exportKeyingMaterial("EXPORTER-zmux-session", null, 32));
            assertThrows(IllegalArgumentException.class, () -> p.client.exportKeyingMaterial("app", null, -1));
            byte[] a = p.client.sessionBinding().orElseThrow(), b = p.server.sessionBinding().orElseThrow();
            assertEquals(32, a.length);
            assertArrayEquals(a, b, "the same at both ends");
        }
    }

    @Test void failedExporterReadClosesWithInternalError() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var bad = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(mp(PipeAuth.exporter().withEndToEnd())).build();
            var good = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(mp(PipeAuth.exporter().withEndToEnd())).build();
            try (var l = ZmuxListener.of(ListenerConfig.of(good))) {
                var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.adapter().handle(p[1].withExporter((x, y, z) -> new byte[z]), JoinHint.inBand(TestAddr.peer("peer")));
                try (var c = Zmux.open(p[0].withExporter((x, y, z) -> null), Role.CLIENT, bad)) {
                    var e = c.awaitClosed();
                    assertInstanceOf(SessionError.TransportClosed.class, e);
                    assertEquals("INTERNAL_ERROR", ((SessionError.TransportClosed) e).name());
                }
            }
        }
    }

    @Test void exporterOnACarrierWithoutOneIsAConfigurationError() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(mp(PipeAuth.exporter().withEndToEnd())).build();
            var p = MemoryPipe.pair(Framing.BYTE_STREAM);
            assertThrows(IllegalArgumentException.class, () -> Zmux.open(p[0], Role.CLIENT, cfg));
            p[1].reader().close();
            p[1].writer().close();
        }
    }

    @Test void splitExporterIsUplinkThenDownlink() {
        var up = MemoryPipe.pair(Framing.BYTE_STREAM);
        var down = MemoryPipe.pair(Framing.BYTE_STREAM);
        var reads = new AtomicInteger();
        var split = PipeIo.split(new PipeIo(null, up[0].writer(), Framing.BYTE_STREAM, null, null).withExporter(counting(reads, (byte) 1)),
                new PipeIo(down[0].reader(), null, Framing.BYTE_STREAM, null, null).withExporter(counting(reads, (byte) 2)));
        assertArrayEquals(new byte[] {1, 1, 2, 2}, split.capabilities().exporter().export("x", null, 2));
        var same = PipeIo.split(new PipeIo(null, up[0].writer(), Framing.BYTE_STREAM, null, null).withExporter(counting(reads, (byte) 1)),
                new PipeIo(down[0].reader(), null, Framing.BYTE_STREAM, null, null).withExporter(counting(reads, (byte) 1)));
        assertArrayEquals(new byte[] {1, 1}, same.capabilities().exporter().export("x", null, 2), "one connection, one value");
    }

    @Test void noExporter() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var p = pair(loops, mp(PipeAuth.psk(new byte[32]).withEndToEnd()), mp(PipeAuth.psk(new byte[32]).withEndToEnd()), x -> x)) {
            assertThrows(NoExporterException.class, () -> p.client.exportKeyingMaterial("app", null, 8));
            assertThrows(IllegalArgumentException.class, () -> p.client.setJoinSecret(PSK), "without a deferred PSK");
            try (var st = p.client.openStream()) { st.write(new byte[] {1}); } // The session lives on.
        }
    }

    @Test void flushOpensAStreamOnTheWire() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var p = pair(loops, mp(PipeAuth.psk(new byte[32]).withEndToEnd()), mp(PipeAuth.psk(new byte[32]).withEndToEnd()), x -> x)) {
            try (var st = p.client.openStream()) {
                st.flush();
                try (var in = p.server.acceptStream(WAIT)) {
                    assertNotNull(in, "the peer sees the stream without a byte");
                    in.write(new byte[] {4});
                    in.finish();
                    assertArrayEquals(new byte[] {4}, st.readAll(4));
                }
            }
        }
    }

    @Test void livenessRoundTrips() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var p = pair(loops, mp(PipeAuth.psk(new byte[32]).withEndToEnd()), mp(PipeAuth.psk(new byte[32]).withEndToEnd()), x -> x)) {
            var want = Liveness.builder().keepAlivePeriod(Duration.ofSeconds(3)).suspectFloor(Duration.ofMillis(60)).deadTimeout(Duration.ofSeconds(2))
                    .noStandbyProbe().probeInterval(Duration.ofSeconds(5)).probeJitter(0.5).build();
            p.client.setLiveness(want);
            assertEquals(want, p.client.liveness());
            p.client.setLiveness(p.client.liveness());
            assertEquals(want, p.client.liveness(), "setting what it returns changes nothing");
            p.client.setLiveness(Liveness.defaults());
            assertEquals(Liveness.defaults(), p.client.liveness(), "not written is the default, not the start value");
            var off = Liveness.builder().noKeepAlive().noProbeInterval().probeJitter(0).build();
            p.client.setLiveness(off);
            assertEquals(off, p.client.liveness());
            assertTrue(p.client.liveness().keepAlivePeriod().isEmpty());
            assertThrows(IllegalArgumentException.class, () -> Liveness.builder().keepAlivePeriod(Duration.ofMillis(1)));
            assertThrows(IllegalArgumentException.class, () -> Liveness.builder().probeJitter(-1));
            var pipe0 = p.client.pipes().getFirst();
            pipe0.update(PipeUpdate.builder().liveness(PipeLiveness.builder().suspectFloor(Duration.ofMillis(100)).deadTimeout(Duration.ofSeconds(2))
                    .probeInterval(Duration.ofSeconds(3)).probeJitter(0).build()).build());
            pipe0.update(PipeUpdate.builder().liveness(PipeLiveness.builder().noProbeInterval().build()).build());
            pipe0.update(PipeUpdate.builder().clearLiveness().build());
        }
    }

    @Test void raisingTheStreamLimitGrantsCredit() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var s = ZmuxConfig.builder().eventLoops(loops).incomingStreams(4).build();
            var c = ZmuxConfig.builder().eventLoops(loops).build();
            try (var l = ZmuxListener.of(ListenerConfig.of(s))) {
                var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.adapter().handle(p[1], JoinHint.inBand(TestAddr.peer("peer")));
                try (var client = Zmux.open(p[0], Role.CLIENT, c); var server = l.accept(WAIT)) {
                    for (int i = 0; i < 4; i++) client.openStream().flush();
                    assertThrows(java.io.InterruptedIOException.class, () -> client.openStream(Duration.ofMillis(100)));
                    assertThrows(IllegalArgumentException.class, () -> server.raiseIncomingStreams(-1, 0));
                    server.raiseIncomingStreams(6, 0);
                    client.openStream(WAIT).flush();
                }
            }
        }
    }

    @Test void shapingLimitsAreReplacedAtRuntime() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var p = pair(loops, mp(PipeAuth.psk(new byte[32]).withEndToEnd()), mp(PipeAuth.psk(new byte[32]).withEndToEnd()), x -> x)) {
            var l = ShapingLimits.defaults().withWire(new io.zmux.shaping.WireAccount(5, 0, 0));
            p.client.setShapingLimits(l);
            assertEquals(l, p.client.shapingLimits());
        }
    }

    /** An admission over byte streams: more prefix on request, variants counted. */
    @Test void admissionAsksForMoreAndCountsVariants() throws Exception {
        var seen = new CopyOnWriteArrayList<Integer>();
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var adm = Admission.of(2, Admission.Decider.immediate((info, prefix) -> {
                seen.add(prefix.remaining());
                if (prefix.remaining() == 2) return Admission.Decision.more(6);
                if (prefix.remaining() == 6) return new Admission.Decision(AdmissionVerdict.ACCEPT, 6, "v", 0, "x");
                return Admission.Decision.CLOSE;
            })).withMaxPeek(16).withPeekTimeout(WAIT);
            var lc = ListenerConfig.builder().session(cfg).admission(adm).build();
            try (var l = ZmuxListener.of(lc)) {
                var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.adapter().handle(p[1], JoinHint.inBand(TestAddr.peer("peer")));
                // The carrier starts with six bytes the accept skips, a byte at a time for the first.
                write(p[0], new byte[] {1});
                write(p[0], new byte[] {2});
                Thread.sleep(50);
                write(p[0], new byte[] {3, 4, 5, 6});
                try (var _ = Zmux.open(p[0], Role.CLIENT, cfg); var s = l.accept(WAIT)) {
                    assertNotNull(s);
                    assertEquals("v", s.admitted().orElse(null));
                    assertEquals(List.of(2, 6), seen);
                    var v = l.variantStats();
                    assertEquals(1, v.size());
                    assertEquals("x", v.getFirst().name());
                    assertEquals(1, v.getFirst().accepted());
                    assertEquals("x", s.pipes().getFirst().stats().orElseThrow().variant());
                }
            }
        }
    }

    /** A join held for the deferred PSK keeps its source's slot until it has its result. */
    @Test void heldJoinKeepsItsSourceSlot() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var s = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(mp(PipeAuth.deferred().withEndToEnd())).build();
            var c = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(mp(PipeAuth.deferred().withEndToEnd())).build();
            try (var l = ZmuxListener.of(ListenerConfig.builder().session(s).maxPendingPerSource(1).build())) {
                var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.adapter().handle(boot[1], JoinHint.inBand(TestAddr.peer("boot")));
                try (var client = Zmux.open(boot[0], Role.CLIENT, c); var server = l.accept(WAIT)) {
                    client.setJoinSecret(PSK);
                    var source = TestAddr.peer("joiner");
                    var j = MemoryPipe.pair(Framing.BYTE_STREAM);
                    l.adapter().handle(j[1], JoinHint.inBand(source));
                    var pipe = Thread.ofVirtual().start(() -> {
                        try { client.addPipe(j[0], PipeOptions.defaults()); } catch (IOException e) { throw new AssertionError(e); }
                    });
                    long t0 = System.nanoTime();
                    while (server.stats().joinPending() == 0 && System.nanoTime() - t0 < WAIT.toNanos()) Thread.sleep(5);
                    assertEquals(1, server.stats().joinPending());
                    assertNull(l.adapter().reserve(source, source), "the held join holds the slot");
                    server.setJoinSecret(PSK);
                    pipe.join(WAIT);
                    t0 = System.nanoTime();
                    io.zmux.adapter.Slot slot;
                    while ((slot = l.adapter().reserve(source, source)) == null && System.nanoTime() - t0 < WAIT.toNanos()) Thread.sleep(5);
                    assertNotNull(slot, "the slot goes with the result");
                    slot.release();
                }
            }
        }
    }

    /** The peek timeout during a round of more prefix ends the carrier undecided: no decision on too short a prefix. */
    @Test void moreRoundTimeoutAbandons() throws Exception {
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var adm = Admission.of(1, Admission.Decider.immediate((i, b) -> {
                calls.incrementAndGet();
                return b.remaining() < 8 ? Admission.Decision.more(8) : Admission.Decision.CLOSE;
            })).withMaxPeek(8).withPeekTimeout(Duration.ofMillis(100));
            try (var l = ZmuxListener.of(ListenerConfig.builder().session(cfg).admission(adm).build())) {
                var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                var h = l.adapter().handle(p[1], JoinHint.inBand(TestAddr.peer("peer")));
                write(p[0], new byte[] {1});
                h.closed().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
                assertEquals(1, calls.get());
                assertEquals(1, l.stats().admission().peekTimedOut());
                assertEquals(0, l.stats().admission().closed());
            }
        }
    }

    /** A size past the largest record size cannot begin a first record. */
    @Test void firstBytesSizeBound() {
        assertEquals(FirstBytes.Kind.OTHER, FirstBytes.classify(new byte[] {(byte) 0x80, 0, 1, 0, 0}, Framing.BYTE_STREAM).kind());
        assertNotEquals(FirstBytes.Kind.OTHER, FirstBytes.classify(new byte[] {(byte) 0x80, 0, (byte) 0xff, (byte) 0xff}, Framing.BYTE_STREAM).kind());
    }

    @Test void moreBeyondTheBoundCloses() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var adm = Admission.of(1, Admission.Decider.immediate((i, b) -> Admission.Decision.more(99))).withMaxPeek(8).withPeekTimeout(WAIT);
            try (var l = ZmuxListener.of(ListenerConfig.builder().session(cfg).admission(adm).build())) {
                var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                var h = l.adapter().handle(p[1], JoinHint.inBand(TestAddr.peer("peer")));
                write(p[0], new byte[] {1});
                h.closed().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
                assertEquals(1, l.stats().admission().closed());
            }
        }
    }

    /** A message-framed carrier: the first message whole is the prefix; skip is its length. */
    @Test void firstMessageIsThePrefix() throws Exception {
        var prefix = new java.util.concurrent.atomic.AtomicReference<byte[]>();
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var adm = Admission.of(1, Admission.Decider.immediate((i, b) -> {
                byte[] copy = new byte[b.remaining()];
                b.duplicate().get(copy);
                prefix.set(copy);
                return Admission.Decision.accept(copy.length);
            })).withMaxPeek(16).withPeekTimeout(WAIT);
            try (var l = ZmuxListener.of(ListenerConfig.builder().session(cfg).admission(adm).build())) {
                var p = MemoryPipe.pair(Framing.MESSAGES);
                l.adapter().handle(p[1], JoinHint.inBand(TestAddr.peer("peer")));
                write(p[0], new byte[] {9, 8, 7});
                try (var _ = Zmux.open(p[0], Role.CLIENT, cfg); var s = l.accept(WAIT)) {
                    assertNotNull(s, "the session's records follow the skipped message");
                    assertArrayEquals(new byte[] {9, 8, 7}, prefix.get());
                }
            }
        }
    }

    @Test void aFirstMessageAboveTheLimitIsClosed() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var adm = Admission.of(1, Admission.Decider.immediate((i, b) -> Admission.Decision.accept(0))).withMaxPeek(4).withPeekTimeout(WAIT);
            try (var l = ZmuxListener.of(ListenerConfig.builder().session(cfg).admission(adm).build())) {
                var p = MemoryPipe.pair(Framing.MESSAGES);
                var h = l.adapter().handle(p[1], JoinHint.inBand(TestAddr.peer("peer")));
                write(p[0], new byte[32]);
                h.closed().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
                assertEquals(1, l.stats().admission().closed());
            }
        }
    }

    private static void write(PipeIo io, byte[] b) throws Exception {
        var done = new java.util.concurrent.CompletableFuture<Void>();
        io.writer().write(OutboundBatch.of(ByteBuffer.wrap(b)), new PipeWriter.Completion() {
            @Override public void completed() { done.complete(null); }
            @Override public void failed(Throwable cause) { done.completeExceptionally(cause); }
        });
        done.get(5, java.util.concurrent.TimeUnit.SECONDS);
    }

    /** A chosen configuration's credential is the session's, not the listener's. */
    @Test void sessionConfigsChooseTheCredential() throws Exception {
        CarrierCredential chosenCredential = new CarrierCredential() {
            @Override public byte[] prefix(Admission.CarrierInfo info) { return new byte[0]; }
            @Override public HeaderField header(Admission.CarrierInfo info) { return new HeaderField("x", "y"); }
        };
        try (EventLoops loops = EventLoops.create(1)) {
            var base = ZmuxConfig.builder().eventLoops(loops).build();
            var chosen = ZmuxConfig.builder().eventLoops(loops).carrierCredential(chosenCredential).build();
            try (var l = ZmuxListener.of(ListenerConfig.builder().session(base).sessionConfigs((info, admitted) -> chosen).build())) {
                var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.adapter().handle(p[1], JoinHint.inBand(TestAddr.peer("peer")));
                try (var _ = Zmux.open(p[0], Role.CLIENT, base); var s = l.accept(WAIT)) {
                    assertSame(chosenCredential, s.credential());
                }
            }
        }
    }

    @Test void sessionConfigsChooseEachSession() throws Exception {
        var infos = new AtomicInteger();
        try (EventLoops loops = EventLoops.create(1)) {
            var base = ZmuxConfig.builder().eventLoops(loops).build();
            var chosen = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(mp(PipeAuth.deferred().withEndToEnd())).build();
            var c = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(mp(PipeAuth.deferred().withEndToEnd())).build();
            var lc = ListenerConfig.builder().session(base).sessionConfigs((info, admitted) -> { infos.incrementAndGet(); return chosen; }).build();
            try (var l = ZmuxListener.of(lc)) {
                var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.adapter().handle(p[1], JoinHint.inBand(TestAddr.peer("peer")));
                try (var _ = Zmux.open(p[0], Role.CLIENT, c); var s = l.accept(WAIT)) {
                    assertNotNull(s);
                    assertEquals(1, infos.get());
                    assertTrue(waitPending(s), "the chosen configuration's deferred PSK");
                }
            }
            var failing = ListenerConfig.builder().session(base).sessionConfigs((info, a) -> { throw new IllegalStateException("no"); }).build();
            try (var l = ZmuxListener.of(failing)) {
                var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                var h = l.adapter().handle(p[1], JoinHint.inBand(TestAddr.peer("peer")));
                var ended = new java.util.concurrent.CompletableFuture<Void>();
                Thread.ofVirtual().start(() -> {
                    try { Zmux.open(p[0], Role.CLIENT, base).close(); }
                    catch (IOException expected) { ended.complete(null); }
                });
                h.closed().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
                ended.get(5, java.util.concurrent.TimeUnit.SECONDS);
                assertEquals(1, l.stats().rejected());
            }
        }
    }

    @Test void variantReachesShapingAndEvents() throws Exception {
        var shaped = new CopyOnWriteArrayList<String>();
        try (EventLoops loops = EventLoops.create(1)) {
            var c = ZmuxConfig.builder().eventLoops(loops).build();
            try (var l = ZmuxListener.of(ListenerConfig.of(c))) {
                var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.adapter().handle(p[1], JoinHint.inBand(TestAddr.peer("peer")));
                try (var client = Zmux.open(p[0].withVariant("w"), Role.CLIENT, c); var _ = l.accept(WAIT)) {
                    client.setShaper(pipe -> { shaped.add(pipe.variant()); return null; });
                    try (var st = client.openStream()) {
                        st.write(new byte[] {1});
                        st.flush();
                    }
                    assertEquals("w", client.pipes().getFirst().stats().orElseThrow().variant());
                    assertEquals(List.of("w"), shaped);
                }
            }
        }
    }

    /** A failure inside the endpoint sends the peer CONNECTION_CLOSE with INTERNAL_ERROR, for an Error too. */
    @Test void aFailureOnTheLoopSendsInternalError() throws Exception {
        for (Throwable t : List.of(new IllegalStateException("bug"), new AssertionError("bug"), new StackOverflowError())) {
            try (EventLoops loops = EventLoops.create(1); var p = pair(loops, mp(PipeAuth.psk(new byte[32]).withEndToEnd()), mp(PipeAuth.psk(new byte[32]).withEndToEnd()), x -> x)) {
                p.client.driver.post(() -> {
                    if (t instanceof Error e) throw e;
                    throw (RuntimeException) t;
                });
                var e = p.server.awaitClosed();
                assertInstanceOf(SessionError.TransportClosed.class, e, t.toString());
                assertEquals("INTERNAL_ERROR", ((SessionError.TransportClosed) e).name());
                assertTrue(((SessionError.TransportClosed) e).remote());
            }
        }
    }

    /**
     * Holds the writes of a writer while closed: a write is in flight until the gate opens.
     * A close or an abort waits for the held writes and then goes on.
     */
    private static final class Gate {
        private boolean open = true;
        private final java.util.ArrayDeque<Runnable> held = new java.util.ArrayDeque<>();

        synchronized void close() { open = false; }

        void open() {
            java.util.List<Runnable> run;
            synchronized (this) { open = true; run = new java.util.ArrayList<>(held); held.clear(); }
            run.forEach(Runnable::run);
        }

        private void run(Runnable r) {
            synchronized (this) { if (!open) { held.add(r); return; } }
            r.run();
        }

        PipeIo wrap(PipeIo io) {
            PipeWriter w = io.writer();
            PipeWriter gated = new PipeWriter() {
                @Override public void write(OutboundBatch batch, Completion done) { run(() -> w.write(batch, done)); }
                @Override public void close() { run(w::close); }
                @Override public void abort(Throwable cause) { run(() -> w.abort(cause)); }
            };
            return io.withHalves(io.reader(), gated);
        }
    }

    /** The CONNECTION_CLOSE of a failure waits for a write still in flight on the carrier, and then goes out. */
    @Test void aFailureWithAWriteInFlightStillSendsInternalError() throws Exception {
        var gate = new Gate();
        try (EventLoops loops = EventLoops.create(1); var p = pair(loops, mp(PipeAuth.psk(new byte[32]).withEndToEnd()), mp(PipeAuth.psk(new byte[32]).withEndToEnd()),
                new java.util.function.UnaryOperator<PipeIo>() {
                    int n;
                    @Override public PipeIo apply(PipeIo io) { return n++ == 1 ? gate.wrap(io) : io; } // the server side comes first, then the client
                })) {
            gate.close();
            p.client.openStream().write(ByteBuffer.wrap(new byte[] {1})); // in flight until the gate opens
            p.client.driver.post(() -> { throw new IllegalStateException("bug"); });
            gate.open();
            var e = p.server.awaitClosed();
            assertInstanceOf(SessionError.TransportClosed.class, e, String.valueOf(e));
            assertEquals("INTERNAL_ERROR", ((SessionError.TransportClosed) e).name());
        }
    }
}
