package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeChannel;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The inbound transform and the admission value (API.md 4.4, 4.7), the unknown-join answer
 * (MULTIPATH.md 5.4), the write overhead hint and the pipe loss statistics (API.md 4.1, 7.2).
 */
@Timeout(60)
class InboundTransformTest {
    static ZmuxConfig.Builder builder(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops)
                .multipath(MultipathConfig.defaults().withMaxPipes(4).withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO));
    }

    static ZmuxConfig config(EventLoops loops) { return builder(loops).build(); }

    static byte[] readAll(ZmuxReceiveStream s) throws Exception { return SessionTest.readAll(s); }

    static Admission valued(Object value) {
        return Admission.of(0, Admission.Decider.immediate((info, prefix) -> Admission.Decision.accept(0, value)));
    }

    /** A carrier whose sending half counts its writes and reports a retransmission count. */
    static PipeIo counted(PipeIo io, List<Integer> writes, long retransmitted) {
        PipeWriter w = io.writer();
        return new PipeIo(io.reader(), new PipeWriter() {
            @Override public void write(OutboundBatch batch, Completion done) { writes.add(batch.bytes()); w.write(batch, done); }
            @Override public OptionalLong retransmitted() { return retransmitted < 0 ? OptionalLong.empty() : OptionalLong.of(retransmitted); }
            @Override public void close() { w.close(); }
            @Override public void abort(Throwable cause) { w.abort(cause); }
        }, io.framing(), io.label(), io.carrierConn(), io.capabilities(), io.carrier());
    }

    @Test void theTransformSeesTheAdmissionValueAndItsLayerCarriesTheSession() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var seen = new LinkedBlockingQueue<InboundTransform.Context>();
            // A layer over the carrier: PipeChannel down, PipeChannel.pipe up, one write each.
            InboundTransform t = (carrier, ctx) -> {
                seen.add(ctx);
                var ch = PipeChannel.of(carrier);
                return CompletableFuture.completedFuture(PipeChannel.pipe(ch, ch, carrier.framing(), 16384).withWriteOverhead(29));
            };
            var shaped = new LinkedBlockingQueue<io.zmux.shaping.ShapedPipe>();
            var cfg = config(loops);
            var serverCfg = builder(loops).shaper(p -> { shaped.add(p); return null; }).build();
            var lc = ListenerConfig.builder().session(serverCfg).admission(valued("tenant-7")).inboundTransform(t).build();
            try (var l = Zmux.listener(lc)) {
                var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.handle(boot[1], JoinHint.inBand(TestAddr.peer("c")).withRequest(RequestHead.NONE));
                try (var c = Zmux.open(boot[0], Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(5))) {
                    var ctx = seen.poll(5, TimeUnit.SECONDS);
                    assertEquals(Optional.of("tenant-7"), ctx.admitted());
                    assertEquals(Optional.of("tenant-7"), s.admitted());
                    assertEquals(Optional.empty(), c.admitted(), "the dialing side has none");
                    var st = c.openStream();
                    st.write("hello".getBytes());
                    st.finish();
                    var in = s.acceptStream();
                    assertArrayEquals("hello".getBytes(), readAll(in));
                    var p = shaped.poll(5, TimeUnit.SECONDS);
                    assertNotNull(p);
                    assertEquals(29, p.writeOverhead(), "the transform's overhead reaches the shaper factory");
                    // A join carries its own value; zmux does not compare it with the session's.
                    try (var r = c.paths().reserve(PipeOptions.builder().probation(Duration.ZERO).build())) {
                        var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                        l.handle(pair[1], JoinHint.join(r.header(), TestAddr.peer("c")));
                        r.attach(pair[0]);
                        ListenerEntryTest.await(() -> l.stats().joins() == 1);
                    }
                    assertEquals(Optional.of("tenant-7"), seen.poll(5, TimeUnit.SECONDS).admitted());
                    ListenerEntryTest.await(() -> s.pipes().size() >= 2);
                    ZmuxPipe joined = s.pipes().stream().filter(x -> x.id() != 0).findFirst().orElseThrow();
                    assertEquals(Optional.of("tenant-7"), joined.admitted());
                    assertEquals(Optional.of("tenant-7"), s.pipes().stream().filter(x -> x.id() == 0).findFirst().orElseThrow().admitted());
                }
            }
        }
    }

    @Test void aFailingTransformClosesTheCarrierAsASetupFailure() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            InboundTransform t = (carrier, ctx) -> CompletableFuture.failedFuture(new java.io.IOException("no"));
            try (var l = Zmux.listener(ListenerConfig.builder().session(config(loops)).inboundTransform(t).build())) {
                var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
                var ended = ListenerEntryTest.watchEnd(boot[0]);
                l.handle(boot[1], JoinHint.inBand(TestAddr.peer("c")));
                ended.get(5, TimeUnit.SECONDS);
                ListenerEntryTest.await(() -> l.stats().setupFailures() == 1);
                assertEquals(0, l.stats().sessions());
            }
        }
    }

    @Test void aTransformThatChangesTheFramingOrNeverCompletesIsRefused() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = builder(loops).timeouts(Timeouts.defaults().withHandshake(Duration.ofMillis(300))).build();
            var calls = new java.util.concurrent.atomic.AtomicInteger();
            var cancelled = new CompletableFuture<Void>();
            InboundTransform t = (carrier, ctx) -> {
                if (calls.incrementAndGet() == 1) return CompletableFuture.completedFuture(carrier.withFraming(Framing.MESSAGES));
                var never = new CompletableFuture<PipeIo>();
                never.whenComplete((v, e) -> { if (never.isCancelled()) cancelled.complete(null); });
                return never;
            };
            try (var l = Zmux.listener(ListenerConfig.builder().session(cfg).inboundTransform(t).build())) {
                for (int i = 0; i < 2; i++) {
                    var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
                    var ended = ListenerEntryTest.watchEnd(boot[0]);
                    l.handle(boot[1], JoinHint.inBand(TestAddr.peer("c")));
                    ended.get(5, TimeUnit.SECONDS);
                }
                cancelled.get(5, TimeUnit.SECONDS);
                ListenerEntryTest.await(() -> l.stats().setupFailures() == 2);
            }
        }
    }

    @Test void theIdentityTransformRunsForEveryCarrierOfASplitBootstrap() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var hints = new LinkedBlockingQueue<JoinHint>();
            InboundTransform t = (carrier, ctx) -> { hints.add(ctx.hint()); return CompletableFuture.completedFuture(carrier); };
            var cfg = config(loops);
            var values = new java.util.concurrent.atomic.AtomicInteger();
            var adm = Admission.of(0, Admission.Decider.immediate((info, prefix) -> Admission.Decision.accept(0, "half-" + values.incrementAndGet())));
            try (var l = Zmux.listener(ListenerConfig.builder().session(cfg).admission(adm).inboundTransform(t).build())) {
                var up = MemoryPipe.pair(Framing.BYTE_STREAM);
                var down = MemoryPipe.pair(Framing.BYTE_STREAM);
                String token = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[16]);
                // The downlink first: the session's value is still the uplink's.
                l.handle(new PipeIo(null, down[1].writer(), Framing.BYTE_STREAM, null, null), JoinHint.pair(token, false, TestAddr.peer("c")));
                l.handle(new PipeIo(up[1].reader(), null, Framing.BYTE_STREAM, null, null), JoinHint.pair(token, true, TestAddr.peer("c")));
                var client = PipeIo.split(new PipeIo(null, up[0].writer(), Framing.BYTE_STREAM, null, null),
                        new PipeIo(down[0].reader(), null, Framing.BYTE_STREAM, null, null));
                try (var c = Zmux.open(client, Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(5))) {
                    assertEquals(2, hints.size());
                    assertEquals(Optional.of("half-2"), s.admitted());
                }
            }
        }
    }

    @Test void anUnknownJoinIsAnsweredOnlyWhenTheListenerSaysSo() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = config(loops);
            try (var home = Zmux.listener(ListenerConfig.of(cfg));
                 var quiet = Zmux.listener(ListenerConfig.of(cfg));
                 var answering = Zmux.listener(ListenerConfig.builder().session(cfg).answerUnknownJoins(true).build())) {
                var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
                home.handle(boot[1], JoinHint.inBand(TestAddr.peer("c")));
                try (var c = Zmux.open(boot[0], Role.CLIENT, cfg); var s = home.accept(Duration.ofSeconds(5))) {
                    for (var l : List.of(quiet, answering)) {
                        try (var r = c.paths().reserve(PipeOptions.builder().probation(Duration.ZERO).build())) {
                            var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                            var got = new ByteArrayOutputStream();
                            var ended = new CompletableFuture<Void>();
                            // Read what the listener writes in place of the joiner.
                            pair[0].reader().start(new PipeReader.Sink() {
                                @Override public void onData(ByteBuffer d, Runnable release) {
                                    synchronized (got) { while (d.hasRemaining()) got.write(d.get()); } release.run();
                                }
                                @Override public void onEnd() { ended.complete(null); }
                                @Override public void onError(Throwable e) { ended.complete(null); }
                            });
                            l.handle(pair[1], JoinHint.join(r.header(), TestAddr.peer("c")));
                            ended.get(5, TimeUnit.SECONDS);
                            ListenerEntryTest.await(() -> l.stats().rejected() == 1);
                            byte[] answer = got.toByteArray();
                            if (l == quiet) assertEquals(0, answer.length);
                            else assertArrayEquals(new byte[] {0x07, 0x1c, (byte) 0x8f, 0x05, (byte) 0xf7, 0x4e, 0x00, 0x00}, answer);
                            pair[0].writer().close();
                        }
                    }
                    assertNotNull(c.info(), "the session lives on");
                }
            }
        }
    }

    @Test void anUnknownJoinOnACarrierWithoutASendingHalfGetsNoAnswer() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = config(loops);
            try (var home = Zmux.listener(ListenerConfig.of(cfg));
                 var answering = Zmux.listener(ListenerConfig.builder().session(cfg).answerUnknownJoins(true).build())) {
                var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
                home.handle(boot[1], JoinHint.inBand(TestAddr.peer("c")));
                try (var c = Zmux.open(boot[0], Role.CLIENT, cfg); var s = home.accept(Duration.ofSeconds(5));
                     var r = c.paths().reserve(PipeOptions.builder().probation(Duration.ZERO).build())) {
                    // An input-only carrier, as an uplink request: the answer would have nowhere to go.
                    var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                    var closed = new CompletableFuture<Void>();
                    PipeReader in = pair[1].reader();
                    PipeReader watched = new PipeReader() {
                        @Override public void start(Sink sink) { in.start(sink); }
                        @Override public void close() { closed.complete(null); in.close(); }
                    };
                    answering.handle(new PipeIo(watched, null, Framing.BYTE_STREAM, "uplink", null), JoinHint.join(r.header(), TestAddr.peer("c")));
                    closed.get(5, TimeUnit.SECONDS);
                    ListenerEntryTest.await(() -> answering.stats().rejected() == 1);
                    pair[0].writer().close();
                    assertNotNull(c.info(), "the session lives on");
                }
            }
        }
    }

    @Test void aJoinAnsweredWithResumeFailedFailsOnlyThatJoinAndBacksOff() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = config(loops);
            try (var home = Zmux.listener(ListenerConfig.of(cfg));
                 var other = Zmux.listener(ListenerConfig.builder().session(cfg).answerUnknownJoins(true).build())) {
                var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
                home.handle(boot[1], JoinHint.inBand(TestAddr.peer("c")));
                try (var c = Zmux.open(boot[0], Role.CLIENT, cfg); var s = home.accept(Duration.ofSeconds(5))) {
                    var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                    other.handle(pair[1], JoinHint.inBand(TestAddr.peer("c")));
                    assertThrows(Exception.class, () -> c.paths().add(pair[0], PipeOptions.builder().probation(Duration.ZERO).build()));
                    ListenerEntryTest.await(() -> c.stats().joinRefusals() == 1);
                    var st = c.openStream();
                    st.write(new byte[] {1});
                    st.finish();
                    assertArrayEquals(new byte[] {1}, readAll(s.acceptStream()));
                }
            }
        }
    }

    @Test void pipeStatsReportLossAndTheCarriersRetransmissions() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = config(loops);
            try (var l = Zmux.listener(ListenerConfig.of(cfg))) {
                var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
                l.handle(boot[1], JoinHint.inBand(TestAddr.peer("c")));
                var writes = java.util.Collections.synchronizedList(new ArrayList<Integer>());
                try (var c = Zmux.open(counted(boot[0], writes, 1234), Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(5))) {
                    var st = c.openStream();
                    st.write(new byte[1000]);
                    st.finish();
                    readAll(s.acceptStream());
                    ListenerEntryTest.await(() -> {
                        try { return c.paths().stats().getFirst().bytesRetransmitted().isPresent(); } catch (Exception e) { return false; }
                    });
                    var p = c.paths().stats().getFirst();
                    assertEquals(1234, p.bytesRetransmitted().getAsLong());
                    assertEquals(OptionalLong.of(0), p.bytesLost(), "nothing lost before a failure");
                    assertEquals(OptionalLong.of(0), p.recordsLost());
                    assertTrue(p.lossRate().isEmpty(), "no sample a second apart yet");
                    assertTrue(s.paths().stats().getFirst().bytesRetransmitted().isEmpty(), "the adapter reports none");
                }
            }
        }
    }

    @Test void theWriteOverheadIsBounded() {
        var io = MemoryPipe.pair(Framing.BYTE_STREAM)[0];
        assertEquals(65535, io.withWriteOverhead(65535).capabilities().writeOverhead());
        assertThrows(IllegalArgumentException.class, () -> io.withWriteOverhead(-1));
        assertThrows(IllegalArgumentException.class, () -> io.withWriteOverhead(65536));
    }

    @Test void theLocatorKeepsEightRandomBytesAfterTheLongestInstanceId() {
        assertTrue(16 - ListenerConfig.MAX_INSTANCE_ID >= 8);
    }

    @Test void aMessageChannelWritesOneMessagePerWrite() throws Exception {
        var pair = MemoryPipe.pair(Framing.MESSAGES);
        try (var a = PipeChannel.of(pair[0]); var b = PipeChannel.of(pair[1])) {
            // A write completes once the reader released it: written from another thread.
            var writer = CompletableFuture.runAsync(() -> {
                try { a.write(ByteBuffer.wrap(new byte[] {1, 2, 3})); a.write(ByteBuffer.wrap(new byte[] {4})); a.shutdownOutput(); }
                catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
            }, r -> Thread.ofVirtual().start(r));
            var got = ByteBuffer.allocate(2);
            assertEquals(2, b.read(got));
            got.clear();
            assertEquals(1, b.read(got), "the rest of the first message");
            got.clear();
            assertEquals(1, b.read(got));
            assertEquals(4, got.get(0));
            writer.get(5, TimeUnit.SECONDS);
            assertEquals(-1, b.read(got.clear()));
        }
    }
}
