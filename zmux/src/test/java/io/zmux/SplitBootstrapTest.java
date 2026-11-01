package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class SplitBootstrapTest {
    static String token(int value) { byte[] token = new byte[16]; token[0] = (byte) value; return Base64.getUrlEncoder().withoutPadding().encodeToString(token); }
    @Test void halvesPairInEitherOrderAndTokenStaysUsedUntilTheSessionEnds() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            try (var listener = Zmux.listener(ListenerConfig.of(config))) {
                for (Framing framing : Framing.values()) for (boolean upFirst : new boolean[] {false, true}) {
                    var up = MemoryPipe.pair(framing); var down = MemoryPipe.pair(framing);
                    String token = token((upFirst ? 1 : 2) + 10 * framing.ordinal());
                    PipeIo incoming = PipeIo.of(up[1].reader(), null).withFraming(framing), outgoing = PipeIo.of(null, down[1].writer()).withFraming(framing);
                    CompletionStage<Void> first = listener.handle(upFirst ? incoming : outgoing, JoinHint.pair(token, upFirst, TestAddr.peer("client"))).closed();
                    var connecting = workers.submit(() -> Zmux.open(PipeIo.split(PipeIo.of(null, up[0].writer()).withFraming(framing),
                            PipeIo.of(down[0].reader(), null).withFraming(framing)), Role.CLIENT, config));
                    // Starting the second half later exercises held first-record delivery.
                    Thread.sleep(10);
                    CompletionStage<Void> second = listener.handle(upFirst ? outgoing : incoming, JoinHint.pair(token, !upFirst, TestAddr.peer("client"))).closed();
                    try (var client = connecting.get(5, TimeUnit.SECONDS); var server = listener.accept(Duration.ofSeconds(5))) {
                        assertNotNull(server); assertFalse(first.toCompletableFuture().isDone());
                        try (var send = client.openStream()) {
                            send.writeChunk(ByteBuffer.wrap(new byte[] {7,8,9})); send.finish();
                            try (var receive = server.acceptStream(); Chunk chunk = receive.readChunk(3)) {
                                byte[] got = new byte[3]; chunk.buffer().get(got); assertArrayEquals(new byte[] {7,8,9}, got);
                            }
                        }
                        AtomicInteger closes = new AtomicInteger();
                        PipeWriter third = new PipeWriter() {
                            @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) { fail("replayed pair wrote data"); }
                            @Override public void close() { closes.incrementAndGet(); }
                        };
                        listener.handle(PipeIo.of(null, third), JoinHint.pair(token, false, TestAddr.peer("client"))).closed()
                                .toCompletableFuture().get(5, TimeUnit.SECONDS);
                        assertEquals(1, closes.get());
                        client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
                    }
                    first.toCompletableFuture().get(5, TimeUnit.SECONDS); second.toCompletableFuture().get(5, TimeUnit.SECONDS);
                }
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }
    @Test void firstRecordTimeoutAndListenerCloseReleaseUnpairedHalves() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var listener = Zmux.listener(ListenerConfig.builder().session(config).firstRecordTimeout(Duration.ofMillis(20)).build());
            try {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var waiting = listener.handle(PipeIo.of(pair[1].reader(), null), JoinHint.pair(token(3), true, TestAddr.peer("client"))).closed();
                waiting.toCompletableFuture().get(5, TimeUnit.SECONDS);
                AtomicInteger closes = new AtomicInteger();
                PipeWriter writer = new PipeWriter() {
                    @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) { fail("unpaired downlink wrote data"); }
                    @Override public void close() { closes.incrementAndGet(); }
                };
                var down = listener.handle(PipeIo.of(null, writer), JoinHint.pair(token(4), false, TestAddr.peer("client"))).closed();
                listener.close(); down.toCompletableFuture().get(5, TimeUnit.SECONDS);
                assertEquals(1, closes.get());
            } finally { listener.close(); }
            assertEquals(0, loops.outstandingBuffers());
        }
    }
    @Test void splitExporterBindsBothHalvesInUplinkDownlinkOrder() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(2).withAuth(PipeAuth.tlsExporter()).withProbation(Duration.ZERO)).build();
            try (var listener = Zmux.listener(ListenerConfig.of(config))) {
                PipeIo.Exporter upKey = (label, context, length) -> { byte[] key = new byte[length]; java.util.Arrays.fill(key, (byte) 3); return key; };
                PipeIo.Exporter downKey = (label, context, length) -> { byte[] key = new byte[length]; java.util.Arrays.fill(key, (byte) 9); return key; };
                var up = MemoryPipe.pair(Framing.BYTE_STREAM); var down = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.handle(PipeIo.of(up[1].reader(), null).withExporter(upKey), JoinHint.pair(token(40), true, TestAddr.peer("client"))).closed();
                listener.handle(PipeIo.of(null, down[1].writer()).withExporter(downKey), JoinHint.pair(token(40), false, TestAddr.peer("client"))).closed();
                try (var client = Zmux.open(PipeIo.split(PipeIo.of(null, up[0].writer()).withExporter(upKey),
                        PipeIo.of(down[0].reader(), null).withExporter(downKey)), Role.CLIENT, config);
                     var server = listener.accept(Duration.ofSeconds(5))) {
                    var newPipe = MemoryPipe.pair(Framing.BYTE_STREAM); listener.handle(newPipe[1], JoinHint.inBand(TestAddr.peer("client"))).closed();
                    var added = client.paths().add(newPipe[0], PipeOptions.defaults());
                    assertEquals(PipeState.Outbound.ACTIVE, added.state().orElseThrow().outbound());
                    client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
                }
            }
            ListenerResourcesTest.until(() -> loops.outstandingBuffers() == 0);
        }
    }
    @Test void stalledPairInputHasABoundedQueueAndReleasesRejectedDeliveries() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var listener = Zmux.listener(ListenerConfig.of(ZmuxConfig.builder().eventLoops(loops).build()))) {
            var sink = new CompletableFuture<PipeReader.Sink>(); AtomicInteger released = new AtomicInteger(), closed = new AtomicInteger();
            PipeReader reader = new PipeReader() {
                @Override public void start(Sink input) { sink.complete(input); }
                @Override public void close() { closed.incrementAndGet(); }
            };
            var lifetime = listener.handle(PipeIo.of(reader, null), JoinHint.pair(token(50), true, TestAddr.peer("client"))).closed();
            PipeReader.Sink input = sink.get(5, TimeUnit.SECONDS);
            var peer = new io.zmux.internal.core.Session(io.zmux.internal.core.CoreConfig.defaults(), 0);
            var info = new io.zmux.internal.core.CarrierInfo(io.zmux.internal.core.Framing.BYTES, false, false, false, false);
            peer.pipes().attachBootstrap(info, info, null); var batch = new io.zmux.internal.core.Batch();
            assertTrue(peer.pollTransmit(0, 0, batch, 65536, 0));
            input.onData(ByteBuffer.wrap(io.zmux.internal.core.BatchFixture.flat(batch)), released::incrementAndGet); batch.discard();
            CompletableFuture<Void> parsed = new CompletableFuture<>();
            loops.group().next().execute(() -> parsed.complete(null)); parsed.get(5, TimeUnit.SECONDS);
            assertEquals(0, released.get(), "complete first record must keep its delivery paused");
            for (int i=0; i<10; i++) input.onData(ByteBuffer.allocate(32 << 10), released::incrementAndGet);
            lifetime.toCompletableFuture().get(5, TimeUnit.SECONDS);
            input.onData(ByteBuffer.allocate(1), released::incrementAndGet);
            assertEquals(12, released.get()); assertEquals(1, closed.get());
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void admittedReaderThatIgnoresReleaseBackpressureFailsItsCarrier() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            PipeIo[] memory = MemoryPipe.pair(Framing.BYTE_STREAM);
            var sink = new CompletableFuture<PipeReader.Sink>();
            PipeReader tapped = new PipeReader() {
                @Override public void start(Sink input) { sink.complete(input); memory[0].reader().start(input); }
                @Override public void close() { memory[0].reader().close(); }
            };
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var server = CompletableFuture.supplyAsync(() -> {
                try { return Zmux.open(memory[1], Role.SERVER, cfg); } catch (Exception e) { throw new CompletionException(e); }
            }, r -> Thread.ofVirtual().start(r));
            ZmuxSession client = Zmux.open(new PipeIo(tapped, memory[0].writer(), Framing.BYTE_STREAM, "memory", null), Role.CLIENT, cfg);
            try (ZmuxSession peer = server.get(5, TimeUnit.SECONDS)) {
                AtomicInteger released = new AtomicInteger();
                var entered = new CountDownLatch(1); var proceed = new CountDownLatch(1);
                client.driver.executeCarrier(() -> { entered.countDown(); try { proceed.await(); } catch (InterruptedException ignored) { } });
                try {
                    assertTrue(entered.await(5, TimeUnit.SECONDS));
                    // No receive memory declared: one unreleased batch may queue, the next one overflows.
                    for (int i = 0; i < 2; i++) sink.get().onData(ByteBuffer.allocate(96 << 10), released::incrementAndGet);
                    assertEquals(1, released.get(), "the overflowing batch is released at once");
                } finally { proceed.countDown(); }
                assertNotNull(client.awaitClosed());
                ListenerResourcesTest.until(() -> released.get() == 2);
            } finally { client.close(); }
        }
    }
}
