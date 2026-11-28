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
    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    @Test void halvesReportHandoffInEitherOrderAndRejectReplayWhileAttached() throws Exception {
        // HS-40, HS-44.
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            try (var listener = Zmux.listener(ListenerConfig.builder().session(config).maxPendingPerSource(2).build())) {
                for (Framing framing : Framing.values()) for (boolean upFirst : new boolean[] {false, true}) {
                    var up = MemoryPipe.pair(framing); var down = MemoryPipe.pair(framing);
                    String token = token(1); // Reuse immediately after the preceding attachment retired.
                    PipeIo incoming = PipeIo.of(up[1].reader(), null).withFraming(framing), outgoing = PipeIo.of(null, down[1].writer()).withFraming(framing);
                    var first = listener.adapter().handle(upFirst ? incoming : outgoing, JoinHint.pair(token, upFirst, TestAddr.peer("client")));
                    // Both owner queues pass the first registration task before submitting its partner.
                    var registered = new CountDownLatch(2);
                    loops.group().next().execute(registered::countDown);
                    loops.group().next().execute(registered::countDown);
                    assertTrue(registered.await(5, TimeUnit.SECONDS));
                    var connecting = workers.submit(() -> Zmux.open(PipeIo.split(PipeIo.of(null, up[0].writer()).withFraming(framing),
                            PipeIo.of(down[0].reader(), null).withFraming(framing)), Role.CLIENT, config));
                    var second = listener.adapter().handle(upFirst ? outgoing : incoming, JoinHint.pair(token, !upFirst, TestAddr.peer("client")));
                    try (var client = connecting.get(5, TimeUnit.SECONDS); var server = listener.accept(Duration.ofSeconds(5))) {
                        assertNotNull(server);
                        first.awaitSetup(); second.awaitSetup();
                        assertFalse(first.closed().isDone());
                        try (var send = client.openStream()) {
                            send.writeChunk(ByteBuffer.wrap(new byte[] {7,8,9})); send.finish();
                            try (var receive = server.acceptStream(); Chunk chunk = receive.readChunk(3)) {
                                byte[] got = chunk.copy(); assertArrayEquals(new byte[] {7,8,9}, got);
                            }
                        }
                        AtomicInteger closes = new AtomicInteger();
                        PipeWriter third = new PipeWriter() {
                            @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) { fail("replayed pair wrote data"); }
                            @Override public void close() { closes.incrementAndGet(); }
                        };
                        listener.adapter().handle(PipeIo.of(null, third), JoinHint.pair(token, false, TestAddr.peer("client"))).closed()
                                .toCompletableFuture().get(5, TimeUnit.SECONDS);
                        assertEquals(1, closes.get());
                        var pipeClosed = server.pipes().getFirst().closed();
                        var held = io.zmux.internal.loop.CarrierActionsTest.holdLastLaneWrite(server.driver);
                        try {
                            var main = io.zmux.internal.loop.CarrierActionsTest.mainReleased(server.driver);
                            client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
                            main.get(5, TimeUnit.SECONDS);
                            var retry = listener.adapter().handle(PipeIo.of(null, third), JoinHint.pair(token, false, null));
                            retry.closed().get(5, TimeUnit.SECONDS);
                            assertThrows(java.io.IOException.class, retry::awaitSetup);
                            assertEquals(2, closes.get());
                            assertFalse(first.closed().isDone());
                            assertFalse(second.closed().isDone());
                            assertFalse(pipeClosed.isDone());
                        } finally { held.completed(); }
                        pipeClosed.get(5, TimeUnit.SECONDS);
                    }
                    first.closed().get(5, TimeUnit.SECONDS); second.closed().get(5, TimeUnit.SECONDS);
                }
            }
            Leaks.none(loops);
        }
    }
    @SuppressWarnings("try")
    @Test void splitParentRetainsRemovedLanesUnderlyingReadWorker() throws Exception {
        // HS-44, HS-52.
        var reading = new CountDownLatch(1);
        var closing = new CountDownLatch(1);
        var exit = new CompletableFuture<Void>();
        try (var loops = EventLoops.create(1); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var boot = Zmux.listener(ListenerConfig.builder().session(cfg).inboundTransform((io, ctx) -> {
                     if (io.writer() != null) return CompletableFuture.completedFuture(io);
                     PipeReader wrapper = new PipeReader() {
                         @Override public void start(Sink sink) { io.reader().start(sink); }
                         @Override public void close() { io.reader().close(); }
                     };
                     return CompletableFuture.completedFuture(io.withHalves(wrapper, null));
                 }).build())) {
                var connection = new Object();
                var up = MemoryPipe.pair(Framing.BYTE_STREAM);
                var down = MemoryPipe.pair(Framing.BYTE_STREAM);
                String key = token(93);
                var first = boot.adapter().handle(PipeIo.of(up[1].reader(), null).withCarrierConn(connection), JoinHint.pair(key, true, null));
                var second = boot.adapter().handle(PipeIo.of(null, down[1].writer()).withCarrierConn(connection), JoinHint.pair(key, false, null));
                var dialing = workers.submit(() -> Zmux.open(PipeIo.split(PipeIo.of(null, up[0].writer()), PipeIo.of(down[0].reader(), null)), Role.CLIENT, cfg));
                try (var client = dialing.get(5, TimeUnit.SECONDS); var server = boot.accept(Duration.ofSeconds(5))) {
                    assertNotNull(server);
                    var header = ByteBuffer.allocate(23).put((byte) 22);
                    io.zmux.internal.wire.FrameEncoder.pipeLane(header, server.core.lanes().key(), 0, 1);
                    header.flip();
                    var input = new java.nio.channels.ReadableByteChannel() {
                        @Override public int read(ByteBuffer dst) {
                            if (header.hasRemaining()) { int n = header.remaining(); dst.put(header); return n; }
                            reading.countDown(); exit.join(); return -1;
                        }
                        @Override public boolean isOpen() { return closing.getCount() != 0; }
                        @Override public void close() { closing.countDown(); }
                    };
                    var lane = boot.adapter().handle(PipeChannel.pipe(input, null, Framing.BYTE_STREAM, 64).withCarrierConn(connection), JoinHint.inBand(null));
                    lane.awaitSetup();
                    assertTrue(reading.await(5, TimeUnit.SECONDS));
                    io.zmux.internal.loop.CarrierActionsTest.resetInputLane(server.driver, 1);
                    assertTrue(closing.await(5, TimeUnit.SECONDS));
                    var released = io.zmux.internal.loop.CarrierActionsTest.mainReleased(server.driver);
                    client.close(); server.close();
                    released.get(5, TimeUnit.SECONDS);
                    var retry = boot.adapter().handle(PipeIo.of(null, MemoryPipe.pair(Framing.BYTE_STREAM)[1].writer()), JoinHint.pair(key, false, null));
                    retry.closed().get(5, TimeUnit.SECONDS);
                    assertThrows(java.io.IOException.class, retry::awaitSetup);
                    assertFalse(first.closed().isDone());
                    assertFalse(second.closed().isDone());
                    assertFalse(lane.closed().isDone());
                    exit.complete(null);
                    lane.closed().get(5, TimeUnit.SECONDS);
                    first.closed().get(5, TimeUnit.SECONDS);
                    second.closed().get(5, TimeUnit.SECONDS);
                    var fresh = boot.adapter().handle(PipeIo.of(null, MemoryPipe.pair(Framing.BYTE_STREAM)[1].writer()), JoinHint.pair(key, false, null));
                    var registered = new CountDownLatch(1);
                    loops.group().next().execute(registered::countDown);
                    assertTrue(registered.await(5, TimeUnit.SECONDS));
                    assertFalse(fresh.closed().isDone());
                    io.zmux.internal.spi.AdapterPorts.setup(fresh).interrupt();
                    fresh.closed().get(5, TimeUnit.SECONDS);
                } finally { exit.complete(null); }
            }
        }
    }

    @Test void withdrawnWaitingHalfKeepsTokenThroughPhysicalClose() throws Exception {
        try (var loops = EventLoops.create(1);
             var listener = Zmux.listener(ListenerConfig.of(ZmuxConfig.builder().eventLoops(loops).build()))) {
            String key = token(91);
            var closing = new CountDownLatch(1);
            var release = new CompletableFuture<Void>();
            PipeWriter writer = new PipeWriter() {
                @Override public void write(OutboundBatch b, Completion done) { fail("unpaired half wrote"); }
                @Override public void close() { closing.countDown(); release.join(); }
            };
            var first = listener.adapter().handle(PipeIo.of(null, writer), JoinHint.pair(key, false, null));
            var registered = new CountDownLatch(1);
            loops.group().next().execute(registered::countDown);
            assertTrue(registered.await(5, TimeUnit.SECONDS));
            io.zmux.internal.spi.AdapterPorts.setup(first).interrupt();
            try {
                assertTrue(closing.await(5, TimeUnit.SECONDS));
                var io = MemoryPipe.pair(Framing.BYTE_STREAM);
                var retry = listener.adapter().handle(PipeIo.of(null, io[1].writer()), JoinHint.pair(key, false, null));
                retry.closed().get(5, TimeUnit.SECONDS);
                assertThrows(java.io.IOException.class, retry::awaitSetup);
                assertFalse(first.closed().isDone());
            } finally { release.complete(null); }
            first.closed().get(5, TimeUnit.SECONDS);
        }
    }

    @Test void rejectedTransformedPairKeepsTokenUntilWrapperFinishesClosing() throws Exception {
        // HS-44.
        var gates = new LinkedBlockingQueue<CompletableFuture<Void>>();
        try (EventLoops loops = EventLoops.create(1);
             var listener = Zmux.listener(ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).noHandshakeTimeout().build())
                     .inboundTransform((io, context) -> {
                         var gate = new CompletableFuture<Void>();
                         PipeWriter writer = new PipeWriter() {
                             @Override public void write(OutboundBatch b, Completion done) { io.writer().write(b, done); }
                             @Override public void close() { io.writer().close(); gates.add(gate); gate.join(); }
                         };
                         return CompletableFuture.completedFuture(io.withHalves(io.reader(), writer));
                     }).build())) {
            String key = token(89);
            for (int cycle = 0; cycle < 2; cycle++) {
                var physical = new CountDownLatch(2);
                PipeReader reader = new PipeReader() {
                    @Override public void start(Sink s) { s.onLent(ByteBuffer.wrap(new byte[]{0}), () -> {}); }
                    @Override public void close() { physical.countDown(); }
                };
                PipeWriter writer = new PipeWriter() {
                    @Override public void write(OutboundBatch b, Completion done) { fail("rejected pair wrote"); }
                    @Override public void close() { physical.countDown(); }
                };
                var up = listener.adapter().handle(PipeIo.of(reader, null), JoinHint.pair(key, true, null));
                var down = listener.adapter().handle(PipeIo.of(null, writer), JoinHint.pair(key, false, null));
                var gate = gates.poll(5, TimeUnit.SECONDS);
                assertNotNull(gate);
                try {
                    assertTrue(physical.await(5, TimeUnit.SECONDS));
                    assertThrows(java.io.IOException.class, up::awaitSetup);
                    var retry = listener.adapter().handle(PipeIo.of(null, MemoryPipe.pair(Framing.BYTE_STREAM)[1].writer()), JoinHint.pair(key, false, null));
                    retry.closed().get(5, TimeUnit.SECONDS);
                    assertThrows(java.io.IOException.class, retry::awaitSetup);
                    assertFalse(up.closed().isDone());
                    assertFalse(down.closed().isDone());
                } finally { gate.complete(null); }
                up.closed().get(5, TimeUnit.SECONDS);
                down.closed().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test void refusalDrainKeepsBothSplitHandlesAndTheTokenUntilItsWriteCompletes() throws Exception {
        // HS-44.
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(2, 0)))
                    .pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).joinTimeout(Duration.ofSeconds(15)).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var c = Zmux.connect(l.localAddress(), ListenerEntryTest.config(loops)); var s = l.accept()) {
                c.addPipe(l.localAddress(), PipeOptions.defaults());
                assertEquals(2, s.pipes().size());
                var key = token(64);
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var write = new CompletableFuture<PipeWriter.Completion>();
                var down = l.adapter().handle(PipeIo.of(null, new PipeWriter() {
                    @Override public void write(OutboundBatch batch, Completion done) { write.complete(done); }
                    @Override public void close() { pair[1].writer().close(); }
                }), JoinHint.pair(key, false, null));
                var up = l.adapter().handle(PipeIo.of(pair[1].reader(), null), JoinHint.pair(key, true, null));
                var joining = workers.submit(() -> c.addPipe(pair[0], PipeOptions.defaults()));
                var pending = write.get(5, TimeUnit.SECONDS);
                try {
                    assertThrows(java.io.IOException.class, up::awaitSetup);
                    assertThrows(java.io.IOException.class, down::awaitSetup);
                    var retry = l.adapter().handle(PipeIo.of(null, MemoryPipe.pair(Framing.BYTE_STREAM)[1].writer()), JoinHint.pair(key, false, null));
                    assertThrows(java.io.IOException.class, retry::awaitSetup);
                    retry.closed().get(3, TimeUnit.SECONDS);
                    assertFalse(up.closed().isDone());
                    assertFalse(down.closed().isDone());
                } finally { pending.failed(new java.io.IOException("drain released")); }
                assertThrows(java.util.concurrent.ExecutionException.class, () -> joining.get(3, TimeUnit.SECONDS));
                up.closed().get(3, TimeUnit.SECONDS);
                down.closed().get(3, TimeUnit.SECONDS);
                var fresh = MemoryPipe.pair(Framing.BYTE_STREAM);
                var d = l.adapter().handle(PipeIo.of(null, fresh[1].writer()), JoinHint.pair(key, false, null));
                var u = l.adapter().handle(PipeIo.of(fresh[1].reader(), null), JoinHint.pair(key, true, null));
                try (var dial = Zmux.open(fresh[0], Role.CLIENT, cfg); var server = l.accept(Duration.ofSeconds(3))) {
                    d.awaitSetup(); u.awaitSetup();
                    SealTest.echo(dial, server);
                }
            }
        }
    }

    @Test void messageFramedJoinPairsInBothArrivalOrders() throws Exception {
        // HS-40.
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ListenerEntryTest.config(loops);
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config));
                 var client = Zmux.connect(listener.localAddress(), config); var server = listener.accept()) {
                for (boolean upFirst : new boolean[]{false, true}) {
                    var up = MemoryPipe.pair(Framing.MESSAGES);
                    var down = MemoryPipe.pair(Framing.MESSAGES);
                    String key = token(upFirst ? 83 : 84);
                    var reader = up[1].withHalves(up[1].reader(), null);
                    var writer = down[1].withHalves(null, down[1].writer());
                    var first = listener.adapter().handle(upFirst ? reader : writer, JoinHint.pair(key, upFirst, null));
                    var registered = new CountDownLatch(2);
                    loops.group().next().execute(registered::countDown);
                    loops.group().next().execute(registered::countDown);
                    assertTrue(registered.await(5, TimeUnit.SECONDS));
                    var adding = workers.submit(() -> client.addPipe(PipeIo.split(up[0].withHalves(null, up[0].writer()),
                            down[0].withHalves(down[0].reader(), null)), PipeOptions.defaults()));
                    var second = listener.adapter().handle(upFirst ? writer : reader, JoinHint.pair(key, !upFirst, null));
                    first.awaitSetup(); second.awaitSetup();
                    assertNotNull(adding.get(5, TimeUnit.SECONDS));
                }
                assertEquals(3, server.pipes().size());
            }
        }
    }

    @Test void handshakeTimeoutAndListenerCloseReleaseUnpairedHalves() throws Exception {
        // HS-41.
        try (EventLoops loops = EventLoops.create(1)) {
            var config = ZmuxConfig.builder().eventLoops(loops).handshakeTimeout(Duration.ofMillis(300)).build();
            var listener = Zmux.listener(ListenerConfig.builder().session(config).firstRecordTimeout(Duration.ofMillis(20)).build());
            try {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var waiting = listener.adapter().handle(PipeIo.of(pair[1].reader(), null), JoinHint.pair(token(3), true, TestAddr.peer("client"))).closed();
                waiting.toCompletableFuture().get(5, TimeUnit.SECONDS);
                AtomicInteger closes = new AtomicInteger();
                PipeWriter writer = new PipeWriter() {
                    @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) { fail("unpaired downlink wrote data"); }
                    @Override public void close() { closes.incrementAndGet(); }
                };
                var down = listener.adapter().handle(PipeIo.of(null, writer), JoinHint.pair(token(3), false, TestAddr.peer("client"))).closed();
                listener.close(); down.toCompletableFuture().get(5, TimeUnit.SECONDS);
                assertEquals(1, closes.get());
            } finally { listener.close(); }
            Leaks.none(loops);
        }
    }
    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    @Test void splitExporterBindsBothHalvesInUplinkDownlinkOrder() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var _ = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(2))).pipeAuth(PipeAuth.exporter().withEndToEnd()).noProbation().build();
            try (var listener = Zmux.listener(ListenerConfig.builder().session(config).maxPendingPerSource(2).build())) {
                PipeIo.Exporter upKey = (label, context, length) -> { byte[] key = new byte[length]; java.util.Arrays.fill(key, (byte) 3); return key; };
                PipeIo.Exporter downKey = (label, context, length) -> { byte[] key = new byte[length]; java.util.Arrays.fill(key, (byte) 9); return key; };
                var up = MemoryPipe.pair(Framing.BYTE_STREAM); var down = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.adapter().handle(PipeIo.of(up[1].reader(), null).withExporter(upKey), JoinHint.pair(token(40), true, TestAddr.peer("client"))).closed();
                listener.adapter().handle(PipeIo.of(null, down[1].writer()).withExporter(downKey), JoinHint.pair(token(40), false, TestAddr.peer("client"))).closed();
                try (var client = Zmux.open(PipeIo.split(PipeIo.of(null, up[0].writer()).withExporter(upKey),
                        PipeIo.of(down[0].reader(), null).withExporter(downKey)), Role.CLIENT, config);
                     var server = listener.accept(Duration.ofSeconds(5))) {
                    var newPipe = MemoryPipe.pair(Framing.BYTE_STREAM); listener.adapter().handle(newPipe[1], JoinHint.inBand(TestAddr.peer("client"))).closed();
                    var added = client.addPipe(newPipe[0], PipeOptions.defaults());
                    assertEquals(PipeState.Outbound.ACTIVE, added.state().orElseThrow().outbound());
                    client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
                }
            }
            SessionTestSupport.await(loops, () -> loops.outstandingBuffers() == 0);
        }
    }
    @Test void stalledPairInputHasABoundedQueueAndReleasesRejectedDeliveries() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var listener = Zmux.listener(ListenerConfig.of(ZmuxConfig.builder().eventLoops(loops).build()))) {
            var sink = new CompletableFuture<PipeReader.Sink>(); AtomicInteger released = new AtomicInteger(), closed = new AtomicInteger();
            PipeReader reader = new PipeReader() {
                @Override public void start(Sink input) { sink.complete(input); }
                @Override public void close() { closed.incrementAndGet(); }
            };
            var lifetime = listener.adapter().handle(PipeIo.of(reader, null), JoinHint.pair(token(50), true, TestAddr.peer("client"))).closed();
            PipeReader.Sink input = sink.get(5, TimeUnit.SECONDS);
            var peer = new io.zmux.internal.core.Session(io.zmux.internal.core.CoreConfig.defaults(), 0);
            var info = new io.zmux.internal.core.CarrierInfo(io.zmux.internal.core.Framing.BYTES, false, false, false, false);
            peer.pipes().attachBootstrap(info, info); var batch = new io.zmux.internal.core.Batch();
            assertTrue(peer.pollTransmit(0, 0, batch, 65536, 0));
            input.onLent(ByteBuffer.wrap(io.zmux.internal.core.BatchFixture.flat(batch)), released::incrementAndGet); batch.discard();
            CompletableFuture<Void> parsed = new CompletableFuture<>();
            loops.group().next().execute(() -> parsed.complete(null)); parsed.get(5, TimeUnit.SECONDS);
            assertEquals(0, released.get(), "complete first record must keep its delivery paused");
            for (int i=0; i<10; i++) input.onLent(ByteBuffer.allocate(32 << 10), released::incrementAndGet);
            lifetime.toCompletableFuture().get(5, TimeUnit.SECONDS);
            input.onLent(ByteBuffer.allocate(1), released::incrementAndGet);
            assertEquals(12, released.get()); assertEquals(1, closed.get());
            Leaks.none(loops);
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
            try (ZmuxSession _ = server.get(5, TimeUnit.SECONDS)) {
                var released = new CountDownLatch(2);
                var entered = new CountDownLatch(1); var proceed = new CountDownLatch(1);
                client.driver.executeCarrier(() -> { entered.countDown(); try { proceed.await(); } catch (InterruptedException ignored) { } });
                try {
                    assertTrue(entered.await(5, TimeUnit.SECONDS));
                    // No receive memory declared: one unreleased batch may queue, the next one overflows.
                    for (int i = 0; i < 2; i++) sink.get().onLent(ByteBuffer.allocate(96 << 10), released::countDown);
                    assertEquals(1, released.getCount(), "the overflowing batch is released at once");
                } finally { proceed.countDown(); }
                assertNotNull(client.awaitClosed());
                assertTrue(released.await(30, TimeUnit.SECONDS));
            } finally { client.close(); }
        }
    }

    @Test void downlinkWaiterCanLeaveAfterMatchingWhileBothDirectionsStayUsable() throws Exception {
        // HS-42.
        var entered = new CompletableFuture<PipeIo>();
        var resume = new CompletableFuture<PipeIo>();
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).noHandshakeTimeout().build();
            try (var listener = Zmux.listener(ListenerConfig.builder().session(cfg)
                    .inboundTransform((io, ctx) -> { entered.complete(io); return resume; }).build())) {
                var up = MemoryPipe.pair(Framing.BYTE_STREAM);
                var down = MemoryPipe.pair(Framing.BYTE_STREAM);
                var output = listener.adapter().handle(PipeIo.of(null, down[1].writer()), JoinHint.pair(token(11), false, null));
                var input = listener.adapter().handle(PipeIo.of(up[1].reader(), null), JoinHint.pair(token(11), true, null));
                var combined = entered.get(5, TimeUnit.SECONDS);
                var waiter = workers.submit(() -> {
                    Thread.currentThread().interrupt();
                    try { assertThrows(java.io.InterruptedIOException.class, output::awaitSetup); }
                    finally { Thread.interrupted(); }
                });
                waiter.get(5, TimeUnit.SECONDS);
                assertFalse(output.closed().isDone());
                resume.complete(combined);
                try (var client = Zmux.open(PipeIo.of(down[0].reader(), up[0].writer()), Role.CLIENT, cfg);
                     var server = listener.accept(Duration.ofSeconds(5))) {
                    input.awaitSetup();
                    assertNotNull(server);
                    client.ping(Duration.ofSeconds(5));
                    server.ping(Duration.ofSeconds(5));
                }
                output.closed().get(5, TimeUnit.SECONDS);
            }
        }
    }

}
