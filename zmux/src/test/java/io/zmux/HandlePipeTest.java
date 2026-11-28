package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class HandlePipeTest {
    @Test void cancellationBeforeTheFirstOwnerTurnNeverInvokesTheTransform() throws Exception {
        // HS-01.
        var paused = new CountDownLatch(1);
        var release = new CompletableFuture<Void>();
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        try (var loops = EventLoops.create(1);
             var listener = Zmux.listener(ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).build())
                     .maxPendingPerSource(2).inboundTransform((io, ctx) -> {
                         calls.incrementAndGet(); return CompletableFuture.completedFuture(io);
                     }).build())) {
            loops.group().next().execute(() -> { paused.countDown(); release.join(); });
            var source = TestAddr.peer("first-turn");
            var counted = new TakeoverTest.Counted();
            try {
                assertTrue(paused.await(5, TimeUnit.SECONDS));
                var h = listener.adapter().handle(counted.wrap(MemoryPipe.pair(Framing.BYTE_STREAM)[1]), JoinHint.inBand(source));
                io.zmux.internal.spi.AdapterPorts.setup(h).interrupt();
                release.complete(null);
                assertThrows(InterruptedIOException.class, h::awaitSetup);
                h.closed().get(5, TimeUnit.SECONDS);
                assertEquals(0, calls.get());
                assertEquals(1, counted.writerCloses.get());
                assertEquals(1, counted.readerCloses.get());
                assertEquals(0, listener.queued.get());
                var slot = listener.adapter().reserve(source, null);
                assertNotNull(slot);
                slot.release();
            } finally { release.complete(null); }
        }
    }

    @Test void failedUpgradeCancelsTheActiveTransformBeforeRetirement() throws Exception {
        // HS-05.
        var started = new CountDownLatch(1);
        var transform = new CompletableFuture<PipeIo>();
        var upgrade = new CompletableFuture<Void>();
        try (var loops = EventLoops.create(1);
             var listener = Zmux.listener(ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).noHandshakeTimeout().build())
                     .inboundTransform((io, context) -> { started.countDown(); return transform; }).build())) {
            var counted = new TakeoverTest.Counted();
            var handled = listener.adapter().admitFirst(counted.wrap(MemoryPipe.pair(Framing.BYTE_STREAM)[1]),
                    JoinHint.inBand(null), () -> upgrade, null);
            assertTrue(started.await(5, TimeUnit.SECONDS));
            io.zmux.internal.loop.CarrierActionsTest.advanceOpeningLinger(loops.group().next());
            upgrade.completeExceptionally(new IOException("upgrade lost"));
            assertThrows(IOException.class, handled::awaitSetup);
            handled.closed().get(5, TimeUnit.SECONDS);
            assertTrue(transform.isCancelled());
            assertEquals(1, listener.stats().setupFailures());
            var again = listener.adapter().reserve(null, null);
            assertNotNull(again);
            again.release();
            assertEquals(1, counted.readerCloses.get());
            assertEquals(1, counted.writerCloses.get());
        }
    }

    @Test void anAcceptedUpgradeCanRetireWhileItsUnacknowledgedStageIsStillPending() throws Exception {
        var never = new CompletableFuture<Void>();
        try (var loops = EventLoops.create(1); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ZmuxConfig.builder().eventLoops(loops).noHandshakeTimeout().build();
            try (var listener = Zmux.listener(ListenerConfig.of(config))) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var h = listener.adapter().admitFirst(pair[1], JoinHint.inBand(null), () -> never, null);
                var dial = workers.submit(() -> Zmux.open(pair[0], Role.CLIENT, config));
                h.awaitSetup();
                try (var client = dial.get(5, TimeUnit.SECONDS); var server = listener.accept()) {
                    io.zmux.internal.loop.CarrierActionsTest.advanceOpeningLinger(loops.group().next());
                    assertFalse(h.closed().isDone(), "expiring a bookkeeping share must not retire a live pipe");
                    try (var stream = server.openStream()) {
                        stream.write(new byte[]{17});
                        stream.finish();
                        try (var peer = client.acceptStream()) { assertArrayEquals(new byte[]{17}, peer.readAll(2)); }
                    }
                }
                h.closed().get(3, TimeUnit.SECONDS);
                assertFalse(never.isDone());
            }
        }
    }

    @Test void takeoverWaitsForTheAcceptanceHeadAndStallsWithoutAByteTarget() throws Exception {
        // HS-18.
        for (boolean target : new boolean[]{false, true}) {
            var head = new CompletableFuture<Void>();
            var started = new CountDownLatch(1);
            var served = new CompletableFuture<Object>();
            var cfg = ListenerConfig.builder().admission(Admission.builder(0, Admission.Decider.immediate(
                    (info, prefix) -> Admission.Decision.takeover("relay").withVariant("relay"))).build());
            if (target) cfg.fallback(t -> { served.complete(t.value()); return null; });
            try (var listener = Zmux.listener(cfg.build())) {
                var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                var h = listener.adapter().handleRequest(p[1], JoinHint.inBand(null), true,
                        t -> { throw new AssertionError("request fallback ran"); },
                        () -> { started.countDown(); return head; });
                if (target) {
                    assertTrue(started.await(5, TimeUnit.SECONDS));
                    assertFalse(served.isDone());
                    assertEquals(0, listener.stats().admission().takeover());
                    assertFalse(h.verdict().isDone());
                    head.complete(null);
                    assertEquals("relay", served.get(5, TimeUnit.SECONDS));
                    assertEquals(AdmissionVerdict.TAKEOVER, h.verdict().get(5, TimeUnit.SECONDS));
                    h.awaitSetup();
                } else {
                    assertEquals(AdmissionVerdict.STALL, h.verdict().get(5, TimeUnit.SECONDS));
                    assertEquals(1, started.getCount());
                    assertThrows(IOException.class, h::awaitSetup);
                }
                assertEquals(new AdmissionStats(0, 0, target ? 1 : 0, target ? 0 : 1, 0, 0, 0, 0, 0), listener.stats().admission());
                assertEquals(java.util.List.of(new Admission.VariantStat("relay", false, 0, 0, target ? 1 : 0, target ? 0 : 1, 0)), listener.variantStats());
            }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
    void returnsWithSessionQueuedAndStillUsable(boolean direct) throws Exception {
        // HS-02.
        try (EventLoops loops = EventLoops.create(1); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var listener = Zmux.listener(ListenerConfig.of(cfg))) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var dial = workers.submit(() -> Zmux.open(pair[0], Role.CLIENT, cfg));
                if (direct) listener.handlePipe(pair[1], JoinHint.inBand(null));
                else {
                    var handled = listener.adapter().handle(pair[1], JoinHint.inBand(null));
                    handled.awaitSetup();
                    io.zmux.internal.spi.AdapterPorts.setup(handled).interrupt();
                }
                assertEquals(1, listener.queued.get());
                try (var client = dial.get(5, TimeUnit.SECONDS); var server = listener.accept();
                     var sender = client.openStream()) {
                    sender.write(new byte[]{42});
                    sender.finish();
                    try (var receiver = server.acceptStream()) { assertArrayEquals(new byte[]{42}, receiver.readAll(2)); }
                    server.close(23, "handed off");
                    var end = assertInstanceOf(SessionError.ApplicationClosed.class, client.closed().get(5, TimeUnit.SECONDS));
                    assertTrue(end.remote());
                    assertEquals(23, end.code());
                    assertEquals("handed off", end.reason());
                }
            }
        }
    }

    enum CommittedResult { QUEUED_SESSION, FALLBACK_RETURNED, FALLBACK_FAILED }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(CommittedResult.class)
    void lateInterruptionDeliversTheCommittedSetup(CommittedResult result) throws Exception {
        // HS-02a. Drive the same hub and setup.await() used by handlePipe, with delivery held after commit.
        var committed = new CountDownLatch(1);
        var deliver = new CompletableFuture<Void>();
        var setup = io.zmux.internal.loop.CarrierActionsTest.beforeResultDelivery(() -> {
            committed.countDown();
            deliver.join();
        });
        var failure = new IOException("fallback failed");
        var served = new java.util.concurrent.atomic.AtomicInteger();
        try (var loops = EventLoops.create(1); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            var options = ListenerConfig.builder().session(config);
            if (result != CommittedResult.QUEUED_SESSION) {
                options.admission(Admission.builder(0, Admission.Decider.immediate((i, p) -> Admission.Decision.FALLBACK)).build())
                        .fallback(t -> {
                            served.incrementAndGet();
                            return result == CommittedResult.FALLBACK_FAILED ? CompletableFuture.failedFuture(failure) : null;
                        });
            }
            try (var listener = Zmux.listener(options.build())) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var receipt = new FutureTask<IOException>(() -> {
                    try { setup.await(); return null; }
                    catch (IOException error) { return error; }
                    finally { assertTrue(Thread.currentThread().isInterrupted()); }
                });
                var caller = Thread.ofVirtual().start(receipt);
                try {
                    Object initialWait = awaitSetupParkOrReturn(caller, null);
                    assertNotNull(initialWait, "caller returned before setup started");
                    setup.receive(pair[1]);
                    listener.hub.external(loops.group().next(), pair[1], JoinHint.inBand(null), false, null,
                            io.zmux.internal.loop.ListenerHub.Entry.Stream.CARRIER, null, setup);
                    var dial = result == CommittedResult.QUEUED_SESSION ? workers.submit(() -> Zmux.open(pair[0], Role.CLIENT, config)) : null;
                    assertTrue(committed.await(5, TimeUnit.SECONDS));
                    assertEquals(dial == null ? 0 : 1, listener.queued.get());
                    assertEquals(dial == null ? 1 : 0, served.get());
                    caller.interrupt();
                    awaitSetupParkOrReturn(caller, initialWait);
                    deliver.complete(null);
                    assertSame(result == CommittedResult.FALLBACK_FAILED ? failure : null, receipt.get(5, TimeUnit.SECONDS));
                    if (dial != null) {
                        try (var client = dial.get(5, TimeUnit.SECONDS); var server = listener.accept();
                             var stream = client.openStream()) {
                            stream.write(new byte[]{42});
                            stream.finish();
                            try (var peer = server.acceptStream()) { assertArrayEquals(new byte[]{42}, peer.readAll(2)); }
                        }
                    }
                } finally {
                    deliver.complete(null);
                    setup.refused(); // Unblock the waiter even if the fixture failed before handoff.
                }
            }
        }
    }

    /** A new CompletableFuture waiter or an early receipt proves the caller processed the interrupt. */
    private static Object awaitSetupParkOrReturn(Thread caller, Object previous) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (caller.isAlive()) {
            Object blocker = LockSupport.getBlocker(caller);
            if (blocker != null && blocker != previous && caller.getState() == Thread.State.WAITING) return blocker;
            if (System.nanoTime() >= deadline) fail("caller did not park waiting for the setup result");
            Thread.onSpinWait();
        }
        return null;
    }

    @Test void waitingCancellingAndRetiringPipesLeaveNoTerminalObservers() throws Exception {
        // HS-54.
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ListenerEntryTest.config(loops);
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var client = Zmux.connect(listener.localAddress(), cfg); var server = listener.accept()) {
                int baseline = observers(client);
                int dependents = client.driver.closedFuture().getNumberOfDependents();
                var main = client.pipes().getFirst();
                for (int i = 0; i < 16; i++) {
                    var wait = main.closed();
                    assertEquals(baseline + 1, observers(client));
                    assertTrue(wait.cancel(false));
                    assertEquals(baseline, observers(client));
                }
                var io = MemoryPipe.pair(Framing.BYTE_STREAM);
                var incoming = workers.submit(() -> { listener.handlePipe(io[1], JoinHint.inBand(null)); return true; });
                var pipe = client.addPipe(io[0], PipeOptions.defaults());
                assertTrue(incoming.get(5, TimeUnit.SECONDS));
                var retired = pipe.closed();
                assertEquals(baseline + 1, observers(client));
                pipe.abort();
                retired.get(5, TimeUnit.SECONDS);
                assertEquals(baseline, observers(client));
                assertEquals(dependents, client.driver.closedFuture().getNumberOfDependents());
                assertFalse(server.driver.closedFuture().isDone());
            }
        }
    }

    private static int observers(ZmuxSession session) throws Exception {
        var count = new CompletableFuture<Integer>();
        assertTrue(session.driver.loop().tryExecute(() -> count.complete(io.zmux.internal.core.ObserverFixture.closedWaiters(session.core))));
        return count.get(5, TimeUnit.SECONDS);
    }

    @Test void joinCanBeWithdrawnAfterTakenWhileReplyIsOutstanding() throws Exception {
        // HS-01.
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ListenerEntryTest.config(loops);
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var client = Zmux.connect(listener.localAddress(), cfg); var server = listener.accept()) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var reply = new CompletableFuture<io.zmux.carrier.PipeWriter.Completion>();
                var held = new io.zmux.carrier.PipeWriter() {
                    @Override public void write(io.zmux.carrier.OutboundBatch batch, Completion done) { reply.complete(done); }
                    @Override public void close() { pair[1].writer().close(); }
                };
                var handle = listener.adapter().handle(PipeIo.of(pair[1].reader(), held), JoinHint.inBand(null));
                var adding = workers.submit(() -> client.addPipe(pair[0], PipeOptions.defaults()));
                var completion = reply.get(5, TimeUnit.SECONDS);
                try {
                    assertTrue(handle.taken().get(5, TimeUnit.SECONDS));
                    io.zmux.internal.spi.AdapterPorts.setup(handle).interrupt();
                    assertThrows(IOException.class, handle::awaitSetup);
                } finally { completion.failed(new IOException("withdrawn")); }
                assertThrows(java.util.concurrent.ExecutionException.class, () -> adding.get(5, TimeUnit.SECONDS));
                handle.closed().get(5, TimeUnit.SECONDS);
                assertEquals(1, server.pipes().size());
            }
        }
    }

    @Test void eitherThrowingJoinAuthorizationCallbackReleasesItsSlotAndBothHalves() throws Exception {
        // HS-07.
        for (boolean allow : new boolean[]{true, false}) {
            var called = new CountDownLatch(1);
            try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                AuthorizeJoin authorize = request -> {
                    if (allow) return true;
                    called.countDown();
                    throw new IllegalStateException("authorization");
                };
                var cfg = ZmuxConfig.builder().eventLoops(loops).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                        .sessionPolicy(new SessionPolicy(0, 0, authorize, new CapacityLimits(4)))
                        .allowRecordMode((i, m, v) -> { if (!allow) return true; called.countDown(); throw new IllegalStateException("mode"); })
                        .build();
                var rejected = new java.util.concurrent.atomic.AtomicInteger();
                try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).maxPendingPerSource(2)
                        .onReject(info -> rejected.incrementAndGet()).rejection(RejectInfo.Action.STALL).build());
                     var client = Zmux.connect(listener.localAddress(), cfg); var server = listener.accept()) {
                    var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                    var counted = new TakeoverTest.Counted();
                    var handled = listener.adapter().handle(counted.wrap(pair[1]), JoinHint.inBand(null));
                    var adding = workers.submit(() -> client.addPipe(pair[0], PipeOptions.defaults()));
                    assertTrue(called.await(5, TimeUnit.SECONDS));
                    assertThrows(IOException.class, handled::awaitSetup);
                    handled.closed().get(5, TimeUnit.SECONDS);
                    assertEquals(1, counted.readerCloses.get());
                    assertEquals(1, counted.writerCloses.get());
                    var slot = listener.adapter().reserve(null, null);
                    assertNotNull(slot);
                    slot.release();
                    assertThrows(java.util.concurrent.ExecutionException.class, () -> adding.get(5, TimeUnit.SECONDS));
                    assertEquals(1, server.pipes().size());
                    assertEquals(0, rejected.get());
                }
            }
        }
    }

    @Test void refusedReplyWithAStalledWriterIsClosedAtTheJoinDeadline() throws Exception {
        // HS-04.
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(2, 0)))
                    .pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).joinTimeout(java.time.Duration.ofMillis(850)).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var client = Zmux.connect(listener.localAddress(), ListenerEntryTest.config(loops)); var server = listener.accept()) {
                var accepted = MemoryPipe.pair(Framing.BYTE_STREAM);
                var first = listener.adapter().handle(accepted[1], JoinHint.inBand(null));
                client.addPipe(accepted[0], PipeOptions.defaults());
                first.awaitSetup();
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var writing = new CompletableFuture<io.zmux.carrier.PipeWriter.Completion>();
                var writer = new io.zmux.carrier.PipeWriter() {
                    @Override public void write(io.zmux.carrier.OutboundBatch batch, Completion done) { writing.complete(done); }
                    @Override public void close() {
                        writing.join().failed(new IOException("closed"));
                        pair[1].writer().close();
                    }
                };
                var counted = new TakeoverTest.Counted();
                var handled = listener.adapter().handle(counted.wrap(PipeIo.of(pair[1].reader(), writer)), JoinHint.inBand(null));
                var adding = workers.submit(() -> client.addPipe(pair[0], PipeOptions.defaults()));
                writing.get(5, TimeUnit.SECONDS);
                assertThrows(IOException.class, handled::awaitSetup);
                handled.closed().get(4, TimeUnit.SECONDS);
                assertEquals(1, counted.readerCloses.get());
                assertEquals(1, counted.writerCloses.get());
                assertEquals(2, server.pipes().size());
                assertThrows(java.util.concurrent.ExecutionException.class, () -> adding.get(5, TimeUnit.SECONDS));
            }
        }
    }

    @SuppressWarnings("try") // Stop the accepting listener while the lane target is paused.
    @Test void closingListenerWithdrawsLaneQueuedOnAnotherSessionsLoop() throws Exception {
        // HS-04, HS-31, HS-52, HS-58.
        try (EventLoops serving = EventLoops.create(1); EventLoops arriving = EventLoops.create(1);
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ListenerEntryTest.config(serving);
            try (var boot = Zmux.listener(ListenerConfig.of(cfg));
                 var lanes = Zmux.listener(ListenerConfig.of(ListenerEntryTest.config(arriving)))) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var connection = new Object();
                var dialing = workers.submit(() -> Zmux.open(pair[0], Role.CLIENT, cfg));
                boot.handlePipe(pair[1].withCarrierConn(connection), JoinHint.inBand(null));
                try (var client = dialing.get(5, TimeUnit.SECONDS); var server = boot.accept()) {
                    var key = server.core.lanes().key();
                    boot.drain("existing lanes continue");
                    for (int i = 0; i <= 2; i++) {
                        var started = new CompletableFuture<PipeReader.Sink>();
                        PipeReader reader = new PipeReader() {
                            @Override public void start(Sink s) { started.complete(s); }
                            @Override public void close() {}
                        };
                        // A foreign listener sees the same key; only boot is allowed to publish this lane.
                        var hub = (i == 0 ? lanes : boot).hub;
                        var handled = hub.reserve(null);
                        assertNotNull(handled);
                        assertTrue(handled.arrive());
                        hub.external(arriving.group().next(), PipeIo.of(reader, null).withCarrierConn(connection),
                                JoinHint.inBand(null), false, null, io.zmux.internal.loop.ListenerHub.Entry.Stream.CARRIER, null, handled);
                        var sink = started.get(5, TimeUnit.SECONDS);
                        var resume = new CompletableFuture<Void>();
                        if (i == 2) {
                            var paused = new CountDownLatch(1);
                            server.driver.executeCarrier(() -> { paused.countDown(); resume.join(); });
                            assertTrue(paused.await(5, TimeUnit.SECONDS));
                        }
                        try {
                            var bytes = java.nio.ByteBuffer.allocate(23).put((byte) 22);
                            io.zmux.internal.wire.FrameEncoder.pipeLane(bytes, key, 0, Math.max(1, i));
                            sink.onLent(bytes.flip(), () -> {});
                            var transferred = new CountDownLatch(1);
                            arriving.group().next().execute(transferred::countDown);
                            assertTrue(transferred.await(5, TimeUnit.SECONDS));
                            if (i == 2) boot.close();
                        } finally { resume.complete(null); }
                        if (i == 1) {
                            handled.await();
                            io.zmux.internal.loop.CarrierActionsTest.resetInputLane(server.driver, i);
                            handled.closed().get(5, TimeUnit.SECONDS);
                            try (var sender = client.openStream()) {
                                sender.write(new byte[]{12});
                                sender.finish();
                                try (var receiver = server.acceptStream()) { assertArrayEquals(new byte[]{12}, receiver.readAll(2)); }
                            }
                        }
                        else { assertThrows(IOException.class, handled::await); handled.closed().get(5, TimeUnit.SECONDS); }
                    }
                    var ended = new CountDownLatch(1);
                    Zmux.handleLane(PipeIo.of(new PipeReader() {
                        @Override public void start(Sink sink) {
                            var b = java.nio.ByteBuffer.allocate(23).put((byte) 22);
                            io.zmux.internal.wire.FrameEncoder.pipeLane(b, key, 0, 3);
                            sink.onLent(b.flip(), () -> {});
                        }
                        @Override public void close() { ended.countDown(); }
                    }, null).withCarrierConn(connection));
                    assertTrue(ended.await(5, TimeUnit.SECONDS), "process-wide lane survived the owning listener's stop");
                    assertEquals(0, lanes.stats().joins());
                    assertEquals(1, boot.stats().joins());
                }
            }
        }
    }

    @Test void readOnlySessionParametersCannotPublishABootstrap() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var workers = Executors.newVirtualThreadPerTaskExecutor();
             var listener = Zmux.listener(ListenerConfig.builder().session(ListenerEntryTest.config(loops)).maxPendingPerSource(2).build())) {
            var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
            var counted = new TakeoverTest.Counted();
            var dialing = workers.submit(() -> Zmux.open(pair[0], Role.CLIENT, listener.config()));
            try {
                assertThrows(IOException.class, () -> listener.handlePipe(counted.wrap(PipeIo.of(pair[1].reader(), null)), JoinHint.inBand(null)));
                assertEquals(0, listener.queued.get());
                var slot = listener.adapter().reserve(null, null);
                assertNotNull(slot);
                slot.release();
            } finally { pair[1].writer().close(); dialing.cancel(true); }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void exporterFailureOrCancellationReleasesCarrierAndSourceSlot(boolean withdraw) throws Exception {
        // HS-01.
        try (EventLoops loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ListenerEntryTest.config(loops);
            var lc = ListenerConfig.builder().session(cfg).maxPendingPerSource(2).build();
            try (var listener = Zmux.listen(NetworkTest.any(), lc);
                 var client = Zmux.connect(listener.localAddress(), cfg); var server = listener.accept()) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var counted = new TakeoverTest.Counted();
                var called = new CountDownLatch(1);
                var resume = new CompletableFuture<Void>();
                var incoming = counted.wrap(pair[1]).withExporter((label, context, length) -> {
                    called.countDown();
                    if (withdraw) { resume.join(); return new byte[length]; }
                    throw new IllegalStateException("export failed");
                }).withCarrierSecure();
                var hint = JoinHint.inBand(null);
                var handled = listener.adapter().handle(incoming, hint);
                var adding = workers.submit(() -> client.addPipe(pair[0].withExporter((l, c, n) -> new byte[n]).withCarrierSecure(),
                        PipeOptions.builder().recordMode(RecordMode.BOUND).build()));
                assertTrue(called.await(5, TimeUnit.SECONDS));
                if (withdraw) {
                    io.zmux.internal.spi.AdapterPorts.setup(handled).interrupt();
                    resume.complete(null);
                    assertThrows(InterruptedIOException.class, handled::awaitSetup);
                } else assertThrows(IOException.class, handled::awaitSetup);
                handled.closed().get(5, TimeUnit.SECONDS);
                assertEquals(1, counted.readerCloses.get());
                assertEquals(1, counted.writerCloses.get());
                var slot = listener.adapter().reserve(null, null);
                assertNotNull(slot);
                slot.release();
                assertThrows(java.util.concurrent.ExecutionException.class, () -> adding.get(5, TimeUnit.SECONDS));
                assertEquals(1, server.pipes().size());
            }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
    void withdrawalBeforeQueuedFallbackSkipsTheHostCall(boolean takeover) throws Exception {
        // HS-12.
        var decision = new CompletableFuture<Admission.Decision>();
        var deciding = new CountDownLatch(1);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        try (EventLoops loops = EventLoops.create(1);
             var listener = Zmux.listener(ListenerConfig.builder().session(ListenerEntryTest.config(loops))
                     .admission(Admission.builder(0, (i, p, deadline) -> { deciding.countDown(); return decision; }).build())
                     .fallback(t -> { calls.incrementAndGet(); t.detach(); return null; }).build())) {
            var counted = new TakeoverTest.Counted();
            var handled = listener.adapter().handle(counted.wrap(MemoryPipe.pair(Framing.BYTE_STREAM)[1]), JoinHint.inBand(null));
            assertTrue(deciding.await(5, TimeUnit.SECONDS));
            var paused = new CountDownLatch(1);
            var resume = new CompletableFuture<Void>();
            loops.group().next().execute(() -> { paused.countDown(); resume.join(); });
            try {
                assertTrue(paused.await(5, TimeUnit.SECONDS));
                decision.complete(takeover ? Admission.Decision.TAKEOVER : Admission.Decision.FALLBACK);
                io.zmux.internal.spi.AdapterPorts.setup(handled).interrupt();
            } finally { resume.complete(null); }
            assertEquals(AdmissionVerdict.CLOSE, handled.verdict().get(5, TimeUnit.SECONDS));
            assertThrows(InterruptedIOException.class, handled::awaitSetup);
            handled.closed().get(5, TimeUnit.SECONDS);
            assertEquals(0, calls.get());
            assertEquals(1, listener.stats().admission().closed());
            assertEquals(0, listener.stats().admission().takeover());
            assertEquals(0, listener.stats().admission().fallback());
            assertEquals(1, counted.readerCloses.get());
            assertEquals(1, counted.writerCloses.get());
        }
    }

    @Test void interruptionSettlesPendingFallbackWithOrWithoutDetach() throws Exception {
        // HS-02a, HS-12.
        for (boolean detach : new boolean[]{true, false}) {
            var entered = new CompletableFuture<Takeover>();
            var never = new CompletableFuture<Void>();
            try (var workers = Executors.newVirtualThreadPerTaskExecutor(); EventLoops loops = EventLoops.create(1);
                 var listener = Zmux.listener(ListenerConfig.builder().session(ListenerEntryTest.config(loops))
                         .admission(Admission.builder(0, Admission.Decider.immediate((i, p) -> Admission.Decision.FALLBACK)).build())
                         .fallback(t -> { if (detach) t.detach(); entered.complete(t); return never; }).build())) {
                var counted = new TakeoverTest.Counted();
                var handled = listener.adapter().handle(counted.wrap(MemoryPipe.pair(Framing.BYTE_STREAM)[1]), JoinHint.inBand(null));
                var host = entered.get(5, TimeUnit.SECONDS);
                var setup = io.zmux.internal.spi.AdapterPorts.setup(handled);
                var removed = new CountDownLatch(1);
                setup.onFinished(removed::countDown);
                var second = workers.submit(() -> assertThrows(InterruptedIOException.class, handled::awaitSetup));
                var first = workers.submit(() -> {
                    Thread.currentThread().interrupt();
                    return assertThrows(InterruptedIOException.class, handled::awaitSetup);
                });
                first.get(5, TimeUnit.SECONDS);
                second.get(5, TimeUnit.SECONDS);
                assertTrue(removed.await(5, TimeUnit.SECONDS));
                if (detach) {
                    assertEquals(0, counted.readerCloses.get() + counted.writerCloses.get());
                    host.pipe().reader().close();
                    host.pipe().writer().close();
                }
                handled.closed().get(5, TimeUnit.SECONDS);
                assertEquals(1, counted.readerCloses.get());
                assertEquals(1, counted.writerCloses.get());
                assertFalse(never.isDone());
            }
        }
    }

    @SuppressWarnings("try") // Exercise listener close while its owner is already running shutdown hooks.
    @Test void theOwnersShutdownHookAloneClosesARegisteredPendingTransport() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var workers = Executors.newVirtualThreadPerTaskExecutor();
             var listener = Zmux.listener(ListenerConfig.of(ListenerEntryTest.config(loops)))) {
            var stopping = new CountDownLatch(1);
            var resume = new CompletableFuture<Void>();
            var owner = loops.group().next();
            owner.execute(() -> owner.onShutdown(() -> { stopping.countDown(); resume.join(); }));
            var started = new CountDownLatch(1);
            var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
            var counted = new TakeoverTest.Counted();
            PipeReader reader = new PipeReader() {
                @Override public void start(Sink s) { pair[1].reader().start(s); started.countDown(); }
                @Override public void close() { pair[1].reader().close(); }
            };
            var handled = listener.adapter().handle(counted.wrap(pair[1].withHalves(reader, pair[1].writer())), JoinHint.inBand(null));
            assertTrue(started.await(5, TimeUnit.SECONDS));
            var ended = new CountDownLatch(1);
            io.zmux.internal.spi.AdapterPorts.setup(handled).onFinished(ended::countDown);
            var closing = workers.submit(loops::close);
            try {
                assertTrue(stopping.await(5, TimeUnit.SECONDS));
                listener.close();
                assertEquals(1, ended.getCount());
            } finally { resume.complete(null); }
            closing.get(5, TimeUnit.SECONDS);
            assertThrows(IOException.class, handled::awaitSetup);
            handled.closed().get(5, TimeUnit.SECONDS);
            assertEquals(1, counted.readerCloses.get());
            assertEquals(1, counted.writerCloses.get());
        }
    }

    @Test void interruptWithdrawsPendingInputAndClosesItOnce() throws Exception {
        // HS-01, HS-02a.
        try (EventLoops loops = EventLoops.create(1);
             var listener = Zmux.listener(ListenerConfig.of(ZmuxConfig.builder().eventLoops(loops).build()))) {
            var started = new CountDownLatch(1);
            var closes = new java.util.concurrent.atomic.AtomicInteger();
            PipeReader reader = new PipeReader() {
                @Override public void start(Sink sink) { started.countDown(); }
                @Override public void close() { closes.incrementAndGet(); }
            };
            var handle = listener.adapter().handle(PipeIo.of(reader, null), JoinHint.inBand(null));
            var result = new CompletableFuture<Throwable>();
            Thread waiter = Thread.ofVirtual().start(() -> {
                try { handle.awaitSetup(); result.complete(null); }
                catch (IOException e) { result.complete(e); }
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            waiter.interrupt();
            assertInstanceOf(InterruptedIOException.class, result.get(5, TimeUnit.SECONDS));
            handle.closed().get(5, TimeUnit.SECONDS);
            assertEquals(1, closes.get());
        }
    }

    @SuppressWarnings("try") // Exercise listener shutdown with the combined handshake pending.
    @Test void pairedTransformIsCancelledByBothRequestsOrListenerClose() throws Exception {
        // HS-43.
        for (boolean stopListener : new boolean[]{false, true}) {
            var entered = new CountDownLatch(1);
            var handshake = new CompletableFuture<PipeIo>();
            try (EventLoops loops = EventLoops.create(2);
                 var listener = Zmux.listener(ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).build())
                         .inboundTransform((io, context) -> { entered.countDown(); return handshake; }).build())) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var token = SplitBootstrapTest.token(88);
                var down = listener.adapter().handle(PipeIo.of(null, pair[1].writer()), JoinHint.pair(token, false, null));
                var up = listener.adapter().handle(PipeIo.of(pair[1].reader(), null), JoinHint.pair(token, true, null));
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                io.zmux.internal.spi.AdapterPorts.setup(up).interrupt();
                var barrier = new CountDownLatch(2);
                loops.group().next().execute(barrier::countDown);
                loops.group().next().execute(barrier::countDown);
                assertTrue(barrier.await(5, TimeUnit.SECONDS));
                assertFalse(handshake.isCancelled());
                if (stopListener) listener.close(); else io.zmux.internal.spi.AdapterPorts.setup(down).interrupt();
                assertThrows(IOException.class, down::awaitSetup);
                assertThrows(IOException.class, up::awaitSetup);
                assertThrows(java.util.concurrent.CancellationException.class, () -> handshake.get(3, TimeUnit.SECONDS));
                assertEquals(0, listener.queued.get());
                down.closed().get(5, TimeUnit.SECONDS);
                up.closed().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test void detachedFallbackEndsSetupWithItsStage() throws Exception {
        // HS-10.
        try (EventLoops loops = EventLoops.create(1)) {
            var supplied = new CompletableFuture<Takeover>();
            var reply = new CompletableFuture<Void>();
            var config = ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).build())
                    .admission(Admission.builder(0, Admission.Decider.immediate((info, prefix) -> Admission.Decision.fallback(null))).build())
                    .fallback(t -> { t.detach(); supplied.complete(t); return reply; }).build();
            try (var listener = Zmux.listener(config)) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var handled = listener.adapter().handle(pair[1], JoinHint.inBand(null));
                var fallback = supplied.get(5, TimeUnit.SECONDS);
                reply.complete(null);
                handled.awaitSetup();
                assertFalse(handled.closed().isDone());
                fallback.pipe().reader().close();
                fallback.pipe().writer().close();
                handled.closed().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @SuppressWarnings("try")
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
    void asynchronousWriterFlushIsAbortedAtTheLingerBound(boolean acknowledgesRelease) throws Exception {
        // HS-02, HS-50.
        var released = new CompletableFuture<Void>();
        var closing = new CountDownLatch(1);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var pipe = MemoryPipe.pair(Framing.BYTE_STREAM);
        class Flush implements io.zmux.carrier.PipeWriter {
            @Override public void write(io.zmux.carrier.OutboundBatch b, Completion c) { pipe[1].writer().write(b, c); }
            @Override public void close() { closing.countDown(); }
            @Override public void abort(Throwable cause) {
                calls.incrementAndGet();
                pipe[1].writer().abort(cause);
                released.complete(null);
            }
        }
        class Acknowledged extends Flush implements io.zmux.internal.spi.AdapterPorts.ReleasingHalf {
            @Override public CompletableFuture<Void> released() { return released; }
        }
        io.zmux.internal.diag.Diagnostics.lingerForTests(java.time.Duration.ofMillis(50));
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var listener = Zmux.listener(ListenerConfig.of(cfg))) {
                var opening = workers.submit(() -> Zmux.open(pipe[0], Role.CLIENT, cfg));
                var h = listener.adapter().handle(PipeIo.of(pipe[1].reader(), acknowledgesRelease ? new Acknowledged() : new Flush()), JoinHint.inBand(null));
                h.awaitSetup();
                try (var client = opening.get(5, TimeUnit.SECONDS); var server = listener.accept()) {
                    server.close();
                    assertTrue(closing.await(2, TimeUnit.SECONDS));
                    h.closed().get(1, TimeUnit.SECONDS);
                    assertEquals(1, calls.get());
                }
            }
        } finally { released.complete(null); io.zmux.internal.diag.Diagnostics.lingerForTests(java.time.Duration.ZERO); }
    }


    @Test void physicalLossAfterHelloBeforePlanningReturnsItsSlotOnce() throws Exception {
        var authorized = new CountDownLatch(1);
        var resume = new CompletableFuture<Void>();
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops)
                    .pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                    .sessionPolicy(new SessionPolicy(0, 0, request -> { authorized.countDown(); resume.join(); return true; }, new CapacityLimits(4))).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).maxPendingPerSource(2).build());
                 var client = Zmux.connect(listener.localAddress(), cfg); var server = listener.accept()) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var handled = listener.adapter().handle(pair[1], JoinHint.inBand(null));
                var adding = workers.submit(() -> client.addPipe(pair[0], PipeOptions.defaults()));
                try {
                    assertTrue(authorized.await(5, TimeUnit.SECONDS));
                    io.zmux.internal.loop.CarrierActionsTest.loseCarrier(handled);
                } finally { resume.complete(null); }
                assertThrows(IOException.class, handled::awaitSetup);
                handled.closed().get(5, TimeUnit.SECONDS);
                assertThrows(java.util.concurrent.ExecutionException.class, () -> adding.get(5, TimeUnit.SECONDS));
                var advanced = new CountDownLatch(1);
                server.driver.loop().execute(advanced::countDown);
                assertTrue(advanced.await(5, TimeUnit.SECONDS));
                assertEquals(1, listener.stats().refused() + listener.stats().rejected());
                var slot = listener.adapter().reserve(null, null);
                assertNotNull(slot);
                slot.release();
            }
        } finally { resume.complete(null); }
    }

    @Test void closingTheAcceptQueueBeforeOrAfterPublicationUsesTheSameCode() throws Exception {
        // HS-35.
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            for (boolean alreadyClosed : new boolean[]{true, false}) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                try (var listener = Zmux.listener(ListenerConfig.of(cfg))) {
                    if (alreadyClosed) listener.closeAccept();
                    var opening = workers.submit(() -> Zmux.open(pair[0], Role.CLIENT, cfg));
                    var handled = listener.adapter().handle(pair[1], JoinHint.inBand(null));
                    if (alreadyClosed) assertThrows(IOException.class, handled::awaitSetup);
                    else {
                        handled.awaitSetup();
                        listener.closeAccept();
                    }
                    try (var peer = opening.get(5, TimeUnit.SECONDS)) {
                        var closed = assertInstanceOf(SessionError.ApplicationClosed.class, peer.closed().get(5, TimeUnit.SECONDS));
                        assertEquals(0, closed.code());
                        assertEquals("listener closed", closed.reason());
                    }
                }
            }
        }
    }

    @Test void requestEntryRequiresAnAcceptanceHeadOperation() throws Exception {
        // HS-17.
        var methods = java.util.Arrays.stream(io.zmux.adapter.ListenerAdapter.class.getDeclaredMethods())
                .filter(m -> m.getName().equals("handleRequest")).toList();
        assertEquals(1, methods.size());
        assertEquals(io.zmux.adapter.Upgrade.class, methods.getFirst().getParameterTypes()[4]);
        var request = io.zmux.internal.loop.ListenerHub.Entry.Request.class.getRecordComponents();
        assertEquals(io.zmux.adapter.Upgrade.class, request[2].getType());
        // An explicit Java null is still possible in any reference-typed parameter.
        try (var l = Zmux.listener(ListenerConfig.builder().maxPendingPerSource(2).fallback(t -> { throw new AssertionError("headless takeover"); })
                .admission(Admission.builder(0, Admission.Decider.immediate((i,p) -> Admission.Decision.TAKEOVER)).build()).build())) {
            for (boolean reserved : new boolean[]{true, false}) {
                var closed = new CountDownLatch(2);
                var closes = new java.util.concurrent.atomic.AtomicInteger();
                var io = PipeIo.of(new PipeReader() {
                    @Override public void start(Sink sink) { fail("invalid request read"); }
                    @Override public void close() { closes.incrementAndGet(); closed.countDown(); }
                }, new io.zmux.carrier.PipeWriter() {
                    @Override public void write(io.zmux.carrier.OutboundBatch b, Completion c) { fail("invalid request wrote"); }
                    @Override public void close() { closes.incrementAndGet(); closed.countDown(); }
                });
                var source = TestAddr.peer("headless");
                var hint = JoinHint.inBand(source);
                if (reserved) {
                    var slot = l.adapter().reserve(source, null);
                    assertNotNull(slot);
                    assertThrows(NullPointerException.class, () -> slot.handleRequest(io, hint, null, null));
                } else assertThrows(NullPointerException.class, () -> l.adapter().handleRequest(io, hint, true, null, null));
                assertTrue(closed.await(3, TimeUnit.SECONDS));
                assertEquals(2, closes.get());
                var again = l.adapter().reserve(source, null);
                assertNotNull(again);
                again.release();
            }
            assertEquals(0, l.stats().admission().handshakeFailed());
            assertEquals(0, l.stats().admission().takeover());
        }
    }

    @Test void shutdownWhileBootstrapEstablishmentIsPendingUsesZero() throws Exception {
        // HS-35. SessionRuntime writes parameters before publishing its ESTABLISHED event.
        var writing = new CountDownLatch(1);
        var release = new CompletableFuture<Void>();
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var l = Zmux.listener(ListenerConfig.of(cfg))) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var io = pair[1].withHalves(pair[1].reader(), new io.zmux.carrier.PipeWriter() {
                    @Override public void write(io.zmux.carrier.OutboundBatch batch, Completion done) {
                        writing.countDown(); release.join(); pair[1].writer().write(batch, done);
                    }
                    @Override public void close() { pair[1].writer().close(); }
                });
                var h = l.adapter().handle(io, JoinHint.inBand(null));
                var dial = workers.submit(() -> Zmux.open(pair[0], Role.CLIENT, cfg));
                try {
                    assertTrue(writing.await(3, TimeUnit.SECONDS));
                    assertEquals(0, l.queued.get());
                    assertEquals(1, l.hub.registered().size());
                    var stopped = l.shutdownAsync(23, "already accepted");
                    release.complete(null);
                    assertThrows(IOException.class, h::awaitSetup);
                    try (var peer = dial.get(3, TimeUnit.SECONDS)) {
                        var end = assertInstanceOf(SessionError.ApplicationClosed.class, peer.closed().get(3, TimeUnit.SECONDS));
                        assertTrue(end.remote());
                        assertEquals(0, end.code());
                        assertEquals("listener closed", end.reason());
                    }
                    stopped.get(5, TimeUnit.SECONDS);
                } finally { release.complete(null); }
            }
        }
    }

    @Test void admissionHandlesAreBoundToTheirIssuingListener() {
        // HS-58. Only the issuing port constructs and consumes either public capability.
        for (var type : java.util.List.of(io.zmux.adapter.Slot.class, io.zmux.adapter.Handled.class)) {
            assertTrue(java.lang.reflect.Modifier.isFinal(type.getModifiers()));
            assertEquals(0, type.getConstructors().length);
            assertTrue(java.util.Arrays.stream(type.getDeclaredFields()).anyMatch(field ->
                    field.getType() == io.zmux.internal.spi.AdapterPorts.Listener.class
                            && java.lang.reflect.Modifier.isPrivate(field.getModifiers()) && java.lang.reflect.Modifier.isFinal(field.getModifiers())));
            for (var method : type.getMethods()) for (var parameter : method.getParameterTypes()) {
                assertNotEquals(ZmuxListener.class, parameter);
                assertNotEquals(io.zmux.adapter.ListenerAdapter.class, parameter);
                assertNotEquals(io.zmux.internal.spi.AdapterPorts.Listener.class, parameter);
            }
            for (var method : io.zmux.adapter.ListenerAdapter.class.getMethods())
                assertFalse(java.util.Arrays.asList(method.getParameterTypes()).contains(type));
        }
    }

    @Test void shutdownUsesTheSuppliedCodeOnlyForAcceptedSessions() throws Exception {
        // HS-35.
        try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().build());
             var acceptedPeer = Zmux.connect(listener.localAddress()); var accepted = listener.accept();
             var queuedPeer = Zmux.connect(listener.localAddress())) {
            var done = listener.shutdownAsync(23, "draining");
            var a = assertInstanceOf(SessionError.ApplicationClosed.class, acceptedPeer.closed().get(5, TimeUnit.SECONDS));
            var b = assertInstanceOf(SessionError.ApplicationClosed.class, queuedPeer.closed().get(5, TimeUnit.SECONDS));
            assertEquals(23, a.code());
            assertEquals("draining", a.reason());
            assertEquals(0, b.code());
            assertEquals("listener closed", b.reason());
            assertNotNull(accepted.awaitClosed());
            done.get(5, TimeUnit.SECONDS);
        }
    }

    @Test void throwingFallbackReturnHookCompletesSetupAndRetiresBothHalves() throws Exception {
        // HS-07.
        var failure = new IllegalStateException("host stage cannot register completion");
        var stage = new CompletableFuture<Void>() {
            @Override public CompletableFuture<Void> whenComplete(java.util.function.BiConsumer<? super Void, ? super Throwable> hook) {
                throw failure;
            }
        };
        try (var l = Zmux.listener(ListenerConfig.builder().maxPendingPerSource(2)
                .admission(Admission.builder(0, Admission.Decider.immediate((i,p) -> Admission.Decision.FALLBACK)).build())
                .fallback(t -> stage).build())) {
            var counted = new TakeoverTest.Counted();
            var source = TestAddr.peer("return-hook");
            var h = l.adapter().handle(counted.wrap(MemoryPipe.pair(Framing.BYTE_STREAM)[1]), JoinHint.inBand(source));
            assertSame(failure, assertThrows(IOException.class, h::awaitSetup).getCause());
            h.closed().get(3, TimeUnit.SECONDS);
            assertEquals(1, counted.readerCloses.get());
            assertEquals(1, counted.writerCloses.get());
            var slot = l.adapter().reserve(source, null);
            assertNotNull(slot);
            slot.release();
        }
    }

    @Test void fallbackReturnIsTheSetupOutcome() throws Exception {
        // HS-07, HS-10, HS-11.
        for (boolean succeeds : new boolean[]{true, false}) {
            var failure = new IOException("host failed");
            try (var loops = EventLoops.create(1);
                 var listener = Zmux.listener(ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).build())
                         .admission(Admission.builder(0, Admission.Decider.immediate((i, p) -> Admission.Decision.FALLBACK)).build())
                         .fallback(t -> succeeds ? CompletableFuture.completedFuture(null) : CompletableFuture.failedFuture(failure)).build())) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                if (succeeds) listener.handlePipe(pair[1], JoinHint.inBand(null));
                else assertSame(failure, assertThrows(IOException.class, () -> listener.handlePipe(pair[1], JoinHint.inBand(null))));
            }
        }
    }

}
