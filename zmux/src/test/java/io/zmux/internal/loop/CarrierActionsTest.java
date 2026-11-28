package io.zmux.internal.loop;

import io.zmux.internal.memory.MemoryBudget;import io.zmux.internal.buffer.ReadBuf;

import io.zmux.internal.spi.AttachmentRelease;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.internal.core.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
public class CarrierActionsTest {
    static class Carrier extends AbstractTransport {
        final int direction;
        final TransportInfo info;
        boolean stoppedInput, stoppedOutput, closed;
        Carrier(SessionHost loop, int direction, Object connection, long bytes) {
            super(loop); this.direction = direction;
            info = new TransportInfo(new CarrierInfo(Framing.BYTES, false, false, false, false), "", connection, bytes, 0);
        }
        @Override public TransportInfo info() { return info; }
        @Override public long privateWorkspace() { return 0; }
        @Override public int directions() { return direction; }
        @Override public boolean writable() { return false; }
        @Override public boolean isClosed() { return closed; }
        @Override protected void begin() {}
        @Override protected boolean accepting() { return false; }
        @Override protected void inputFailed(java.io.IOException cause) {}
        @Override protected void send(io.zmux.carrier.OutboundBatch b) { fail("test carrier must not write"); }
        @Override public void shutdownInput() { stoppedInput = true; }
        @Override public void shutdownOutput() { stoppedOutput = true; }
        @Override void closePhysical(CarrierSetup.Bound.Permit permit, Throwable cause) { closed = true; }
    }
    /** A fresh carrier whose settle callback gates the gap between the terminal transition and result delivery. */
    public static CarrierSetup beforeResultDelivery(Runnable settled) {
        var setup = new CarrierSetup();
        setup.onSettled(settled);
        return setup;
    }

    /** Advance the real timer heap of this listener's selected host without waiting for wall time. */
    public static void advanceOpeningLinger(EventLoop loop) throws Exception {
        ExternalTransportTest.run(loop, () -> loop.runTimers(Time.add(loop.timerNow(), Session.MAX_LINGER)));
    }

    @Test void lostCandidateRunsItsSessionAbortAfterTheCurrentOwnerTurn() throws Exception {
        var loop = new EventLoop("candidate-unwind", true, BufferPool.DEFAULT_IDLE_BYTES);
        var called = new java.util.concurrent.atomic.AtomicBoolean();
        var setup = new CarrierSetup();
        try {
            ExternalTransportTest.run(loop, () -> {
                var carrier = new Carrier(loop, Action.DIR_BIDI, null, 0);
                setup.own(() -> carrier);
                setup.phase(loop, () -> called.set(true));
                var binding = setup.attach(carrier, null);
                setup.transportClosed(carrier);
                binding.close();
                assertFalse(called.get());
                assertFalse(setup.closed().isDone());
            });
            setup.closed().get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertTrue(called.get());
        } finally { loop.close(); }
    }

    @Test void failureDuringCarrierCleanupDoesNotRootTheSession() throws Exception {
        var runtime = failDuringCarrierCleanup();
        for (int attempt = 0; attempt < 200 && !runtime.refersTo(null); attempt++) {
            System.gc();
            Thread.sleep(20);
        }
        assertTrue(runtime.refersTo(null), "unreachable session retained by its carrier watcher");
    }

    private static java.lang.ref.WeakReference<SessionRuntime> failDuringCarrierCleanup() throws Exception {
        var loop = new EventLoop("carrier-watch-cleanup-failure", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            var reference = new java.util.concurrent.atomic.AtomicReference<java.lang.ref.WeakReference<SessionRuntime>>();
            ExternalTransportTest.run(loop, () -> {
                var driver = new SessionRuntime(loop, new Session(CoreConfig.defaults(), Clock.now()), DriverHooks.NONE);
                var carriers = driver.carriers();
                var unsubscribeCalled = new java.util.concurrent.atomic.AtomicBoolean();
                var first = new Carrier(loop, Action.DIR_OUT, null, 0) {
                    @Override public Runnable watchCarrier(Runnable changed) {
                        return () -> {
                            unsubscribeCalled.set(true);
                            throw new IllegalStateException("watcher unsubscribe failed");
                        };
                    }
                };
                var second = new Carrier(loop, Action.DIR_OUT, null, 0) {
                    @Override public Runnable watchCarrier(Runnable changed) { return () -> fail("cleanup must stop at the first carrier"); }
                };
                carriers.attach(first, 0, 0, null);
                carriers.attach(second, 1, 0, null);
                driver.failInternalNow(new IllegalStateException("session failure"));
                assertTrue(unsubscribeCalled.get());
                assertTrue(driver.finished());
                assertSame(second, carriers.get(0).t, "the remaining watcher must still be attached");
                reference.set(new java.lang.ref.WeakReference<>(driver));
            });
            return reference.get();
        } finally { loop.close(); }
    }

    @Test void reentrantSessionEndCannotSealOutACarrierBeingRemoved() throws Exception {
        var loop = new EventLoop("reentrant-release", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            ExternalTransportTest.run(loop, () -> {
                var runtime = new SessionRuntime(loop, new Session(CoreConfig.defaults(), Clock.now()), DriverHooks.NONE);
                var carriers = runtime.carriers();
                var physical = new java.util.concurrent.CompletableFuture<Void>();
                var carrier = new Carrier(loop, Action.DIR_BIDI, null, 0) {
                    @Override public java.util.concurrent.CompletionStage<Void> whenClosed() { return physical; }
                    @Override void closePhysical(CarrierSetup.Bound.Permit permit, Throwable cause) {
                        super.closePhysical(permit, cause);
                        assertFalse(carriers.released().isDone());
                    }
                };
                carriers.remove(carriers.attach(carrier, 0, 0, null), null);
                assertFalse(carriers.released().isDone());
                physical.complete(null);
                assertTrue(carriers.released().isDone());
            });
        } finally { loop.close(); }
    }

    @Test void rejectedOwnerSubmissionStillDisposesTheConsumedCloseRight() throws Exception {
        var loop = new EventLoop("release-after-stop", true, BufferPool.DEFAULT_IDLE_BYTES);
        var waiting = new java.util.concurrent.CountDownLatch(1);
        var resume = new java.util.concurrent.CompletableFuture<Void>();
        var setup = new CarrierSetup();
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        try {
            loop.execute(() -> {
                setup.own(() -> new Carrier(loop, Action.DIR_IN, null, 0) {
                    @Override void closePhysical(CarrierSetup.Bound.Permit permit, Throwable cause) { calls.incrementAndGet(); super.closePhysical(permit, cause); }
                });
                loop.close();
                waiting.countDown();
                resume.join();
            });
            assertTrue(waiting.await(5, java.util.concurrent.TimeUnit.SECONDS));
            setup.refused();
            assertFalse(setup.closed().isDone());
            assertEquals(0, calls.get());
            resume.complete(null);
            loop.close();
            assertEquals(1, calls.get());
        } finally { resume.complete(null); loop.close(); }
    }

    @Test void processAndOutboundLaneOwnTheirNativeCarrierBeforeRefusal() throws Exception {
        var loop = new EventLoop("lane-owners", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            for (boolean outbound : new boolean[]{false, true}) {
                var raw = new Carrier(loop, Action.DIR_IN, new Object(), 0);
                class Native implements io.zmux.carrier.PipeReader, NativePipe {
                    @Override public Transport claim(io.zmux.carrier.PipeIo io) { return raw; }
                    @Override public void start(Sink sink) { fail("expected native transport"); }
                    @Override public void close() { fail("physical carrier owns the half"); }
                }
                var half = io.zmux.carrier.PipeIo.of(new Native(), null);
                if (outbound) {
                    ExternalTransportTest.run(loop, () -> {
                        var runtime = new SessionRuntime(loop, new Session(CoreConfig.defaults(), Clock.now()), DriverHooks.NONE);
                        var parent = new Carrier(loop, Action.DIR_BIDI, new Object(), 0) {
                            @Override public java.util.concurrent.CompletionStage<io.zmux.carrier.PipeIo> openLane() {
                                return java.util.concurrent.CompletableFuture.completedFuture(half);
                            }
                        };
                        runtime.carriers().attach(parent, 0, 0, null);
                        runtime.apply(new Action(Action.Kind.OPEN_LANE, 0, 1, Action.DIR_OUT));
                    });
                } else {
                    ListenerHub.laneInput(loop, half);
                    ExternalTransportTest.run(loop, () -> raw.inbound().ended(raw, false, false));
                }
                ExternalTransportTest.run(loop, () -> {
                    assertNotSame(CarrierSetup.NONE, raw.setup());
                    assertTrue(raw.closed);
                    assertThrows(java.io.IOException.class, raw.setup()::await);
                });
            }
        } finally { loop.close(); }
    }

    @Test void borrowedProtocolTypeHasNoPhysicalOwnershipOperations() throws Exception {
        var view = CarrierSetup.class.getDeclaredMethod("borrow").getReturnType();
        for (var method : view.getMethods()) {
            assertFalse(java.util.Set.of("close", "closeRight", "moveTo", "abort").contains(method.getName()), method.toString());
        }
    }

    @Test void refusingAnOutboundLaneDoesNotResetTheSameInboundLaneNumber() throws Exception {
        EventLoop loop = new EventLoop("carrier-directions", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            ExternalTransportTest.run(loop, () -> {
                var driver = new SessionRuntime(loop, new Session(CoreConfig.defaults(), Clock.now()), DriverHooks.NONE);
                Carrier inbound = new Carrier(loop, Action.DIR_IN, null, 0);
                driver.carriers().attach(inbound, 0, 1, null);
                driver.apply(new Action(Action.Kind.RESET_LANE, 0, 1, Action.DIR_OUT));
                assertFalse(inbound.closed);
                driver.apply(new Action(Action.Kind.CLOSE, 0, 0, Action.DIR_IN));
                assertTrue(inbound.closed);
            });
        } finally { loop.close(); }
    }
    public static Runnable selectedEviction(ListenerHub listener, SessionRuntime candidate) {
        return listener.eviction(candidate);
    }

    public static void resetInputLane(SessionRuntime driver, long lane) throws Exception {
        ExternalTransportTest.run((EventLoop) driver.loop(),
                () -> driver.apply(new Action(Action.Kind.RESET_LANE, 0, lane, Action.DIR_IN)));
    }

    /** Observe the physical pipe independently of the two public attachment signals. */
    public static java.util.concurrent.CompletableFuture<Void> mainReleased(SessionRuntime driver) throws Exception {
        var signal = new java.util.concurrent.CompletableFuture<Void>();
        ExternalTransportTest.run((EventLoop) driver.loop(), () -> {
            Transport main = driver.carriers().get(0).t;
            main.whenClosed().thenCompose(v -> main.writesReleased()).whenComplete((v, e) -> signal.complete(null));
        });
        return signal;
    }

    /** Install an actual external lane on the session and leave its adapter write completion outstanding. */
    public static io.zmux.carrier.PipeWriter.Completion holdLastLaneWrite(SessionRuntime driver) throws Exception {
        var written = new java.util.concurrent.CompletableFuture<io.zmux.carrier.PipeWriter.Completion>();
        var writer = new io.zmux.carrier.PipeWriter() {
            @Override public void write(io.zmux.carrier.OutboundBatch batch, Completion done) { written.complete(done); }
            @Override public void close() {}
        };
        ExternalTransportTest.run((EventLoop) driver.loop(), () -> {
            var lane = new ExternalTransport(driver.loop(), io.zmux.carrier.PipeIo.of(null, writer));
            var attached = driver.carriers().attach(lane, 0, 1, null);
            lane.write(new Batch());
            driver.carriers().remove(attached, null);
        });
        return written.get(5, java.util.concurrent.TimeUnit.SECONDS);
    }

    @Test void setupFailureAndTheForwardingViewConsumeOnePhysicalCloseRight() throws Exception {
        var loop = new EventLoop("forwarded-owner", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            ExternalTransportTest.run(loop, () -> {
                var calls = new java.util.concurrent.atomic.AtomicInteger();
                var raw = new Carrier(loop, Action.DIR_BIDI, null, 0) {
                    @Override void closePhysical(CarrierSetup.Bound.Permit permit, Throwable cause) { calls.incrementAndGet(); }
                };
                var setup = new CarrierSetup();
                setup.own(() -> raw);
                var view = new Forwarded(raw, null, false, true);
                var error = new java.io.IOException("transform failed");
                setup.fail(error);
                view.abort(error);
                assertEquals(1, calls.get());
            });
        } finally { loop.close(); }
    }

    @Test void transferringTheLastShareMakesTheOriginalGuardInert() {
        var release = new AttachmentRelease();
        var done = release.completion();
        var work = new java.util.concurrent.CompletableFuture<Void>();
        var pending = release.fork();
        pending.until(work);
        pending.close();
        assertThrows(IllegalStateException.class, pending::fork);
        release.close();
        assertFalse(done.isDone());
        work.complete(null);
        assertTrue(done.isDone());
    }

    @Test void publicationAndWithdrawalHaveOneWinner() throws Exception {
        try (var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var cancelled = new CarrierSetup();
            var withdrawn = workers.submit(() -> {
                Thread.currentThread().interrupt();
                assertThrows(java.io.InterruptedIOException.class, cancelled::await);
            });
            withdrawn.get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertNull(cancelled.claim());

            var publishing = new CarrierSetup();
            var publication = publishing.claim();
            assertNotNull(publication);
            var started = new java.util.concurrent.CountDownLatch(1);
            var waiting = workers.submit(() -> {
                Thread.currentThread().interrupt();
                started.countDown();
                publishing.await();
                return Thread.currentThread().isInterrupted();
            });
            assertTrue(started.await(5, java.util.concurrent.TimeUnit.SECONDS));
            publishing.interrupt();
            publication.commit();
            assertTrue(waiting.get(5, java.util.concurrent.TimeUnit.SECONDS));
        }
    }

    @Test void splitReservationOutlastsTheMainCarrierAndItsLanesLastWrite() throws Exception {
        var loop = new EventLoop("attachment-release", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            ExternalTransportTest.run(loop, () -> {
                var driver = new SessionRuntime(loop, new Session(CoreConfig.defaults(), Clock.now()), DriverHooks.NONE);
                var main = new Carrier(loop, Action.DIR_BIDI, null, 0);
                var setup = new CarrierSetup();
                var retired = setup.closed();
                setup.own(() -> main);
                var carriers = driver.carriers();
                var root = carriers.attach(main, 0, 0, null);
                var write = new java.util.concurrent.CompletableFuture<Void>();
                var lane = carriers.attach(new Carrier(loop, Action.DIR_OUT, null, 0) {
                    @Override public java.util.concurrent.CompletionStage<Void> writesReleased() { return write; }
                }, 0, 1, null);
                carriers.remove(root, null);
                setup.refused(); // The paired carrier's own I/O has ended.
                assertFalse(retired.isDone());
                carriers.remove(lane, null);
                assertFalse(retired.isDone());
                write.complete(null);
                assertTrue(retired.isDone());
            });
        } finally { loop.close(); }
    }

    /** A carrier whose graceful close never completes. */
    static final class Hanging extends Carrier {
        @Override public java.util.concurrent.CompletionStage<Void> whenClosed() { return pending; }
        private final java.util.concurrent.CompletableFuture<Void> pending = new java.util.concurrent.CompletableFuture<>();
        volatile Throwable aborted;
        Hanging(EventLoop loop) { super(loop, Action.DIR_BIDI, null, 0); }
        @Override protected java.util.function.Consumer<Throwable> closeEscalation() { return cause -> aborted = cause; }
    }
    @Test void actualReleaseRemovesTheLingerTimerImmediately() throws Exception {
        var loop = new EventLoop("close-release", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            ExternalTransportTest.run(loop, () -> {
                var driver = new SessionRuntime(loop, new Session(CoreConfig.defaults(), Clock.now()), DriverHooks.NONE);
                var carrier = new Hanging(loop);
                var member = driver.carriers().attach(carrier, 0, 1, null);
                int before = loop.timerCount();
                driver.carriers().remove(member, null);
                assertEquals(before + 1, loop.timerCount());
                carrier.pending.complete(null);
                assertEquals(before, loop.timerCount());
                assertNull(carrier.aborted);
            });
        } finally { loop.close(); }
    }

    @Test void aGracefulCloseThatNeverCompletesIsAbortedAfterTheLinger() throws Exception {
        EventLoop loop = new EventLoop("close-bound", true, BufferPool.DEFAULT_IDLE_BYTES);
        io.zmux.internal.diag.Diagnostics.lingerForTests(java.time.Duration.ofMillis(300));
        try {
            Hanging graceful = new Hanging(loop), abortive = new Hanging(loop);
            long[] t0 = new long[1];
            ExternalTransportTest.run(loop, () -> {
                var driver = new SessionRuntime(loop, new Session(CoreConfig.defaults(), Clock.now()), DriverHooks.NONE);
                var a = driver.carriers().attach(graceful, 0, 1, null);
                var b = driver.carriers().attach(abortive, 0, 2, null);
                t0[0] = System.nanoTime();
                driver.carriers().remove(a, null);
                driver.carriers().remove(b, new java.io.IOException("failed"));
                assertNull(graceful.aborted);
            });
            long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(3);
            while (graceful.aborted == null && System.nanoTime() < until) Thread.sleep(2);
            assertNotNull(graceful.aborted, "the graceful close was bounded");
            long took = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0[0]);
            assertTrue(took >= 250 && took < 1_500, "aborted after the linger: " + took + " ms");
            assertNull(abortive.aborted, "an abortive close needs no bound");
        } finally {
            io.zmux.internal.diag.Diagnostics.lingerForTests(java.time.Duration.ZERO);
            loop.close();
        }
    }
    @Test void closingInputLeavesTheBidirectionalOutputUsable() throws Exception {
        EventLoop loop = new EventLoop("carrier-half-close", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            ExternalTransportTest.run(loop, () -> {
                var driver = new SessionRuntime(loop, new Session(CoreConfig.defaults(), Clock.now()), DriverHooks.NONE);
                Carrier carrier = new Carrier(loop, Action.DIR_BIDI, null, 0);
                driver.attachBootstrap(carrier);
                driver.apply(new Action(Action.Kind.CLOSE, 0, 0, Action.DIR_IN));
                assertTrue(carrier.stoppedInput);
                assertFalse(carrier.stoppedOutput); assertFalse(carrier.closed);
                driver.apply(new Action(Action.Kind.SHUTDOWN, 0, 0, Action.DIR_OUT));
                assertTrue(carrier.stoppedOutput);
            });
        } finally { loop.close(); }
    }
    @Test void connectionChargeIsReservedBeforeAttachmentAndLivesThroughPhysicalClose() throws Exception {
        EventLoop loop = new EventLoop("carrier-budget", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            ExternalTransportTest.run(loop, () -> {
                long output = CarrierCapacity.OUTBOUND_WORKSPACE;
                var budget = new MemoryBudget(150 + output, null);
                Object connection = new Object();
                class HoldingCarrier extends Carrier {
                    private final java.util.concurrent.CompletableFuture<Void> released = new java.util.concurrent.CompletableFuture<>();
                    HoldingCarrier(SessionHost host, int directions, long bytes) { super(host, directions, connection, bytes); }
                    @Override public java.util.concurrent.CompletionStage<Void> whenClosed() { return released; }
                }
                var first = new HoldingCarrier(loop, Action.DIR_IN, 100);
                var sibling = new HoldingCarrier(loop, Action.DIR_OUT, 0);
                var rejected = new HoldingCarrier(loop, Action.DIR_IN, 151);
                assertTrue(CarrierCapacity.reserve(first, budget, false));
                assertTrue(CarrierCapacity.reserve(sibling, budget, false));
                assertFalse(CarrierCapacity.reserve(rejected, budget, false));
                assertEquals(100 + output, budget.used());
                rejected.reject(null);
                rejected.released.complete(null);
                assertEquals(100 + output, budget.used());

                var larger = new HoldingCarrier(loop, Action.DIR_IN, 150);
                assertTrue(CarrierCapacity.reserve(larger, budget, false));
                var core = new Session(CoreConfig.defaults(), Clock.now());
                var set = new SessionRuntime(loop, core, DriverHooks.NONE).carriers();
                var attached = set.attach(larger, 0, 0, null);
                first.reject(null);
                first.released.complete(null);
                sibling.reject(null);
                sibling.released.complete(null);
                set.remove(attached, null);
                assertEquals(150, budget.used());
                larger.released.complete(null);
                assertEquals(0, budget.used());
            });
        } finally { loop.close(); }
    }

    @Test void rejectedLaneImmediatelyDrivesTheProtocolClose() throws Exception {
        EventLoop loop = new EventLoop("lane-admission-close", true, BufferPool.DEFAULT_IDLE_BYTES);
        java.util.concurrent.CompletableFuture<Void> closing = new java.util.concurrent.CompletableFuture<>();
        try {
            ExternalTransportTest.run(loop, () -> {
                CoreConfig config = CoreConfig.defaults();
                Session core = new Session(config, Clock.now());
                SessionRuntime driver = new SessionRuntime(loop, core, DriverHooks.NONE);
                Object connection = new Object();
                Carrier bootstrap = new Carrier(loop, Action.DIR_BIDI, connection, 0);
                driver.attachBootstrap(bootstrap);
                CoreConfig remote = CoreConfig.builder().role(CoreConfig.Role.RESPONDER).transportRole(CoreConfig.Role.RESPONDER).build();
                Session peer = new Session(remote, Clock.now());
                peer.pipes().attachBootstrap(bootstrap.info().carrier(), bootstrap.info().carrier());
                Batch batch = new Batch();
                assertTrue(peer.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, Clock.now()));
                java.nio.ByteBuffer bytes = java.nio.ByteBuffer.wrap(io.zmux.internal.core.BatchFixture.flat(batch));
                ReadBuf input = new ReadBuf(bytes, null);
                core.receive(0, 0, input, bytes, 0, bytes.limit(), Clock.now());
                input.unref(); batch.discard();
                assertTrue(core.established());
                driver.resourceListener(view -> { if (view.state() == SessionState.CLOSING) closing.complete(null); });
                // Let the attach/peer-parameter round settle before the invalid admission.
                loop.execute(() -> {
                    java.nio.ByteBuffer body = java.nio.ByteBuffer.allocate(64);
                    io.zmux.internal.wire.FrameEncoder.pipeLane(body, core.lanes().key(), 0, 0);
                    assertNull(driver.acceptLane(new Carrier(loop, Action.DIR_IN, connection, 0),
                            java.util.Arrays.copyOf(body.array(), body.position())));
                });
            });
            closing.get(5, java.util.concurrent.TimeUnit.SECONDS);
        } finally { loop.close(); }
    }


    public static void loseCarrier(io.zmux.adapter.Handled h) {
        var setup = (CarrierSetup) io.zmux.internal.spi.AdapterPorts.setup(h);
        CarrierSetup.connectionLost(setup.borrow(), new java.io.IOException("lost after Hello"));
    }

    @Test void finishedSetupCannotRegisterAnotherWorker() throws Exception {
        // HS-06.
        var setup = new CarrierSetup();
        setup.refused();
        var never = new java.util.concurrent.CompletableFuture<Void>();
        var share = setup.cleanup();
        assertFalse(share.active());
        share.until(never);
        share.close();
        setup.track(never);
        assertNull(setup.own(() -> { throw new AssertionError("late carrier conversion"); }));
        assertEquals(0, never.getNumberOfDependents());
        assertTrue(setup.closed().isDone());
    }

    private static final java.util.regex.Pattern LOSS_SIGNAL =
            java.util.regex.Pattern.compile("\\bconnectionLost\\s*\\(|::\\s*connectionLost\\b");

    @Test void onlyPhysicalTransportsAndTheirOwnerMaySignalConnectionLoss() throws Exception {
        var root = java.nio.file.Path.of("").toAbsolutePath();
        while (!java.nio.file.Files.isDirectory(root.resolve(".mvn"))) root = root.getParent();
        var owners = java.util.Set.of(CarrierSetup.class, PhysicalTransport.class, AbstractTransport.class,
                ExternalTransport.class, SocketTransport.class, SealedTransport.class, TlsTransport.class);
        var allowed = owners.stream().map(type -> type.getName().replace('.', '/') + ".java")
                .collect(java.util.stream.Collectors.toSet());
        try (var modules = java.nio.file.Files.list(root)) {
            for (var module : modules.toList()) {
                var sources = module.resolve("src/main/java");
                if (!java.nio.file.Files.isDirectory(sources)) continue;
                try (var files = java.nio.file.Files.walk(sources)) {
                    for (var file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                        if (LOSS_SIGNAL.matcher(java.nio.file.Files.readString(file)).find())
                            assertTrue(module.getFileName().toString().equals("zmux")
                                    && allowed.contains(sources.relativize(file).toString().replace(java.io.File.separatorChar, '/')),
                                    () -> "connectionLost bypasses the physical owner in " + file);
                    }
                }
            }
        }
    }

    @Test void physicalOwnershipMethodsAreAbsentFromPublicCarrierViews() {
        for (var m : PhysicalTransport.class.getDeclaredMethods()) {
            if (m.getName().equals("close")) assertTrue(java.lang.reflect.Modifier.isPrivate(m.getModifiers()), m::toString);
        }
        for (var type : java.util.List.of(Transport.class, SocketTransport.class, TlsTransport.class,
                ExternalTransport.class, SealedTransport.class)) {
            for (var method : type.getMethods()) {
                assertFalse(java.util.Set.of("close", "closeRight", "closePhysical", "movePhysical",
                        "moveTo", "detachPhysical").contains(method.getName()), method::toString);
            }
        }
    }

}
