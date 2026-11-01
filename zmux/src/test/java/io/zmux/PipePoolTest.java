package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(40)
class PipePoolTest {
    @FunctionalInterface interface Checked { boolean get() throws Exception; }
    static void await(Checked condition) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.get() && System.nanoTime() < until) Thread.sleep(3);
        assertTrue(condition.get(), "condition did not become true");
    }
    static void inactive(ZmuxPipe pipe) throws Exception {
        assertTrue(isInactive(pipe), "pipe still active");
    }
    static boolean isInactive(ZmuxPipe pipe) throws Exception {
        // An empty snapshot: retirement completed already.
        return pipe.stats().map(state -> (state.outbound() == null || java.util.Set.of(PipeState.Outbound.FAILED, PipeState.Outbound.CLOSED).contains(state.outbound()))
                && (state.inbound() == null || state.inbound() == PipeState.Inbound.ENDED)).orElse(true);
    }
    static ZmuxConfig config(EventLoops loops, int cap) {
        return ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(cap).withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO)).build();
    }
    static PipePool fast(PipePool.Dial dial) {
        return PipePool.of(dial).withInspectionInterval(Duration.ofMillis(5))
                .withBackoff(new PipePool.Backoff(Duration.ofMillis(10), Duration.ofMillis(40), 0));
    }
    static final class Fixture implements AutoCloseable {
        final ZmuxListener listener; final ZmuxSession client, server;
        Fixture(ZmuxConfig cfg) throws Exception {
            listener = Zmux.listener(ListenerConfig.of(cfg)); var io = MemoryPipe.pair(Framing.BYTE_STREAM);
            listener.handle(io[1], JoinHint.inBand(TestAddr.peer("bootstrap"))).closed(); client = Zmux.open(io[0], Role.CLIENT, cfg);
            server = listener.accept(Duration.ofSeconds(3));
        }
        PipePool.Dial dial() { return attempt -> {
            var io = MemoryPipe.pair(Framing.BYTE_STREAM); listener.handle(io[1], JoinHint.inBand(TestAddr.peer("pool"))).closed();
            return attempt.join(io[0]);
        }; }
        @Override public void close() throws Exception { client.close(); server.close(); client.awaitClosed(); server.awaitClosed(); listener.close(); }
    }

    @Test void maintainsTcpMembersAndReplacesFailureWithoutOwningTrafficScheduling() throws Exception {
        try (EventLoops loops = EventLoops.create(3)) {
            var cfg = config(loops, 4);
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg));
                 var client = Zmux.connect(listener.localAddress(), cfg);
                 var server = listener.accept(Duration.ofSeconds(3))) {
                var dials = new AtomicInteger(); var cancel = new Cancellation();
                PipePool pool = fast(attempt -> {
                    dials.incrementAndGet(); return attempt.join(listener.localAddress());
                }).withCount(2);
                var maintenance = client.paths().maintain(pool, cancel);
                await(() -> maintenance.pipes().size() == 2);
                long old = maintenance.pipes().getFirst().id(); maintenance.pipes().getFirst().abort();
                await(() -> maintenance.pipes().size() == 2 && maintenance.pipes().stream().noneMatch(p -> p.id() == old));
                try (var stream = client.openStream()) {
                    stream.write(new byte[] {1, 2, 3}); stream.finish();
                    try (var peer = server.acceptStream()) { assertArrayEquals(new byte[] {1, 2, 3}, SessionTest.readAll(peer)); peer.finish(); }
                }
                cancel.cancel(); var result = maintenance.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
                assertTrue(result.joined() >= 3); assertTrue(dials.get() >= 3); assertEquals(2, result.pipes().size());
                for (var pipe : result.pipes()) assertEquals(PipeState.Outbound.ACTIVE, pipe.stats().orElseThrow().outbound());
                assertEquals(0, CancellationTest.registrations(cancel));
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void fullMaintenanceTargetPreventsDialingAndCancellationStillWakesTheWorker() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new Fixture(config(loops, 2))) {
            for (int i = 0; i < 1; i++) {
                var occupied = MemoryPipe.pair(Framing.BYTE_STREAM); f.listener.handle(occupied[1], JoinHint.inBand(TestAddr.peer("occupied"))).closed();
                f.client.paths().add(occupied[0], PipeOptions.defaults());
            }
            assertFalse(f.client.joinReady()); var calls = new AtomicInteger(); var cancel = new Cancellation();
            var maintenance = f.client.paths().maintain(fast(attempt -> { calls.incrementAndGet(); throw new IOException("must not dial"); }), cancel);
            Thread.sleep(50); assertEquals(0, calls.get()); cancel.cancel();
            assertEquals(0, maintenance.completion().toCompletableFuture().get(2, TimeUnit.SECONDS).joined());
            assertEquals(0, CancellationTest.registrations(cancel));
        }
    }

    @Test void replacementMustBecomeUsableBeforeOldPipeIsRetired() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var f = new Fixture(config(loops, 3))) {
            var options = PipeOptions.builder().probation(Duration.ofMillis(150)).build();
            var pool = fast(f.dial()).withOptions(options).withRotation(new PipePool.Rotation(0, Duration.ofMillis(80), 0));
            var maintenance = f.client.paths().maintain(pool);
            try {
                await(() -> maintenance.pipes().size() == 1); var original = maintenance.pipes().getFirst();
                await(() -> maintenance.pipes().size() == 2);
                var replacement = maintenance.pipes().get(1);
                assertTrue(replacement.stats().orElseThrow().inProbation()); assertEquals(PipeState.Outbound.ACTIVE, original.stats().orElseThrow().outbound());
                await(() -> maintenance.pipes().getFirst().id() != original.id());
                assertFalse(maintenance.pipes().getFirst().stats().orElseThrow().inProbation());
            } finally { maintenance.close(); maintenance.completion().toCompletableFuture().get(3, TimeUnit.SECONDS); }
        }
    }

    @Test void rotationFailuresKeepTheOriginalAndRespectBackoff() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new Fixture(config(loops, 3))) {
            var calls = new AtomicInteger(); var times = new CopyOnWriteArrayList<Long>();
            var pool = fast(attempt -> {
                if (calls.getAndIncrement() == 0) return f.dial().open(attempt);
                times.add(System.nanoTime()); throw new IOException("route unavailable");
            }).withRotation(new PipePool.Rotation(0, Duration.ofMillis(20), 0));
            var maintenance = f.client.paths().maintain(pool);
            try {
                await(() -> times.size() >= 3); assertEquals(1, maintenance.pipes().size());
                assertEquals(PipeState.Outbound.ACTIVE, maintenance.pipes().getFirst().stats().orElseThrow().outbound());
                for (int i = 1; i < times.size(); i++) assertTrue(times.get(i) - times.get(i-1) >= TimeUnit.MILLISECONDS.toNanos(10));
            } finally { maintenance.close(); maintenance.completion().toCompletableFuture().get(3, TimeUnit.SECONDS); }
        }
    }

    @Test void cancellationAndAttemptDeadlineInterruptTheDialWorker() throws Exception {
        for (boolean deadline : new boolean[] {false, true}) {
            try (EventLoops loops = EventLoops.create(2); var f = new Fixture(config(loops, 3))) {
                var entered = new CountDownLatch(1); var exited = new AtomicInteger(); var cancel = new Cancellation();
                var pool = fast(attempt -> {
                    entered.countDown();
                    try { new CountDownLatch(1).await(); throw new AssertionError("unexpected wake"); }
                    finally { exited.incrementAndGet(); }
                }).withAttemptTimeout(Duration.ofMillis(40)).withBackoff(new PipePool.Backoff(Duration.ofSeconds(5), Duration.ofSeconds(5), 0));
                var maintenance = f.client.paths().maintain(pool, cancel);
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                if (deadline) await(() -> exited.get() == 1); cancel.cancel();
                var result = maintenance.completion().toCompletableFuture().get(2, TimeUnit.SECONDS);
                await(() -> exited.get() == 1); assertEquals(0, result.joined());
                assertEquals(0, CancellationTest.registrations(cancel));
            }
        }
    }

    @Test void receiveOnlyPoolsUseTheExistingOutOfBandReservationPath() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new Fixture(config(loops, 3))) {
            var pool = fast(attempt -> {
                try (var reservation = attempt.reserve()) {
                    var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                    pair[0].writer().close(); pair[1].reader().close();
                    f.listener.handle(PipeIo.of(null, pair[1].writer()), JoinHint.join(reservation.header(), TestAddr.peer("pool"))).closed();
                    return reservation.attach(PipeIo.of(pair[0].reader(), null));
                }
            }).withOptions(PipeOptions.builder().direction(PipeOptions.Direction.RECEIVE_ONLY).receivePriority(123456).build());
            var maintenance = f.client.paths().maintain(pool);
            try {
                await(() -> maintenance.pipes().size() == 1);
                var stats = maintenance.pipes().getFirst().stats().orElseThrow();
                assertFalse(stats.outbound() != null); assertTrue(stats.inbound() != null); assertEquals(123456, stats.receivePriority());
            } finally { maintenance.close(); maintenance.completion().toCompletableFuture().get(3, TimeUnit.SECONDS); }
        }
    }
    @Test void failedReplacementReleasesItsRemainingDirectionAndQuota() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var f = new Fixture(config(loops, 3))) {
            var pairs = new CopyOnWriteArrayList<PipeIo[]>();
            var pool = fast(attempt -> {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM); pairs.add(pair);
                f.listener.handle(pair[1], JoinHint.inBand(TestAddr.peer("replacement"))).closed(); return attempt.join(pair[0]);
            }).withOptions(PipeOptions.builder().probation(Duration.ofMillis(200)).build())
                    .withRotation(new PipePool.Rotation(0, Duration.ofMillis(100), 0));
            var run = f.client.paths().maintain(pool);
            try {
                await(() -> run.pipes().size() == 2); var old = run.pipes().getFirst(); var failed = run.pipes().get(1);
                assertFalse(f.client.joinReady()); pairs.get(1)[1].writer().close();
                await(() -> pairs.size() >= 3);
                assertEquals(PipeState.Outbound.ACTIVE, old.stats().orElseThrow().outbound());
                inactive(failed);
            } finally { run.close(); run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS); }
        }
    }

    @Test void badDialCannotAdoptOrAbortAnotherCallersPipe() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new Fixture(config(loops, 4))) {
            var foreign = new AtomicReference<ZmuxPipe>(); var ours = new AtomicReference<ZmuxPipe>();
            var run = f.client.paths().maintain(fast(attempt -> {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM); f.listener.handle(pair[1], JoinHint.inBand(TestAddr.peer("application"))).closed();
                foreign.set(f.client.paths().add(pair[0], PipeOptions.defaults()));
                ours.set(f.dial().open(attempt));
                return foreign.get();
            }).withBackoff(new PipePool.Backoff(Duration.ofSeconds(5), Duration.ofSeconds(5), 0)));
            await(() -> ours.get() != null); run.close();
            var result = run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(PipeState.Outbound.ACTIVE, foreign.get().stats().orElseThrow().outbound());
            inactive(ours.get()); assertTrue(result.pipes().isEmpty());
        }
    }

    @Test void cancellationCallbackFailureReclaimsTheJustJoinedCandidate() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new Fixture(config(loops, 3))) {
            var created = new AtomicReference<ZmuxPipe>(); var failed = new CountDownLatch(1);
            var run = f.client.paths().maintain(fast(attempt -> {
                attempt.cancellation().onCancel(() -> { failed.countDown(); throw new IllegalStateException("broken callback"); });
                var pipe = f.dial().open(attempt); created.set(pipe); return pipe;
            }).withBackoff(new PipePool.Backoff(Duration.ofSeconds(5), Duration.ofSeconds(5), 0)));
            assertTrue(failed.await(3, TimeUnit.SECONDS)); run.close();
            var result = run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertNotNull(created.get()); inactive(created.get());
            assertTrue(result.pipes().isEmpty()); assertEquals(0, result.joined()); assertNotNull(f.client.ping());
        }
    }

    @Test void aDialIgnoringInterruptsCannotHoldMaintenanceAndIsReclaimedWhenItReturns() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new Fixture(config(loops, 3))) {
            var created = new AtomicReference<ZmuxPipe>(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var run = f.client.paths().maintain(fast(attempt -> {
                var pipe = f.dial().open(attempt); created.set(pipe); entered.countDown();
                while (true) { try { release.await(); break; } catch (InterruptedException ignored) { /* Deliberately unresponsive. */ } }
                return pipe;
            }).withAttemptTimeout(Duration.ofMillis(50)).withBackoff(new PipePool.Backoff(Duration.ofSeconds(5), Duration.ofSeconds(5), 0)));
            assertTrue(entered.await(3, TimeUnit.SECONDS)); run.close();
            var result = run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertTrue(result.pipes().isEmpty()); assertEquals(0, result.joined());
            release.countDown();
            await(() -> isInactive(created.get()));
        }
    }

    @Test void timedOutCandidateIsJoinedToItsWorkerAndThenReclaimed() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new Fixture(config(loops, 3))) {
            var created = new AtomicReference<ZmuxPipe>(); var exited = new CountDownLatch(1);
            var run = f.client.paths().maintain(fast(attempt -> {
                var pipe = f.dial().open(attempt); created.set(pipe);
                try { new CountDownLatch(1).await(); } catch (InterruptedException expected) { /* Completion raced the deadline. */ }
                finally { exited.countDown(); }
                return pipe;
            }).withAttemptTimeout(Duration.ofMillis(200)).withBackoff(new PipePool.Backoff(Duration.ofSeconds(5), Duration.ofSeconds(5), 0)));
            assertTrue(exited.await(3, TimeUnit.SECONDS)); run.close();
            var result = run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            // The abandoned attempt returns after counting down: its creation is reclaimed then.
            await(() -> isInactive(created.get())); assertTrue(result.pipes().isEmpty());
        }
    }

    @Test void independentPoolsKeepTheirMembersAndRegistrationsSeparate() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new Fixture(config(loops, 4))) {
            var cancellation = new Cancellation();
            var first = f.client.paths().maintain(fast(f.dial()), cancellation);
            var second = f.client.paths().maintain(fast(f.dial()), cancellation);
            await(() -> first.pipes().size() == 1 && second.pipes().size() == 1);
            assertNotEquals(first.pipes().getFirst().id(), second.pipes().getFirst().id());
            first.close(); first.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            first.pipes().getFirst().abort();
            assertEquals(PipeState.Outbound.ACTIVE, second.pipes().getFirst().stats().orElseThrow().outbound());
            cancellation.cancel(); second.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(0, CancellationTest.registrations(cancellation));
            assertEquals(0, (Integer)f.client.run(op -> io.zmux.internal.core.ObserverFixture.closedWaiters(f.client.core)));
        }
    }

    @Test void rotationDialYieldsToAMemberFailureBeforeItsDeadline() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var f = new Fixture(config(loops, 4))) {
            var calls = new AtomicInteger(); var rotating = new CountDownLatch(1); var yielded = new CountDownLatch(1);
            var repaired = new CountDownLatch(1);
            var run = f.client.paths().maintain(fast(attempt -> {
                int n = calls.incrementAndGet();
                if (n == 3) {
                    rotating.countDown();
                    try { new CountDownLatch(1).await(); throw new AssertionError("rotation should yield"); }
                    finally { yielded.countDown(); }
                }
                if (n > 4) throw new IOException("pause further rotation");
                var pipe = f.dial().open(attempt); if (n == 4) repaired.countDown(); return pipe;
            }).withCount(2).withRotation(new PipePool.Rotation(0, Duration.ofMillis(30), 0))
                    .withAttemptTimeout(Duration.ofSeconds(10)));
            try {
                assertTrue(rotating.await(3, TimeUnit.SECONDS)); assertEquals(2, run.pipes().size());
                long lost = run.pipes().get(1).id(); run.pipes().get(1).abort();
                assertTrue(yielded.await(2, TimeUnit.SECONDS)); assertTrue(repaired.await(2, TimeUnit.SECONDS));
                await(() -> run.pipes().size() == 2 && run.pipes().stream().noneMatch(p -> p.id() == lost));
            } finally { run.close(); run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS); }
        }
    }

    @Test void closingTheSessionStopsADialAndUnregistersItsWorker() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new Fixture(config(loops, 3))) {
            var entered = new CountDownLatch(1); var exited = new AtomicBoolean();
            var run = f.client.paths().maintain(fast(attempt -> {
                entered.countDown();
                try { new CountDownLatch(1).await(); throw new AssertionError("unexpected wake"); }
                finally { exited.set(true); }
            }));
            assertTrue(entered.await(3, TimeUnit.SECONDS)); f.client.close();
            var result = run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            await(exited::get); assertInstanceOf(SessionClosedException.class, result.cause());
        }
    }

    @Test void stalledTlsAttemptCancelsPromptlyAndOwnerShutdownClosesItToo() throws Exception {
        for (boolean shutdown : new boolean[] {false, true}) {
            try (EventLoops loops = EventLoops.create(2); var f = new Fixture(config(loops, 3));
                 var listener = java.nio.channels.ServerSocketChannel.open();
                 var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
                listener.bind(NetworkTest.any());
                var accepted = tasks.submit(() -> listener.accept());
                var cancel = new Cancellation();
                var tls = TestTls.client();
                var run = f.client.paths().maintain(fast(attempt -> attempt.joinTls((java.net.InetSocketAddress)listener.getLocalAddress(), TlsOptions.client(tls, "localhost"))), cancel);
                try (var socket = accepted.get(3, TimeUnit.SECONDS)) {
                    if (shutdown) loops.close(); else cancel.cancel();
                    var result = run.completion().toCompletableFuture().get(2, TimeUnit.SECONDS);
                    assertEquals(0, result.joined());
                    if (shutdown) assertInstanceOf(SessionClosedException.class, result.cause());
                    var eof = tasks.submit(() -> {
                        var bytes = java.nio.ByteBuffer.allocate(4096);
                        while (socket.read(bytes) >= 0) bytes.clear();
                        return true;
                    });
                    assertTrue(eof.get(2, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test void cancellationOfAQuotaHeldJoinClosesItsCarrierBeforeReturning() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var f = new Fixture(config(loops, 2))) {
            var held = new CountDownLatch(1); var lifetime = new AtomicReference<CompletionStage<Void>>();
            var run = f.client.paths().maintain(fast(attempt -> {
                // Simulate concurrent application joins taking all physical attachment headroom.
                int cap = f.client.core.config().maxPipes() + f.client.core.config().headroom();
                for (int i = 1; i < cap; i++) {
                    var other = MemoryPipe.pair(Framing.BYTE_STREAM); f.listener.handle(other[1], JoinHint.inBand(TestAddr.peer("application"))).closed();
                    f.client.paths().add(other[0], PipeOptions.defaults());
                }
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                lifetime.set(f.listener.handle(pair[1], JoinHint.inBand(TestAddr.peer("held"))).closed()); held.countDown();
                return attempt.join(pair[0]);
            }).withAttemptTimeout(Duration.ofSeconds(10)));
            assertTrue(held.await(3, TimeUnit.SECONDS));
            await(() -> f.client.paths().stats().stream().anyMatch(p -> p.outbound() == PipeState.Outbound.JOINING));
            run.close(); run.completion().toCompletableFuture().get(2, TimeUnit.SECONDS);
            lifetime.get().toCompletableFuture().get(2, TimeUnit.SECONDS);
            await(() -> f.client.paths().stats().stream().noneMatch(p -> p.outbound() == PipeState.Outbound.JOINING));
        }
    }

    @Test void interruptedIdPublicationWaitsForCreationAndAbandonAcknowledgement() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new Fixture(config(loops, 3))) {
            var pair = MemoryPipe.pair(Framing.BYTE_STREAM); f.listener.handle(pair[1], JoinHint.inBand(TestAddr.peer("pending-publication"))).closed();
            var created = f.client.paths().add(pair[0], PipeOptions.defaults());
            var publication = new CompletableFuture<Long>(); var finished = new CompletableFuture<IOException>();
            var interrupted = new AtomicBoolean();
            Thread waiter = Thread.ofVirtual().start(() -> {
                try { f.client.joined(publication); finished.complete(null); }
                catch (IOException error) { finished.complete(error); }
                finally { interrupted.set(Thread.currentThread().isInterrupted()); }
            });
            try {
                await(() -> waiter.getState() == Thread.State.WAITING); waiter.interrupt();
                assertThrows(TimeoutException.class, () -> finished.get(50, TimeUnit.MILLISECONDS));
                publication.complete(created.id());
                assertInstanceOf(java.io.InterruptedIOException.class, finished.get(2, TimeUnit.SECONDS));
                waiter.join(); assertTrue(interrupted.get());
                inactive(created);
            } finally { publication.complete(created.id()); waiter.interrupt(); waiter.join(); }
        }
    }

    @Test void aReplacementFillsALostSlotBeforeRetiringAnyHealthyOriginal() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var f = new Fixture(config(loops, 4))) {
            var calls = new AtomicInteger();
            var run = f.client.paths().maintain(fast(attempt -> {
                if (calls.incrementAndGet() > 3) throw new IOException("pause further rotation");
                return f.dial().open(attempt);
            }).withCount(2).withOptions(PipeOptions.builder().probation(Duration.ofMillis(150)).build())
                    .withRotation(new PipePool.Rotation(0, Duration.ofMillis(80), 0))
                    .withBackoff(new PipePool.Backoff(Duration.ofMillis(20), Duration.ofSeconds(5), 0)));
            try {
                await(() -> run.pipes().size() == 2); var healthy = run.pipes().getFirst(); var lost = run.pipes().get(1);
                await(() -> run.pipes().size() == 3); lost.abort();
                await(() -> run.pipes().size() == 2 && run.pipes().stream().noneMatch(p -> p.id() == lost.id()));
                assertEquals(PipeState.Outbound.ACTIVE, healthy.stats().orElseThrow().outbound());
            } finally {
                run.close(); assertEquals(0, run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS).rotations());
            }
        }
    }

    @Test void closingTheRunClosesAReplacementBeingRotatedInAndKeepsTheMember() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var f = new Fixture(config(loops, 3))) {
            var calls = new AtomicInteger();
            // The replacement's revalidation is held, so it is still being rotated in at the close.
            var run = f.client.paths().maintain(fast(attempt -> {
                if (calls.incrementAndGet() == 1) return f.dial().open(attempt);
                if (calls.get() > 2) throw new IOException("pause further rotation");
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM); var gate = new SpliceTest.GateWriter(pair[0].writer());
                f.listener.handle(pair[1], JoinHint.inBand(TestAddr.peer("replacement"))).closed();
                var pipe = attempt.join(PipeIo.of(pair[0].reader(), gate));
                gate.paused = true; pipe.revalidate(); return pipe;
            }).withOptions(PipeOptions.builder().probation(Duration.ofMillis(20)).build())
                    .withRotation(new PipePool.Rotation(0, Duration.ofMillis(50), 0)));
            await(() -> run.pipes().size() == 2);
            var original = run.pipes().getFirst(); var replacement = run.pipes().get(1);
            run.close();
            var result = run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(java.util.List.of(original.id()), result.pipes().stream().map(ZmuxPipe::id).toList());
            await(() -> isInactive(replacement));
            assertEquals(PipeState.Outbound.ACTIVE, original.stats().orElseThrow().outbound());
        }
    }

    @Test void replacementRevalidationMustFinishBeforeHandover() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var f = new Fixture(config(loops, 3))) {
            var calls = new AtomicInteger(); var held = new AtomicReference<SpliceTest.GateWriter>();
            var run = f.client.paths().maintain(fast(attempt -> {
                if (calls.incrementAndGet() == 1) return f.dial().open(attempt);
                if (calls.get() > 2) throw new IOException("pause further rotation");
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM); var gate = new SpliceTest.GateWriter(pair[0].writer());
                f.listener.handle(pair[1], JoinHint.inBand(TestAddr.peer("revalidation"))).closed();
                var pipe = attempt.join(PipeIo.of(pair[0].reader(), gate));
                gate.paused = true; held.set(gate); pipe.revalidate(); return pipe;
            }).withOptions(PipeOptions.builder().probation(Duration.ofMillis(100)).build())
                    .withRotation(new PipePool.Rotation(0, Duration.ofMillis(50), 0)));
            try {
                await(() -> run.pipes().size() == 2); var original = run.pipes().getFirst(); var replacement = run.pipes().get(1);
                await(() -> !replacement.stats().orElseThrow().inProbation()); assertTrue(replacement.stats().orElseThrow().revalidating());
                Thread.sleep(30); assertEquals(PipeState.Outbound.ACTIVE, original.stats().orElseThrow().outbound());
                assertEquals(2, run.pipes().size()); held.get().release();
                await(() -> run.pipes().getFirst().id() == replacement.id());
                assertFalse(replacement.stats().orElseThrow().revalidating());
            } finally {
                if (held.get() != null) held.get().release();
                run.close(); run.completion().toCompletableFuture().get(3, TimeUnit.SECONDS);
            }
        }
    }

}
