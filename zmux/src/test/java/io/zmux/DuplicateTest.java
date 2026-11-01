package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class DuplicateTest {
    static DuplicateOptions ids(int local, int peer) {
        return DuplicateOptions.defaults().withIdentities(new byte[] {(byte)local}, new byte[] {(byte)peer});
    }
    record Held(ZmuxStream local, ZmuxStream peer) implements AutoCloseable {
        static Held open(Pair pair) throws Exception {
            var a = pair.client().openStream(); a.write(new byte[] {1});
            var b = pair.server().acceptStream(Duration.ofSeconds(3)); return new Held(a,b);
        }
        void finish() throws Exception {
            local.write(new byte[] {2}); local.finish();
            assertArrayEquals(new byte[] {1,2}, SessionTest.readAll(peer));
            peer.write(new byte[] {3}); peer.finish();
            assertArrayEquals(new byte[] {3}, SessionTest.readAll(local));
        }
        @Override public void close() { local.close(); peer.close(); }
    }
    static Pair symmetric(ZmuxConfig cfg) throws Exception {
        var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
        var accepting = CompletableFuture.supplyAsync(() -> {
            try { return Zmux.open(pair[1], Role.PEER, cfg); } catch(IOException e) { throw new CompletionException(e); }
        }, r -> Thread.ofVirtual().start(r));
        return new Pair(Zmux.open(pair[0], Role.PEER, cfg), accepting.get(3,TimeUnit.SECONDS));
    }
    static Pair plainPair(ZmuxConfig cfg) throws Exception {
        var carriers = MemoryPipe.pair(Framing.BYTE_STREAM);
        var accepting = CompletableFuture.supplyAsync(() -> {
            try {
                var core = cfg.toCore(io.zmux.internal.core.CoreConfig.Role.RESPONDER,io.zmux.internal.core.CoreConfig.Role.RESPONDER);
                core.local().present &= ~io.zmux.internal.wire.TransportParams.ANY_ZMUX;
                var session = Zmux.started(io.zmux.internal.loop.Bootstrap.external(() -> cfg.eventLoops.group().next(),core,carriers[1],cfg.hooks), null);
                session.awaitEstablished(Long.MAX_VALUE); return session;
            } catch(IOException e) { throw new CompletionException(e); }
        }, r -> Thread.ofVirtual().start(r));
        return new Pair(Zmux.open(carriers[0], Role.CLIENT, cfg),accepting.get(3,TimeUnit.SECONDS));
    }
    static void expectReason(DuplicateResolutionException.Reason reason, ZmuxSession a, ZmuxSession b, DuplicateOptions opts) {
        assertEquals(reason, assertThrows(DuplicateResolutionException.class, () -> Zmux.resolveDuplicate(a,b,opts)).reason());
    }

    @Test void bothEndpointsKeepTheSameSessionAndDrainExistingStreams() throws Exception {
        try (EventLoops loops = EventLoops.create(4)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (Pair first = Pair.memory(cfg); Pair second = Pair.memory(cfg); Held old = Held.open(second)) {
                var a = Zmux.resolveDuplicate(first.client(),second.server(),ids(1,255));
                assertSame(first.client(),a.survivor()); assertSame(second.server(),a.loser());
                assertEquals(DuplicateResult.Rule.ENDPOINT_IDENTITY,a.rule()); assertFalse(a.rejoinStarted());
                assertThrows(GoingAwayException.class,second.server()::openStream);
                var b = Zmux.resolveDuplicate(first.server(),second.client(),ids(255,1));
                assertSame(first.server(),b.survivor()); assertSame(second.client(),b.loser());
                assertThrows(GoingAwayException.class,second.client()::openStream);
                old.finish(); old.close();
                PipePoolTest.await(() -> second.client().isClosed() && second.server().isClosed());
                try (var fresh = first.client().openStream()) { fresh.write(new byte[] {9}); fresh.finish();
                    try (var accepted = first.server().acceptStream(Duration.ofSeconds(3))) {
                        assertArrayEquals(new byte[] {9},SessionTest.readAll(accepted)); accepted.finish();
                    }
                }
            }
            assertEquals(0,loops.outstandingBuffers());
        }
    }
    @Test void noncesUseOneCommonDomainAndBothEndpointsAgree() throws Exception {
        for (boolean mp : new boolean[] {true,false}) try (EventLoops loops = EventLoops.create(3)) {
            var cfg = mp ? PipePoolTest.config(loops,3) : ZmuxConfig.builder().eventLoops(loops).build();
            try (Pair one = mp ? Pair.memory(cfg) : symmetric(cfg); Pair two = mp ? Pair.memory(cfg) : symmetric(cfg);
                 Held holdOne = Held.open(one); Held holdTwo = Held.open(two)) {
                if (!mp) {
                    expectReason(DuplicateResolutionException.Reason.UNKNOWN_DIALER,one.client(),two.server(),ids(1,2));
                    one.client().designateDialer(true); one.server().designateDialer(false);
                    two.client().designateDialer(true); two.server().designateDialer(false);
                    assertThrows(IllegalStateException.class,() -> one.client().designateDialer(false));
                }
                var a = Zmux.resolveDuplicate(one.client(),two.server());
                var b = Zmux.resolveDuplicate(one.server(),two.client());
                assertEquals(a.survivor() == one.client(), b.survivor() == one.server());
                assertEquals(mp ? DuplicateResult.Rule.SESSION_NONCE : DuplicateResult.Rule.ROLE_NONCE,a.rule());
                assertEquals(a.rule(),b.rule());
            }
        }
    }
    @Test void undecidableOrEqualInputsChangeNeitherSession() throws Exception {
        try (EventLoops loops = EventLoops.create(3)) {
            var plain = ZmuxConfig.builder().eventLoops(loops).build(); var mp = PipePoolTest.config(loops,3);
            try (Pair one = Pair.memory(plain); Pair two = Pair.memory(mp)) {
                expectReason(DuplicateResolutionException.Reason.SAME_SESSION,one.client(),one.client(),ids(1,2));
                expectReason(DuplicateResolutionException.Reason.SAME_DIALER,one.client(),two.client(),ids(1,2));
                expectReason(DuplicateResolutionException.Reason.NO_COMMON_INPUT,one.client(),two.server(),DuplicateOptions.defaults());
                expectReason(DuplicateResolutionException.Reason.EQUAL_VALUES,one.client(),two.server(),ids(1,1));
                try (Held a = Held.open(one); Held b = Held.open(two)) { a.finish(); b.finish(); }
            }
        }
    }
    @Test void inputIdentitiesAreDefensivelyCopied() {
        byte[] local = {1}, peer = {2}; var opts = DuplicateOptions.defaults().withIdentities(local,peer);
        local[0]=9; peer[0]=9; opts.localIdentity()[0]=8;
        assertArrayEquals(new byte[] {1},opts.localIdentity()); assertArrayEquals(new byte[] {2},opts.peerIdentity());
        assertThrows(IllegalArgumentException.class,() -> DuplicateOptions.defaults().withIdentities(local,null));
    }
    @Test void rejoinIsOneAttemptAndFailureCannotUndoResolution() throws Exception {
        try (EventLoops loops = EventLoops.create(4)) {
            var cfg = PipePoolTest.config(loops,3);
            try (var survivor = new PipePoolTest.Fixture(cfg); Pair loser = Pair.memory(cfg); Held old = Held.open(loser)) {
                var calls = new AtomicInteger(); var cancel = new Cancellation();
                var route = PipePool.of(attempt -> { calls.incrementAndGet(); throw new IOException("route unavailable"); });
                var result = Zmux.resolveDuplicate(survivor.server,loser.client(),ids(2,1).withRejoin(route).withCancellation(cancel));
                assertSame(survivor.server,result.survivor()); assertTrue(result.rejoinStarted());
                var joined = result.rejoin().completion().toCompletableFuture().get(3,TimeUnit.SECONDS);
                assertEquals(1,calls.get()); assertEquals(0,joined.joined()); assertEquals(1,joined.failures()); assertNotNull(joined.cause());
                assertEquals(0,CancellationTest.registrations(cancel));
                assertThrows(GoingAwayException.class,loser.client()::openStream);
                old.finish();
            }
            try (var survivor = new PipePoolTest.Fixture(cfg); Pair loser = Pair.memory(cfg); Held old = Held.open(loser)) {
                var calls = new AtomicInteger();
                var route = PipePool.of(attempt -> { calls.incrementAndGet(); throw new IOException("must not dial"); });
                var result = Zmux.resolveDuplicate(survivor.client,loser.server(),ids(1,2).withRejoin(route));
                assertFalse(result.rejoinStarted()); assertEquals(0,calls.get());
            }
        }
    }
    @Test void successfulOneShotUsesTheMaintenanceOwnershipAndCleanupPath() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var fixture = new PipePoolTest.Fixture(PipePoolTest.config(loops,3))) {
            var cancellation = new Cancellation(); var calls = new AtomicInteger();
            var run = PipeMaintenance.startOnce(fixture.client,PipePool.of(attempt -> {
                calls.incrementAndGet(); return fixture.dial().open(attempt);
            }),cancellation);
            var result = run.completion().toCompletableFuture().get(3,TimeUnit.SECONDS);
            assertEquals(1,calls.get()); assertEquals(1,result.joined()); assertNull(result.cause());
            assertEquals(PipeState.Outbound.ACTIVE, result.pipes().getFirst().stats().orElseThrow().outbound());
            assertEquals(0,CancellationTest.registrations(cancellation));
        }
    }
    @Test void cancellingRejoinWaitsForItsDialWorkerWithoutUndoingTheDecision() throws Exception {
        try (EventLoops loops = EventLoops.create(3)) {
            var cfg = PipePoolTest.config(loops,3);
            try (Pair one = Pair.memory(cfg); Pair two = Pair.memory(cfg); Held hold = Held.open(two)) {
                var entered = new CountDownLatch(1); var exited = new AtomicInteger(); var cancel = new Cancellation();
                var route = PipePool.of(attempt -> {
                    entered.countDown();
                    try { new CountDownLatch(1).await(); throw new AssertionError(); }
                    finally { exited.incrementAndGet(); }
                });
                var result = Zmux.resolveDuplicate(one.server(),two.client(),ids(2,1).withRejoin(route).withCancellation(cancel));
                assertTrue(result.rejoinStarted()); assertTrue(entered.await(2,TimeUnit.SECONDS)); cancel.cancel();
                result.rejoin().completion().toCompletableFuture().get(3,TimeUnit.SECONDS);
                // The interrupted dial is abandoned, not awaited (PipeMaintenance): it exits soon after.
                long until = System.nanoTime() + 2_000_000_000L;
                while (exited.get() == 0 && System.nanoTime() < until) Thread.sleep(1);
                assertEquals(1,exited.get()); assertEquals(0,CancellationTest.registrations(cancel));
                assertThrows(GoingAwayException.class,two.client()::openStream);
                hold.finish();
            }
        }
    }
    @Test void plainPeersDrainExistingStreamsButRefuseNewOnesWithoutGoaway() throws Exception {
        try (EventLoops loops = EventLoops.create(3)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (Pair one = plainPair(cfg); Pair two = plainPair(cfg); Held held = Held.open(two)) {
                assertFalse(two.client().info().goAway());
                var resolved = Zmux.resolveDuplicate(one.server(),two.client(),ids(2,1));
                assertSame(two.client(),resolved.loser());
                assertThrows(GoingAwayException.class,two.client()::openStream);
                held.finish(); held.close();
                PipePoolTest.await(() -> two.client().isClosed());
            }
        }
    }
    @Test void losingSessionWakesAlreadyBlockedOpeners() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).limits(Limits.defaults().withIncomingStreams(4)).build();
            try (Pair one = Pair.memory(cfg); Pair two = Pair.memory(cfg); Held held = Held.open(two);
                 Held second = Held.open(two); Held third = Held.open(two); Held fourth = Held.open(two)) {
                var blocked = workers.submit(() -> two.client().openStream());
                PipePoolTest.await(() -> (Boolean)two.client().run(op -> two.client().core.streams().openersWaiting(true)));
                Zmux.resolveDuplicate(one.server(),two.client(),ids(2,1));
                assertInstanceOf(GoingAwayException.class,assertThrows(ExecutionException.class,() -> blocked.get(3,TimeUnit.SECONDS)).getCause());
                held.finish();
            }
        }
    }
    @Test void oneShotSkipsDialWhenTargetIsAlreadyFull() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var fixture = new PipePoolTest.Fixture(PipePoolTest.config(loops,2))) {
            var occupied = PipeMaintenance.startOnce(fixture.client,PipePool.of(fixture.dial()),null);
            occupied.completion().toCompletableFuture().get(3,TimeUnit.SECONDS);
            var calls = new AtomicInteger();
            var skipped = PipeMaintenance.startOnce(fixture.client,PipePool.of(attempt -> {
                calls.incrementAndGet(); throw new IOException("must not dial");
            }),null).completion().toCompletableFuture().get(3,TimeUnit.SECONDS);
            assertEquals(0,calls.get()); assertEquals(0,skipped.joined()); assertNull(skipped.cause());
        }
    }
    @Test void plainDrainIncludesStreamsOpenedBeforeTheirFirstWrite() throws Exception {
        for (boolean finish : new boolean[] {false,true}) try (EventLoops loops = EventLoops.create(3)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (Pair one = plainPair(cfg); Pair two = plainPair(cfg); var provisional = two.client().openStream()) {
                Zmux.resolveDuplicate(one.server(),two.client(),ids(2,1));
                assertFalse(two.client().isClosed());
                if (finish) {
                    provisional.write(new byte[] {4}); provisional.finish();
                    try (var accepted = two.server().acceptStream(Duration.ofSeconds(3))) {
                        assertArrayEquals(new byte[] {4},SessionTest.readAll(accepted)); accepted.finish();
                    }
                    assertArrayEquals(new byte[0],SessionTest.readAll(provisional));
                }
                provisional.close();
                PipePoolTest.await(() -> two.client().isClosed());
            }
        }
    }
}
