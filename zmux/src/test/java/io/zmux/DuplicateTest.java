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
            try { return Zmux.open(pair[1], Role.AUTO, cfg); } catch(IOException e) { throw new CompletionException(e); }
        }, r -> Thread.ofVirtual().start(r));
        return new Pair(Zmux.open(pair[0], Role.AUTO, cfg), accepting.get(3,TimeUnit.SECONDS));
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
    static void expectReason(DuplicateUndecidedException.Reason reason, ZmuxSession a, ZmuxSession b, DuplicateOptions opts) {
        assertEquals(reason, assertThrows(DuplicateUndecidedException.class, () -> Zmux.resolveDuplicate(a,b,opts)).reason());
    }

    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
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
                second.client().awaitClosed();
                second.server().awaitClosed();
                try (var fresh = first.client().openStream()) { fresh.write(new byte[] {9}); fresh.finish();
                    try (var accepted = first.server().acceptStream(Duration.ofSeconds(3))) {
                        assertArrayEquals(new byte[] {9},SessionTest.readAll(accepted)); accepted.finish();
                    }
                }
            }
            Leaks.none(loops);
        }
    }
    @Test void noncesUseOneCommonDomainAndBothEndpointsAgree() throws Exception {
        for (boolean mp : new boolean[] {true,false}) try (EventLoops loops = EventLoops.create(3)) {
            var cfg = mp ? SessionTestSupport.config(loops,3) : ZmuxConfig.builder().eventLoops(loops).build();
            try (Pair one = mp ? Pair.memory(cfg) : symmetric(cfg); Pair two = mp ? Pair.memory(cfg) : symmetric(cfg);
                 Held _ = Held.open(one); Held _ = Held.open(two)) {
                if (!mp) {
                    expectReason(DuplicateUndecidedException.Reason.UNKNOWN_DIALER,one.client(),two.server(),ids(1,2));
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
            var plain = ZmuxConfig.builder().eventLoops(loops).build(); var mp = SessionTestSupport.config(loops,3);
            try (Pair one = Pair.memory(plain); Pair two = Pair.memory(mp)) {
                expectReason(DuplicateUndecidedException.Reason.SAME_SESSION,one.client(),one.client(),ids(1,2));
                expectReason(DuplicateUndecidedException.Reason.SAME_DIALER,one.client(),two.client(),ids(1,2));
                expectReason(DuplicateUndecidedException.Reason.NO_COMMON_INPUT,one.client(),two.server(),DuplicateOptions.defaults());
                expectReason(DuplicateUndecidedException.Reason.EQUAL_VALUES,one.client(),two.server(),ids(1,1));
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
    private static final MemberSpec REJOIN = new MemberSpec("", java.util.List.of(new AttachmentSpec(Directions.BOTH, PipeOptions.defaults())));

    @Test void rejoinIsOneAttemptAndFailureCannotUndoResolution() throws Exception {
        try (EventLoops loops = EventLoops.create(4)) {
            var cfg = SessionTestSupport.config(loops,3);
            try (var survivor = new SessionTestSupport.Fixture(cfg); Pair loser = Pair.memory(cfg); Held old = Held.open(loser)) {
                var calls = new AtomicInteger(); var cancel = new Cancellation();
                CarrierOpener route = (setup, request) -> {
                    calls.incrementAndGet();
                    return new CarrierOpener.Opened(java.util.List.of(), new IOException("route unavailable"));
                };
                var result = Zmux.resolveDuplicate(survivor.server,loser.client(),ids(2,1).withRejoin(REJOIN, route).withCancellation(cancel));
                assertSame(survivor.server,result.survivor()); assertTrue(result.rejoinStarted());
                var joined = result.rejoin().completion().toCompletableFuture().get(3,TimeUnit.SECONDS);
                assertEquals(1,calls.get()); assertEquals(Attempt.Disposition.DIAL_FAILED, joined.disposition()); assertNotNull(joined.error());
                assertThrows(GoingAwayException.class,loser.client()::openStream);
                old.finish();
            }
            try (var survivor = new SessionTestSupport.Fixture(cfg); Pair loser = Pair.memory(cfg); Held _ = Held.open(loser)) {
                var calls = new AtomicInteger();
                CarrierOpener route = (setup, request) -> { calls.incrementAndGet(); throw new IOException("must not dial"); };
                var result = Zmux.resolveDuplicate(survivor.client,loser.server(),ids(1,2).withRejoin(REJOIN, route));
                assertFalse(result.rejoinStarted()); assertEquals(0,calls.get());
            }
        }
    }
    @Test void cancellingRejoinSignalsItsOpenerWithoutUndoingTheDecision() throws Exception {
        try (EventLoops loops = EventLoops.create(3)) {
            var cfg = SessionTestSupport.config(loops,3);
            try (Pair one = Pair.memory(cfg); Pair two = Pair.memory(cfg); Held hold = Held.open(two)) {
                var entered = new CountDownLatch(1);
                var exited = new java.util.concurrent.CompletableFuture<Void>();
                var cancel = new Cancellation();
                CarrierOpener route = (setup, request) -> {
                    var release = new java.util.concurrent.CompletableFuture<Void>();
                    var registration = setup.onCancel(() -> release.complete(null));
                    try {
                        entered.countDown();
                        release.join();
                        return new CarrierOpener.Opened(java.util.List.of());
                    } finally {
                        registration.close();
                        exited.complete(null);
                    }
                };
                var result = Zmux.resolveDuplicate(one.server(),two.client(),ids(2,1).withRejoin(REJOIN, route).withCancellation(cancel));
                try {
                    assertTrue(result.rejoinStarted());
                    assertTrue(entered.await(30, TimeUnit.SECONDS));
                } finally {
                    cancel.cancel();
                }
                assertEquals(Attempt.Disposition.CANCELLED, result.rejoin().completion().get(30, TimeUnit.SECONDS).disposition());
                exited.get(30, TimeUnit.SECONDS);
                assertThrows(GoingAwayException.class,two.client()::openStream);
                hold.finish();
            }
        }
    }
    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    @Test void plainPeersDrainExistingStreamsButRefuseNewOnesWithoutGoaway() throws Exception {
        try (EventLoops loops = EventLoops.create(3)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (Pair one = plainPair(cfg); Pair two = plainPair(cfg); Held held = Held.open(two)) {
                assertFalse(two.client().info().peerIsZmux());
                var resolved = Zmux.resolveDuplicate(one.server(),two.client(),ids(2,1));
                assertSame(two.client(),resolved.loser());
                assertThrows(GoingAwayException.class,two.client()::openStream);
                held.finish(); held.close();
                two.client().awaitClosed();
            }
        }
    }
    @Test void losingSessionWakesAlreadyBlockedOpeners() throws Exception {
        try (EventLoops loops = EventLoops.create(3)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).maxIncomingStreams(4).build();
            try (Pair one = Pair.memory(cfg); Pair two = Pair.memory(cfg); Held held = Held.open(two);
                 Held _ = Held.open(two); Held _ = Held.open(two); Held _ = Held.open(two)) {
                var blocked = two.client().openStreamAsync();
                assertEquals(true, two.client().run(op -> two.client().core.streams().openersWaiting(true)));
                Zmux.resolveDuplicate(one.server(),two.client(),ids(2,1));
                assertInstanceOf(GoingAwayException.class,assertThrows(ExecutionException.class,() -> blocked.get(3,TimeUnit.SECONDS)).getCause());
                held.finish();
            }
        }
    }
    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
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
                two.client().awaitClosed();
            }
        }
    }
}
