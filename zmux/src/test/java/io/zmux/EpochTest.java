package io.zmux;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/** Session epochs over real TCP: the exchange on a Sealed pipe, and joins that follow it. */
@org.junit.jupiter.api.Timeout(30)
class EpochTest {
    private static final byte[] PSK = new byte[32];
    private static final Duration WAIT = Duration.ofSeconds(5);

    private static ZmuxConfig config(EventLoops loops) {
        return builder(loops).epochAfterFirstPipe(false).build();
    }

    private static ZmuxConfig.Builder builder(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(PipeAuth.psk(PSK).withEndToEnd()).noProbation()
                .resumeTimeout(Duration.ofSeconds(10));
    }

    /** A session keyed by a pre-shared key advances once its first Sealed pipe exists, unasked; turned off, it stays. */
    @Test void theFirstSecurePipeAdvances() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = builder(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                assertEquals(0, c.epoch());
                c.addPipe(l.localAddress(), PipeOptions.defaults());
                ListenerEntryTest.await(() -> epoch(c) == 1 && epoch(s) == 1);
                assertEquals(new SessionEvent.EpochCommitted(1), nextEpochEvent(c));
            }
            var off = builder(loops).epochAfterFirstPipe(false).noEpochPeriod().build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(off)); var c = Zmux.connect(l.localAddress(), off); var s = l.accept(WAIT)) {
                var joined = c.addPipe(l.localAddress(), PipeOptions.defaults());
                ListenerEntryTest.await(() -> s.pipes().size() == 2);
                assertTrue(joined.stats().orElseThrow().replaceBy().isEmpty());
                c.ping(WAIT); // a round trip after the join: an exchange would have started by now
                assertEquals(0, c.epoch());
                assertEquals(0, s.epoch());
            }
        }
    }

    /** Leaving a suspension advances once, back in Stable. */
    @Test void aResumptionAdvances() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                c.pipes().getFirst().abort();
                ResumeTest.event(c, SessionEvent.SessionSuspended.class);
                assertEquals(0, c.epoch());
                c.addPipe(l.localAddress(), PipeOptions.defaults());
                ResumeTest.event(c, SessionEvent.SessionResumed.class);
                ListenerEntryTest.await(() -> epoch(c) == 1 && epoch(s) == 1);
            }
        }
    }

    /** The period and the count have defaults and are turned off with their own methods; zero and negative values are rejected. */
    @Test void defaultsAndOffSettings() {
        var d = ZmuxConfig.defaults();
        assertEquals(java.util.Optional.of(Duration.ofHours(1)), d.epochPeriod());
        assertEquals(java.util.OptionalLong.of(65536), d.epochJoins());
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().epochPeriod(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().epochJoins(0));
        var off = ZmuxConfig.builder().noEpochPeriod().noEpochJoins().epochAfterFirstPipe(false).epochAfterResume(false).build();
        assertEquals(java.util.Optional.empty(), off.epochPeriod());
        assertEquals(java.util.OptionalLong.empty(), off.epochJoins());
        assertFalse(off.isEpochAfterFirstPipe());
        assertFalse(off.isEpochAfterResume());
        assertTrue(ZmuxConfig.builder().build().isEpochAfterFirstPipe());
        assertTrue(ZmuxConfig.builder().build().isEpochAfterResume());
    }

    /**
     * Both ends move to epoch 1; a join afterwards uses the new key phase and the server's new
     * Locator, which the listener finds. The next exchange waits for the retention of epoch 0.
     */
    @Test void advanceThenJoin() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                var bootstrap = c.pipes().getFirst().stats().orElseThrow();
                assertEquals(0, bootstrap.epoch().orElseThrow());
                var old = c.addPipe(l.localAddress(), PipeOptions.defaults());
                long replacement = old.stats().orElseThrow().replaceBy().orElseThrow();
                ListenerEntryTest.await(() -> s.pipes().size() == 2);
                long before = s.stats().joinAuthFailures();
                c.advanceEpoch();
                ListenerEntryTest.await(() -> epoch(c) == 1 && epoch(s) == 1);
                assertEquals(new SessionEvent.EpochCommitted(1), nextEpochEvent(c));
                assertEquals(new SessionEvent.EpochCommitted(1), nextEpochEvent(s));
                long beforeJoin = System.nanoTime();
                var joined = c.addPipe(l.localAddress(), PipeOptions.defaults());
                long afterJoin = System.nanoTime();
                ListenerEntryTest.await(() -> s.pipes().size() == 3);
                var nextStats = joined.stats().orElseThrow();
                assertEquals(1, nextStats.epoch().orElseThrow());
                var peerStats = s.pipeStats().stream().filter(p -> p.pipe().equals(joined.id())).findFirst().orElseThrow();
                assertEquals(1, peerStats.epoch().orElseThrow());
                assertEquals(0, old.stats().orElseThrow().epoch().orElseThrow());
                assertEquals(replacement, old.stats().orElseThrow().replaceBy().orElseThrow());
                long due = nextStats.replaceBy().orElseThrow() - Duration.ofHours(2).toNanos();
                assertTrue(due - beforeJoin >= 0 && afterJoin - due >= 0);
                assertEquals(before, s.stats().joinAuthFailures());
                c.advanceEpoch();
                c.addPipe(l.localAddress(), PipeOptions.defaults());
                assertEquals(1, c.epoch());
                assertEquals(1, s.epoch());
            }
        }
    }

    /**
     * The answerer's new Locator is routable before its answer leaves: the offerer switches on the
     * answer, and its next join names that Locator while the answerer's loop has not yet run the
     * round's events. The server's loop is held right after the round that wrote its answer until
     * the listener has decided that join.
     */
    @Test void aLocatorIsRoutableBeforeItsAnswerLeaves() throws Exception {
        // Own loops for each end; three on the server, taken in turn, so the second join's carrier
        // is read on a loop other than the held session's.
        try (EventLoops server = EventLoops.create(3); EventLoops client = EventLoops.create(1)) {
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(config(server))); var c = Zmux.connect(l.localAddress(), config(client)); var s = l.accept(WAIT);
                 var ex = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                c.addPipe(l.localAddress(), PipeOptions.defaults());
                ListenerEntryTest.await(() -> s.pipes().size() == 2);
                var held = new java.util.concurrent.CountDownLatch(1);
                var installed = new java.util.concurrent.CompletableFuture<Void>();
                s.driver.loop().execute(() -> {
                    s.driver.eventSink(e -> {
                        if (e.kind() != io.zmux.internal.core.Event.Kind.EPOCH_COMMITTED) return;
                        held.countDown();
                        // Bounded only against a hang: the join is decided while the loop waits here.
                        long end = System.nanoTime() + WAIT.toNanos();
                        while (l.stats().rejected() == 0 && l.hub.offeredJoins() == 0 && System.nanoTime() < end) Thread.onSpinWait();
                    });
                    installed.complete(null);
                });
                installed.get(WAIT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
                c.advanceEpoch(); // the client offers; it uses epoch 1 once the answer arrives
                ListenerEntryTest.await(() -> epoch(c) == 1);
                var joined = ex.submit(() -> c.addPipe(l.localAddress(), PipeOptions.defaults()));
                joined.get(WAIT.toMillis() * 2, java.util.concurrent.TimeUnit.MILLISECONDS);
                org.junit.jupiter.api.Assertions.assertTrue(held.await(0, java.util.concurrent.TimeUnit.MILLISECONDS));
                assertEquals(0, l.stats().rejected());
                ListenerEntryTest.await(() -> s.pipes().size() == 3);
            }
        }
    }

    @Test void advanceNeedsMultipath() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var single = ZmuxConfig.builder().eventLoops(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(single)); var c = Zmux.connect(l.localAddress(), single); var _ = l.accept(WAIT)) {
                assertThrows(NotNegotiatedException.class, c::advanceEpoch);
            }
        }
    }

    private static long epoch(ZmuxSession z) {
        try {
            return z.epoch();
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static SessionEvent nextEpochEvent(ZmuxSession z) throws Exception {
        while (true) {
            SessionEvent e = z.nextEvent(WAIT);
            if (e instanceof SessionEvent.EpochCommitted || e instanceof SessionEvent.EpochDeleted) return e;
        }
    }
}
