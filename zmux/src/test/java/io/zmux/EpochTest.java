package io.zmux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
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
        return ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(PipeAuth.psk(PSK).withEndToEnd()).probation(Duration.ZERO)
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
            var off = config(loops);
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(off)); var c = Zmux.connect(l.localAddress(), off); var s = l.accept(WAIT)) {
                c.addPipe(l.localAddress(), PipeOptions.defaults());
                ListenerEntryTest.await(() -> s.pipes().size() == 2);
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

    /** Zero is the default of the period and of the count, and a negative value turns either off. */
    @Test void zeroIsTheDefaultAndNegativeIsOff() {
        var b = ZmuxConfig.builder().epochPeriod(Duration.ZERO).epochJoins(0);
        assertEquals(Duration.ZERO, b.build().epochPeriod().orElseThrow());
        var off = ZmuxConfig.builder().epochPeriod(Duration.ofSeconds(-1)).epochJoins(-1).epochAfterFirstPipe(false).epochAfterResume(false).build();
        assertEquals(-1, off.epochJoins());
        assertEquals(false, off.epochAfterFirstPipe());
        assertEquals(false, off.epochAfterResume());
        assertEquals(true, ZmuxConfig.builder().build().epochAfterFirstPipe());
        assertEquals(true, ZmuxConfig.builder().build().epochAfterResume());
    }

    /**
     * Both ends move to epoch 1; a join afterwards uses the new key phase and the server's new
     * Locator, which the listener finds. The next exchange waits for the retention of epoch 0.
     */
    @Test void advanceThenJoin() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                c.addPipe(l.localAddress(), PipeOptions.defaults());
                ListenerEntryTest.await(() -> s.pipes().size() == 2);
                long before = s.stats().joinAuthFailures();
                c.advanceEpoch();
                ListenerEntryTest.await(() -> epoch(c) == 1 && epoch(s) == 1);
                assertEquals(new SessionEvent.EpochCommitted(1), nextEpochEvent(c));
                assertEquals(new SessionEvent.EpochCommitted(1), nextEpochEvent(s));
                c.addPipe(l.localAddress(), PipeOptions.defaults());
                ListenerEntryTest.await(() -> s.pipes().size() == 3);
                assertEquals(before, s.stats().joinAuthFailures());
                c.advanceEpoch();
                c.addPipe(l.localAddress(), PipeOptions.defaults());
                assertEquals(1, c.epoch());
                assertEquals(1, s.epoch());
            }
        }
    }

    @Test void forceNeedsRedialAndAdvanceNeedsMultipath() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = config(loops);
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(WAIT)) {
                assertThrows(IllegalStateException.class, () -> c.forceEpochAdvance(WAIT));
            }
            var single = ZmuxConfig.builder().eventLoops(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(single)); var c = Zmux.connect(l.localAddress(), single); var s = l.accept(WAIT)) {
                assertThrows(NotNegotiatedException.class, c::advanceEpoch);
            }
        }
    }

    /** A forced advance waits for the new epoch, then replaces every carrier through the redial hook. */
    @Test void forceAdvanceWaitsForTheEpochAndRedials() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var listener = Zmux.listener(ListenerConfig.of(config(loops)));
            var dials = new java.util.concurrent.atomic.AtomicInteger();
            var client = builder(loops).epochAfterFirstPipe(false).redial((need, cancellation) -> {
                dials.incrementAndGet();
                var io = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.handle(io[1], JoinHint.inBand(TestAddr.peer("redial")));
                return io[0];
            }).build();
            try (listener) {
                var io = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.handle(io[1], JoinHint.inBand(TestAddr.peer("first")));
                try (var c = Zmux.open(io[0], Role.CLIENT, client); var s = listener.accept(WAIT)) {
                    var joined = MemoryPipe.pair(Framing.BYTE_STREAM);
                    listener.handle(joined[1], JoinHint.inBand(TestAddr.peer("joined")));
                    c.addPipe(joined[0], PipeOptions.defaults());
                    ListenerEntryTest.await(() -> s.pipes().size() == 2);
                    c.forceEpochAdvance(WAIT);
                    assertEquals(1, c.epoch());
                    assertEquals(2, dials.get());
                }
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
