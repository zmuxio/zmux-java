package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class CarrierSelectionTest {
    /** A kind whose carriers reach the listener over memory pipes. */
    static Candidate reaching(CarrierKind kind, ZmuxListener l, AtomicInteger dials) {
        return Candidate.of(kind, (cred, timeout) -> {
            dials.incrementAndGet();
            var m = MemoryPipe.pair(Framing.BYTE_STREAM);
            l.handle(m[1], JoinHint.inBand(TestAddr.peer("client"))).closed();
            return m[0];
        });
    }

    /** A kind an intermediary refuses. */
    static Candidate refused(CarrierKind kind, AtomicInteger dials) {
        return Candidate.of(kind, (cred, timeout) -> {
            dials.incrementAndGet();
            throw new HttpStatusException(403, "Forbidden");
        });
    }

    /** A kind whose carriers open but never reach the server: its peer never answers. */
    static Candidate silent(CarrierKind kind, AtomicInteger dials, List<PipeIo> keep) {
        return Candidate.of(kind, (cred, timeout) -> {
            dials.incrementAndGet();
            var m = MemoryPipe.pair(Framing.BYTE_STREAM);
            keep.add(m[1]);
            return m[0];
        });
    }

    static List<SessionEvent> carrierEvents(ZmuxSession s, int n) throws Exception {
        var out = new ArrayList<SessionEvent>();
        while (out.size() < n) {
            SessionEvent e = s.nextEvent(Duration.ofSeconds(5));
            if (e instanceof SessionEvent.CarrierCandidate || e instanceof SessionEvent.CarrierChosen) out.add(e);
        }
        return out;
    }

    static void echo(ZmuxSession client, ZmuxListener l) throws Exception {
        try (var server = l.accept(Duration.ofSeconds(5))) { echo(client, server); }
    }

    static void echo(ZmuxSession client, ZmuxSession server) throws Exception {
        try (var st = client.openStream()) {
            st.write(new byte[] {1, 2, 3});
            st.finish();
            try (var in = server.acceptStream(Duration.ofSeconds(5))) { assertArrayEquals(new byte[] {1, 2, 3}, in.readAll(16)); }
        }
    }

    @Test void anExplicitModeReportsAFailureAndNeverFallsBack() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var dials = new AtomicInteger();
            var e = assertThrows(ModeException.class, () -> Zmux.dial(CarrierMode.explicit(refused(CarrierKind.WEBSOCKET, dials)), cfg, null));
            assertEquals(List.of(new CandidateResult(CarrierKind.WEBSOCKET, CandidateOutcome.REFUSED, false, e.results().getFirst().error())), e.results());
            assertEquals(1, dials.get());
            try (var l = Zmux.listener(ListenerConfig.of(cfg))) {
                try (var s = Zmux.dial(CarrierMode.explicit(reaching(CarrierKind.STREAMING, l, dials)), cfg, null)) {
                    assertEquals(CarrierKind.STREAMING, s.stats().carrier());
                    assertEquals(CandidateOutcome.USABLE, s.stats().carrierCandidates().getFirst().outcome());
                    echo(s, l);
                }
            }
        }
    }

    /** A kind whose carriers open at once and then fail with a refusing HTTP status, as a streaming request does. */
    static Candidate refusedLater(CarrierKind kind) {
        return Candidate.of(kind, (cred, timeout) -> new PipeIo(new PipeReader() {
            @Override public void start(Sink sink) { sink.onError(new HttpStatusException(403, "Forbidden")); }
            @Override public void close() {}
        }, new PipeWriter() {
            @Override public void write(OutboundBatch batch, Completion done) { done.completed(); }
            @Override public void close() {}
        }, Framing.BYTE_STREAM, "refused later", new Object()));
    }

    @Test void aCarrierRefusedAfterItOpenedIsReportedRefused() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var e = assertThrows(ModeException.class, () -> Zmux.dial(CarrierMode.explicit(refusedLater(CarrierKind.STREAMING)), cfg,
                    ModePolicy.defaults().withTimer(3, Duration.ofSeconds(2), Duration.ofSeconds(2))));
            assertEquals(CandidateOutcome.REFUSED, e.results().getFirst().outcome(), String.valueOf(e.results().getFirst().error()));
            try (var l = Zmux.listener(ListenerConfig.of(cfg))) {
                var dials = new AtomicInteger();
                try (var s = Zmux.dial(CarrierMode.auto(refusedLater(CarrierKind.STREAMING), reaching(CarrierKind.EXCHANGE, l, dials)), cfg,
                        ModePolicy.defaults().withNoJoin(true))) {
                    assertEquals(CandidateOutcome.REFUSED, s.stats().carrierCandidates().getFirst().outcome());
                    assertEquals(CarrierKind.EXCHANGE, s.stats().carrier());
                }
            }
        }
    }

    @Test void autoTriesTheCandidatesInOrderAndRemembersTheOutcomes() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var l = Zmux.listener(ListenerConfig.of(cfg))) {
                var cache = new CarrierCache();
                var policy = ModePolicy.of(cache, "server").withTimer(3, Duration.ofMillis(200), Duration.ofMillis(300));
                var ws = new AtomicInteger(); var st = new AtomicInteger(); var ex = new AtomicInteger();
                var keep = new ArrayList<PipeIo>();
                var mode = CarrierMode.auto(refused(CarrierKind.WEBSOCKET, ws), silent(CarrierKind.STREAMING, st, keep),
                        reaching(CarrierKind.EXCHANGE, l, ex));
                long t0 = System.nanoTime();
                try (var s = Zmux.dial(mode, cfg, policy)) {
                    assertTrue(System.nanoTime() - t0 >= 150_000_000, "the silent candidate waited its timer");
                    var tries = s.stats().carrierCandidates();
                    assertEquals(List.of(CandidateOutcome.REFUSED, CandidateOutcome.TIMED_OUT, CandidateOutcome.USABLE),
                            tries.stream().map(CandidateResult::outcome).toList());
                    assertEquals(CarrierKind.EXCHANGE, s.stats().carrier());
                    var ev = carrierEvents(s, 4);
                    assertEquals(new SessionEvent.CarrierChosen(CarrierKind.EXCHANGE), ev.getLast());
                    assertEquals(CarrierKind.WEBSOCKET, ((SessionEvent.CarrierCandidate) ev.getFirst()).result().kind());
                    echo(s, l);
                    // The silent carrier carried the client's parameters and then ended, without CONNECTION_CLOSE.
                    var got = new java.io.ByteArrayOutputStream();
                    var end = new java.util.concurrent.CompletableFuture<Void>();
                    keep.getFirst().reader().start(new PipeReader.Sink() {
                        @Override public void onData(ByteBuffer data, Runnable release) {
                            byte[] a = new byte[data.remaining()]; data.get(a); synchronized (got) { got.writeBytes(a); } release.run();
                        }
                        @Override public void onEnd() { end.complete(null); }
                        @Override public void onError(Throwable cause) { end.complete(null); }
                    });
                    end.get(5, java.util.concurrent.TimeUnit.SECONDS);
                    byte[] b; synchronized (got) { b = got.toByteArray(); }
                    int first = b[0] & 0xff, n = 1 << (first >> 6);
                    long len = first & 0x3f;
                    for (int k = 1; k < n; k++) len = len << 8 | (b[k] & 0xff);
                    assertEquals(n + len, b.length, "one record, the parameters");
                }
                assertTrue(cache.backingOff("server", CarrierKind.WEBSOCKET));
                assertTrue(cache.backingOff("server", CarrierKind.STREAMING));
                assertTrue(cache.usable("server", CarrierKind.EXCHANGE));
                // The next dial skips the kinds backing off.
                try (var s = Zmux.dial(mode, cfg, policy)) {
                    assertEquals(1, ws.get());
                    assertEquals(1, st.get());
                    assertEquals(2, ex.get());
                    assertEquals(1, s.stats().carrierCandidates().size());
                    echo(s, l);
                }
            }
        }
    }

    @Test void autoEstablishesOnTheRobustKindAndMovesToAPreferredOne() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(4)
                    .withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO)).build();
            try (var l = Zmux.listener(ListenerConfig.of(cfg))) {
                var ws = new AtomicInteger(); var ex = new AtomicInteger();
                var policy = ModePolicy.defaults().withSettle(Duration.ofMillis(200));
                try (var s = Zmux.dial(CarrierMode.auto(reaching(CarrierKind.WEBSOCKET, l, ws), reaching(CarrierKind.EXCHANGE, l, ex)), cfg, policy)) {
                    assertEquals(CarrierKind.EXCHANGE, s.stats().carrier(), "established on the exchange carrier");
                    var ev = carrierEvents(s, 4);
                    assertEquals(new SessionEvent.CarrierChosen(CarrierKind.WEBSOCKET), ev.getLast());
                    var joined = (SessionEvent.CarrierCandidate) ev.get(2);
                    assertTrue(joined.result().joined());
                    assertEquals(CandidateOutcome.USABLE, joined.result().outcome());
                    assertEquals(CarrierKind.WEBSOCKET, s.stats().carrier());
                    // The bootstrap pipe of the replaced kind drains and retires: on a loaded host the
                    // exchange carrier's drain may run to its T_fail fallback (8 x PTO) plus the linger.
                    // A drain completes with the peer's PIPE_ABANDON (MULTIPATH.md 8.2), so the server
                    // session stays open until then: ending it first ends the session, not the pipe.
                    try (var server = l.accept(Duration.ofSeconds(5))) {
                        echo(s, server);
                        long until = System.nanoTime() + 30_000_000_000L;
                        while (s.paths().list().stream().anyMatch(p -> p.id() == 0) && !s.isClosed() && System.nanoTime() < until) Thread.sleep(10);
                        assertFalse(s.isClosed(), "session open");
                        assertTrue(s.paths().list().stream().noneMatch(p -> p.id() == 0), "pipe 0 retired: " + s.paths().list());
                        echo(s, server);
                    }
                    assertEquals(1, ws.get());
                    assertEquals(1, ex.get());
                }
            }
        }
    }

    @Test void theCacheBacksOffFailedKindsAndForgetsOldSuccesses() {
        var c = new CarrierCache(Duration.ofSeconds(10), Duration.ofSeconds(60), Duration.ofSeconds(150));
        long t = 1_000;
        c.record("k", CarrierKind.SPLIT, false, t);
        assertEquals(t + 60_000_000_000L, c.failUntil("k", CarrierKind.SPLIT, t));
        c.record("k", CarrierKind.SPLIT, false, t);
        assertEquals(t + 120_000_000_000L, c.failUntil("k", CarrierKind.SPLIT, t));
        c.record("k", CarrierKind.SPLIT, false, t);
        assertEquals(t + 150_000_000_000L, c.failUntil("k", CarrierKind.SPLIT, t), "bounded");
        assertEquals(Long.MIN_VALUE, c.failUntil("k", CarrierKind.SPLIT, t + 150_000_000_000L));
        assertEquals(Long.MIN_VALUE, c.failUntil("other", CarrierKind.SPLIT, t));
        c.record("k", CarrierKind.SPLIT, true, t);
        assertTrue(c.good("k", CarrierKind.SPLIT, t + 9_000_000_000L));
        assertFalse(c.good("k", CarrierKind.SPLIT, t + 10_000_000_000L), "expired");
        c.record("k", CarrierKind.SPLIT, false, t);
        assertEquals(t + 60_000_000_000L, c.failUntil("k", CarrierKind.SPLIT, t), "a success resets the backoff");
        assertEquals(Duration.ofSeconds(3), ModePolicy.defaults().timer(Duration.ofSeconds(5)));
        assertEquals(Duration.ofSeconds(1), ModePolicy.defaults().timer(Duration.ZERO));
        assertEquals(Duration.ofMillis(1500), ModePolicy.defaults().timer(Duration.ofMillis(500)));
    }

    /** The statistics keep the latest 64 tries. */
    @Test void selectionBookkeepingIsBounded() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxSession c = pair.client();
            var tries = new ArrayList<CandidateResult>();
            for (int i = 0; i < 100; i++) tries.add(new CandidateResult(CarrierKind.values()[i % CarrierKind.values().length],
                    CandidateOutcome.values()[i % CandidateOutcome.values().length], i == 99, null));
            c.reportCarrier(tries, null);
            var kept = c.stats().carrierCandidates();
            assertEquals(64, kept.size());
            assertEquals(tries.subList(36, 100), kept);
        }
    }
}
