package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.Candidate;
import io.zmux.CandidateOutcome;
import io.zmux.CarrierKind;
import io.zmux.CarrierChoiceException;
import io.zmux.SessionEvent;
import io.zmux.ZmuxSession;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The carrier selection through intermediaries that behave as common deployments do: auto mode
 * picks the most preferred kind that passes, an explicit kind never falls back.
 */
@Timeout(180)
class ProxyModeTest {
    record Case(String name, TestProxy.Profile profile, CarrierKind want) {}

    /** Auto mode without joins, through each profile and directly: the first kind, by preference, that passes. */
    @Test void autoPicksTheBestKindThatPasses() throws Exception {
        List<Case> cases = List.of(
                new Case("direct", null, CarrierKind.WEBSOCKET),
                new Case("pass-through", TestProxy.Profile.PASS, CarrierKind.WEBSOCKET),
                // No WebSocket upgrade. Inject a failure on streaming and split uploads at the buffering boundary.
                new Case("nginx defaults", TestProxy.Profile.nginxDefaults().withBufferedUploadFailure(), CarrierKind.EXCHANGE),
                new Case("buffering cdn", TestProxy.Profile.bufferingCdn().withBufferedUploadFailure(), CarrierKind.EXCHANGE));
        try (var f = ModeHttpTest.fixture(false); var c = f.client(b -> b)) {
            for (Case k : cases) {
                try (TestProxy px = k.profile == null ? null : new TestProxy(f.base, k.profile)) {
                    URI url = px == null ? f.base : px.base;
                    var candidates = ModeHttpTest.candidates(c, url);
                    try (var s = ModeHttpTest.open(f.config, candidates)) {
                        assertChoice(s, candidates, k.want, k.name);
                        f.echo(s);
                    }
                }
            }
        }
    }

    /** Behind a front that passes WebSocket only, the other kinds are refused and WebSocket is chosen, however it ranks. */
    @Test void aWebSocketOnlyFront() throws Exception {
        try (var f = ModeHttpTest.fixture(false); var c = f.client(b -> b); var px = new TestProxy(f.base, TestProxy.Profile.webSocketFront())) {
            List<Candidate> all = ModeHttpTest.candidates(c, px.base);
            // WebSocket last; the other carriers are redirected to a host they refuse.
            List<Candidate> order = List.of(all.get(1), all.get(2), all.get(3), all.get(0));
            try (var s = ModeHttpTest.open(f.config, order)) {
                assertChoice(s, order, CarrierKind.WEBSOCKET, "WebSocket-only front");
                f.echo(s);
            }
        }
    }

    private static void assertChoice(ZmuxSession session, List<Candidate> order, CarrierKind winner, String name)
            throws Exception {
        var tries = session.stats().carrierCandidates();
        var message = name + ": " + ModeHttpTest.tries(session);
        assertEquals(winner, session.stats().carrier(), message);
        assertEquals(order.getFirst().kind(), tries.getFirst().kind(), message);
        assertEquals(winner, tries.getLast().kind(), message);
        assertEquals(CandidateOutcome.USABLE, tries.getLast().outcome(), message);

        // Once a prepared carrier succeeds, outcomes of pending speculative attempts can be discarded.
        // The explicit cases below exercise individual refusals.
        var refused = new HashSet<CarrierKind>();
        for (var candidate : order) {
            if (candidate.kind() == winner) break;
            refused.add(candidate.kind());
        }
        for (var attempt : tries.subList(0, tries.size() - 1)) {
            assertTrue(refused.remove(attempt.kind()), message);
            assertEquals(CandidateOutcome.REFUSED, attempt.outcome(), message);
        }

        int reported = 0;
        while (true) {
            switch (session.nextEvent()) {
                case SessionEvent.CarrierCandidate attempt -> assertEquals(tries.get(reported++), attempt.result(), message);
                case SessionEvent.CarrierChosen choice -> {
                    assertEquals(tries.size(), reported, message);
                    assertEquals(new SessionEvent.CarrierChosen(winner), choice, message);
                    return;
                }
                default -> { }
            }
        }
    }

    /** An explicit kind that the path does not pass fails, without another kind being tried. */
    @Test void anExplicitKindNeverFallsBack() throws Exception {
        try (var f = ModeHttpTest.fixture(false); var c = f.client(b -> b);
             var nginx = new TestProxy(f.base, TestProxy.Profile.nginxDefaults().withBufferedUploadFailure());
             var ws = new TestProxy(f.base, TestProxy.Profile.webSocketFront())) {
            var nc = ModeHttpTest.candidates(c, nginx.base);
            var wc = ModeHttpTest.candidates(c, ws.base);
            for (Candidate k : List.of(nc.get(0), nc.get(1), nc.get(2), wc.get(3))) {
                var e = assertThrows(CarrierChoiceException.class, () -> ModeHttpTest.open(f.config, List.of(k)), k.kind().toString());
                assertEquals(1, e.results().size(), k.kind() + ": " + e.results());
                assertEquals(k.kind(), e.results().getFirst().kind());
                assertEquals(CandidateOutcome.REFUSED, e.results().getFirst().outcome());
            }
            // Explicit and passing: nothing else is tried.
            try (ZmuxSession s = ModeHttpTest.open(f.config, List.of(nc.get(3)))) {
                assertEquals(CarrierKind.EXCHANGE, s.stats().carrier());
                assertEquals(1, s.stats().carrierCandidates().size());
                f.echo(s);
            }
        }
    }
}
