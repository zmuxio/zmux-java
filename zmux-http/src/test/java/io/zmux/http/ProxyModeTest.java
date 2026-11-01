package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.Candidate;
import io.zmux.CandidateOutcome;
import io.zmux.CarrierKind;
import io.zmux.CarrierMode;
import io.zmux.ModeException;
import io.zmux.SessionEvent;
import io.zmux.Zmux;
import io.zmux.ZmuxSession;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The carrier selection through intermediaries that behave as common deployments do: auto mode
 * picks the most preferred kind that passes, an explicit kind never falls back.
 */
@Timeout(180)
class ProxyModeTest {
    record Case(String name, TestProxy.Profile profile, CarrierKind want, int tries) {}

    /** Auto mode without joins, through each profile and directly: the first kind, by preference, that passes. */
    @Test void autoPicksTheBestKindThatPasses() throws Exception {
        List<Case> cases = List.of(
                new Case("direct", null, CarrierKind.WEBSOCKET, 1),
                new Case("pass-through", TestProxy.Profile.PASS, CarrierKind.WEBSOCKET, 1),
                // No WebSocket (refused: the upgrade arrives as a plain GET), request bodies
                // buffered (streaming and split time out), responses streamed on request.
                new Case("nginx defaults", TestProxy.Profile.nginxDefaults(), CarrierKind.EXCHANGE, 4),
                new Case("buffering cdn", TestProxy.Profile.bufferingCdn(3000), CarrierKind.EXCHANGE, 4));
        try (var f = ModeHttpTest.fixture(false); var c = f.client(b -> b)) {
            for (Case k : cases) {
                try (TestProxy px = k.profile == null ? null : new TestProxy(f.base, k.profile)) {
                    URI url = px == null ? f.base : px.base;
                    try (var s = Zmux.dial(CarrierMode.auto(ModeHttpTest.candidates(c, url)), f.config, ModeHttpTest.fast(true))) {
                        var tries = s.stats().carrierCandidates();
                        assertEquals(k.want, s.stats().carrier(), k.name + ": " + ModeHttpTest.tries(s));
                        assertEquals(k.tries, tries.size(), k.name + ": " + ModeHttpTest.tries(s));
                        for (int i = 0; i < tries.size(); i++) {
                            assertEquals(i == tries.size() - 1, tries.get(i).outcome() == CandidateOutcome.USABLE, k.name + ": " + ModeHttpTest.tries(s));
                        }
                        if (k.tries > 1) assertEquals(CandidateOutcome.REFUSED, tries.getFirst().outcome(), k.name + ": the upgrade is refused");
                        // One event per try, then the choice.
                        List<SessionEvent> ev = new ArrayList<>();
                        while (ev.size() <= k.tries) {
                            SessionEvent e = s.nextEvent(Duration.ofSeconds(5));
                            assertNotNull(e, k.name);
                            if (e instanceof SessionEvent.CarrierCandidate || e instanceof SessionEvent.CarrierChosen) ev.add(e);
                        }
                        assertEquals(new SessionEvent.CarrierChosen(k.want), ev.getLast(), k.name);
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
            // WebSocket last; the exchange carrier, refused, fails at its deadline.
            List<Candidate> order = List.of(all.get(1), all.get(2), all.get(3), all.get(0));
            try (var s = Zmux.dial(CarrierMode.auto(order), f.config, ModeHttpTest.fast(true))) {
                assertEquals(CarrierKind.WEBSOCKET, s.stats().carrier(), ModeHttpTest.tries(s).toString());
                var tries = s.stats().carrierCandidates();
                assertEquals(4, tries.size());
                assertEquals(CandidateOutcome.REFUSED, tries.get(0).outcome(), () -> String.valueOf(tries.get(0).error()) + " / " + (tries.get(0).error() instanceof io.zmux.SessionClosedException sc ? sc.error() : null));
                assertEquals(CandidateOutcome.REFUSED, tries.get(1).outcome());
                assertNotEquals(CandidateOutcome.USABLE, tries.get(2).outcome());
                f.echo(s);
            }
        }
    }

    /** An explicit kind that the path does not pass fails, without another kind being tried. */
    @Test void anExplicitKindNeverFallsBack() throws Exception {
        try (var f = ModeHttpTest.fixture(false); var c = f.client(b -> b);
             var nginx = new TestProxy(f.base, TestProxy.Profile.nginxDefaults()); var ws = new TestProxy(f.base, TestProxy.Profile.webSocketFront())) {
            var nc = ModeHttpTest.candidates(c, nginx.base);
            var wc = ModeHttpTest.candidates(c, ws.base);
            var policy = ModeHttpTest.fast(true);
            for (Candidate k : List.of(nc.get(0), nc.get(1), nc.get(2), wc.get(3))) {
                var e = assertThrows(ModeException.class, () -> Zmux.dial(CarrierMode.explicit(k), f.config, policy), k.kind().toString());
                assertEquals(1, e.results().size(), k.kind() + ": " + e.results());
                assertEquals(k.kind(), e.results().getFirst().kind());
                assertNotEquals(CandidateOutcome.USABLE, e.results().getFirst().outcome());
            }
            // Explicit and passing: nothing else is tried.
            try (ZmuxSession s = Zmux.dial(CarrierMode.explicit(nc.get(3)), f.config, policy)) {
                assertEquals(CarrierKind.EXCHANGE, s.stats().carrier());
                assertEquals(1, s.stats().carrierCandidates().size());
                f.echo(s);
            }
        }
    }
}
