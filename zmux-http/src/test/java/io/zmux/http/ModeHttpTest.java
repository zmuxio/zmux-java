package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.Candidate;
import io.zmux.CandidateOutcome;
import io.zmux.CarrierKind;
import io.zmux.CarrierMode;
import io.zmux.ModePolicy;
import io.zmux.MultipathConfig;
import io.zmux.PipeAuth;
import io.zmux.SessionEvent;
import io.zmux.Zmux;
import io.zmux.ZmuxConfig;
import io.zmux.ZmuxSession;
import io.zmux.shaping.ShapedPipe;
import io.zmux.shaping.Shaper;
import io.zmux.shaping.ShaperFactory;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The HTTP candidates in the carrier selection of {@link Zmux#dial(CarrierMode, io.zmux.ZmuxConfig, ModePolicy)}. */
@Timeout(120)
class ModeHttpTest {
    static final byte[] PSK = ExchangeTest.PSK;

    /** The server's exchange carrier: its hold time below the buffering CDN's 3 s response timeout. */
    static ExchangeConfig serverExchange() { return ExchangeConfig.builder().psk(PSK).holdTime(Duration.ofSeconds(1)).build(); }

    /** The client's exchange carrier: a short deadline, so that a refused path fails within the test. */
    static ExchangeConfig clientExchange() { return ExchangeConfig.builder().psk(PSK).deadline(Duration.ofSeconds(3)).build(); }

    /** A unified endpoint with every adapter; multipath: joins allowed. */
    static Fixture fixture(boolean multipath) throws Exception {
        return fixture(multipath, null);
    }

    static Function<ZmuxConfig.Builder, ZmuxConfig.Builder> session(boolean multipath, ShaperFactory shaper) {
        return b -> (multipath ? b.multipath(MultipathConfig.defaults().withMaxPipes(4).withAuth(PipeAuth.psk(new byte[32]))
                .withProbation(Duration.ZERO)) : b).shaper(shaper);
    }

    /** The same, its sessions shaped by shaper. */
    static Fixture fixture(boolean multipath, ShaperFactory shaper) throws Exception {
        return new Fixture(session(multipath, shaper), b -> b, b -> b.streaming().exchange(serverExchange()).webSocket(), false, null);
    }

    /** A factory that records the pipes it is asked for and shapes none of them. */
    static final class Seen implements ShaperFactory {
        final List<ShapedPipe> pipes = new CopyOnWriteArrayList<>();
        @Override public Shaper create(ShapedPipe pipe) { pipes.add(pipe); return null; }
        List<CarrierKind> kinds() { return pipes.stream().map(ShapedPipe::carrier).toList(); }
        void awaitKind(CarrierKind k) throws InterruptedException {
            long until = System.nanoTime() + 5_000_000_000L;
            while (!kinds().contains(k) && System.nanoTime() < until) Thread.sleep(5);
            assertTrue(kinds().contains(k), "no " + k + " pipe: " + pipes);
        }
    }

    /** The HTTP kinds to url, by preference. */
    static List<Candidate> candidates(HttpCarrierClient c, URI url) {
        return List.of(c.webSocketCandidate(WebSocketTest.ws(url), Map.of(), List.of()), c.streamingCandidate(url, Map.of()),
                c.splitCandidate(url, Map.of()), c.exchangeCandidate(url.toString(), clientExchange()));
    }

    static ModePolicy fast(boolean noJoin) {
        return ModePolicy.defaults().withTimer(3, Duration.ofMillis(400), Duration.ofSeconds(1)).withSettle(Duration.ofMillis(200)).withNoJoin(noJoin);
    }

    static List<String> tries(ZmuxSession s) {
        List<String> out = new ArrayList<>();
        for (var r : s.stats().carrierCandidates()) out.add(r.kind() + ":" + r.outcome() + (r.joined() ? "(join)" : ""));
        return out;
    }

    @Test void eachKindExplicitly() throws Exception {
        try (var f = fixture(false); var c = f.client(b -> b)) {
            for (Candidate k : candidates(c, f.base)) {
                try (var s = Zmux.dial(CarrierMode.explicit(k), f.config, null)) {
                    assertEquals(k.kind(), s.stats().carrier());
                    assertEquals(List.of(k.kind() + ":" + CandidateOutcome.USABLE), tries(s));
                    f.echo(s);
                }
            }
        }
    }

    /** Each kind's pipes reach the shaper factories of both sides with that kind. */
    @Test void eachKindIsShapedAsItsKindOnBothSides() throws Exception {
        Seen server = new Seen();
        try (var f = fixture(false, server); var c = f.client(b -> b)) {
            for (Candidate k : candidates(c, f.base)) {
                Seen client = new Seen();
                server.pipes.clear();
                var cfg = session(false, client).apply(ZmuxConfig.builder().eventLoops(f.loops)).build();
                try (var s = Zmux.dial(CarrierMode.explicit(k), cfg, null)) {
                    f.echo(s);
                    client.awaitKind(k.kind());
                    server.awaitKind(k.kind());
                    assertEquals(Set.of(k.kind()), Set.copyOf(client.kinds()), k.kind() + " client");
                    assertEquals(Set.of(k.kind()), Set.copyOf(server.kinds()), k.kind() + " server");
                }
            }
        }
    }

    /** A pipe the mode layer joins to move to another kind is shaped as that kind on both sides. */
    @Test void aJoinedKindIsShapedAsTheNewKind() throws Exception {
        Seen server = new Seen(), client = new Seen();
        try (var f = fixture(true, server); var c = f.client(b -> b)) {
            var cfg = session(true, client).apply(ZmuxConfig.builder().eventLoops(f.loops)).build();
            try (var s = Zmux.dial(CarrierMode.auto(candidates(c, f.base)), cfg, fast(false))) {
                assertEquals(CarrierKind.EXCHANGE, s.stats().carrier());
                client.awaitKind(CarrierKind.WEBSOCKET);
                server.awaitKind(CarrierKind.WEBSOCKET);
                assertEquals(CarrierKind.EXCHANGE, client.pipes.getFirst().carrier());
                assertEquals(CarrierKind.EXCHANGE, server.pipes.getFirst().carrier());
                for (ShapedPipe p : client.pipes) assertEquals(p.id() == 0, p.carrier() == CarrierKind.EXCHANGE, p.toString());
                f.echo(s);
            }
        }
    }

    /** Established on the exchange carrier, the session moves to the most preferred kind by joining it, without a second session. */
    @Test void autoJoinsThePreferredKind() throws Exception {
        try (var f = fixture(true); var c = f.client(b -> b)) {
            try (var s = Zmux.dial(CarrierMode.auto(candidates(c, f.base)), f.config, fast(false))) {
                assertEquals(CarrierKind.EXCHANGE, s.stats().carrier(), "established on the exchange carrier");
                SessionEvent e;
                do {
                    e = s.nextEvent(Duration.ofSeconds(10));
                    assertNotNull(e, "no choice of a preferred kind: " + tries(s));
                } while (!(e instanceof SessionEvent.CarrierChosen ch && ch.kind() == CarrierKind.WEBSOCKET));
                assertEquals(CarrierKind.WEBSOCKET, s.stats().carrier());
                long until = System.nanoTime() + 5_000_000_000L;
                while (s.paths().list().stream().anyMatch(p -> p.id() == 0) && System.nanoTime() < until) Thread.sleep(10);
                assertTrue(s.paths().list().stream().noneMatch(p -> p.id() == 0), "the exchange pipe retired: " + s.paths().list());
                f.echo(s);
                assertNull(f.listener.accept(Duration.ofMillis(200)), "a second session");
            }
            ExchangeTest.waitFor(() -> f.endpoint.exchange.live() == 0, "the retired exchange carrier ends");
        }
    }

    @Test void anExplicitKindThatIsRefusedIsReported() throws Exception {
        try (var f = new Fixture(b -> b, b -> b, b -> b.streaming(), false, null); var c = f.client(b -> b)) {
            var e = assertThrows(io.zmux.ModeException.class,
                    () -> Zmux.dial(CarrierMode.explicit(candidates(c, f.base).getFirst()), f.config, null));
            assertEquals(1, e.results().size());
            assertEquals(CandidateOutcome.REFUSED, e.results().getFirst().outcome());
        }
    }
}
