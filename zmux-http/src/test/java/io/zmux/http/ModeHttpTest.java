package io.zmux.http;

import io.zmux.CapacityLimits;
import io.zmux.SessionPolicy;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.Candidate;
import io.zmux.CandidateOutcome;
import io.zmux.CarrierKind;
import io.zmux.ClockDomain;
import io.zmux.PipeAuth;
import io.zmux.SessionController;
import io.zmux.SessionTarget;
import io.zmux.CarrierOpener;
import io.zmux.ZmuxConfig;
import io.zmux.ZmuxSession;
import io.zmux.shaping.ShapedPipe;
import io.zmux.shaping.Shaper;
import io.zmux.shaping.ShaperFactory;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The HTTP candidates in the bootstrap selection of the session controller. */
@Timeout(120)
class ModeHttpTest {
    static final byte[] PSK = ExchangeTest.PSK;

    /** A short hold for the exchange carrier keeps the fixture's idle polls short. */
    static ExchangeConfig serverExchange() { return ExchangeConfig.builder(PSK).holdTime(Duration.ofSeconds(1)).build(); }

    static ExchangeConfig clientExchange() { return ExchangeConfig.builder(PSK).build(); }

    /** A unified endpoint with every adapter; multipath: joins allowed. */
    static Fixture fixture(boolean multipath) throws Exception {
        return fixture(multipath, null);
    }

    static Function<ZmuxConfig.Builder, ZmuxConfig.Builder> session(boolean multipath, ShaperFactory shaper) {
        return b -> (multipath ? b.sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation() : b).shaper(shaper);
    }

    /** The same, its sessions shaped by shaper. */
    static Fixture fixture(boolean multipath, ShaperFactory shaper) throws Exception {
        return new Fixture(session(multipath, shaper), b -> b, b -> b.streaming().exchange(serverExchange()).webSocket(), false, null);
    }

    /** A factory that records the pipes it is asked for and shapes none of them. */
    static final class Seen implements ShaperFactory {
        final List<ShapedPipe> pipes = new CopyOnWriteArrayList<>();
        private final Map<CarrierKind, CompletableFuture<Void>> created = new ConcurrentHashMap<>();
        @Override public Shaper create(ShapedPipe pipe) {
            pipes.add(pipe);
            signal(pipe.carrier()).complete(null);
            return null;
        }
        List<CarrierKind> kinds() { return pipes.stream().map(ShapedPipe::carrier).toList(); }
        private CompletableFuture<Void> signal(CarrierKind kind) {
            return created.computeIfAbsent(kind, ignored -> new CompletableFuture<>());
        }
        void awaitKind(CarrierKind kind) throws Exception { signal(kind).get(); }
    }

    /** The HTTP kinds to url, by preference. */
    static List<Candidate> candidates(HttpCarrierClient c, URI url) {
        return List.of(c.webSocketCandidate(WebSocketTest.ws(url), Map.of(), List.of()), c.streamingCandidate(url, Map.of()),
                c.splitCandidate(url, Map.of()), c.exchangeCandidate(url.toString(), clientExchange()));
    }

    static ZmuxSession open(ZmuxConfig config, List<Candidate> carriers) throws java.io.IOException {
        var registrations = carriers.stream().map(carrier -> new SessionController.Candidate(carrier.kind().toString(),
                carrier.kind().toString(), carrier.kind(), carrier.variant(), 0, false, true, List.of(),
                (setup, request) -> new CarrierOpener.Opened(List.of(carrier.dialer().dial(request.credential(), request.timeout()))))).toList();
        var preparation = carriers.size() > 1 ? new PreparationClock() : null;
        var options = new SessionController.Options(java.util.UUID.randomUUID().toString(), registrations,
                SessionController.Choice.STATIC,
                SessionController.Limits.DEFAULT.withMaximumOpeners(1).withAttemptTimeout(Duration.ofSeconds(6)),
                null, preparation == null ? ClockDomain.system() : new ClockDomain(preparation), Map.of());
        try (var controller = new SessionController(options)) {
            try {
                return controller.open(config, SessionTarget.empty()).session();
            } finally {
                if (preparation != null) preparation.resume();
            }
        }
    }

    /** During setup, opener and handshake results drive progress. Resume live timers for the opened session. */
    private static final class PreparationClock implements ClockDomain.Source {
        private final CompletableFuture<Long> resumed = new CompletableFuture<>();

        @Override public long nanoTime() {
            Long origin = resumed.getNow(null);
            return origin == null ? 0 : System.nanoTime() - origin;
        }

        @Override public Instant wallTime() { return Instant.EPOCH.plusNanos(nanoTime()); }

        @Override public Runnable schedule(long delay, Runnable task) {
            var scheduled = new AtomicReference<Runnable>();
            var cancelled = (Runnable) () -> {};
            if (delay == 0) {
                task.run();
                return cancelled;
            }
            long deadline = nanoTime() + delay;
            resumed.thenRun(() -> {
                if (scheduled.get() == cancelled) return;
                ClockDomain system = ClockDomain.system();
                var stop = system.schedule(system.now() + Math.max(0, deadline - nanoTime()), task);
                if (!scheduled.compareAndSet(null, stop)) stop.run();
            });
            return () -> {
                var stop = scheduled.getAndSet(cancelled);
                if (stop != null) stop.run();
            };
        }

        void resume() { resumed.complete(System.nanoTime()); }
    }

    static List<String> tries(ZmuxSession s) {
        List<String> out = new ArrayList<>();
        for (var r : s.stats().carrierCandidates()) out.add(r.kind() + ":" + r.outcome() + (r.joined() ? "(join)" : ""));
        return out;
    }

    @Test void eachKindExplicitly() throws Exception {
        try (var f = fixture(false); var c = f.client(b -> b)) {
            for (Candidate k : candidates(c, f.base)) {
                try (var s = open(f.config, List.of(k))) {
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
                try (var s = open(cfg, List.of(k))) {
                    f.echo(s);
                    client.awaitKind(k.kind());
                    server.awaitKind(k.kind());
                    assertEquals(Set.of(k.kind()), Set.copyOf(client.kinds()), k.kind() + " client");
                    assertEquals(Set.of(k.kind()), Set.copyOf(server.kinds()), k.kind() + " server");
                }
            }
        }
    }

    @Test void anExplicitKindThatIsRefusedIsReported() throws Exception {
        try (var f = new Fixture(b -> b, b -> b, b -> b.streaming(), false, null); var c = f.client(b -> b)) {
            var e = assertThrows(io.zmux.CarrierChoiceException.class,
                    () -> open(f.config, List.of(candidates(c, f.base).getFirst())));
            assertEquals(1, e.results().size());
            assertEquals(CandidateOutcome.REFUSED, e.results().getFirst().outcome());
        }
    }
}
