package io.zmux.http;

import io.zmux.CapacityLimits;
import io.zmux.SessionPolicy;

import io.zmux.PipeLiveness;
import io.zmux.Liveness;
import io.zmux.PipeAuth;
import io.zmux.PipeOptions;
import io.zmux.Directions;
import io.zmux.PipeId;
import io.zmux.SessionEvent;
import io.zmux.Role;
import io.zmux.Zmux;
import io.zmux.ZmuxSession;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A split carrier through a proxy that goes silent toward the client only: the uplink still
 * carries bytes without acknowledgements. The session reports the failure and
 * the controller joins another pair.
 */
@Timeout(90)
class SplitSilenceTest {
    private static void awaitFailures(ZmuxSession session, Set<PipeId> pipes) throws Exception {
        var pending = new HashSet<>(pipes);
        while (!pending.isEmpty()) {
            if (session.nextEventAsync().get(45, TimeUnit.SECONDS) instanceof SessionEvent.PipeFailed failed) {
                pending.remove(failed.pipe());
            }
        }
    }

    @Test void aSilencedDownlinkFailsTheUnacknowledgedUplinkAndTheControllerRejoins() throws Exception {
        java.util.function.UnaryOperator<io.zmux.ZmuxConfig.Builder> mp = b -> b.sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation()
                .stabilityHold(Duration.ZERO).resumeTimeout(Duration.ofSeconds(30)).liveness(Liveness.builder().probeInterval(Duration.ofSeconds(1)).build());
        try (var f = new Fixture(mp, b -> b, b -> b.streaming(), false, null);
             var px = new TestProxy(f.base, TestProxy.Profile.PASS); var c = f.client(b -> b)) {
            var s = Zmux.open(c.pipe(px.base, Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null), Role.CLIENT, f.config);
            var server = f.listener.accept(Duration.ofSeconds(5));
            var options = PipeOptions.builder().liveness(PipeLiveness.builder().probeInterval(Duration.ofSeconds(1)).build()).build();
            try (var controller = SplitJoinTest.controller(c, px.base, options);
                 var run = controller.manage(s, SplitJoinTest.target())) {
                var first = SplitJoinTest.member(s, member -> member.stable() == Directions.BOTH);
                var boot = s.pipes().getFirst().id();
                px.silenceResponses();
                awaitFailures(s, Set.of(boot, first.pipes().getFirst()));
                SplitJoinTest.member(s, member -> !member.id().equals(first.id()));
                Fixture.exchange(s, server, 64 << 10);
                s.close();
                s.closed().get(30, TimeUnit.SECONDS);
                run.done().get(30, TimeUnit.SECONDS);
                server.closed().get(30, TimeUnit.SECONDS);
            }
        }
    }
}
