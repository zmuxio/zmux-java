package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.PipeOptions;
import io.zmux.PipePool;
import io.zmux.Role;
import io.zmux.Zmux;
import io.zmux.ZmuxPipe;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A split carrier joined as a further pipe: an uplink and a downlink, kept by a pool as one member. */
@Timeout(60)
class SplitJoinTest {
    static void await(BooleanSupplier c) throws InterruptedException {
        long until = System.nanoTime() + 10_000_000_000L;
        while (!c.getAsBoolean()) {
            assertTrue(System.nanoTime() < until, "condition not reached");
            Thread.sleep(5);
        }
    }

    @Test void aSplitJoinPoolKeepsOnePipeAcrossItsPairedRequests() throws Exception {
        try (var f = new Fixture(ModeHttpTest.session(true, null), b -> b, b -> b.streaming(), false, null);
             var c = f.client(b -> b)) {
            try (var s = Zmux.open(c.pipe(f.base, Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null), Role.CLIENT, f.config);
                 var server = f.listener.accept(Duration.ofSeconds(5))) {
                var pool = PipePool.join(a -> List.of(c.splitJoin(a.session(), f.base, Map.of(), null, a.options())));
                try (var run = s.maintainPipes(pool)) {
                    await(() -> run.pipes().size() == 1);
                    List<ZmuxPipe> first = run.pipes();
                    var st = first.getFirst().stats().orElseThrow();
                    assertTrue(st.outbound() != null && st.inbound() != null, "one bidirectional attachment");
                    var old = new HashSet<io.zmux.PipeId>();
                    for (ZmuxPipe p : first) old.add(p.id());
                    // The loss of the pipe replaces the member.
                    first.getFirst().abort();
                    await(() -> run.pipes().size() == 1 && run.pipes().stream().noneMatch(p -> old.contains(p.id())));
                    Fixture.exchange(s, server, 4);
                }
            }
        }
    }
}
