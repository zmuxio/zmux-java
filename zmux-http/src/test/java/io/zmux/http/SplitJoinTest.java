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

    @Test void aSplitBootstrapCannotJoinButASplitJoinPoolKeepsItsPairWhole() throws Exception {
        try (var f = new Fixture(ModeHttpTest.session(true, null), b -> b, b -> b.streaming(), false, null);
             var c = f.client(b -> b)) {
            try (var s = Zmux.open(c.pipe(f.base, Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null), Role.CLIENT, f.config);
                 var server = f.listener.accept(Duration.ofSeconds(5))) {
                var boot = c.splitPipe(f.base, Map.of(), null);
                try {
                    assertThrows(IllegalArgumentException.class, () -> s.paths().add(boot, PipeOptions.defaults()));
                } finally {
                    boot.reader().close();
                    boot.writer().abort(new java.io.IOException("refused"));
                }
                var pool = PipePool.join(a -> c.splitJoin(a.session(), f.base, Map.of(), null, a.options()))
                        .withInspectionInterval(Duration.ofMillis(5));
                try (var run = s.paths().maintain(pool)) {
                    await(() -> run.pipes().size() == 2);
                    List<ZmuxPipe> first = run.pipes();
                    boolean up = false, down = false;
                    for (ZmuxPipe p : first) {
                        var st = p.stats().orElseThrow();
                        up |= st.outbound() != null && st.inbound() == null;
                        down |= st.inbound() != null && st.outbound() == null;
                    }
                    assertTrue(up && down, "an uplink and a downlink");
                    var old = new HashSet<Long>();
                    for (ZmuxPipe p : first) old.add(p.id());
                    // The loss of one half replaces the member whole.
                    first.getFirst().abort();
                    await(() -> run.pipes().size() == 2 && run.pipes().stream().noneMatch(p -> old.contains(p.id())));
                    Fixture.exchange(s, server, 4);
                }
            }
        }
    }
}
