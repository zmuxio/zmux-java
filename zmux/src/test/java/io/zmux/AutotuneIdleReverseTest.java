package io.zmux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A receiver whose send direction is idle has only its own control frames to sample the RTT with;
 * they are acknowledged late on a long round trip, and that must not keep its receive window at the
 * initial value (a pipe without a sample takes the late entry).
 */
@Timeout(30)
class AutotuneIdleReverseTest {
    @Test
    void theWindowGrowsWithAnIdleReverseDirection() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops)
                    .sessionPolicy(new SessionPolicy(new CapacityLimits(4))).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var relay = new TcpRelay(listener.localAddress())) {
                relay.delayMs = 50;
                var accepted = ex.submit((Callable<ZmuxSession>) listener::accept);
                try (var c = Zmux.connect(relay.address(), cfg); var s = accepted.get(10, TimeUnit.SECONDS)) {
                    byte[] data = new byte[4 << 20];
                    var st = c.openStream();
                    var writer = ex.submit(() -> { st.write(data); st.finish(); return null; });
                    var in = s.acceptStream();
                    assertEquals(data.length, SessionTest.readAll(in).length);
                    writer.get(10, TimeUnit.SECONDS);
                    long w = in.stats().recvWindow();
                    assertTrue(w > 256 << 10, "receive window stayed at " + w);
                }
            }
        }
    }
}
