package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.net.InetSocketAddress;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** What connect and accept decided is visible in info() on the returning thread. */
class InfoVisibilityTest {
    @Test @Timeout(60)
    void establishedIsVisibleOnReturn() throws Exception {
        try (var loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var l = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), ListenerConfig.of(cfg));
                 var dialer = Zmux.listener(ListenerConfig.of(cfg))) {
                for (int i = 0; i < 300; i++) {
                    try (var c = dialer.connect(l.localAddress())) {
                        assertTrue(c.info().established(), "connect returned before info() showed established, round " + i);
                        try (var s = l.accept(Duration.ofSeconds(5))) {
                            assertNotNull(s);
                            s.awaitEstablished(Long.MAX_VALUE);
                            assertTrue(s.info().established(), "accepted session, round " + i);
                        }
                    }
                }
            }
        }
    }
}
