package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A session's end closes its socket, an abort over an already ended connection included. */
@Timeout(60)
class SocketCloseTest {
    @Test void endedSessionsHoldNoDescriptors() throws Exception {
        Assumptions.assumeTrue(Files.isDirectory(FDS), "descriptor counts need " + FDS);
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build())) {
                cycle(l, cfg, 2); // Whatever a first use opens once stays out of the count.
                long before = descriptors();
                cycle(l, cfg, 40);
                long end = System.nanoTime() + 5_000_000_000L;
                while (descriptors() > before + 8 && System.nanoTime() < end) Thread.sleep(20);
                assertTrue(descriptors() <= before + 8, before + " -> " + descriptors());
            }
        }
    }

    private static final Path FDS = Path.of("/dev/fd");

    private static long descriptors() throws Exception {
        try (var list = Files.list(FDS)) { return list.count(); }
    }

    private static void cycle(ZmuxListener l, ZmuxConfig cfg, int n) throws Exception {
        for (int i = 0; i < n; i++) {
            var c = Zmux.connect(l.localAddress(), cfg);
            var s = l.accept(Duration.ofSeconds(5));
            c.close();
            c.closed().toCompletableFuture().get();
            s.closed().toCompletableFuture().get();
        }
    }
}
