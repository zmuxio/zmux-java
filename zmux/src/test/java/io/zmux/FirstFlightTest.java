package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The held first flight: nothing leaves before the release, and a flush does not wait for it. */
@Timeout(30)
class FirstFlightTest {
    private static ZmuxConfig.Builder held() {
        return ZmuxConfig.builder().earlyData(EarlyData.ZMUX_PEER).holdFirstFlight(true);
    }

    private static CompletableFuture<ZmuxSession> server(PipeIo io) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return Zmux.open(io, Role.SERVER, ZmuxConfig.builder().build());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, r -> Thread.ofVirtual().start(r));
    }

    @Test
    void nothingLeavesBeforeTheRelease() throws Exception {
        PipeIo[] p = MemoryPipe.pair(Framing.BYTE_STREAM);
        CompletableFuture<ZmuxSession> server = server(p[1]);
        try (ZmuxSession c = Zmux.open(p[0], Role.CLIENT, held().build())) {
            ZmuxStream st = c.openStream();
            byte[] hello = "HELLO, first flight".getBytes(StandardCharsets.UTF_8);
            st.write(ByteBuffer.wrap(hello));
            long t0 = System.nanoTime();
            st.flush();
            assertTrue(System.nanoTime() - t0 < TimeUnit.SECONDS.toNanos(1), "a held flush returns at once");
            Thread.sleep(50);
            assertFalse(server.isDone(), "the server got the parameters");
            assertEquals(0, c.stats().bytesSent());
            c.releaseFirstFlight();
            c.releaseFirstFlight();
            try (ZmuxSession s = server.get(10, TimeUnit.SECONDS)) {
                ZmuxStream in = s.acceptStream();
                ByteBuffer got = ByteBuffer.allocate(hello.length);
                while (got.hasRemaining() && in.read(got) >= 0) { }
                assertArrayEquals(hello, got.array());
                assertEquals(0, c.stats().firstFlightTimeouts());
            }
        }
    }

    @Test
    void theHandshakeTimeoutReleasesIt() throws Exception {
        PipeIo[] p = MemoryPipe.pair(Framing.BYTE_STREAM);
        CompletableFuture<ZmuxSession> server = server(p[1]);
        ZmuxConfig cfg = held().handshakeTimeout(Duration.ofMillis(200)).build();
        try (ZmuxSession c = Zmux.open(p[0], Role.CLIENT, cfg); ZmuxSession s = server.get(10, TimeUnit.SECONDS)) {
            assertEquals(1, c.stats().firstFlightTimeouts());
            assertNotNull(s);
        }
    }

    @Test
    void itNeedsClientEarlyData() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().holdFirstFlight(true).build());
        PipeIo[] p = MemoryPipe.pair(Framing.BYTE_STREAM);
        assertThrows(IllegalArgumentException.class, () -> Zmux.open(p[1], Role.SERVER, held().build()));
    }
}
