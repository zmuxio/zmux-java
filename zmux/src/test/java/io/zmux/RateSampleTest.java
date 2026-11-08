package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.*;
import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Queue-proven rate samples: a carrier that takes every byte into a deep buffer never refuses
 * one, so only a range held back by the path (adjusted RTT at least 5 x min_rtt) tells its
 * rate; that also holds for bytes sent before the pipe became SUSPECT.
 */
@Timeout(60)
class RateSampleTest {
    private static final double SLOW = 3e4;

    private record Bulk(ZmuxSession client, ZmuxSession server, ZmuxListener listener, SlowPathTest.Link up, Thread writer)
            implements AutoCloseable {
        long rate() {
            return client.pipeStats().getFirst().deliveryRate().orElse(-1);
        }

        @Override public void close() throws Exception {
            writer.interrupt();
            client.close(); server.close(); listener.close();
            writer.join(5_000);
        }
    }

    /** A session over one pipe whose client-to-server direction runs through a deep-buffered link. */
    private static Bulk bulk(EventLoops loops, double rate) throws Exception {
        ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(2).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).build();
        var listener = Zmux.listener(ListenerConfig.of(cfg));
        PipeIo[] io = MemoryPipe.pair(Framing.BYTE_STREAM);
        var up = new SlowPathTest.Link(io[0].writer(), Duration.ofMillis(20), rate, 16 << 20);
        var down = new SlowPathTest.Link(io[1].writer(), Duration.ofMillis(20), 5e6, 16 << 20);
        listener.handle(new PipeIo(io[1].reader(), down, Framing.BYTE_STREAM, null, null), JoinHint.inBand(TestAddr.peer("deep")));
        var client = Zmux.open(new PipeIo(io[0].reader(), up, Framing.BYTE_STREAM, null, null), Role.CLIENT, cfg);
        var server = listener.accept(Duration.ofSeconds(10));
        Thread.ofVirtual().start(() -> {
            try (var st = server.acceptStream(Duration.ofSeconds(10))) {
                st.inputStream().transferTo(java.io.OutputStream.nullOutputStream());
            } catch (IOException ended) { /* The session closed. */ }
        });
        var st = client.openStream();
        Thread writer = Thread.ofVirtual().start(() -> {
            byte[] b = new byte[16 << 10];
            try {
                while (true) st.write(b);
            } catch (IOException ended) { /* The session closed. */ }
        });
        return new Bulk(client, server, listener, up, writer);
    }

    @Test void aSlowPipeBehindADeepBufferReadsItsRate() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var b = bulk(loops, SLOW)) {
            Thread.sleep(4_000);
            long r = b.rate();
            assertTrue(r > 0 && r < 3 * SLOW, "R_p " + r + " B/s on a " + (long) SLOW + " B/s pipe");
        }
    }

    @Test void aPipeThatSlowsDownAndBecomesSuspectDoesNotKeepItsOldRate() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var b = bulk(loops, 5e6)) {
            Thread.sleep(1_000);
            long fast = b.rate();
            b.up().rate(SLOW);
            // The max filter keeps the fast samples for its 10 s window; then only samples of the
            // slow ranges are left, which arrive while the pipe is SUSPECT.
            Thread.sleep(12_000);
            long r = b.rate();
            assertTrue(r > 0 && r < fast / 10, "R_p " + r + " B/s after slowing from " + fast + " to " + (long) SLOW + " B/s");
        }
    }
}
