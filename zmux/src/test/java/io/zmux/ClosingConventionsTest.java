package io.zmux;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/** The shared conventions of closing, waiting and runtime settings */
@Timeout(30)
final class ClosingConventionsTest {
    private static final Duration WAIT = Duration.ofSeconds(5);

    @Test void aShutdownCarriesItsCodeAndReason() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            Pair p = Pair.memory(cfg);
            p.client().shutdown(7, "done", WAIT);
            var end = p.server().awaitClosed(WAIT);
            var closed = assertInstanceOf(SessionError.ApplicationClosed.class, end);
            assertEquals(7, closed.code());
            assertEquals("done", closed.reason());
            p.close();
        }
    }

    @Test void aShutdownBoundClosesWithTheSameCode() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            Pair p = Pair.memory(cfg);
            var open = p.client().openStream();
            open.write(new byte[] {1});
            var held = p.server().acceptStream(WAIT);
            // The open stream holds the graceful close; the bound closes at once.
            p.client().shutdownAsync(9, "bound").orTimeout(100, TimeUnit.MILLISECONDS);
            var closed = assertInstanceOf(SessionError.ApplicationClosed.class, p.server().awaitClosed(WAIT));
            assertEquals(9, closed.code());
            held.close();
            open.close();
            p.close();
        }
    }

    @Test void timedWaitsThrowAndNeverReturnNull() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var l = ZmuxListener.of(ListenerConfig.of(cfg))) {
                assertThrows(SocketTimeoutException.class, () -> l.accept(Duration.ofMillis(10)));
                Pair p = Pair.memory(cfg);
                assertThrows(SocketTimeoutException.class, () -> p.client().awaitClosed(Duration.ofMillis(10)));
                assertThrows(NullPointerException.class, () -> p.client().ping(null));
                l.close();
                assertThrows(ListenerClosedException.class, l::accept);
                p.close();
            }
        }
    }

    @Test void aListenerShutdownShutsItsSessionsDownAndClosesThemAtItsBound() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var l = ZmuxListener.of(ListenerConfig.of(cfg));
            var boot = MemoryPipe.pair(Framing.BYTE_STREAM);
            l.adapter().handle(boot[1], JoinHint.inBand(TestAddr.peer("c")));
            try (var client = Zmux.open(boot[0], Role.CLIENT, cfg); var server = l.accept(WAIT)) {
                var open = client.openStream();
                open.write(new byte[] {1});
                var held = server.acceptStream(WAIT);
                l.shutdown(5, "going", Duration.ofMillis(200));
                assertTrue(l.isClosed());
                var closed = assertInstanceOf(SessionError.ApplicationClosed.class, client.awaitClosed(WAIT));
                assertEquals(5, closed.code());
                held.close();
                open.close();
            }
        }
    }

    @Test void aPipeEndsWithItsSessionAndTakesItsBandThroughUpdate() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            Pair p = Pair.memory(cfg);
            ZmuxPipe pipe = p.client().pipes().getFirst();
            pipe.update(PipeUpdate.builder().qualityBand(new QualityBand(Duration.ofMillis(5), 1.5)).build());
            pipe.update(PipeUpdate.builder().clearQualityBand().build());
            var done = pipe.closed();
            assertFalse(done.isDone());
            p.close();
            done.get(5, TimeUnit.SECONDS);
            pipe.awaitClosed(WAIT);
            pipe.abort(); // idempotent, and nothing to report on an ended session
        }
    }

    @Test void readAllBeyondItsLimitStopsTheStream() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            Pair p = Pair.memory(cfg);
            var out = p.client().openStream();
            out.write(new byte[64]);
            out.finish();
            var in = p.server().acceptStream(WAIT);
            var e = assertThrows(LimitExceededException.class, () -> in.readAll(16));
            assertEquals(16, e.limit());
            in.close();
            out.close();
            p.close();
        }
    }
}
