package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A dial through a closed carrier client is final: neither a redial hook nor a pipe pool retries it. */
@Timeout(30)
@SuppressWarnings("try") // the server session is held open for the test and closed with it
class ClientClosedDialTest {
    private static ZmuxConfig.Builder cfg(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation()
                .resumeTimeout(Duration.ofSeconds(10));
    }

    @Test void aRedialHookFailingWithAClosedClientIsNotRetried() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var calls = new AtomicInteger();
            var cfg = cfg(loops).redialBackoff(new Backoff(Duration.ofMillis(1), Duration.ofMillis(2), 0)).redial((need, c) -> {
                calls.incrementAndGet();
                throw new CarrierClientClosedException("closed");
            }).build();
            try (var listener = Zmux.listener(ListenerConfig.of(cfg(loops).build()))) {
                var io = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.adapter().handle(io[1], JoinHint.inBand(TestAddr.peer("first")));
                try (var c = Zmux.open(io[0], Role.CLIENT, cfg); var s = listener.accept(Duration.ofSeconds(5))) {
                    c.pipes().getFirst().abort();
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (calls.get() < 1) {
                        assertTrue(System.nanoTime() - until < 0);
                        java.util.concurrent.locks.LockSupport.parkNanos(1_000_000);
                    }
                    Thread.sleep(200); // many backoffs of 2 ms: a retry would have run
                    assertEquals(1, calls.get());
                }
            }
        }
    }

    @Test void aPoolDialFailingWithAClosedClientEndsThePool() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            try (var listener = Zmux.listener(ListenerConfig.of(cfg(loops).build()))) {
                var io = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.adapter().handle(io[1], JoinHint.inBand(TestAddr.peer("first")));
                try (var c = Zmux.open(io[0], Role.CLIENT, cfg(loops).build()); var s = listener.accept(Duration.ofSeconds(5))) {
                    var calls = new AtomicInteger();
                    var m = c.maintainPipes(PipePoolTest.fast(attempt -> {
                        calls.incrementAndGet();
                        throw new CarrierClientClosedException("closed");
                    }).withCount(1), new Cancellation());
                    var result = m.completion().get(10, TimeUnit.SECONDS);
                    assertInstanceOf(CarrierClientClosedException.class, result.cause());
                    assertEquals(1, calls.get());
                }
            }
        }
    }
}
