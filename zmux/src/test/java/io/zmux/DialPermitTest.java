package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A session with a redial hook and a pipe pool dials one further pipe at a time, whichever dials it. */
@Timeout(60)
@SuppressWarnings("try") // the server session is held open for the test and closed with it
class DialPermitTest {
    @Test void redialAndPoolNeverDialAtOnce() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            AtomicInteger inflight = new AtomicInteger(), most = new AtomicInteger(), dials = new AtomicInteger();
            var both = new CountDownLatch(2);
            var listenerHolder = new ZmuxListener[1];
            // Each dial holds a while, waiting for the other to start: with the permit it never does.
            java.util.function.Supplier<io.zmux.carrier.PipeIo> dialOne = () -> {
                most.accumulateAndGet(inflight.incrementAndGet(), Math::max);
                dials.incrementAndGet();
                both.countDown();
                try { both.await(300, TimeUnit.MILLISECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                inflight.decrementAndGet();
                var io = MemoryPipe.pair(Framing.BYTE_STREAM);
                listenerHolder[0].adapter().handle(io[1], JoinHint.inBand(TestAddr.peer("dial")));
                return io[0];
            };
            var cfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(6).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation()
                    .resumeTimeout(Duration.ofSeconds(10))
                    .redial((need, cancellation) -> dialOne.get()).build();
            // The server's sessions have no hook of their own: only the client dials.
            var serverCfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(6).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation()
                    .resumeTimeout(Duration.ofSeconds(10)).build();
            try (var listener = ZmuxListener.of(ListenerConfig.of(serverCfg))) {
                listenerHolder[0] = listener;
                var io = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.adapter().handle(io[1], JoinHint.inBand(TestAddr.peer("first")));
                try (var c = Zmux.open(io[0], Role.CLIENT, cfg); var s = listener.accept(Duration.ofSeconds(5))) {
                    // The pool starts its dial (it holds a while), and the session loses its only pipe:
                    // the hook wants it back too, but waits for the pool's dial, which restores it.
                    var m = c.maintainPipes(PipePoolTest.fast(attempt -> attempt.join(dialOne.get())).withCount(1), new Cancellation());
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                    while (dials.get() < 1) { assertTrue(System.nanoTime() - until < 0); java.util.concurrent.locks.LockSupport.parkNanos(1_000_000); }
                    c.pipes().getFirst().abort();
                    while (m.pipes().isEmpty()) { assertTrue(System.nanoTime() - until < 0, "the pool's pipe never joined"); java.util.concurrent.locks.LockSupport.parkNanos(1_000_000); }
                    // The hook's dial, if it still finds the pipe missing, starts only after the pool's settled.
                    while (dials.get() < 2 && c.pipeStats().size() < 1) { assertTrue(System.nanoTime() - until < 0); java.util.concurrent.locks.LockSupport.parkNanos(1_000_000); }
                    assertEquals(1, most.get(), "two dials ran at once");
                    m.close();
                }
            }
        }
    }

    @Test void aHungRedialHookDoesNotStarveThePool() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var hookEntered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var listenerHolder = new ZmuxListener[1];
            var cfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(6).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation()
                    .resumeTimeout(Duration.ofSeconds(10)).joinTimeout(Duration.ofMillis(300))
                    .redial((need, cancellation) -> {
                        hookEntered.countDown();
                        // Ignores cancellation and interruption until the test ends.
                        boolean interrupted = false;
                        while (true) {
                            try { release.await(); break; } catch (InterruptedException e) { interrupted = true; }
                        }
                        if (interrupted) Thread.currentThread().interrupt();
                        throw new java.io.IOException("released");
                    }).build();
            var serverCfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(6).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation()
                    .resumeTimeout(Duration.ofSeconds(10)).build();
            try (var listener = ZmuxListener.of(ListenerConfig.of(serverCfg))) {
                listenerHolder[0] = listener;
                var io = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.adapter().handle(io[1], JoinHint.inBand(TestAddr.peer("first")));
                try (var c = Zmux.open(io[0], Role.CLIENT, cfg); var s = listener.accept(Duration.ofSeconds(5))) {
                    c.pipes().getFirst().abort(); // the hook wants the pipe back and hangs
                    assertTrue(hookEntered.await(10, TimeUnit.SECONDS));
                    var m = c.maintainPipes(PipePoolTest.fast(attempt -> {
                        var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                        listenerHolder[0].adapter().handle(pair[1], JoinHint.inBand(TestAddr.peer("pool")));
                        return attempt.join(pair[0]);
                    }).withCount(1), new Cancellation());
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (m.pipes().isEmpty()) {
                        assertTrue(System.nanoTime() - until < 0, "the hung hook starved the pool's dial");
                        java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
                    }
                    m.close();
                } finally {
                    release.countDown();
                }
            }
        }
    }
}
