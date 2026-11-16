package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The redial run outlives failed attempts, and every pipe input an attempt delivers is handed off or closed exactly once. */
@Timeout(60)
@SuppressWarnings("try") // the server session is held open for the test and closed with it
class RedialerTest {
    /** A SocketTimeoutException (an InterruptedIOException) from the hook is a failed attempt, not the session's close. */
    @Test void aTimedOutAttemptIsRetried() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var listenerHolder = new ZmuxListener[1];
            var calls = new AtomicInteger();
            var second = new CountDownLatch(1);
            var cfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(6).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation()
                    .resumeTimeout(Duration.ofSeconds(10))
                    .redialBackoff(new Backoff(Duration.ofMillis(1), Duration.ofMillis(1), 0))
                    .redial((need, cancellation) -> {
                        if (calls.incrementAndGet() == 1) throw new SocketTimeoutException("connect timed out");
                        second.countDown();
                        var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                        listenerHolder[0].adapter().handle(pair[1], JoinHint.inBand(TestAddr.peer("redial")));
                        return pair[0];
                    }).build();
            var serverCfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(6).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation()
                    .resumeTimeout(Duration.ofSeconds(10)).build();
            try (var listener = ZmuxListener.of(ListenerConfig.of(serverCfg))) {
                listenerHolder[0] = listener;
                var io = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.adapter().handle(io[1], JoinHint.inBand(TestAddr.peer("first")));
                try (var c = Zmux.open(io[0], Role.CLIENT, cfg); var s = listener.accept(Duration.ofSeconds(5))) {
                    c.pipes().getFirst().abort();
                    assertTrue(second.await(20, TimeUnit.SECONDS), "the redial run stopped after a timed-out attempt");
                }
            }
        }
    }

    /** A result delivered between the wait's deadline and the claim is closed, not leaked. */
    @Test void aResultRacingTheDeadlineIsClosed() throws Exception {
        var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
        var outcome = new CompletableFuture<PipeIo>();
        var abandoned = new AtomicInteger();
        assertThrows(SocketTimeoutException.class, () -> Redialer.settle(outcome, f -> {
            f.complete(pair[0]); // the hook's thread wins the outcome just as the wait gives up
            throw new TimeoutException();
        }, abandoned::incrementAndGet));
        assertEquals(1, abandoned.get());
        assertClosed(pair[1]);
    }

    /** A result delivered after the claim is closed by the attempt's own thread (its complete fails). */
    @Test void aResultAfterTheClaimIsRefused() throws Exception {
        var outcome = new CompletableFuture<PipeIo>();
        assertThrows(SocketTimeoutException.class, () -> Redialer.settle(outcome, f -> { throw new TimeoutException(); }, () -> { }));
        assertFalse(outcome.complete(MemoryPipe.pair(Framing.BYTE_STREAM)[0]));
    }

    /** A result the wait returns is handed off, never closed here. */
    @Test void aDeliveredResultIsHandedOff() throws Exception {
        var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
        var outcome = CompletableFuture.completedFuture(pair[0]);
        assertSame(pair[0], Redialer.settle(outcome, CompletableFuture::get, () -> fail("abandoned a delivered result")));
    }

    /** The input at the other end of peer was closed: its writer's abort reaches peer's reader. */
    private static void assertClosed(PipeIo peer) {
        var failed = new CompletableFuture<Throwable>();
        peer.reader().start(new io.zmux.carrier.PipeReader.Sink() {
            @Override public void onChunk(Chunk chunk, Runnable consumed) { consumed.run(); }
            @Override public void onEnd() { failed.complete(null); }
            @Override public void onError(Throwable cause) { failed.complete(cause); }
        });
        assertTrue(failed.isDone(), "the pipe input was leaked");
    }
}
