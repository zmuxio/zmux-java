package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** {@link ZmuxPipe#awaitAttached}: the one definition of the peer holding a pipe. */
@Timeout(20)
class PeerAttachedTest {
    static final Duration WAIT = Duration.ofSeconds(3);
    /** Adding returns at the verified Reply, so the wait for the attachment is the test's own. */
    static final PipeOptions NO_WAIT = PipeOptions.builder().returnOnReply(true).build();

    static ZmuxPipe lateJoin(PipePoolTest.Fixture f, long delayMillis, PipeOptions options) throws Exception {
        var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
        f.listener.adapter().handle(pair[1], JoinHint.inBand(TestAddr.peer("late-confirm"))).closed();
        return f.client.addPipe(PipeIo.of(pair[0].reader(), new PipePoolTest.LateConfirm(pair[0].writer(), delayMillis)), options);
    }

    static boolean held(ZmuxSession acceptor, ZmuxPipe p) throws Exception {
        return acceptor.pipes().stream().anyMatch(q -> q.id().value() == p.id().value());
    }

    @Test void aJoinedPipeIsAttachedOnlyOnceTheAcceptorReadItsConfirm() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 3))) {
            for (ZmuxPipe q : f.client.pipes()) if (q.id().value() == 0) q.awaitAttached(WAIT);
            for (ZmuxPipe q : f.server.pipes()) q.awaitAttached(WAIT);
            ZmuxPipe p = lateJoin(f, 300, NO_WAIT);
            var attached = p.attached().orTimeout(WAIT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            assertFalse(held(f.server, p), "the Confirm is still held");
            assertFalse(attached.isDone(), "attached before the acceptor read the Confirm");
            attached.get(3, TimeUnit.SECONDS);
            assertTrue(held(f.server, p), "attached but the acceptor does not hold the pipe");
            p.awaitAttached(WAIT);
            // The acceptor's view of a pipe the peer joined: attached from the start.
            for (ZmuxPipe q : f.server.pipes()) if (q.id().value() == p.id().value()) q.awaitAttached(Duration.ofMillis(100));
        }
    }

    @Test void closeAndAbortRemoveThePipeEvenWithTheInterruptFlagSet() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 5))) {
            for (boolean graceful : new boolean[] {true, false}) {
                ZmuxPipe p = lateJoin(f, 0, PipeOptions.defaults());
                p.awaitAttached(WAIT);
                Thread.currentThread().interrupt();
                try {
                    if (graceful) p.close(); else p.abort();
                    assertTrue(Thread.currentThread().isInterrupted(), "the flag is kept");
                } finally {
                    Thread.interrupted();
                }
                p.awaitClosed(WAIT);
                assertFalse(f.client.pipes().stream().anyMatch(q -> q.id().value() == p.id().value()), "the pipe is still on the session");
            }
        }
    }

    @Test void aPipeThisEndpointAbortedReportsTheAbortInItsWait() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 3))) {
            ZmuxPipe p = lateJoin(f, 60_000, NO_WAIT); // joined, never attached: the Confirm is held
            var waiting = p.attached();
            p.abort();
            var e = assertThrows(ExecutionException.class, () -> waiting.get(5, TimeUnit.SECONDS));
            assertInstanceOf(PipeAbortedException.class, e.getCause());
            assertThrows(PipeAbortedException.class, () -> p.awaitAttached(WAIT));
            // Another handle of the same pipe, while it is still on the session: the core says who aborted it.
            f.client.pipes().stream().filter(q -> q.id().value() == p.id().value()).findFirst()
                    .ifPresent(q -> assertThrows(PipeAbortedException.class, () -> q.awaitAttached(WAIT)));
        }
    }

    @Test void aReceiveOnlyPipeRefusesAQualityBand() {
        assertThrows(IllegalArgumentException.class, () -> PipeOptions.builder()
                .direction(PipeOptions.Direction.RECEIVE_ONLY).qualityBand(QualityBand.builder().rateRatio(2).build()).build());
    }

    @Test void aReceiveOnlyJoinIsAttachedByARecord() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 3))) {
            ZmuxPipe p = lateJoin(f, 100, PipeOptions.builder().direction(PipeOptions.Direction.RECEIVE_ONLY).returnOnReply(true).build());
            p.awaitAttached(WAIT);
            assertTrue(held(f.server, p));
        }
    }

    @Test void theWaitEndsAtItsDeadlineTheRetirementAndTheSessionsEnd() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var f = new PipePoolTest.Fixture(PipePoolTest.config(loops, 4))) {
            ZmuxPipe slow = lateJoin(f, 1_000, NO_WAIT);
            assertThrows(SocketTimeoutException.class, () -> slow.awaitAttached(Duration.ofMillis(50)));
            var cancelled = slow.attached();
            cancelled.cancel(false);
            assertTrue(cancelled.isCancelled());

            var gone = slow.attached();
            slow.close();
            var e = assertThrows(ExecutionException.class, () -> gone.get(3, TimeUnit.SECONDS));
            assertInstanceOf(PipeGoneException.class, e.getCause());
            assertThrows(PipeGoneException.class, () -> slow.awaitAttached(WAIT));

            ZmuxPipe other = lateJoin(f, 1_000, NO_WAIT);
            var ended = other.attached();
            f.client.close();
            var c = assertThrows(ExecutionException.class, () -> ended.get(3, TimeUnit.SECONDS));
            assertInstanceOf(java.io.IOException.class, c.getCause());
            assertFalse(c.getCause() instanceof SocketTimeoutException);
        }
    }
}
