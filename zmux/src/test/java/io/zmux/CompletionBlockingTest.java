package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Blocking calls on a session's completion thread: allowed, except behind an asynchronous operation that needs it. */
@Timeout(30)
class CompletionBlockingTest {
    /** A stream pair whose server side is accepted: the client writes a byte first. */
    private static ZmuxStream[] accepted(Pair pair) throws Exception {
        ZmuxStream c = pair.client().openStream();
        c.write(new byte[] {1});
        ZmuxStream s = pair.server().acceptStream(Duration.ofSeconds(3));
        assertEquals(1, s.inputStream().read());
        return new ZmuxStream[] {c, s};
    }

    /** Runs body in a dependent stage of a read the peer completes later: on the session's completion thread. */
    private static <T> CompletableFuture<T> onCompletionThread(ZmuxStream trigger, ZmuxStream triggerPeer, java.util.concurrent.Callable<T> body)
            throws Exception {
        var stage = trigger.readChunkAsync(1).thenApply(chunk -> {
            chunk.close();
            try { return body.call(); } catch (Exception e) { throw new java.util.concurrent.CompletionException(e); }
        });
        assertFalse(stage.isDone(), "the read waits, so its dependent runs on the completion thread");
        triggerPeer.write(new byte[] {2});
        return stage;
    }

    @Test void aBlockingReadBehindAPendingAsyncReadOfTheSameDirectionThrowsInsteadOfDeadlocking() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream[] target = accepted(pair), trigger = accepted(pair);
            var pending = target[0].readAsync(ByteBuffer.allocate(8)); // holds the client's read turn, waiting for data
            assertFalse(pending.isDone());
            var blocked = onCompletionThread(trigger[0], trigger[1], () -> target[0].read(ByteBuffer.allocate(8)));
            var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> blocked.get(5, TimeUnit.SECONDS));
            assertInstanceOf(CompletionThreadBlockedException.class, failure.getCause());
            // The pending read still completes: nothing was taken from it.
            target[1].write(new byte[] {9});
            assertEquals(1, pending.get(5, TimeUnit.SECONDS));
        }
    }

    @Test void ordinaryBlockingCallsStillWorkOnTheCompletionThread() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream[] target = accepted(pair), trigger = accepted(pair);
            var done = onCompletionThread(trigger[0], trigger[1], () -> {
                target[0].write(new byte[] {5}); // a blocking write
                ByteBuffer got = ByteBuffer.allocate(1);
                target[0].read(got); // a blocking read that waits for the peer
                return got.get(0);
            });
            assertEquals(5, target[1].inputStream().read());
            target[1].write(new byte[] {6});
            assertEquals((byte) 6, done.get(5, TimeUnit.SECONDS));
        }
    }

    @Test void aBlockingWriteBehindAPendingAsyncWriteThrows() throws Exception {
        var config = ZmuxConfig.builder().initialStreamReceiveWindow(16 * 1024)
                .maxStreamReceiveWindow(16 * 1024).sendBuffer(16 * 1024).sendBufferMax(16 * 1024);
        try (EventLoops loops = EventLoops.create(1); var pair = Pair.memory(config.eventLoops(loops).build())) {
            ZmuxStream[] target = accepted(pair), trigger = accepted(pair);
            var pending = target[0].writeAsync(ByteBuffer.allocate(1 << 20)); // the peer reads nothing: it waits for credit
            assertFalse(pending.isDone());
            var blocked = onCompletionThread(trigger[0], trigger[1], () -> { target[0].write(new byte[] {1}); return null; });
            var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> blocked.get(5, TimeUnit.SECONDS));
            assertInstanceOf(CompletionThreadBlockedException.class, failure.getCause());
            var finish = onCompletionThread(trigger[0], trigger[1], () -> { target[0].finish(); return null; });
            assertInstanceOf(CompletionThreadBlockedException.class,
                    assertThrows(java.util.concurrent.ExecutionException.class, () -> finish.get(5, TimeUnit.SECONDS)).getCause());
            target[1].inputStream().readNBytes(1 << 20);
            pending.get(5, TimeUnit.SECONDS);
        }
    }

    @Test void aRefusedSpliceLeavesBothStreamsToTheApplication() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream[] target = accepted(pair), other = accepted(pair), trigger = accepted(pair);
            var pending = target[0].readAsync(ByteBuffer.allocate(8));
            assertFalse(pending.isDone());
            // target as the first and as the second stream of the splice: both refused, neither stream kept by a carrier.
            var first = onCompletionThread(trigger[0], trigger[1], () -> Zmux.splice(target[0], other[0]));
            assertInstanceOf(CompletionThreadBlockedException.class,
                    assertThrows(java.util.concurrent.ExecutionException.class, () -> first.get(5, TimeUnit.SECONDS)).getCause());
            var second = onCompletionThread(trigger[0], trigger[1], () -> Zmux.splice(other[0], target[0]));
            assertInstanceOf(CompletionThreadBlockedException.class,
                    assertThrows(java.util.concurrent.ExecutionException.class, () -> second.get(5, TimeUnit.SECONDS)).getCause());
            target[1].write(new byte[] {9});
            assertEquals(1, pending.get(5, TimeUnit.SECONDS));
            target[0].write(new byte[] {3});
            assertEquals(3, target[1].inputStream().read());
            other[0].write(new byte[] {4});
            assertEquals(4, other[1].inputStream().read());
        }
    }
}
