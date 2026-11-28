package io.zmux;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.loop.RecvLane;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The wait for read readiness: success for data and for every end, failure only with its own bound, nothing consumed. */
@Timeout(60)
class ReadReadinessTest {
    private static ZmuxStream accepted(Pair p, ZmuxStream opened) throws Exception {
        opened.write("x".getBytes(UTF_8)); // the first frame, so that the peer accepts it
        ZmuxStream s = p.server().acceptStream();
        byte[] one = new byte[1];
        assertEquals(1, s.read(one, 0, 1));
        return s;
    }

    @Test void dataIsReadableAndNothingIsConsumed() throws Exception {
        try (var loops = EventLoops.create(1); Pair p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream c = p.client().openStream();
            ZmuxStream s = accepted(p, c);
            assertThrows(SocketTimeoutException.class, () -> s.awaitReadable(Duration.ZERO), "nothing yet: the bound is the only failure");
            c.write("hello".getBytes(UTF_8));
            s.awaitReadable(Duration.ofSeconds(10));
            s.awaitReadable(); // a wait takes nothing, so it repeats
            byte[] b = new byte[16];
            assertEquals(5, s.read(b, 0, b.length));
            assertEquals("hello", new String(b, 0, 5, UTF_8));
        }
    }

    @Test void anOwnerDeadlineExpiresAnAlreadyRegisteredReadinessWait() throws Exception {
        try (var loops = EventLoops.create(1); Pair p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream c = p.client().openStream();
            ZmuxStream s = accepted(p, c);
            var timeout = Duration.ofMinutes(1);
            s.setReadTimeout(timeout);
            var pending = waiting(s, ZmuxSession.until(s.readTimeout().orElseThrow()));
            assertFalse(pending.isDone());
            s.session.driver.loop().advanceTimersForTests(timeout.toNanos()).get(30, TimeUnit.SECONDS);
            assertInstanceOf(SocketTimeoutException.class,
                    assertThrows(ExecutionException.class, () -> pending.get(30, TimeUnit.SECONDS)).getCause());
        }
    }

    @Test void aFinIsReadyAndTheReadReportsItOnce() throws Exception {
        try (var loops = EventLoops.create(1); Pair p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream c = p.client().openStream();
            ZmuxStream s = accepted(p, c);
            c.finish();
            s.awaitReadable(Duration.ofSeconds(10));
            assertEquals(-1, s.read(ByteBuffer.allocate(4)));
        }
    }

    @Test void aResetIsReadyAndTheReadFailsWithIt() throws Exception {
        try (var loops = EventLoops.create(1); Pair p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream c = p.client().openStream();
            ZmuxStream s = accepted(p, c);
            c.reset(7);
            s.awaitReadable(Duration.ofSeconds(10)); // the wait returns; the read reports the reset
            var e = assertThrows(StreamResetException.class, () -> s.read(ByteBuffer.allocate(4)));
            assertEquals(7, e.code());
        }
    }

    @Test void aLocalStopIsReady() throws Exception {
        try (var loops = EventLoops.create(1); Pair p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream c = p.client().openStream();
            ZmuxStream s = accepted(p, c);
            s.stop(3);
            s.awaitReadable(Duration.ofSeconds(10));
            assertThrows(java.io.IOException.class, () -> s.read(ByteBuffer.allocate(4)));
        }
    }

    @Test void theSessionsEndWakesAnAlreadyRegisteredWaiter() throws Exception {
        try (var loops = EventLoops.create(1); Pair p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream c = p.client().openStream();
            ZmuxStream s = accepted(p, c);
            var pending = waiting(s, Long.MAX_VALUE);
            assertFalse(pending.isDone());
            p.client().close();
            assertEquals(-1L, pending.get(30, TimeUnit.SECONDS));
            assertThrows(java.io.IOException.class, () -> s.read(ByteBuffer.allocate(4)));
        }
    }

    @Test void tryReadSeparatesNothingNowFromTheEnd() throws Exception {
        try (var loops = EventLoops.create(1); Pair p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream c = p.client().openStream();
            ZmuxStream s = accepted(p, c);
            ByteBuffer dst = ByteBuffer.allocate(16);
            assertEquals(0, s.tryRead(dst), "nothing readable now: 0, never a wait");
            assertEquals(0, dst.position());
            assertEquals(0, s.tryRead(ByteBuffer.allocate(0)), "no room is 0 as well");
            c.write("hello".getBytes(UTF_8));
            s.awaitReadable(Duration.ofSeconds(10));
            assertEquals(5, s.tryRead(dst), "the wait, then the read without waiting");
            assertEquals(5, dst.position(), "the buffer advances by the bytes taken");
            assertEquals(0, s.tryRead(dst), "read to the last byte, not yet finished: still nothing now");
            c.finish();
            s.awaitReadable(Duration.ofSeconds(10));
            assertEquals(-1, s.tryRead(dst), "FIN after everything was read: the end");
            assertEquals(-1, s.tryRead(dst), "and it stays the end");
        }
    }

    @Test void tryReadReportsAResetAsTheStreamsError() throws Exception {
        try (var loops = EventLoops.create(1); Pair p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream c = p.client().openStream();
            ZmuxStream s = accepted(p, c);
            c.reset(9);
            s.awaitReadable(Duration.ofSeconds(10));
            var e = assertThrows(StreamResetException.class, () -> s.tryRead(ByteBuffer.allocate(4)));
            assertEquals(9, e.code());
        }
    }

    @Test void tryReadAfterALocalStopFails() throws Exception {
        try (var loops = EventLoops.create(1); Pair p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream c = p.client().openStream();
            ZmuxStream s = accepted(p, c);
            s.stop(3);
            assertThrows(java.io.IOException.class, () -> s.tryRead(ByteBuffer.allocate(4)));
        }
    }

    @Test void tryReadReturnsZeroWhileAnotherReadHoldsTheStream() throws Exception {
        try (var loops = EventLoops.create(1); Pair p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream c = p.client().openStream();
            ZmuxStream s = accepted(p, c);
            var pending = s.readAsync(ByteBuffer.allocate(8)); // holds the read turn until data arrives
            assertEquals(0, s.tryRead(ByteBuffer.allocate(8)));
            c.write("ab".getBytes(UTF_8));
            assertEquals(2, pending.get(10, java.util.concurrent.TimeUnit.SECONDS));
        }
    }

    /** A wait never queues behind a read: with the read stalled (its completion step waits for the completion thread), data is still reported. */
    @Test void awaitReadableDoesNotQueueBehindAReadInProgress() throws Exception {
        try (var loops = EventLoops.create(1); Pair p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream c = p.client().openStream(), c2 = p.client().openStream();
            ZmuxStream s = accepted(p, c), s2 = accepted(p, c2);
            var blocked = new java.util.concurrent.CountDownLatch(1);
            var release = new java.util.concurrent.CountDownLatch(1);
            var testThread = Thread.currentThread();
            // The completion thread is held by the stage of another stream's read, which nothing has completed yet.
            var other = s2.readAsync(ByteBuffer.allocate(1)).thenRun(() -> {
                assertNotSame(testThread, Thread.currentThread());
                blocked.countDown();
                try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            });
            var pending = s.readAsync(ByteBuffer.allocate(8)); // holds the read turn
            c2.write(new byte[] {1});
            assertTrue(blocked.await(10, java.util.concurrent.TimeUnit.SECONDS));
            try {
                c.write("ab".getBytes(UTF_8)); // pushed to s, whose read cannot take it until the completion thread is free
                s.awaitReadable(Duration.ofSeconds(10)); // not queued behind the read: reports the data at once
                assertFalse(pending.isDone());
            } finally { release.countDown(); }
            assertEquals(2, pending.get(10, java.util.concurrent.TimeUnit.SECONDS));
            other.get(10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private static ZmuxFuture<Long> waiting(ZmuxStream stream, long deadline) {
        var op = stream.in.awaitReadableAsync(deadline);
        return ZmuxSession.future(op, result -> {
            if (result instanceof java.io.IOException failure) throw failure;
            assertEquals(true, result);
            return RecvLane.readable(op);
        });
    }

    /** Any number of registered waits proceed at once. */
    @Test void manyWaitersMayRegisterAtOnce() throws Exception {
        try (var loops = EventLoops.create(1); Pair p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            ZmuxStream c = p.client().openStream();
            ZmuxStream s = accepted(p, c);
            var waiters = new ArrayList<ZmuxFuture<Long>>();
            for (int i = 0; i < 8; i++) waiters.add(waiting(s, Long.MAX_VALUE));
            assertTrue(waiters.stream().noneMatch(ZmuxFuture::isDone));
            c.write("x".getBytes(UTF_8));
            for (var w : waiters) assertEquals(1L, w.get(30, TimeUnit.SECONDS));
            assertEquals(1, s.tryRead(ByteBuffer.allocate(4)));
        }
    }
}
