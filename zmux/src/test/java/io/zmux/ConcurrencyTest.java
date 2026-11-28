package io.zmux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Races between application calls and the owner, with live streams at the close cut. */
@Timeout(120)
class ConcurrencyTest {
    @Test
    void queuedOperationsRaceClose() throws Exception {
        int count = 8;
        var loops = EventLoops.create(2);
        try (loops) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).build();
            try (var pair = Pair.memory(cfg); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                var start = new CountDownLatch(1);
                var reads = new ArrayList<ZmuxFuture<Integer>>();
                var tasks = new ArrayList<Future<?>>();
                var accepted = new ArrayList<ZmuxStream>();
                try {
                    for (int i = 0; i < count; i++) {
                        int step = i % 4;
                        var stream = pair.client().openStream();
                        stream.write(new byte[1]);
                        reads.add(stream.readAsync(ByteBuffer.allocate(1)));
                        var peer = pair.server().acceptStream();
                        assertEquals(stream.id(), peer.id());
                        accepted.add(peer); // Keep peer handles alive until the session settles the reads.
                        tasks.add(workers.submit(() -> {
                            try (stream) {
                                start.await();
                                switch (step) {
                                    case 0 -> stream.write(new byte[32]);
                                    case 1 -> stream.reset(7);
                                    case 2 -> stream.finish();
                                    case 3 -> pair.client().datagrams().send(ByteBuffer.allocate(1));
                                    default -> throw new AssertionError(step);
                                }
                            } catch (SessionClosedException expected) {
                                var closed = assertInstanceOf(SessionError.ApplicationClosed.class, expected.error());
                                assertEquals(1, closed.code());
                            }
                            return null;
                        }));
                    }
                    assertTrue(reads.stream().noneMatch(ZmuxFuture::isDone));
                } finally { start.countDown(); }
                pair.client().abort(1, "race");
                for (Future<?> task : tasks) task.get(30, TimeUnit.SECONDS);
                for (var read : reads) {
                    var failure = assertThrows(ExecutionException.class, () -> read.get(30, TimeUnit.SECONDS));
                    assertTrue(failure.getCause() instanceof IOException);
                }
                for (var peer : accepted) peer.close();
            }
        }
        assertEquals(0, loops.liveThreads());
        Leaks.none(loops, "read buffers leaked");
    }

    @Test
    void blockingOnTheLoopFailsAtOnce() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (Pair p = Pair.memory(cfg)) {
                CompletableFuture<Throwable> got = new CompletableFuture<>();
                p.client().driver.post(() -> {
                    try {
                        p.client().ping();
                        got.complete(null);
                    } catch (Throwable t) {
                        got.complete(t);
                    }
                });
                assertTrue(got.get(10, TimeUnit.SECONDS) instanceof IllegalStateException);
            }
        }
    }

    @Test
    void cleaningTheHandleResetsTheStream() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var stream = pair.client().openStream();
            stream.write(new byte[] {1, 2, 3});
            try (var peer = pair.server().acceptStream()) {
                var cleanable = ZmuxStream.class.getDeclaredField("cleanable");
                cleanable.setAccessible(true);
                ((java.lang.ref.Cleaner.Cleanable) cleanable.get(stream)).clean();
                var reset = assertThrows(StreamResetException.class, () -> {
                    while (peer.read(ByteBuffer.allocate(16)) > 0) { /* Drain the reliable prefix. */ }
                });
                assertEquals(0x5a4d0001L, reset.code());
            }
        }
    }

    @Test
    void writesAfterCloseFailWithTheSessionError() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            Pair p = Pair.memory(cfg);
            ZmuxStream a = p.client().openStream();
            p.server().abort(3, "x");
            p.client().awaitClosed();
            SessionClosedException e = assertThrows(SessionClosedException.class, () -> a.write(new byte[10]));
            assertTrue(e.error() instanceof SessionError.ApplicationClosed ac && ac.code() == 3);
            assertThrows(SessionClosedException.class, () -> p.client().openStream());
            p.close();
        }
    }
}
