package io.zmux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Races between application threads and the loop: random operations while the session
 * closes; every call returns (no lost wake-up, no deadlock) and nothing leaks.
 */
@Timeout(120)
class ConcurrencyTest {

    @RepeatedTest(5)
    void randomOperationsRaceClose() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).datagrams(true).build();
            Pair p = Pair.memory(cfg);
            AtomicInteger done = new AtomicInteger();
            AtomicInteger served = new AtomicInteger();
            AtomicBoolean unexpected = new AtomicBoolean();
            try (var ex = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<?>> fs = new ArrayList<>();
                fs.add(ex.submit(() -> {
                    while (true) {
                        ZmuxStream s;
                        try {
                            s = p.server().acceptStream();
                        } catch (IOException e) {
                            return null;
                        }
                        served.incrementAndGet();
                        ex.submit(() -> {
                            try {
                                byte[] b = new byte[4096];
                                while (s.read(b, 0, b.length) >= 0) s.write(b, 0, 16);
                                s.finish();
                            } catch (IOException e) {
                                // Reset, stopped or session gone: expected outcomes.
                            } finally {
                                s.close();
                            }
                            return null;
                        });
                    }
                }));
                // One stream that surely reaches the server, so the close races live traffic
                // even when every random stream happens to end before its first frame.
                ZmuxStream keeper = p.client().openStream();
                keeper.write(new byte[1]);
                for (int t = 0; t < 32; t++) {
                    int seed = t;
                    fs.add(ex.submit(() -> {
                        SplittableRandom r = new SplittableRandom(seed);
                        try {
                            for (int i = 0; i < 50; i++) {
                                ZmuxStream s = p.client().openStream();
                                int ops = r.nextInt(6);
                                for (int k = 0; k < ops; k++) {
                                    switch (r.nextInt(6)) {
                                        case 0 -> s.write(new byte[r.nextInt(20000)]);
                                        case 1 -> s.reset(r.nextInt(5));
                                        case 2 -> s.stop(r.nextInt(5));
                                        case 3 -> p.client().datagrams().send(ByteBuffer.allocate(r.nextInt(100)));
                                        case 4 -> s.finish();
                                        default -> {
                                            s.setReadTimeout(Duration.ofMillis(5));
                                            try {
                                                s.read(new byte[100], 0, 100);
                                            } catch (java.net.SocketTimeoutException e) {
                                                // Fine.
                                            }
                                        }
                                    }
                                }
                                s.close();
                            }
                        } catch (ZmuxException | java.net.SocketTimeoutException e) {
                            // Session closed under us or a stream outcome: expected.
                        } catch (IllegalStateException e) {
                            // A finished direction: expected.
                        } catch (Exception e) {
                            unexpected.set(true);
                            throw e;
                        }
                        done.incrementAndGet();
                        return null;
                    }));
                }
                // Close only once a stream got through, so the race is between close and live traffic, not start-up.
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (served.get() == 0 && System.nanoTime() < until) Thread.sleep(1);
                Thread.sleep(seedDelay());
                p.client().close(new ApplicationError(1, "race"));
                for (Future<?> f : fs) f.get(60, TimeUnit.SECONDS);
            }
            assertEquals(32, done.get());
            assertTrue(served.get() > 0, "no stream reached the server before the close");
            assertTrue(!unexpected.get());
            p.client().awaitClosed();
            p.server().awaitClosed();
            loops.close();
            assertEquals(0, loops.liveThreads());
            assertEquals(0, loops.outstandingBuffers(), "read buffers leaked");
        }
    }

    private static long seedDelay() {
        return System.nanoTime() % 30;
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
    void droppedHandleResetsTheStream() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (Pair p = Pair.memory(cfg)) {
                openAndDrop(p.client());
                ZmuxStream b = p.server().acceptStream();
                StreamResetException e = null;
                for (int i = 0; i < 200 && e == null; i++) {
                    System.gc();
                    b.setReadTimeout(Duration.ofMillis(20));
                    try {
                        byte[] buf = new byte[16];
                        while (b.read(buf, 0, 16) > 0) { }
                    } catch (StreamResetException x) {
                        e = x;
                    } catch (java.net.SocketTimeoutException x) {
                        // Not collected yet.
                    }
                }
                assertTrue(e != null, "no reset after the handle became unreachable");
                assertEquals(0, e.code());
            }
        }
    }

    private static void openAndDrop(ZmuxSession s) throws IOException {
        ZmuxStream a = s.openStream();
        a.write(new byte[] {1, 2, 3});
    }

    @Test
    void writesAfterCloseFailWithTheSessionError() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            Pair p = Pair.memory(cfg);
            ZmuxStream a = p.client().openStream();
            p.server().close(new ApplicationError(3, "x"));
            p.client().awaitClosed();
            SessionClosedException e = assertThrows(SessionClosedException.class, () -> a.write(new byte[10]));
            assertTrue(e.error() instanceof SessionError.ApplicationClosed ac && ac.code() == 3);
            assertThrows(SessionClosedException.class, () -> p.client().openStream());
            p.close();
        }
    }
}
