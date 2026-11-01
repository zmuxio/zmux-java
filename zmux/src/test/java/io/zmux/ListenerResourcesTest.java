package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(40)
class ListenerResourcesTest {
    static void until(java.util.function.BooleanSupplier condition) throws Exception {
        long end = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean()) { assertTrue(System.nanoTime() < end, "condition timed out"); Thread.sleep(2); }
    }

    static ZmuxConfig config(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(2).withAuth(PipeAuth.psk(new byte[32])).withResumeTimeout(Duration.ofSeconds(30))).build();
    }

    @Test void maxSuspendedEvictsTheOldestAndReleasesResources() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = config(loops);
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).maxSuspended(0).build())) {
                var accepted = ex.submit((Callable<ZmuxSession>)listener::accept);
                var c = Zmux.connect(listener.localAddress(), cfg); var s = accepted.get();
                c.paths().list().getFirst().abort();
                assertInstanceOf(SessionError.ResumeTimeout.class, s.awaitClosed());
                c.close(); c.awaitClosed();
                until(() -> listener.memoryUsed() == 0);
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void pressureEvictsSuspendedBuffersAndReportsRecoveryOffLoop() throws Exception {
        try (EventLoops loops = EventLoops.create(3); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = config(loops);
            AtomicInteger enter = new AtomicInteger(), recovered = new AtomicInteger();
            ListenerConfig options = ListenerConfig.builder().session(cfg).memoryBudget(128 << 10)
                    .memoryPressureListener((used, budget) -> {
                        assertFalse(io.zmux.internal.loop.SessionHost.onHostThread());
                        if (used >= budget) enter.incrementAndGet(); else recovered.incrementAndGet();
                    }).build();
            try (var listener = Zmux.listen(NetworkTest.any(), options)) {
                var first = ex.submit((Callable<ZmuxSession>)listener::accept);
                var c1 = Zmux.connect(listener.localAddress(), cfg); var s1 = first.get();
                var a = c1.openStream(); a.write(new byte[96 << 10]);
                var held1 = s1.acceptStream();
                until(() -> listener.memoryUsed() >= 96 << 10);
                var second = ex.submit((Callable<ZmuxSession>)listener::accept);
                var c2 = Zmux.connect(listener.localAddress(), cfg); var s2 = second.get();
                var b = c2.openStream(); b.write(new byte[96 << 10]);
                var held2 = s2.acceptStream();
                until(() -> enter.get() > 0);
                c1.paths().list().getFirst().abort();
                assertInstanceOf(SessionError.ResumeTimeout.class, s1.awaitClosed());
                // Consuming the other session brings the shared budget below its hysteresis.
                byte[] bytes = new byte[96 << 10];
                int offset = 0;
                while (offset < bytes.length) offset += held2.read(bytes, offset, bytes.length - offset);
                until(() -> recovered.get() > 0);
                held1.close(); held2.close(); a.close(); b.close();
                c1.close(); c2.close(); s2.close();
                c1.awaitClosed(); c2.awaitClosed(); s2.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }
}
