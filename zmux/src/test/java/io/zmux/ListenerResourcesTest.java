package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(40)
class ListenerResourcesTest {
    static ZmuxConfig config(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).sessionPolicy(new SessionPolicy(new CapacityLimits(2))).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).resumeTimeout(Duration.ofSeconds(30)).build();
    }

    @Test void maxSuspendedEvictsTheOldestAndReleasesResources() throws Exception {
        EventLoops loops = EventLoops.create(2);
        try (loops; var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = config(loops);
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).maxSuspended(0).build())) {
                var accepted = ex.submit((Callable<ZmuxSession>)listener::accept);
                var c = Zmux.connect(listener.localAddress(), cfg); var s = accepted.get();
                c.pipes().getFirst().abort();
                assertInstanceOf(SessionError.ResumeTimeout.class, s.awaitClosed());
                c.close(); c.awaitClosed();
                assertEquals(0, listener.adapter().memoryUsed());
            }
        }
        assertEquals(0, loops.outstandingBuffers());
    }

    // Capture the real eviction task while suspended and dispatch it only after a successful resume.
    @Test void recoveredEvictionCandidateSurvivesPressure() throws Exception {
        // HS-31, HS-56.
        try (var loops = EventLoops.create(2)) {
            var cfg = config(loops);
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).noMemoryBudget().build());
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(); var writer = c.openStream()) {
                writer.write(new byte[8192]);
                try (var held = s.acceptStream()) {
                    assertEquals(writer.id(), held.id());
                    c.pipes().getFirst().abort();
                    while (!(s.nextEvent(Duration.ofSeconds(5)) instanceof SessionEvent.SessionSuspended)) { /* event barrier */ }
                    var evict = io.zmux.internal.loop.CarrierActionsTest.selectedEviction(l.hub, s.driver);
                    l.drain("resume allowed");
                    c.addPipe(l.localAddress(), PipeOptions.defaults());
                    while (!(s.nextEvent(Duration.ofSeconds(5)) instanceof SessionEvent.SessionResumed)) { /* event barrier */ }
                    assertTrue(l.adapter().memoryUsed() > 1);
                    l.setLimits(l.limits().toBuilder().memoryBudget(l.adapter().memoryUsed()).build());
                    var checked = new CompletableFuture<Void>();
                    s.driver.executeCarrier(() -> { evict.run(); checked.complete(null); });
                    checked.get(5, TimeUnit.SECONDS);
                    assertNotNull(s.ping());
                    l.setLimits(l.limits().toBuilder().noMemoryBudget().build());
                }
            }
        }
    }

    @Test void pressureEvictsSuspendedBuffersAndReportsRecoveryOffLoop() throws Exception {
        EventLoops loops = EventLoops.create(3);
        try (loops; var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = config(loops);
            var enter = new CompletableFuture<Void>();
            var recovered = new CompletableFuture<Void>();
            ListenerConfig options = ListenerConfig.builder().session(cfg).noMemoryBudget()
                    .memoryPressureListener((used, budget) -> {
                        assertFalse(io.zmux.internal.loop.SessionHost.onHostThread());
                        if (used >= budget) enter.complete(null); else recovered.complete(null);
                    }).build();
            try (var listener = Zmux.listen(NetworkTest.any(), options)) {
                var first = ex.submit((Callable<ZmuxSession>)listener::accept);
                var c1 = Zmux.connect(listener.localAddress(), cfg); var s1 = first.get();
                var a = c1.openStream(); a.write(new byte[96 << 10]);
                var held1 = s1.acceptStream();
                var second = ex.submit((Callable<ZmuxSession>)listener::accept);
                var c2 = Zmux.connect(listener.localAddress(), cfg); var s2 = second.get();
                var b = c2.openStream(); b.write(new byte[96 << 10]);
                var held2 = s2.acceptStream();
                listener.setLimits(listener.limits().toBuilder().memoryBudget(listener.adapter().memoryUsed()).build());
                enter.get(20, TimeUnit.SECONDS);
                c1.pipes().getFirst().abort();
                assertInstanceOf(SessionError.ResumeTimeout.class, s1.awaitClosed());
                // Consuming the other session brings the shared budget below its hysteresis.
                byte[] bytes = new byte[96 << 10];
                int offset = 0;
                while (offset < bytes.length) offset += held2.read(bytes, offset, bytes.length - offset);
                recovered.get(20, TimeUnit.SECONDS);
                held1.close(); held2.close(); a.close(); b.close();
                c1.close(); c2.close(); s2.close();
                c1.awaitClosed(); c2.awaitClosed(); s2.awaitClosed();
            }
        }
        assertEquals(0, loops.outstandingBuffers());
    }
}
