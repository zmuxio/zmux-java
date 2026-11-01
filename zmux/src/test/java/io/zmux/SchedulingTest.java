package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(40)
class SchedulingTest {
    private static final MultipathConfig MULTIPATH = MultipathConfig.of(3, PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO);

    private static ZmuxConfig.Builder config(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).multipath(MULTIPATH);
    }

    @Test void customPoliciesUseValuesAndCanChangeOnline() throws Exception {
        try (EventLoops loops = EventLoops.create(2);
             var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            AtomicInteger calls = new AtomicInteger();
            AtomicBoolean blocked = new AtomicBoolean();
            AtomicReference<ZmuxStream> activeStream = new AtomicReference<>();
            AtomicReference<ZmuxSession> session = new AtomicReference<>();
            Scheduler policy = Scheduler.custom((requests, pipes) -> {
                calls.incrementAndGet();
                assertThrows(UnsupportedOperationException.class, () -> pipes.clear());
                if (session.get() != null) {
                    assertThrows(IllegalStateException.class, () -> session.get().ping());
                    if (activeStream.get() != null) assertThrows(IllegalStateException.class, () -> activeStream.get().write(new byte[] {9}));
                    blocked.set(true);
                }
                int selected = 0;
                for (int i = 0; i < pipes.size(); i++) if (pipes.get(i).id() > pipes.get(selected).id()) selected = i;
                int pick = selected;
                return requests.stream().map(q -> Scheduler.Answer.pipe(pick, 16384)).toList();
            });
            ZmuxConfig cfg = config(loops).multipath(MULTIPATH.withScheduler(policy)).build();
            try (ZmuxListener listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg))) {
                Future<ZmuxSession> sf = ex.submit((Callable<ZmuxSession>)listener::accept);
                ZmuxSession c = Zmux.connect(listener.localAddress(), cfg), s = sf.get();
                session.set(c);
                ZmuxPipe added = c.paths().add(listener.localAddress(), PipeOptions.builder().weight(0).receivePriority(12).receiveStandby(true).build());
                assertEquals(0, added.stats().orElseThrow().weight());
                var peer = s.paths().list().stream().filter(p -> p.id() == added.id()).findFirst().orElseThrow().stats().orElseThrow();
                assertEquals(12, peer.sendPriority()); assertTrue(peer.standby());
                assertEquals(PipeStats.SettingSource.PEER, peer.sendPrioritySource()); assertEquals(PipeStats.SettingSource.PEER, peer.standbySource());
                added.setSendPriority(9);
                added.setWeight(7);
                assertEquals(9, added.stats().orElseThrow().sendPriority());
                assertEquals(PipeStats.SettingSource.LOCAL, added.stats().orElseThrow().sendPrioritySource());
                assertEquals(7, added.stats().orElseThrow().weight());
                long before = added.stats().orElseThrow().bytesSent();
                Future<byte[]> incoming = ex.submit(() -> {
                    ZmuxStream stream = s.acceptStream();
                    byte[] bytes = SessionTest.readAll(stream);
                    stream.finish(); stream.close(); return bytes;
                });
                ZmuxStream stream = c.openStream(); stream.setRedundant(false);
                activeStream.set(stream);
                byte[] data = new byte[8 << 20];
                stream.write(data); stream.finish();
                assertArrayEquals(data, incoming.get());
                assertTrue(added.stats().orElseThrow().bytesSent() > before);
                assertTrue(calls.get() > 0); assertTrue(blocked.get());
                PipePoolTest.await(() -> c.stats().schedulerAnswers() > 0);
                long fallbacks = c.stats().schedulerFallbacks();
                assertEquals(2, c.paths().stats().size());
                stream.setScheduler(Scheduler.custom((q, p) -> { throw new IllegalArgumentException("bad picker"); }));
                // A failing policy must fall back, without failing the session.
                Future<byte[]> fallback = ex.submit(() -> SessionTest.readAll(s.acceptStream()));
                ZmuxStream second = c.openStream();
                second.setRedundant(false);
                second.setScheduler(Scheduler.custom((q, p) -> { throw new IllegalStateException("bad picker"); }));
                second.write(new byte[] {1, 2, 3}); second.finish();
                assertArrayEquals(new byte[] {1, 2, 3}, fallback.get());
                PipePoolTest.await(() -> c.stats().schedulerFallbacks() > fallbacks);
                c.setScheduler(Scheduler.SPREAD_ROUND_ROBIN);
                c.setQualityBand(new QualityBand(Duration.ofMillis(1), 1.5));
                c.setCapacityFloor(new CapacityFloor(Double.MAX_VALUE, 1.25));
                boolean below = false;
                while (!below) below = c.nextEvent() instanceof SessionEvent.CapacityBelow;
                added.clearSendPriority();
                assertEquals(PipeStats.SettingSource.DEFAULT, added.stats().orElseThrow().sendPrioritySource());
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void invalidQualitySettingsAreRejectedAtTheBoundary() {
        assertThrows(IllegalArgumentException.class, () -> new CapacityFloor(Double.NaN, 0));
        assertThrows(IllegalArgumentException.class, () -> new CapacityFloor(1, 0.5));
        assertThrows(IllegalArgumentException.class, () -> new QualityBand(Duration.ZERO, 1, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new Scheduler.Answer(0, -1, false));
    }
}
