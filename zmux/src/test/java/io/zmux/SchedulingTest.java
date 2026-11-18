package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(40)
class SchedulingTest {

    private static ZmuxConfig.Builder config(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).maxPipes(3).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation();
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
                for (int i = 0; i < pipes.size(); i++) if (pipes.get(i).id().value() > pipes.get(selected).id().value()) selected = i;
                int pick = selected;
                return requests.stream().map(q -> Scheduler.Answer.pipe(pick, 16384)).toList();
            });
            ZmuxConfig cfg = config(loops).scheduler(policy).build();
            try (ZmuxListener listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg))) {
                Future<ZmuxSession> sf = ex.submit((Callable<ZmuxSession>)listener::accept);
                ZmuxSession c = Zmux.connect(listener.localAddress(), cfg), s = sf.get();
                session.set(c);
                ZmuxPipe added = c.addPipe(listener.localAddress(), PipeOptions.builder().weight(0).receivePriority(12).receiveStandby(true).build());
                assertEquals(0, added.stats().orElseThrow().weight());
                var peer = s.pipes().stream().filter(p -> p.id().value() == added.id().value()).findFirst().orElseThrow().stats().orElseThrow();
                assertEquals(12, peer.sendPriority()); assertTrue(peer.standby());
                assertEquals(PipeStats.SettingSource.PEER, peer.sendPrioritySource()); assertEquals(PipeStats.SettingSource.PEER, peer.standbySource());
                added.update(PipeUpdate.builder().sendPriority(9).build());
                added.update(PipeUpdate.builder().weight(7).build());
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
                assertEquals(2, c.pipeStats().size());
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
                c.setQualityBand(QualityBand.builder().rtt(Duration.ofMillis(1)).rateRatio(1.5).build());
                c.clearQualityBand(); c.setQualityBand(QualityBand.builder().rtt(Duration.ofMillis(1)).rateRatio(1.5).build());
                c.setCapacityFloor(new CapacityFloor(Long.MAX_VALUE));
                boolean below = false;
                while (!below) below = c.nextEvent() instanceof SessionEvent.CapacityBelow;
                added.update(PipeUpdate.builder().clearSendPriority().build());
                assertEquals(PipeStats.SettingSource.DEFAULT, added.stats().orElseThrow().sendPrioritySource());
                c.close(); s.close(); c.awaitClosed(); s.awaitClosed();
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }

    @Test void invalidQualitySettingsAreRejectedAtTheBoundary() {
        assertThrows(IllegalArgumentException.class, () -> new CapacityFloor(-1));
        assertThrows(IllegalArgumentException.class, () -> new CapacityFloor(0), "0 is not off: clearCapacityFloor is");
        assertThrows(IllegalArgumentException.class, () -> new CapacityFloor(1, 0), "0 is not the default");
        assertThrows(IllegalArgumentException.class, () -> new CapacityFloor(1, 0.5));
        assertThrows(IllegalArgumentException.class, () -> new CapacityFloor(1, -1));
        assertThrows(IllegalArgumentException.class, () -> new CapacityFloor(1, Double.NaN));
        assertEquals(1.0, new CapacityFloor(1, 1).recoveryRatio()); // no hysteresis
        assertEquals(CapacityFloor.DEFAULT_RECOVERY_RATIO, new CapacityFloor(1).recoveryRatio());
        assertThrows(IllegalArgumentException.class, () -> new Scheduler.Answer(0, -1, false));
    }
}
