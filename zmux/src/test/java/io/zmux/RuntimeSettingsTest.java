package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Every runtime setting reads back under its name; timeouts are positive or cleared, never null. */
@org.junit.jupiter.api.Timeout(60)
class RuntimeSettingsTest {
    @Test void sessionAndStreamSettingsReadBack() throws Exception {
        try (var loops = EventLoops.create(1);
             var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).scheduler(Scheduler.SPREAD_ETA).build())) {
            var s = pair.client();
            assertEquals(Scheduler.SPREAD_ETA, s.scheduler());
            s.setScheduler(Scheduler.AFFINITY);
            assertEquals(Scheduler.AFFINITY, s.scheduler());
            assertEquals(Optional.empty(), s.qualityBand());
            var band = QualityBand.builder().rtt(Duration.ofMillis(5)).build();
            s.setQualityBand(band);
            assertEquals(Optional.of(band), s.qualityBand());
            s.clearQualityBand();
            assertEquals(Optional.empty(), s.qualityBand());
            assertEquals(Optional.empty(), s.capacityFloor());
            s.setCapacityFloor(new CapacityFloor(1000));
            assertEquals(Optional.of(new CapacityFloor(1000)), s.capacityFloor());
            s.clearCapacityFloor();
            assertEquals(Optional.empty(), s.capacityFloor());

            var st = s.openStream();
            assertEquals(new Priority(3, true), st.priority()); // the local default
            st.setPriority(new Priority(1, true));
            assertEquals(new Priority(1, true), st.priority());
            assertEquals(Optional.empty(), st.scheduler());
            st.setScheduler(Scheduler.SPREAD_ROUND_ROBIN);
            assertEquals(Optional.of(Scheduler.SPREAD_ROUND_ROBIN), st.scheduler());
            st.clearScheduler();
            assertEquals(Optional.empty(), st.scheduler());
            assertEquals(Optional.empty(), st.redundant());
            st.setRedundant(false);
            assertEquals(Optional.of(false), st.redundant());
            st.clearRedundant();
            assertEquals(Optional.empty(), st.redundant());
            assertEquals(Optional.empty(), st.peerPriority());
            st.setPeerPriority(new Priority(2, false));
            assertEquals(Optional.of(new Priority(2, false)), st.peerPriority());

            assertEquals(Optional.empty(), st.readTimeout());
            assertEquals(Optional.empty(), st.writeTimeout());
            st.setReadTimeout(Duration.ofSeconds(1));
            st.setWriteTimeout(Duration.ofSeconds(2));
            assertEquals(Optional.of(Duration.ofSeconds(1)), st.readTimeout());
            assertEquals(Optional.of(Duration.ofSeconds(2)), st.writeTimeout());
            st.clearReadTimeout();
            st.clearWriteTimeout();
            assertEquals(Optional.empty(), st.readTimeout());
            assertEquals(Optional.empty(), st.writeTimeout());
            assertThrows(NullPointerException.class, () -> st.setReadTimeout(null));
            assertThrows(IllegalArgumentException.class, () -> st.setWriteTimeout(Duration.ZERO));
            assertThrows(IllegalArgumentException.class, () -> st.setReadTimeout(Duration.ofMillis(-1)));
        }
    }

    @Test void sendBufferAndReceiveWindowReadBackWhatTheStreamHolds() throws Exception {
        long configured = ZmuxConfig.defaults().sendBuffer();
        try (var loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var st = pair.client().openStream();
            assertEquals((int) configured, st.sendBuffer(), "copied from the session's configuration at creation");
            var initial = st.receiveWindow();
            assertTrue(initial.initial() > 0, "the configured initial window");
            assertEquals(0, initial.max(), "automatic: no fixed cap");
            st.setSendBuffer(1 << 20);
            assertEquals(1 << 20, st.sendBuffer());
            st.setSendBuffer(1);
            assertEquals(16 << 10, st.sendBuffer(), "as the setter applies it: at least 16 KiB");
            st.setReceiveWindow(2L << 20, 8L << 20);
            assertEquals(new ReceiveWindow(2L << 20, 8L << 20), st.receiveWindow());
            // 0 is the automatic cap: it does not keep the previous one.
            st.setReceiveWindow(2L << 20, 0);
            assertEquals(new ReceiveWindow(2L << 20, 0), st.receiveWindow(), "max 0 makes the cap automatic");
            // A lower initial changes only the setting read back; max takes the new cap.
            st.setReceiveWindow(4096, 4L << 20);
            assertEquals(new ReceiveWindow(4096, 4L << 20), st.receiveWindow(), "as set");
            // An initial below 4 KiB is an invalid argument that changes nothing: 0 and 4095 are refused, 4096 taken.
            assertThrows(IllegalArgumentException.class, () -> st.setReceiveWindow(0, 0));
            assertThrows(IllegalArgumentException.class, () -> st.setReceiveWindow(4095, 0));
            assertEquals(new ReceiveWindow(4096, 4L << 20), st.receiveWindow());
            st.setReceiveWindow(3L << 20, 4L << 20);
            assertEquals(new ReceiveWindow(3L << 20, 4L << 20), st.receiveWindow(), "max equal to initial is valid");
            // A nonzero max below initial is an invalid argument and changes nothing.
            assertThrows(IllegalArgumentException.class, () -> st.setReceiveWindow(8L << 20, 4L << 20));
            assertThrows(IllegalArgumentException.class, () -> st.setReceiveWindow(1L << 20, 1));
            assertThrows(IllegalArgumentException.class, () -> st.setReceiveWindow(-1, 0));
            assertEquals(new ReceiveWindow(3L << 20, 4L << 20), st.receiveWindow(), "rejected calls change nothing");
            st.setReceiveWindow(2L << 20, 4L << 20);
            assertThrows(IllegalArgumentException.class, () -> new ReceiveWindow(-1, 0));
            st.setScheduler(Scheduler.SPREAD_ROUND_ROBIN);
            st.setRedundant(true);
            st.setPriority(new Priority(5, false));
            st.setReadTimeout(Duration.ofSeconds(3));
            // After the session's end every getter answers, with the value held last.
            pair.client().close();
            pair.client().awaitClosed();
            assertEquals(16 << 10, st.sendBuffer());
            assertEquals(new ReceiveWindow(2L << 20, 4L << 20), st.receiveWindow());
            assertEquals(Optional.of(Scheduler.SPREAD_ROUND_ROBIN), st.scheduler());
            assertEquals(Optional.of(true), st.redundant());
            assertEquals(new Priority(5, false), st.priority());
            assertEquals(Optional.of(Duration.ofSeconds(3)), st.readTimeout());
        }
    }

    /** Max 0 makes the cap automatic even where the session configures a fixed one. */
    @Test void zeroMaxOverridesTheConfiguredCap() throws Exception {
        try (var loops = EventLoops.create(1);
             var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).maxStreamReceiveWindow(1L << 20).build())) {
            var st = pair.client().openStream();
            assertEquals(1L << 20, st.receiveWindow().max(), "the configured cap until set");
            st.setReceiveWindow(st.receiveWindow().initial(), 0);
            assertEquals(0, st.receiveWindow().max(), "set automatic");
        }
    }

    @Test void aStreamsSettingsAreItsOwn() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var a = pair.client().openStream();
            var b = pair.client().openStream();
            a.setSendBuffer(1 << 20);
            a.setReceiveWindow(3L << 20, 0);
            assertEquals((int) ZmuxConfig.defaults().sendBuffer(), b.sendBuffer());
            assertNotEquals(a.receiveWindow(), b.receiveWindow());
        }
    }

    @Test void sessionGettersAnswerAfterTheSessionEnded() throws Exception {
        var liveness = Liveness.builder().suspectFloor(Duration.ofSeconds(2)).build();
        var limits = io.zmux.shaping.ShapingLimits.defaults();
        try (var loops = EventLoops.create(1);
             var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).scheduler(Scheduler.SPREAD_ETA).build())) {
            var s = pair.client();
            assertNotNull(s.liveness());
            assertNotNull(s.shapingLimits());
            s.setLiveness(liveness);
            s.setShapingLimits(limits);
            s.setScheduler(Scheduler.AFFINITY);
            var band = QualityBand.builder().rtt(Duration.ofMillis(5)).build();
            s.setQualityBand(band);
            s.setCapacityFloor(new CapacityFloor(1000));
            s.close();
            s.awaitClosed();
            assertEquals(liveness, s.liveness());
            assertEquals(limits, s.shapingLimits());
            assertEquals(Scheduler.AFFINITY, s.scheduler());
            assertEquals(Optional.of(band), s.qualityBand());
            assertEquals(Optional.of(new CapacityFloor(1000)), s.capacityFloor());
        }
    }

    /** A setting of a part the stream does not have is refused, and nothing is sent that would make the peer close the session. */
    @Test void aSettingOfAMissingPartIsRefusedAndTheSessionLives() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var out = pair.client().openSendStream();
            out.write(new byte[] {1});
            var in = pair.server().acceptReceiveStream(Duration.ofSeconds(5));
            assertThrows(IllegalStateException.class, () -> ((ZmuxReceiveStream) out).setReceiveWindow(1L << 20, 0));
            assertThrows(IllegalStateException.class, () -> ((ZmuxSendStream) in).setSendBuffer(1 << 20));
            assertEquals(ZmuxConfig.defaults().sendBuffer(), ((ZmuxSendStream) in).sendBuffer(), "a refused call holds nothing");
            // Both sides keep working: a window update for a part that does not exist would end the session.
            out.write(new byte[] {2});
            out.finish();
            assertEquals(1, in.read(new byte[1], 0, 1));
            assertEquals(1, in.read(new byte[1], 0, 1));
            assertEquals(-1, in.read(new byte[1], 0, 1));
            assertFalse(pair.server().isClosed());
        }
    }

    /** The initial and the cap are one setting: a reader never sees two setters' halves mixed, and the end state is one setter's. */
    @Test void concurrentReceiveWindowSettersLeaveOneSettingBehind() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var st = pair.client().openStream();
            var unset = st.receiveWindow();
            var failure = new java.util.concurrent.atomic.AtomicReference<AssertionError>();
            var stop = new java.util.concurrent.atomic.AtomicBoolean();
            var reader = Thread.ofVirtual().start(() -> {
                while (!stop.get()) {
                    var w = st.receiveWindow();
                    // Every setter below sets max = 2 * initial; before the first one the setting is the configured one.
                    if (!w.equals(unset) && w.max() != 2 * w.initial()) { failure.compareAndSet(null, new AssertionError("mixed " + w)); return; }
                }
            });
            var writers = new Thread[4];
            for (int t = 0; t < writers.length; t++) {
                long base = (t + 1) * 8192L;
                writers[t] = Thread.ofVirtual().start(() -> {
                    for (int i = 0; i < 2000; i++) st.setReceiveWindow(base + i, 2 * (base + i));
                });
            }
            for (var w : writers) w.join();
            stop.set(true);
            reader.join();
            assertNull(failure.get());
            var end = st.receiveWindow();
            assertEquals(2 * end.initial(), end.max());
        }
    }
}
