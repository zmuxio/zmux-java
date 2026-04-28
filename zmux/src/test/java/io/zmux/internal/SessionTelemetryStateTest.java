package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

final class SessionTelemetryStateTest {
    private static long invokeInitKeepaliveJitterState(long seed) throws Exception {
        Method method = SessionTelemetryState.class.getDeclaredMethod("initKeepaliveJitterState", long.class);
        method.setAccessible(true);
        return ((Long) method.invoke(null, seed)).longValue();
    }

    private static void setLongField(Object target, String name, long value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.setLong(target, value);
    }

    private static long getLongField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getLong(target);
    }

    @Test
    void initKeepaliveJitterStateReturnsNonZeroState() throws Exception {
        assertNotEquals(0L, invokeInitKeepaliveJitterState(0L), "default jitter seed must allocate a non-zero state");
        assertEquals(123L, invokeInitKeepaliveJitterState(123L), "explicit jitter seed should be preserved");
    }

    @Test
    void initKeepaliveJitterStateAllocatesDistinctDefaultSeeds() throws Exception {
        long first = invokeInitKeepaliveJitterState(0L);
        long second = invokeInitKeepaliveJitterState(0L);

        assertNotEquals(0L, first, "first default jitter seed must be non-zero");
        assertNotEquals(0L, second, "second default jitter seed must be non-zero");
        assertNotEquals(first, second, "default jitter seeds should be distinct per telemetry instance");
    }

    @Test
    void nextKeepaliveJitterStaysWithinConfiguredWindow() throws Exception {
        TestTelemetryOwner owner = new TestTelemetryOwner();
        SessionTelemetryState telemetry = new SessionTelemetryState(
                owner,
                ZmuxConfig.builder().build(),
                System.nanoTime(),
                Instant.now()
        );
        owner.telemetry = telemetry;
        long baseIntervalNanos = Duration.ofMillis(80L).toNanos();
        long windowNanos = baseIntervalNanos / 8L;
        setLongField(telemetry, "keepaliveJitterState", 1L);

        for (int i = 0; i < 16; i++) {
            long jitterNanos = telemetry.nextKeepaliveJitterNanos(baseIntervalNanos);
            assertTrue(
                    jitterNanos >= 0L && jitterNanos <= windowNanos,
                    "keepalive jitter must stay within the configured lead window"
            );
        }
    }

    @Test
    void nextKeepaliveJitterAdvancesState() throws Exception {
        TestTelemetryOwner owner = new TestTelemetryOwner();
        SessionTelemetryState telemetry = new SessionTelemetryState(
                owner,
                ZmuxConfig.builder().build(),
                System.nanoTime(),
                Instant.now()
        );
        owner.telemetry = telemetry;
        setLongField(telemetry, "keepaliveJitterState", 1L);

        long first = telemetry.nextKeepaliveJitterNanos(Duration.ofSeconds(64L).toNanos());
        long second = telemetry.nextKeepaliveJitterNanos(Duration.ofSeconds(64L).toNanos());

        assertNotEquals(first, second, "consecutive keepalive jitter samples should advance deterministic state");
    }

    @Test
    void resetReadIdlePingDueAppliesBoundedLeadJitter() throws Exception {
        TestTelemetryOwner owner = new TestTelemetryOwner();
        SessionTelemetryState telemetry = new SessionTelemetryState(
                owner,
                ZmuxConfig.builder()
                        .keepaliveInterval(Duration.ofMillis(80L))
                        .build(),
                System.nanoTime(),
                Instant.now()
        );
        owner.telemetry = telemetry;
        long baseNanos = TimeUnit.SECONDS.toNanos(456L);
        setLongField(telemetry, "keepaliveJitterState", 1L);

        telemetry.resetReadIdlePingDueLocked(baseNanos);
        long dueNanos = getLongField(telemetry, "readIdlePingDueAtNanos");

        assertTrue(
                dueNanos >= baseNanos + Duration.ofMillis(70L).toNanos()
                        && dueNanos <= baseNanos + Duration.ofMillis(80L).toNanos(),
                "read-idle keepalive deadline should stay within the bounded lead-jitter window"
        );
    }

    @Test
    void resetReadIdlePingDueUsesFreshJitterPerDeadline() throws Exception {
        TestTelemetryOwner owner = new TestTelemetryOwner();
        SessionTelemetryState telemetry = new SessionTelemetryState(
                owner,
                ZmuxConfig.builder()
                        .keepaliveInterval(Duration.ofMillis(64L))
                        .build(),
                System.nanoTime(),
                Instant.now()
        );
        owner.telemetry = telemetry;
        long baseNanos = TimeUnit.SECONDS.toNanos(789L);
        setLongField(telemetry, "keepaliveJitterState", 1L);

        telemetry.resetReadIdlePingDueLocked(baseNanos);
        long first = getLongField(telemetry, "readIdlePingDueAtNanos");
        telemetry.resetReadIdlePingDueLocked(baseNanos);
        long second = getLongField(telemetry, "readIdlePingDueAtNanos");

        assertNotEquals(first, second, "read-idle keepalive deadlines should resample jitter on each reset");
    }

    @Test
    void resetReadIdlePingDueDesynchronizesDistinctSessions() throws Exception {
        long baseNanos = TimeUnit.SECONDS.toNanos(900L);
        SessionTelemetryState first = new SessionTelemetryState(
                new TestTelemetryOwner(),
                ZmuxConfig.builder().keepaliveInterval(Duration.ofSeconds(1L)).build(),
                System.nanoTime(),
                Instant.now()
        );
        SessionTelemetryState second = new SessionTelemetryState(
                new TestTelemetryOwner(),
                ZmuxConfig.builder().keepaliveInterval(Duration.ofSeconds(1L)).build(),
                System.nanoTime(),
                Instant.now()
        );
        setLongField(first, "keepaliveJitterState", 1L);
        setLongField(second, "keepaliveJitterState", 2L);

        boolean stayedAligned = true;
        for (int i = 0; i < 4; i++) {
            first.resetReadIdlePingDueLocked(baseNanos);
            long firstDue = getLongField(first, "readIdlePingDueAtNanos");
            second.resetReadIdlePingDueLocked(baseNanos);
            long secondDue = getLongField(second, "readIdlePingDueAtNanos");
            if (firstDue != secondDue) {
                stayedAligned = false;
                break;
            }
        }

        assertFalse(stayedAligned, "distinct telemetry sessions should not keep identical keepalive deadlines across repeated resets");
    }

    @Test
    void pingQueueFailureClearsActivePing() {
        TestTelemetryOwner owner = new TestTelemetryOwner();
        owner.enqueueError = new IOException("queue failed");
        SessionTelemetryState telemetry = new SessionTelemetryState(
                owner,
                ZmuxConfig.builder().build(),
                System.nanoTime(),
                Instant.now()
        );
        owner.telemetry = telemetry;

        assertThrows(IOException.class, () -> telemetry.ping(new byte[]{1, 2, 3}, Duration.ofSeconds(1)));

        assertFalse(telemetry.hasActivePingLocked(), "failed queue must not leave an active ping");
        assertEquals(0L, telemetry.outstandingPingBytesLocked(), "failed queue must release retained ping bytes");
        assertEquals(1, owner.telemetryNotifications, "ping slot waiters should be woken after cleanup");
        assertEquals(1, owner.writerNotifications, "keepalive writer wake should be refreshed after cleanup");
    }

    @Test
    void pingResponseTimeoutClearsOwnActivePingSlot() {
        TestTelemetryOwner owner = new TestTelemetryOwner();
        SessionTelemetryState telemetry = new SessionTelemetryState(
                owner,
                ZmuxConfig.builder().build(),
                System.nanoTime(),
                Instant.now()
        );
        owner.telemetry = telemetry;

        assertThrows(
                PingTimeoutException.class,
                () -> telemetry.ping(new byte[]{1, 2, 3}, Duration.ofMillis(20))
        );

        assertFalse(telemetry.hasActivePingLocked(), "timed-out ping must release its active slot");
        assertEquals(0L, telemetry.outstandingPingBytesLocked(), "timed-out ping must release retained ping bytes");
        assertEquals(1, owner.telemetryNotifications, "ping slot waiters should be woken after timeout cleanup");
    }

    @Test
    void pingResponseTimeoutRefreshesKeepaliveIdleSchedules() throws Exception {
        TestTelemetryOwner owner = new TestTelemetryOwner();
        SessionTelemetryState telemetry = new SessionTelemetryState(
                owner,
                ZmuxConfig.builder()
                        .keepaliveInterval(Duration.ofSeconds(10))
                        .build(),
                System.nanoTime(),
                Instant.now()
        );
        owner.telemetry = telemetry;
        setLongField(telemetry, "readIdlePingDueAtNanos", 1L);
        setLongField(telemetry, "writeIdlePingDueAtNanos", 2L);

        assertThrows(
                PingTimeoutException.class,
                () -> telemetry.ping(new byte[]{1, 2, 3}, Duration.ofMillis(20))
        );

        assertEquals(0L, telemetry.outstandingPingBytesLocked(), "timed-out ping must release retained ping bytes");
        assertFalse(getLongField(telemetry, "readIdlePingDueAtNanos") <= 1L, "clearing an active ping should refresh the read-idle keepalive deadline");
        assertFalse(getLongField(telemetry, "writeIdlePingDueAtNanos") <= 2L, "clearing an active ping should refresh the write-idle keepalive deadline");
    }

    @Test
    void adaptiveRttTimeoutSaturatesBeforeApplyingConfiguredCap() {
        long capped = SessionRuntime.adaptiveRttTimeout(
                Long.MAX_VALUE / 2L + 1L,
                Duration.ofMillis(500).toNanos(),
                Duration.ofSeconds(5).toNanos(),
                4,
                Duration.ofMillis(50).toNanos()
        );

        assertEquals(Duration.ofSeconds(5).toNanos(), capped);
    }

    @Test
    void adaptiveRttTimeoutRequiresPositiveBase() {
        long capped = SessionRuntime.adaptiveRttTimeout(
                Long.MAX_VALUE / 2L + 1L,
                0L,
                Duration.ofSeconds(5).toNanos(),
                4,
                Duration.ofMillis(50).toNanos()
        );

        assertEquals(0L, capped);
    }

    @Test
    void adaptiveRttFloorSaturatesIndependentlyFromTimeoutBase() {
        long floor = SessionRuntime.adaptiveRttFloor(
                Long.MAX_VALUE / 2L + 1L,
                4,
                Duration.ofMillis(50).toNanos()
        );

        assertEquals(Long.MAX_VALUE, floor);
    }

    @Test
    void defaultKeepaliveTimeoutSaturatesIntervalBeforeApplyingMaxCap() {
        TestTelemetryOwner owner = new TestTelemetryOwner();
        SessionTelemetryState telemetry = new SessionTelemetryState(
                owner,
                ZmuxConfig.builder()
                        .keepaliveInterval(Duration.ofNanos(Long.MAX_VALUE))
                        .keepaliveTimeout(Duration.ZERO)
                        .build(),
                System.nanoTime(),
                Instant.now()
        );
        owner.telemetry = telemetry;

        assertEquals(Duration.ofSeconds(60).toNanos(), telemetry.effectiveKeepaliveTimeoutNanosLocked());
    }

    @Test
    void keepaliveWakeUsesEarliestReadWriteOrMaxPingDeadline() throws Exception {
        TestTelemetryOwner owner = new TestTelemetryOwner();
        SessionTelemetryState telemetry = new SessionTelemetryState(
                owner,
                ZmuxConfig.builder()
                        .keepaliveInterval(Duration.ofSeconds(10))
                        .keepaliveMaxPingInterval(Duration.ofSeconds(30))
                        .build(),
                System.nanoTime(),
                Instant.now()
        );
        owner.telemetry = telemetry;
        long nowNanos = TimeUnit.SECONDS.toNanos(100L);

        setLongField(telemetry, "readIdlePingDueAtNanos", nowNanos + 10L);
        setLongField(telemetry, "writeIdlePingDueAtNanos", nowNanos + 1_000L);
        setLongField(telemetry, "maxPingDueAtNanos", nowNanos + 10_000L);

        assertEquals(
                10L,
                telemetry.nextKeepaliveWakeNanosLocked(nowNanos),
                "keepalive scheduling should probe when either read or write side is idle, not wait for both"
        );
    }

    @Test
    void missingIdleSchedulesAreRecoveredFromLastActivityTimes() throws Exception {
        TestTelemetryOwner owner = new TestTelemetryOwner();
        SessionTelemetryState telemetry = new SessionTelemetryState(
                owner,
                ZmuxConfig.builder()
                        .keepaliveInterval(Duration.ofSeconds(10))
                        .keepaliveMaxPingInterval(Duration.ofSeconds(30))
                        .build(),
                System.nanoTime(),
                Instant.now()
        );
        owner.telemetry = telemetry;
        long lastInboundNanos = TimeUnit.SECONDS.toNanos(100L);
        long lastWriteNanos = TimeUnit.SECONDS.toNanos(200L);
        long nowNanos = TimeUnit.SECONDS.toNanos(500L);
        setLongField(telemetry, "lastInboundFrameAtNanos", lastInboundNanos);
        setLongField(telemetry, "lastTransportWriteAtNanos", lastWriteNanos);

        telemetry.ensureKeepaliveSchedulesLocked(nowNanos);

        assertTrue(
                getLongField(telemetry, "readIdlePingDueAtNanos") < nowNanos,
                "read-idle recovery should preserve the last inbound activity baseline"
        );
        assertTrue(
                getLongField(telemetry, "writeIdlePingDueAtNanos") < nowNanos,
                "write-idle recovery should preserve the last transport-write baseline"
        );
        assertTrue(
                getLongField(telemetry, "maxPingDueAtNanos") > nowNanos,
                "max-ping recovery should still start from the current scheduler time"
        );
    }

    @Test
    void queuedOutboundControlDoesNotRefreshWriteIdleDeadline() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder()
                        .keepaliveInterval(Duration.ofSeconds(10L))
                        .build(),
                0L,
                Settings.defaults()
        );
        long dueNanos = TimeUnit.SECONDS.toNanos(123L);
        SessionRuntimeTestSupport.setLongField(runtime, "writeIdlePingDueAtNanos", dueNanos);

        runtime.enqueueControlLocked(new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[8]));

        assertEquals(
                dueNanos,
                SessionRuntimeTestSupport.getLongField(runtime, "writeIdlePingDueAtNanos"),
                "queued outbound control is not transport progress and must not refresh write-idle keepalive"
        );
    }

    @Test
    void completedTransportWriteRefreshesWriteIdleDeadline() throws Exception {
        TestTelemetryOwner owner = new TestTelemetryOwner();
        SessionTelemetryState telemetry = new SessionTelemetryState(
                owner,
                ZmuxConfig.builder()
                        .keepaliveInterval(Duration.ofMillis(80L))
                        .build(),
                System.nanoTime(),
                Instant.now()
        );
        owner.telemetry = telemetry;
        long writeCompletedAtNanos = TimeUnit.SECONDS.toNanos(456L);
        setLongField(telemetry, "keepaliveJitterState", 1L);
        setLongField(telemetry, "writeIdlePingDueAtNanos", 1L);

        telemetry.noteTransportWriteCompletedLocked(writeCompletedAtNanos);

        long dueNanos = getLongField(telemetry, "writeIdlePingDueAtNanos");
        assertTrue(
                dueNanos >= writeCompletedAtNanos + Duration.ofMillis(70L).toNanos()
                        && dueNanos <= writeCompletedAtNanos + Duration.ofMillis(80L).toNanos(),
                "completed transport writes should refresh write-idle keepalive from the actual write time"
        );
        assertEquals(
                writeCompletedAtNanos,
                getLongField(telemetry, "lastTransportWriteAtNanos"),
                "transport progress should surface the completed write timestamp"
        );
    }

    private static final class TestTelemetryOwner implements SessionTelemetryState.Owner {
        private final Object lock = new Object();
        private SessionTelemetryState telemetry;
        private IOException enqueueError;
        private int telemetryNotifications;
        private int writerNotifications;

        @Override
        public Object lock() {
            return this.lock;
        }

        @Override
        public SessionState state() {
            return SessionState.READY;
        }

        @Override
        public boolean closeFrameQueued() {
            return false;
        }

        @Override
        public IOException sessionErrorLocked() {
            return new IOException("session failed");
        }

        @Override
        public byte[] buildPingPayloadLocked(byte[] payload) {
            return payload == null ? new byte[0] : Arrays.copyOf(payload, payload.length);
        }

        @Override
        public void enqueuePingLocked(byte[] payload) throws IOException {
            if (this.enqueueError != null) {
                throw this.enqueueError;
            }
        }

        @Override
        public void notifyTelemetryWaitersLocked() {
            this.telemetryNotifications++;
        }

        @Override
        public void notifyWriterWaitersLocked() {
            this.writerNotifications++;
        }

        @Override
        public void notifyStreamWriteWaitersLocked() {
        }

        @Override
        public long trackedSessionMemoryLocked() {
            return this.telemetry == null ? 0L : this.telemetry.outstandingPingBytesLocked();
        }

        @Override
        public boolean sessionMemoryWakeNeededLocked(long previousTracked) {
            return false;
        }

        @Override
        public void waitOnLock() {
            throw new AssertionError("ping queue failure should not wait for a ping slot");
        }

        @Override
        public void waitOnLockNanos(long waitNanos) {
            throw new AssertionError("ping queue failure should not wait for a ping slot");
        }

        @Override
        public boolean allowLocalNonCloseControlLocked() {
            return true;
        }
    }
}
