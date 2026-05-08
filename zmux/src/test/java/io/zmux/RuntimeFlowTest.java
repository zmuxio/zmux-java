package io.zmux;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class RuntimeFlowTest {
    @Test
    void queueWouldBlockUsesSaturatingAddition() {
        assertTrue(RuntimeFlow.queueWouldBlock(
                false,
                Long.MAX_VALUE - 1L,
                0L,
                8L,
                Long.MAX_VALUE - 1L,
                Long.MAX_VALUE
        ));
        assertTrue(RuntimeFlow.queueWouldBlock(
                false,
                0L,
                Long.MAX_VALUE - 1L,
                8L,
                Long.MAX_VALUE,
                Long.MAX_VALUE - 1L
        ));
    }

    @Test
    void queueWouldBlockPreservesZeroRequestAndMemoryRules() {
        assertFalse(RuntimeFlow.queueWouldBlock(false, Long.MAX_VALUE, Long.MAX_VALUE, 0L, 1L, 1L));
        assertTrue(RuntimeFlow.queueWouldBlock(true, 0L, 0L, 0L, 1L, 1L));
    }

    @Test
    void memoryWakeRequiresReleasedMemoryBelowThreshold() {
        assertTrue(RuntimeFlow.memoryWakeNeeded(8L, 7L, 8L));
        assertTrue(RuntimeFlow.memoryWakeNeeded(7L, 6L, 8L));
        assertFalse(RuntimeFlow.memoryWakeNeeded(9L, 8L, 8L));
        assertFalse(RuntimeFlow.memoryWakeNeeded(8L, 8L, 8L));
    }

    @Test
    void gainedCreditRequiresTransitionFromUnavailableToAvailable() {
        assertTrue(RuntimeFlow.gainedCredit(0L, 1L));
        assertTrue(RuntimeFlow.gainedCredit(-1L, 1L));
        assertFalse(RuntimeFlow.gainedCredit(1L, 2L));
        assertFalse(RuntimeFlow.gainedCredit(0L, 0L));
    }

    @Test
    void elapsedNanosPreservesNanoTimeWrapAndClampsBackwardsClock() {
        assertEquals(21L, RuntimeFlow.elapsedNanos(Long.MIN_VALUE + 10L, Long.MAX_VALUE - 10L));
        assertEquals(Long.MAX_VALUE, RuntimeFlow.elapsedNanos(Long.MAX_VALUE, Long.MIN_VALUE + 1L));
        assertEquals(0L, RuntimeFlow.elapsedNanos(100L, 200L));
        assertEquals(1L, RuntimeFlow.positiveElapsedNanos(7L, 7L));
        assertTrue(RuntimeFlow.elapsedExceeds(21L, 10L, 10L));
        assertFalse(RuntimeFlow.elapsedExceeds(20L, 10L, 10L));
    }

    @Test
    void repoDefaultCapsMatchRuntimePolicy() {
        assertEquals(256L * 1024L, RuntimeFlow.repoDefaultPerStreamDataHighWatermark(1L));
        assertEquals(4L * 1024L * 1024L, RuntimeFlow.repoDefaultSessionDataHighWatermark(1L));
        assertEquals(64L * 1024L, RuntimeFlow.repoDefaultUrgentLaneCap(1L));
        assertEquals(4L * 1024L * 1024L, RuntimeFlow.visibleAcceptBacklogBytesHardCap(1L));
        assertEquals(64L * 1024L, RuntimeFlow.aggregateLateDataCap(1L));
        assertEquals(1024L, RuntimeFlow.lateDataPerStreamCap(0L, 1L));
    }

    @Test
    void receiveWindowExceededUsesRemainingCredit() {
        assertFalse(RuntimeFlow.receiveWindowExceeded(8L, 10L, 2L));
        assertTrue(RuntimeFlow.receiveWindowExceeded(8L, 10L, 3L));
        assertTrue(RuntimeFlow.receiveWindowExceeded(Long.MAX_VALUE - 1L, Long.MAX_VALUE, 2L));
    }
}
