package io.zmux.internal;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DeadlineBudgetTest {
    @Test
    void unboundedTimeoutBudgetReportsSaturatedRemainingTime() {
        assertEquals(Long.MAX_VALUE, TimeoutBudget.unbounded().remainingNanos());
    }

    @Test
    void hugeTimeoutBudgetDoesNotExpireImmediately() {
        TimeoutBudget budget = TimeoutBudget.fromTimeout(Duration.ofNanos(Long.MAX_VALUE));

        assertTrue(budget.remainingNanos() > 0L);
    }

    @Test
    void streamDeadlineRemainingSaturatesAtLongMax() {
        assertEquals(Long.MAX_VALUE, StreamRuntime.remainingDeadlineNanosLocked(Long.MAX_VALUE));
    }

    @Test
    void monotonicDeadlineMathSaturatesOverflowingSubtraction() {
        assertEquals(Long.MAX_VALUE, TimeoutBudget.remainingNanosUntil(Long.MAX_VALUE, -1L));
        assertEquals(Long.MAX_VALUE, TimeoutBudget.remainingNanosUntil(Long.MAX_VALUE - 1L, -2L));
        assertEquals(Long.MIN_VALUE, TimeoutBudget.remainingNanosUntil(Long.MIN_VALUE, 1L));
        assertEquals(Long.MIN_VALUE, TimeoutBudget.remainingNanosUntil(Long.MIN_VALUE + 1L, 2L));
        assertEquals(5L, TimeoutBudget.remainingNanosUntil(-5L, -10L));
        assertEquals(5L, TimeoutBudget.positiveRemainingNanosUntil(-5L, -10L));
        assertEquals(-1L, TimeoutBudget.remainingNanosUntil(9L, 10L));
        assertEquals(1L, TimeoutBudget.positiveRemainingNanosUntil(Long.MIN_VALUE, 1L));
        assertEquals(1L, TimeoutBudget.positiveRemainingNanosUntil(9L, 10L));
        assertEquals(0L, TimeoutBudget.positiveRemainingNanosUntil(0L, 10L));
    }

    @Test
    void writerWakeDeadlineRemainingSaturatesAtLongMax() {
        assertEquals(Long.MAX_VALUE, SessionWriterCoordinator.remainingDeadlineNanos(Long.MAX_VALUE, -1L));
        assertEquals(Long.MAX_VALUE, SessionWriterCoordinator.remainingDeadlineNanos(Long.MAX_VALUE - 1L, -2L));
    }
}
