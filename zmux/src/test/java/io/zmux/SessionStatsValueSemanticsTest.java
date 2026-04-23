package io.zmux;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class SessionStatsValueSemanticsTest {
    @Test
    void emptyStatsCompareByValue() {
        SessionStats left = SessionStats.empty(SessionState.READY);
        SessionStats right = SessionStats.empty(SessionState.READY);

        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());
    }

    @Test
    void nestedStatsCompareByValue() {
        assertEquals(
                new SessionStats.QueueStats(1, 2, 3, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
                new SessionStats.QueueStats(1, 2, 3, 4L, 5L, 6L, 7L, 8L, 9L, 10L)
        );
        assertEquals(
                new SessionStats.ProvisionalStats(1, 2, 3, 4, 5L, true, false, true, false, 6L, 7L),
                new SessionStats.ProvisionalStats(1, 2, 3, 4, 5L, true, false, true, false, 6L, 7L)
        );
        assertEquals(
                new SessionStats.HiddenStateStats(1, 2, 3, true, false, 4, 5, 6L, 7L, 8L),
                new SessionStats.HiddenStateStats(1, 2, 3, true, false, 4, 5, 6L, 7L, 8L)
        );
        assertEquals(
                new SessionStats.DiagnosticStats(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18),
                new SessionStats.DiagnosticStats(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18)
        );
        assertEquals(
                new SessionStats.RetainedBucketStats(1L, 2L),
                new SessionStats.RetainedBucketStats(1L, 2L)
        );
        assertEquals(
                new SessionStats.RetainedStateBreakdownStats(
                        new SessionStats.RetainedBucketStats(1L, 2L),
                        new SessionStats.RetainedBucketStats(3L, 4L),
                        new SessionStats.RetainedBucketStats(5L, 6L),
                        new SessionStats.RetainedBucketStats(7L, 8L),
                        new SessionStats.RetainedBucketStats(9L, 10L)
                ),
                new SessionStats.RetainedStateBreakdownStats(
                        new SessionStats.RetainedBucketStats(1L, 2L),
                        new SessionStats.RetainedBucketStats(3L, 4L),
                        new SessionStats.RetainedBucketStats(5L, 6L),
                        new SessionStats.RetainedBucketStats(7L, 8L),
                        new SessionStats.RetainedBucketStats(9L, 10L)
                )
        );
        assertEquals(
                new SessionStats.PressureStats(
                        1L,
                        2L,
                        SessionStats.RetainedStateBreakdownStats.empty(),
                        3L,
                        4L,
                        true,
                        5L,
                        6L,
                        7L,
                        8L,
                        9L,
                        10L
                ),
                new SessionStats.PressureStats(
                        1L,
                        2L,
                        SessionStats.RetainedStateBreakdownStats.empty(),
                        3L,
                        4L,
                        true,
                        5L,
                        6L,
                        7L,
                        8L,
                        9L,
                        10L
                )
        );
    }
}
