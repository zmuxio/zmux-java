package io.zmux.internal;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

final class StopSendingGracefulPolicyTest {
    @Test
    void rejectsAbortiveOrUnopenedPaths() {
        assertFalse(StopSendingGracefulPolicy.evaluate(new StopSendingGracefulPolicy.Input(
                true, false, true, true, 0L, 0L, 0L, 0L, 0L, Duration.ZERO
        )).attempt(), "recv-abortive path should not attempt graceful finish");

        assertFalse(StopSendingGracefulPolicy.evaluate(new StopSendingGracefulPolicy.Input(
                false, true, true, true, 0L, 0L, 0L, 0L, 0L, Duration.ZERO
        )).attempt(), "local-opener-needed path should not attempt graceful finish");
    }

    @Test
    void allowsCommittedEmptyTail() {
        StopSendingGracefulPolicy.Decision decision = StopSendingGracefulPolicy.evaluate(
                new StopSendingGracefulPolicy.Input(false, false, true, true, 0L, 0L, 0L, 0L, 0L, Duration.ZERO)
        );

        assertTrue(decision.attempt(), "empty committed tail should allow graceful finish");
        assertEquals(0L, decision.committedTail(), "empty committed tail mismatch");
    }

    @Test
    void prefersInflightTailWithoutRateBudget() {
        StopSendingGracefulPolicy.Decision decision = StopSendingGracefulPolicy.evaluate(
                new StopSendingGracefulPolicy.Input(false, false, true, true, 1024L, 64L, 256L, 0L, 0L, Duration.ZERO)
        );

        assertTrue(decision.attempt(), "small unavoidable inflight tail should still gracefully finish");
        assertEquals(64L, decision.tailBudget(), "static fragment-derived tail budget mismatch");
    }

    @Test
    void usesRateBudgetForQueuedOnlyTail() {
        StopSendingGracefulPolicy.Decision decision = StopSendingGracefulPolicy.evaluate(
                new StopSendingGracefulPolicy.Input(false, false, true, true, 768L, 0L, 256L, 16L << 10, 0L, Duration.ofMillis(100))
        );

        assertTrue(decision.attempt(), "fast-link queued-only tail should gracefully finish");
        assertEquals(768L, decision.queuedOnlyTail(), "queued-only tail mismatch");
        assertTrue(decision.tailBudget() >= 768L, "rate-derived tail budget should cover queued-only tail");
    }

    @Test
    void rateBudgetUsesWideMultiplyDivide() {
        long budget = StopSendingGracefulPolicy.gracefulRateBudget(
                Long.MAX_VALUE,
                Duration.ofSeconds(2)
        );

        assertEquals(Long.MAX_VALUE, budget, "overflowing rate budget should saturate instead of under-clamping");
    }

    @Test
    void keepsUsingInflightTailEvenWhenQueuedTailIsLarge() {
        StopSendingGracefulPolicy.Decision decision = StopSendingGracefulPolicy.evaluate(
                new StopSendingGracefulPolicy.Input(false, false, true, true, 768L, 64L, 256L, 1024L, 0L, Duration.ofMillis(100))
        );

        assertTrue(decision.attempt(), "small unavoidable inflight tail should win over large suppressible queued tail");
        assertEquals(64L, decision.inflightTail(), "inflight tail mismatch");
        assertTrue(decision.queuedOnlyTail() > decision.tailBudget(), "queued-only tail should still exceed the budget in this scenario");
    }

    @Test
    void explicitTailCapWins() {
        StopSendingGracefulPolicy.Decision decision = StopSendingGracefulPolicy.evaluate(
                new StopSendingGracefulPolicy.Input(false, false, true, true, 384L, 0L, 256L, 16L << 10, 256L, Duration.ofMillis(100))
        );

        assertFalse(decision.attempt(), "explicit tail cap below committed tail should force reset");
        assertEquals(256L, decision.tailBudget(), "explicit tail cap should override computed budget");
    }

    @Test
    void explicitTailCapStillAllowsSmallInflightTail() {
        StopSendingGracefulPolicy.Decision decision = StopSendingGracefulPolicy.evaluate(
                new StopSendingGracefulPolicy.Input(false, false, true, true, 384L, 64L, 256L, 16L << 10, 256L, Duration.ofMillis(100))
        );

        assertTrue(decision.attempt(), "explicit tail cap should still allow a small unavoidable inflight tail");
        assertEquals(64L, decision.inflightTail(), "inflight tail mismatch");
    }

    @Test
    void drainWindowUsesOverrideOrRepositoryDefault() {
        assertEquals(
                StopSendingGracefulPolicy.REPO_DEFAULT_DRAIN_WINDOW,
                StopSendingGracefulPolicy.drainWindow(Duration.ZERO),
                "zero override should use the repository-default drain window"
        );
        assertEquals(
                Duration.ofMillis(250),
                StopSendingGracefulPolicy.drainWindow(Duration.ofMillis(250)),
                "explicit override should win"
        );
    }
}
