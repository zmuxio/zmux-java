package io.zmux;

import java.time.Duration;

final class StopSendingGracefulPolicy {
    static final Duration REPO_DEFAULT_DRAIN_WINDOW = Duration.ofMillis(100L);

    private StopSendingGracefulPolicy() {
    }

    static Decision evaluate(Input input) {
        long committedTail = committedTail(input.queuedDataBytes(), input.inflightQueued());
        long inflightTail = Math.max(0L, input.inflightQueued());
        long queuedOnlyTail = queuedOnlyTail(input.queuedDataBytes(), input.inflightQueued());
        long tailBudget = tailBudget(
                input.fragmentCap(),
                input.explicitTailCap(),
                input.sendRateEstimate(),
                drainWindow(input.drainWindow())
        );

        boolean attempt = false;
        if (!input.recvAbortive() && !input.needsLocalOpener()) {
            if (committedTail == 0L) {
                attempt = input.localOpened() && input.sendCommitted();
            } else if (inflightTail > 0L) {
                attempt = input.sendCommitted() && inflightTail <= tailBudget;
            } else {
                attempt = input.sendCommitted() && queuedOnlyTail <= tailBudget;
            }
        }

        return new Decision(attempt, tailBudget, committedTail, inflightTail, queuedOnlyTail);
    }

    static long committedTail(long queuedDataBytes, long inflightQueued) {
        return Math.max(Math.max(0L, queuedDataBytes), Math.max(0L, inflightQueued));
    }

    static long queuedOnlyTail(long queuedDataBytes, long inflightQueued) {
        long queued = Math.max(0L, queuedDataBytes);
        long inflight = Math.max(0L, inflightQueued);
        if (queued <= inflight) {
            return 0L;
        }
        return queued - inflight;
    }

    static long tailBudget(long fragmentCap, long explicitTailCap, long sendRateEstimate, Duration drainWindow) {
        if (explicitTailCap > 0L) {
            return explicitTailCap;
        }
        long budget = staticTailCap(fragmentCap);
        long rateBudget = gracefulRateBudget(sendRateEstimate, drainWindow);
        return Math.max(budget, rateBudget);
    }

    static long staticTailCap(long fragmentCap) {
        if (fragmentCap <= 0L) {
            return 0L;
        }
        long scaled = Math.max(1L, fragmentCap / 4L);
        return Math.min(512L, scaled);
    }

    static long gracefulRateBudget(long rateBytesPerSecond, Duration window) {
        if (rateBytesPerSecond <= 0L || window == null || window.isNegative() || window.isZero()) {
            return 0L;
        }
        long windowNanos;
        try {
            windowNanos = window.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
        if (windowNanos <= 0L) {
            return 0L;
        }
        long budget = RuntimeFlow.saturatingMulDivFloor(rateBytesPerSecond, windowNanos, 1_000_000_000L);
        return budget == 0L ? 1L : budget;
    }

    static Duration drainWindow(Duration override) {
        if (override != null && !override.isNegative() && !override.isZero()) {
            return override;
        }
        return REPO_DEFAULT_DRAIN_WINDOW;
    }

    static final class Input {
        private final boolean recvAbortive;
        private final boolean needsLocalOpener;
        private final boolean localOpened;
        private final boolean sendCommitted;
        private final long queuedDataBytes;
        private final long inflightQueued;
        private final long fragmentCap;
        private final long sendRateEstimate;
        private final long explicitTailCap;
        private final Duration drainWindow;

        Input(boolean recvAbortive,
              boolean needsLocalOpener,
              boolean localOpened,
              boolean sendCommitted,
              long queuedDataBytes,
              long inflightQueued,
              long fragmentCap,
              long sendRateEstimate,
              long explicitTailCap,
              Duration drainWindow) {
            this.recvAbortive = recvAbortive;
            this.needsLocalOpener = needsLocalOpener;
            this.localOpened = localOpened;
            this.sendCommitted = sendCommitted;
            this.queuedDataBytes = queuedDataBytes;
            this.inflightQueued = inflightQueued;
            this.fragmentCap = fragmentCap;
            this.sendRateEstimate = sendRateEstimate;
            this.explicitTailCap = explicitTailCap;
            this.drainWindow = drainWindow;
        }

        boolean recvAbortive() {
            return recvAbortive;
        }

        boolean needsLocalOpener() {
            return needsLocalOpener;
        }

        boolean localOpened() {
            return localOpened;
        }

        boolean sendCommitted() {
            return sendCommitted;
        }

        long queuedDataBytes() {
            return queuedDataBytes;
        }

        long inflightQueued() {
            return inflightQueued;
        }

        long fragmentCap() {
            return fragmentCap;
        }

        long sendRateEstimate() {
            return sendRateEstimate;
        }

        long explicitTailCap() {
            return explicitTailCap;
        }

        Duration drainWindow() {
            return drainWindow;
        }
    }

    static final class Decision {
        private final boolean attempt;
        private final long tailBudget;
        private final long committedTail;
        private final long inflightTail;
        private final long queuedOnlyTail;

        Decision(boolean attempt, long tailBudget, long committedTail, long inflightTail, long queuedOnlyTail) {
            this.attempt = attempt;
            this.tailBudget = tailBudget;
            this.committedTail = committedTail;
            this.inflightTail = inflightTail;
            this.queuedOnlyTail = queuedOnlyTail;
        }

        boolean attempt() {
            return attempt;
        }

        long tailBudget() {
            return tailBudget;
        }

        long committedTail() {
            return committedTail;
        }

        long inflightTail() {
            return inflightTail;
        }

        long queuedOnlyTail() {
            return queuedOnlyTail;
        }
    }
}
