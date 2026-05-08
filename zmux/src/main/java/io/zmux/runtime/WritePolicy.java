package io.zmux.runtime;

import io.zmux.SchedulerHint;
import io.zmux.Settings;
import java.util.concurrent.TimeUnit;

final class WritePolicy {
    static final int DEFAULT_WRITE_BURST_FRAMES = 16;
    static final int MILD_WRITE_BURST_FRAMES = 8;
    static final int STRONG_WRITE_BURST_FRAMES = 4;
    static final int SATURATED_WRITE_BURST_FRAMES = 2;
    static final long DEFAULT_FRAGMENT_TIME_BUDGET_NANOS = TimeUnit.MILLISECONDS.toNanos(200L);
    static final long MILD_FRAGMENT_TIME_BUDGET_NANOS = TimeUnit.MILLISECONDS.toNanos(150L);
    static final long STRONG_FRAGMENT_TIME_BUDGET_NANOS = TimeUnit.MILLISECONDS.toNanos(100L);
    static final long SATURATED_FRAGMENT_TIME_BUDGET_NANOS = TimeUnit.MILLISECONDS.toNanos(50L);
    private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1L);
    private static final int[] WRITE_BURST_LIMITS = {
            DEFAULT_WRITE_BURST_FRAMES,
            MILD_WRITE_BURST_FRAMES,
            STRONG_WRITE_BURST_FRAMES,
            SATURATED_WRITE_BURST_FRAMES
    };
    private static final long[] FRAGMENT_TIME_BUDGETS = {
            DEFAULT_FRAGMENT_TIME_BUDGET_NANOS,
            MILD_FRAGMENT_TIME_BUDGET_NANOS,
            STRONG_FRAGMENT_TIME_BUDGET_NANOS,
            SATURATED_FRAGMENT_TIME_BUDGET_NANOS
    };

    private WritePolicy() {
    }

    static int writeBurstLimit(long priority, SchedulerHint hint) {
        int band = priorityBand(priority);
        if (band == 0 && hint == SchedulerHint.LATENCY) {
            return MILD_WRITE_BURST_FRAMES;
        }
        return WRITE_BURST_LIMITS[band];
    }

    static long fragmentCap(long maxPayload, long prefixLen, long priority, SchedulerHint hint) {
        long effectiveMaxPayload = maxPayload > 0L ? maxPayload : Settings.defaults().maxFramePayload();
        long effectivePrefix = Math.max(0L, prefixLen);
        if (effectivePrefix >= effectiveMaxPayload) {
            return 0L;
        }

        long available = effectiveMaxPayload - effectivePrefix;
        if (priority >= 16L) {
            return scaledFragmentCap(available, 1L, 4L);
        }
        if (priority >= 4L) {
            return scaledFragmentCap(available, 1L, 2L);
        }
        if (priority >= 1L) {
            return scaledFragmentCap(available, 3L, 4L);
        }
        return hint == SchedulerHint.LATENCY
                ? scaledFragmentCap(available, 1L, 2L)
                : available;
    }

    static long rateLimitedFragmentCap(long baseCap, long estimatedSendRateBytesPerSecond, long priority, SchedulerHint hint) {
        if (baseCap <= 0L || estimatedSendRateBytesPerSecond <= 0L) {
            return baseCap;
        }
        long budgetNanos = fragmentTimeBudgetNanos(priority, hint);
        if (budgetNanos <= 0L) {
            return baseCap;
        }
        long rateCap = saturatingMulDivFloor(estimatedSendRateBytesPerSecond, budgetNanos, NANOS_PER_SECOND);
        if (rateCap <= 0L) {
            rateCap = 1L;
        }
        return Math.min(baseCap, rateCap);
    }

    static long fragmentTimeBudgetNanos(long priority, SchedulerHint hint) {
        int band = priorityBand(priority);
        if (band == 0 && hint == SchedulerHint.LATENCY) {
            return STRONG_FRAGMENT_TIME_BUDGET_NANOS;
        }
        return FRAGMENT_TIME_BUDGETS[band];
    }

    static long scaledFragmentCap(long max, long numerator, long denominator) {
        if (max <= 0L) {
            return 0L;
        }
        if (denominator == 0L) {
            return max;
        }
        long scaled = RuntimeFlow.saturatingMulDivFloor(max, Math.max(0L, numerator), denominator);
        if (scaled <= 0L) {
            return 1L;
        }
        return Math.min(max, scaled);
    }

    private static long saturatingMulDivFloor(long value, long multiplier, long divisor) {
        return RuntimeFlow.saturatingMulDivFloor(value, multiplier, divisor);
    }

    private static int priorityBand(long priority) {
        if (priority >= 16L) {
            return 3;
        }
        if (priority >= 4L) {
            return 2;
        }
        return priority >= 1L ? 1 : 0;
    }
}
