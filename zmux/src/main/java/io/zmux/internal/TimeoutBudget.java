package io.zmux.internal;

import java.time.Duration;

public final class TimeoutBudget {
    private static final TimeoutBudget UNBOUNDED = new TimeoutBudget(false, 0L);

    private final boolean bounded;
    private final long deadlineNanos;

    private TimeoutBudget(boolean bounded, long deadlineNanos) {
        this.bounded = bounded;
        this.deadlineNanos = deadlineNanos;
    }

    public static TimeoutBudget unbounded() {
        return UNBOUNDED;
    }

    public static TimeoutBudget fromTimeout(Duration timeout) {
        if (timeout == null) {
            return UNBOUNDED;
        }
        long waitNanos = positiveNanos(timeout);
        long nowNanos = System.nanoTime();
        long deadlineNanos = waitNanos <= 0L ? nowNanos : saturatingAdd(nowNanos, waitNanos);
        return new TimeoutBudget(true, deadlineNanos);
    }

    private static long positiveNanos(Duration duration) {
        if (duration == null || duration.isNegative() || duration.isZero()) {
            return 0L;
        }
        try {
            return duration.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    public static long remainingNanosUntil(long deadlineNanos) {
        return remainingNanosUntil(deadlineNanos, System.nanoTime());
    }

    public static long remainingNanosUntil(long deadlineNanos, long nowNanos) {
        if (deadlineNanos == 0L) {
            return 0L;
        }
        if (deadlineNanos == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        if (nowNanos < 0L && deadlineNanos > Long.MAX_VALUE + nowNanos) {
            return Long.MAX_VALUE;
        }
        return deadlineNanos - nowNanos;
    }

    public static long positiveRemainingNanosUntil(long deadlineNanos, long nowNanos) {
        if (deadlineNanos <= 0L) {
            return 0L;
        }
        long remainingNanos = remainingNanosUntil(deadlineNanos, nowNanos);
        return remainingNanos <= 0L ? 1L : remainingNanos;
    }

    private static long saturatingAdd(long left, long right) {
        if (right <= 0L) {
            return left;
        }
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    public boolean bounded() {
        return bounded;
    }

    public long deadlineNanos() {
        return deadlineNanos;
    }

    public long remainingNanos() {
        if (!bounded) {
            return Long.MAX_VALUE;
        }
        return remainingNanosUntil(deadlineNanos);
    }

    public boolean expired() {
        return bounded && remainingNanos() <= 0L;
    }
}
