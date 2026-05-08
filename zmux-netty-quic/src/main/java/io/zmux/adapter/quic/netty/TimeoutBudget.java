package io.zmux.adapter.quic.netty;

import java.time.Duration;

final class TimeoutBudget {
    private static final TimeoutBudget UNBOUNDED = new TimeoutBudget(false, 0L);

    private final boolean bounded;
    private final long deadlineNanos;

    private TimeoutBudget(boolean bounded, long deadlineNanos) {
        this.bounded = bounded;
        this.deadlineNanos = deadlineNanos;
    }

    static TimeoutBudget unbounded() {
        return UNBOUNDED;
    }

    static TimeoutBudget fromTimeout(Duration timeout) {
        if (timeout == null) {
            return UNBOUNDED;
        }
        long waitNanos = NettyQuicSupport.durationToPositiveNanos(timeout);
        long nowNanos = System.nanoTime();
        long deadlineNanos = waitNanos <= 0L ? nowNanos : NettyQuicSupport.saturatingAdd(nowNanos, waitNanos);
        return new TimeoutBudget(true, deadlineNanos);
    }

    static long remainingNanosUntil(long deadlineNanos) {
        return remainingNanosUntil(deadlineNanos, System.nanoTime());
    }

    static long remainingNanosUntil(long deadlineNanos, long nowNanos) {
        if (deadlineNanos == 0L) {
            return 0L;
        }
        if (deadlineNanos == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        return saturatingSubtract(deadlineNanos, nowNanos);
    }

    boolean bounded() {
        return bounded;
    }

    long remainingNanos() {
        if (!bounded) {
            return Long.MAX_VALUE;
        }
        return remainingNanosUntil(deadlineNanos);
    }

    long deadlineNanos() {
        return deadlineNanos;
    }

    boolean expired() {
        return bounded && remainingNanos() <= 0L;
    }

    private static long saturatingSubtract(long left, long right) {
        long result = left - right;
        if (((left ^ right) & (left ^ result)) >= 0L) {
            return result;
        }
        return left < right ? Long.MIN_VALUE : Long.MAX_VALUE;
    }
}
