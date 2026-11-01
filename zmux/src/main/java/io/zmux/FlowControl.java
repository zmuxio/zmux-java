package io.zmux;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Receive windows, send buffering and the memory budget.
 *
 * @param initialStreamWindow the initial stream receive window, at least 4 KiB
 * @param maxStreamWindow 0 auto-tunes within the memory budget (a cap of 0 would stop every
 *     stream); a value is a fixed cap
 * @param initialConnectionWindow the initial connection receive window, at least 16 KiB
 * @param maxConnectionWindow 0 auto-tunes within the memory budget (a cap of 0 would stop the
 *     connection); a value is a fixed cap
 * @param sendBuffer the initial written-but-unreleased budget, at least 1; multipath grows it
 *     up to maxSendBuffer. Each direction has a 64 KiB floor, subject to its explicit stream limit
 * @param maxSendBuffer the multipath growth maximum; 0 (no growth room at all, below every
 *     sendBuffer) grows above the 64 MiB floor within the memory budget
 * @param memoryBudget the session's own memory budget, default 1 GiB; 0 lets nothing grow beyond the floors;
 *     empty ({@link #withoutMemoryBudget()}) is unlimited. A listener's budget replaces it
 * @param latencyBudget the window auto-tuner latency budget
 */
public record FlowControl(long initialStreamWindow, long maxStreamWindow, long initialConnectionWindow,
                          long maxConnectionWindow, long sendBuffer, long maxSendBuffer, OptionalLong memoryBudget,
                          Duration latencyBudget) {
    private static final FlowControl DEFAULTS = new FlowControl(256 << 10, 0, 1 << 20, 0, 4L << 20, 0, OptionalLong.of(1L << 30),
            Duration.ofNanos(io.zmux.internal.core.CoreConfig.DEFAULT_LATENCY_BUDGET));

    public FlowControl {
        if (initialStreamWindow < 4096) throw new IllegalArgumentException("initialStreamWindow below 4096");
        if (maxStreamWindow < 0) throw new IllegalArgumentException("negative maxStreamWindow");
        if (initialConnectionWindow < 16384) throw new IllegalArgumentException("initialConnectionWindow below 16384");
        if (maxConnectionWindow < 0) throw new IllegalArgumentException("negative maxConnectionWindow");
        if (sendBuffer < 1) throw new IllegalArgumentException("sendBuffer below 1");
        if (maxSendBuffer < 0) throw new IllegalArgumentException("negative maxSendBuffer");
        if (Objects.requireNonNull(memoryBudget, "memoryBudget").orElse(0) < 0) throw new IllegalArgumentException("negative memoryBudget");
        Timeouts.nonNegative(latencyBudget, "latencyBudget");
    }

    public static FlowControl defaults() { return DEFAULTS; }

    public FlowControl withInitialStreamWindow(long n) { return new FlowControl(n, maxStreamWindow, initialConnectionWindow, maxConnectionWindow, sendBuffer, maxSendBuffer, memoryBudget, latencyBudget); }
    public FlowControl withMaxStreamWindow(long n) { return new FlowControl(initialStreamWindow, n, initialConnectionWindow, maxConnectionWindow, sendBuffer, maxSendBuffer, memoryBudget, latencyBudget); }
    public FlowControl withInitialConnectionWindow(long n) { return new FlowControl(initialStreamWindow, maxStreamWindow, n, maxConnectionWindow, sendBuffer, maxSendBuffer, memoryBudget, latencyBudget); }
    public FlowControl withMaxConnectionWindow(long n) { return new FlowControl(initialStreamWindow, maxStreamWindow, initialConnectionWindow, n, sendBuffer, maxSendBuffer, memoryBudget, latencyBudget); }
    public FlowControl withSendBuffer(long n) { return new FlowControl(initialStreamWindow, maxStreamWindow, initialConnectionWindow, maxConnectionWindow, n, maxSendBuffer, memoryBudget, latencyBudget); }
    public FlowControl withMaxSendBuffer(long n) { return new FlowControl(initialStreamWindow, maxStreamWindow, initialConnectionWindow, maxConnectionWindow, sendBuffer, n, memoryBudget, latencyBudget); }
    public FlowControl withMemoryBudget(long n) { return new FlowControl(initialStreamWindow, maxStreamWindow, initialConnectionWindow, maxConnectionWindow, sendBuffer, maxSendBuffer, OptionalLong.of(n), latencyBudget); }
    /** An unlimited memory budget. */
    public FlowControl withoutMemoryBudget() { return new FlowControl(initialStreamWindow, maxStreamWindow, initialConnectionWindow, maxConnectionWindow, sendBuffer, maxSendBuffer, OptionalLong.empty(), latencyBudget); }
    public FlowControl withLatencyBudget(Duration d) { return new FlowControl(initialStreamWindow, maxStreamWindow, initialConnectionWindow, maxConnectionWindow, sendBuffer, maxSendBuffer, memoryBudget, d); }
}
