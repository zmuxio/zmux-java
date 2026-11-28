package io.zmux;

import io.zmux.internal.core.Time;
import io.zmux.internal.loop.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/** Identifies a monotonic time base and timer source shared by sessions and their controller. */
public final class ClockDomain {
    /** A monotonic clock available from any thread. Timer cancellation must be idempotent. */
    public interface Source {
        long nanoTime();
        Instant wallTime();
        Runnable schedule(long delayNanos, Runnable task);
    }

    private static final ClockDomain SYSTEM = new ClockDomain();
    private final Source source;
    private final long origin;
    private final AtomicLong floor = new AtomicLong();

    /** Creates a distinct clock domain. */
    public ClockDomain(Source source) {
        this.source = Objects.requireNonNull(source);
        origin = source.nanoTime();
    }

    private ClockDomain() {
        source = null;
        origin = 0;
    }

    public static ClockDomain system() { return SYSTEM; }

    /** Monotonic time in nanoseconds since the domain's origin. */
    public long now() {
        return source == null ? Math.max(0, Clock.now()) : floor.accumulateAndGet(source.nanoTime() - origin, Math::max);
    }

    Instant wallTime() { return source == null ? Instant.now() : source.wallTime(); }

    Instant wallTime(long at) { return wallTime().minusNanos(Math.max(0, Time.sub(now(), at))); }

    /** Schedules a task at a reading of this domain's clock. Returns a cancellation action. */
    public Runnable schedule(long deadline, Runnable task) {
        if (deadline == Time.NEVER) return () -> {};
        long delay = Math.max(0, Time.sub(deadline, now()));
        if (source != null) return source.schedule(delay, task);
        var resolved = new AtomicBoolean();
        var worker = Thread.ofVirtual().name("zmux-clock").start(() -> {
            while (!resolved.get()) {
                long left = Time.sub(deadline, now());
                if (left <= 0) {
                    if (resolved.compareAndSet(false, true)) task.run();
                    return;
                }
                LockSupport.parkNanos(left);
            }
        });
        return () -> {
            if (resolved.compareAndSet(false, true)) LockSupport.unpark(worker);
        };
    }
}
