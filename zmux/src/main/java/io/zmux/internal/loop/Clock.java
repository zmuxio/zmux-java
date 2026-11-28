package io.zmux.internal.loop;

/** The monotonic clock of the core: nanoseconds since class initialisation, never negative. */
public final class Clock {
    private Clock() {}

    public static long now() {
        return fromNanoTime(System.nanoTime());
    }

    /** Converts the reading of an already sampled monotonic clock. */
    public static long fromNanoTime(long reading) {
        return io.zmux.internal.core.Time.fromNanoTime(reading);
    }
}
