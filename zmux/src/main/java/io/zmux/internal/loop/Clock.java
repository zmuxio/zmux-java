package io.zmux.internal.loop;

/** The monotonic clock of the core: nanoseconds since class initialisation, never negative. */
public final class Clock {
    private static final long ORIGIN = System.nanoTime();

    private Clock() {}

    public static long now() {
        return System.nanoTime() - ORIGIN;
    }
}
