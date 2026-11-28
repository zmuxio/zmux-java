package io.zmux.nettytest;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/** A watchdog for positive observations of a real carrier whose statistics are snapshots. */
public final class Eventually {
    private Eventually() {}

    /** Reads the state watched by {@link #await}. */
    @FunctionalInterface
    public interface Condition<E extends Exception> {
        boolean ready() throws E;
    }

    public static <E extends Exception> void await(Duration timeout, String observation, Condition<E> condition)
            throws E, InterruptedException {
        long started = System.nanoTime();
        while (!condition.ready()) {
            if (System.nanoTime() - started >= timeout.toNanos()) throw new AssertionError("did not observe " + observation);
            TimeUnit.MILLISECONDS.sleep(5);
        }
    }
}
