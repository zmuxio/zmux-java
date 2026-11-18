package io.zmux;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Leak checks that wait for the release of read buffers. A session's whole end, and the other
 * endpoint's end, complete on their own loops after the call that returned: a check made
 * at once reads a moment, not the outcome. A buffer still held after the bound is a leak.
 */
final class Leaks {
    private static final Duration BOUND = Duration.ofSeconds(10);

    private Leaks() {}

    static void none(EventLoops loops) throws InterruptedException { none(loops, (String) null); }

    static void none(EventLoops loops, String message) throws InterruptedException { none(loops, () -> message); }

    static void none(EventLoops loops, Supplier<String> message) throws InterruptedException {
        long end = System.nanoTime() + BOUND.toNanos();
        while (loops.outstandingBuffers() != 0 && System.nanoTime() < end) Thread.sleep(2);
        assertEquals(0, loops.outstandingBuffers(), message);
    }
}
