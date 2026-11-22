package io.zmux.internal.spi;

import io.zmux.EventLoops;
import io.zmux.adapter.Handled;
import io.zmux.internal.diag.Diagnostics;
import io.zmux.internal.loop.CarrierSetup;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Lifecycle probes for sibling adapter tests using private event loops. */
public final class AdapterTestSupport {
    private AdapterTestSupport() {}

    public static CompletableFuture<Void> advanceTimers(Handled handled, Duration elapsed) {
        return setup(handled).advanceTimersForTests(elapsed.toNanos());
    }

    /** Runs a timer turn in a fixture with one owner loop. */
    public static CompletableFuture<Void> advanceTimers(EventLoops loops, Duration elapsed) {
        return Diagnostics.advanceTimersForTests(loops, elapsed.toNanos());
    }

    public static boolean holdsAdmissionSlot(Handled handled) {
        return setup(handled).holdsAdmissionSlotForTests();
    }

    private static CarrierSetup setup(Handled handled) {
        return (CarrierSetup) AdapterPorts.setup(handled);
    }
}
