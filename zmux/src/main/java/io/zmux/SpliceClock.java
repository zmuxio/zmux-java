package io.zmux;

import io.zmux.internal.loop.SessionRuntime;
import java.time.Duration;

/** Clock and cancellable wakeups of the splice owner. */
interface SpliceClock {
    Duration MAX_DELAY = Duration.ofNanos(Long.MAX_VALUE - 1);

    long now();
    Runnable schedule(long at, Runnable wake);

    static SpliceClock on(SessionRuntime runtime) {
        var domain = runtime.clockDomain();
        return new SpliceClock() {
            @Override public long now() { return domain.now(); }
            @Override public Runnable schedule(long at, Runnable wake) { return domain.schedule(at, wake); }
        };
    }
}
