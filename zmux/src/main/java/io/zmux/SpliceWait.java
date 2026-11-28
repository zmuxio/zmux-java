package io.zmux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Waiting conditions for one splice output group, measured from its admission. */
public record SpliceWait(Duration delay, List<Resume> signals, Duration timeout) {
    public SpliceWait {
        Objects.requireNonNull(delay);
        signals = List.copyOf(signals);
        Objects.requireNonNull(timeout);
        if (delay.isNegative() || timeout.isZero() || timeout.isNegative()
                || delay.compareTo(SpliceClock.MAX_DELAY) > 0 || timeout.compareTo(SpliceClock.MAX_DELAY) > 0)
            throw new IllegalArgumentException("invalid splice wait");
    }

    public static SpliceWait after(Duration delay, Duration timeout) {
        return new SpliceWait(delay, List.of(), timeout);
    }

    public static SpliceWait until(Resume signal, Duration timeout) {
        return new SpliceWait(Duration.ZERO, List.of(signal), timeout);
    }

    /** A one-shot signal. Calling {@link #signal()} before attaching a wait also satisfies it. */
    public static final class Resume {
        private final List<Runnable> listeners = new ArrayList<>();
        private boolean signalled;

        public Resume() {}

        public void signal() {
            List<Runnable> ready;
            synchronized (this) {
                if (signalled) return;
                signalled = true;
                ready = List.copyOf(listeners);
                listeners.clear();
            }
            ready.forEach(Runnable::run);
        }

        synchronized boolean signalled() { return signalled; }

        Runnable listen(Runnable wake) {
            synchronized (this) {
                if (!signalled) {
                    listeners.add(wake);
                    return () -> remove(wake);
                }
            }
            wake.run();
            return () -> {};
        }

        private synchronized void remove(Runnable listener) { listeners.remove(listener); }
    }
}
