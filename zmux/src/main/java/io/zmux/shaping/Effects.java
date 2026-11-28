package io.zmux.shaping;

/** Updates the pending intents on a pipe. Times use the monotonic session clock. */
public final class Effects {
    private static final int INJECT = 1, WAKE = 2, GATE = 4, WEIGHT = 8, STOP = 16;
    private int set;
    private boolean injectNone, wakeNone;
    private long injectId, injectAt, wakeAt, gateUntil, gateRate, gateBurst, weight;
    private int injectLength;

    /** No effects. */
    public Effects() {}

    /** Replaces the pending injection from an outbound or timer event. */
    public Effects inject(long requestId, long atNanos, int padBytes) {
        set |= INJECT; injectNone = false; injectId = requestId; injectAt = atNanos; injectLength = padBytes; return this;
    }

    /** Removes the pending injection. */
    public Effects noInject() { set |= INJECT; injectNone = true; return this; }

    /** Replaces the wake-up: an {@link EventKind#TIMER} at this time. */
    public Effects wake(long atNanos) { set |= WAKE; wakeNone = false; wakeAt = atNanos; return this; }

    /** Removes the wake-up. */
    public Effects noWake() { set |= WAKE; wakeNone = true; return this; }

    /**
     * Replaces the gate on the pipe's stream data: held until this time, then a rate in bytes per second (0: no limit) and
     * a burst in bytes (0: one largest record). The time only ever moves a hold later.
     */
    public Effects gate(long pauseUntilNanos, long rate, long burst) {
        set |= GATE; gateUntil = pauseUntilNanos; gateRate = rate; gateBurst = burst; return this;
    }

    /** The pipe's weight in scheduler views; 0: none. */
    public Effects weight(long w) { set |= WEIGHT; weight = w; return this; }

    /** Shaping of the pipe ends. */
    public Effects stop() { set |= STOP; return this; }

    public boolean isEmpty() { return set == 0; }
    public boolean isInjectSet() { return (set & INJECT) != 0; }
    public boolean isInjectNone() { return injectNone; }
    public long injectId() { return injectId; }
    public long injectAtNanos() { return injectAt; }
    public int injectLength() { return injectLength; }
    public boolean isWakeSet() { return (set & WAKE) != 0; }
    public boolean isWakeNone() { return wakeNone; }
    public long wakeAtNanos() { return wakeAt; }
    public boolean isGateSet() { return (set & GATE) != 0; }
    public long gatePauseUntilNanos() { return gateUntil; }
    public long gateRate() { return gateRate; }
    public long gateBurst() { return gateBurst; }
    public boolean isWeightSet() { return (set & WEIGHT) != 0; }
    public long weight() { return weight; }
    public boolean isStopRequested() { return (set & STOP) != 0; }

    /** Resets every item. */
    public void clear() { set = 0; }
}
