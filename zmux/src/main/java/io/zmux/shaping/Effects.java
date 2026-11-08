package io.zmux.shaping;

/**
 * What a {@link Shaper} changes on its pipe. The session keeps one pending injection, one wake-up, one gate and one
 * weight per pipe; an item not set stays as it is, also once it was used: an answer to {@link EventKind#INJECTED},
 * {@link EventKind#INJECT_DROPPED} or {@link EventKind#WAKE} clears what it no longer wants, or the session goes on with the
 * old value. Times are absolute, on the session clock of {@link Event#nowNanos()}.
 */
public final class Effects {
    private static final int INJECT = 1, WAKE = 2, GATE = 4, WEIGHT = 8, STOP = 16;
    private int set;
    private boolean injectNone, wakeNone;
    private long injectAt, wakeAt, gateUntil, gateRate, gateBurst, weight;
    private int injectLength;

    /** No effects. */
    public Effects() {}

    /**
     * Replaces the pending injection: a record of PADDING only on lane 0 of the pipe, at this time and of this length on
     * the carrier, its own write.
     */
    public Effects inject(long atNanos, int length) {
        set |= INJECT; injectNone = false; injectAt = atNanos; injectLength = length; return this;
    }

    /** Removes the pending injection. */
    public Effects noInject() { set |= INJECT; injectNone = true; return this; }

    /** Replaces the wake-up: an {@link EventKind#WAKE} at this time. */
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
    public boolean hasInject() { return (set & INJECT) != 0; }
    public boolean injectNone() { return injectNone; }
    public long injectAtNanos() { return injectAt; }
    public int injectLength() { return injectLength; }
    public boolean hasWake() { return (set & WAKE) != 0; }
    public boolean wakeNone() { return wakeNone; }
    public long wakeAtNanos() { return wakeAt; }
    public boolean hasGate() { return (set & GATE) != 0; }
    public long gatePauseUntilNanos() { return gateUntil; }
    public long gateRate() { return gateRate; }
    public long gateBurst() { return gateBurst; }
    public boolean hasWeight() { return (set & WEIGHT) != 0; }
    public long weight() { return weight; }
    public boolean stopRequested() { return (set & STOP) != 0; }

    /** Resets every item. */
    public void clear() { set = 0; }
}
