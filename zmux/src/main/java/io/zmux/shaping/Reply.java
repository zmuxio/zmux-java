package io.zmux.shaping;

/** The answer to an {@link Event}. A cleared reply changes nothing; an item not set stays unchanged. */
public final class Reply {
    private static final int INJECT = 1, GATE = 2, TIMER0 = 4, TIMER1 = 8, WEIGHT = 16, STOP = 32;
    private int set;
    private boolean injectCancel;
    private long injectAfter;
    private int injectLength;
    private long gatePause, gateRate, gateBurst;
    private final boolean[] timerCancel = new boolean[2];
    private final long[] timerAfter = new long[2];
    private double weight;

    /** A cleared reply. */
    public Reply() {}

    /**
     * Replaces the pending injection: a record of PADDING only on lane 0 of the pipe, at most
     * one pending. afterNanos from now, negative: now; length in frames bytes, raised to the
     * smallest such record.
     */
    public Reply inject(long afterNanos, int length) {
        set |= INJECT; injectCancel = false; injectAfter = afterNanos; injectLength = length; return this;
    }

    /** Cancels the pending injection. */
    public Reply cancelInject() { set |= INJECT; injectCancel = true; return this; }

    /**
     * Replaces the gate on the pipe's stream data: a pause from now (0: none), a rate in bytes
     * per second (0: no limit) and a burst in bytes (0: the record limit). All zero removes it.
     */
    public Reply gate(long pauseNanos, long rate, long burst) {
        set |= GATE; gatePause = pauseNanos; gateRate = rate; gateBurst = burst; return this;
    }

    /** Arms timer slot 0 or 1 afterNanos from now (negative: now). */
    public Reply timer(int slot, long afterNanos) {
        int bit = slotBit(slot); set |= bit; timerCancel[slot] = false; timerAfter[slot] = afterNanos; return this;
    }

    /** Disarms timer slot 0 or 1. */
    public Reply cancelTimer(int slot) { set |= slotBit(slot); timerCancel[slot] = true; return this; }

    /** The pipe's shaping weight in scheduler views; a value that is not positive and finite is ignored. */
    public Reply weight(double w) { set |= WEIGHT; weight = w; return this; }

    /** Shaping of the pipe ends. */
    public Reply stop() { set |= STOP; return this; }

    private static int slotBit(int slot) {
        if (slot != 0 && slot != 1) throw new IllegalArgumentException("timer slot " + slot);
        return slot == 0 ? TIMER0 : TIMER1;
    }

    public boolean isEmpty() { return set == 0; }
    public boolean hasInject() { return (set & INJECT) != 0; }
    public boolean injectCancelled() { return injectCancel; }
    public long injectAfterNanos() { return injectAfter; }
    public int injectLength() { return injectLength; }
    public boolean hasGate() { return (set & GATE) != 0; }
    public long gatePauseNanos() { return gatePause; }
    public long gateRate() { return gateRate; }
    public long gateBurst() { return gateBurst; }
    public boolean hasTimer(int slot) { return (set & slotBit(slot)) != 0; }
    public boolean timerCancelled(int slot) { return timerCancel[slot]; }
    public long timerAfterNanos(int slot) { return timerAfter[slot]; }
    public boolean hasWeight() { return (set & WEIGHT) != 0; }
    public double weight() { return weight; }
    public boolean stopRequested() { return (set & STOP) != 0; }

    /** Resets every item. */
    public void clear() { set = 0; }
}
