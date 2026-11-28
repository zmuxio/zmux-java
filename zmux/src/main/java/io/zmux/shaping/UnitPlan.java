package io.zmux.shaping;

/** Size and timing decisions shared by record and native datagram plans. */
public final class UnitPlan {
    private int padTo, padMultiple, pad;
    private long delayNanos;

    public UnitPlan() {}

    public UnitPlan padTo(int bytes) { padTo = bytes; return this; }
    public UnitPlan padMultiple(int bytes) { padMultiple = bytes; return this; }
    public UnitPlan pad(int bytes) { pad = bytes; return this; }
    public UnitPlan delayNanos(long nanos) { delayNanos = nanos; return this; }
    public int padTo() { return padTo; }
    public int padMultiple() { return padMultiple; }
    public int pad() { return pad; }
    public long delayNanos() { return delayNanos; }

    /** Applies the size rules in order, using the length at the carrier interface. */
    public static int target(int length, int to, int multiple, int add) {
        long size = Math.max(length, to);
        if (multiple > 0 && size % multiple != 0) size += multiple - size % multiple;
        return (int) Math.min(size + Math.max(add, 0), Integer.MAX_VALUE);
    }

    public void clear() {
        padTo = padMultiple = pad = 0;
        delayNanos = 0;
    }
}
