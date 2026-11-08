package io.zmux.shaping;

/**
 * An allowance for what a session sends on its own: records of PADDING only, and injections merged into a record. Its
 * unit is the wire byte. Padding that goes with a record is never refused and does not draw on it. All fields zero: not set.
 *
 * @param initial available from the start, in [0, {@link #MAX_INITIAL}]
 * @param perMb added per 10^6 bytes this endpoint sends of its own, in [0, {@link #MAX_RATE}]
 * @param perSec added per second, in [0, {@link #MAX_RATE}]
 */
public record WireAccount(long initial, long perMb, long perSec) {
    public static final long MAX_INITIAL = 1L << 53;
    public static final long MAX_RATE = 1L << 43;
    /** Not set. */
    public static final WireAccount NONE = new WireAccount(0, 0, 0);

    public WireAccount {
        if (initial < 0 || initial > MAX_INITIAL) throw new IllegalArgumentException("initial outside [0, 2^53]");
        if (perMb < 0 || perMb > MAX_RATE) throw new IllegalArgumentException("perMb outside [0, 2^43]");
        if (perSec < 0 || perSec > MAX_RATE) throw new IllegalArgumentException("perSec outside [0, 2^43]");
    }

    /** Whether any field is set. */
    public boolean isSet() { return initial != 0 || perMb != 0 || perSec != 0; }

    /** Field by field the smaller where both are set, else the one that is. */
    WireAccount under(WireAccount o) {
        if (!isSet()) return o;
        if (!o.isSet()) return this;
        return new WireAccount(Math.min(initial, o.initial), Math.min(perMb, o.perMb), Math.min(perSec, o.perSec));
    }
}
