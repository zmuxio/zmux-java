package io.zmux.shaping;

/** The capacity and refill rates of one session direction's cover account, in wire bytes. */
public record WireAccount(long ceiling, long initial, long perMb, long perSec) {
    public static final long MAX_CEILING = 1L << 53;
    public static final long MAX_RATE = 1L << 43;

    public WireAccount {
        if (ceiling < 0 || ceiling > MAX_CEILING) throw new IllegalArgumentException("ceiling outside [0, 2^53]");
        if (initial < 0 || initial > ceiling) throw new IllegalArgumentException("initial outside [0, ceiling]");
        if (perMb < 0 || perMb > MAX_RATE) throw new IllegalArgumentException("perMb outside [0, 2^43]");
        if (perSec < 0 || perSec > MAX_RATE) throw new IllegalArgumentException("perSec outside [0, 2^43]");
    }

    WireAccount under(WireAccount other) {
        if (other == null) return this;
        return new WireAccount(Math.min(ceiling, other.ceiling), Math.min(initial, other.initial),
                Math.min(perMb, other.perMb), Math.min(perSec, other.perSec));
    }
}
