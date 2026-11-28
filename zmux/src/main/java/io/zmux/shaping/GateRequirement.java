package io.zmux.shaping;

/** Worst recovery over the reachable rate/burst pairs before preamble negotiation. */
public record GateRequirement(long waitMicros, long slowestRate) {
    public static final GateRequirement NONE = new GateRequirement(0, 0);

    public GateRequirement {
        if (waitMicros < 0 || slowestRate < 0 || slowestRate > WireAccount.MAX_RATE) {
            throw new IllegalArgumentException("invalid gate requirement");
        }
    }

    public static long waitMicros(long rate, long burst, long minTokens) {
        if (rate == 0) return 0;
        // The recovery quantum has a 64-byte floor even when the burst is smaller.
        //noinspection MathClampMigration
        long quantum = Math.max(64, Math.min(burst, rate / 1000));
        long need = Math.max(0, Math.subtractExact(quantum, minTokens));
        return Math.ceilDiv(Math.multiplyExact(need, 1_000_000), rate);
    }

    public GateRequirement combine(GateRequirement other) {
        long slow = slowestRate == 0 || other.slowestRate == 0 ? Math.max(slowestRate, other.slowestRate)
                : Math.min(slowestRate, other.slowestRate);
        return new GateRequirement(Math.max(waitMicros, other.waitMicros), slow);
    }

    public boolean fits(long delayMicros, int preamble) {
        if (waitMicros > delayMicros) return false;
        return slowestRate == 0 || Math.ceilDiv(preamble * 1_000_000L, slowestRate) <= delayMicros;
    }
}
