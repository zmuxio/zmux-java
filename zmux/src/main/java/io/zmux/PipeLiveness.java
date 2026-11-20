package io.zmux;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;

/**
 * A pipe's own liveness settings, the four of {@link Liveness} that apply to one pipe: held by
 * {@link PipeOptions.Builder#liveness}, by a pool's options, by a pool decision
 * ({@link PoolDecision#liveness()}) and by {@link PipeUpdate.Builder#liveness}. Each setting
 * not written follows the session's ({@link #inherit()} for all four). Immutable.
 */
public final class PipeLiveness {
    private static final PipeLiveness INHERIT = builder().build();

    private final Duration suspectFloor, deadTimeout, probeInterval; // null: the session's
    private final boolean noProbeInterval, noDeadTimeout;
    private final double probeJitter; // NaN: the session's

    private PipeLiveness(Builder b) {
        suspectFloor = b.suspectFloor; deadTimeout = b.deadTimeout; probeInterval = b.probeInterval;
        noProbeInterval = b.noProbeInterval; noDeadTimeout = b.noDeadTimeout; probeJitter = b.probeJitter;
    }

    /** Every setting follows the session's. */
    public static PipeLiveness inherit() { return INHERIT; }

    public static Builder builder() { return new Builder(); }

    /** A builder holding these settings. */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.suspectFloor = suspectFloor; b.deadTimeout = deadTimeout; b.probeInterval = probeInterval;
        b.noProbeInterval = noProbeInterval; b.noDeadTimeout = noDeadTimeout; b.probeJitter = probeJitter;
        return b;
    }

    /** The pipe's suspicion floor; empty: the session's. */
    public Optional<Duration> suspectFloor() { return Optional.ofNullable(suspectFloor); }

    /**
     * The pipe's dead timeout; empty: off when {@link #isNoDeadTimeout()}, else the carrier's default (off when it
     * reports its own failure), else the session's.
     */
    public Optional<Duration> deadTimeout() { return Optional.ofNullable(deadTimeout); }

    /** Whether the pipe's stall-based failure is off ({@link Builder#noDeadTimeout()}). */
    public boolean isNoDeadTimeout() { return noDeadTimeout; }

    /** The pipe's idle probe interval; empty: the session's, or off when {@link #isNoProbeInterval()}. */
    public Optional<Duration> probeInterval() { return Optional.ofNullable(probeInterval); }

    /** Whether the pipe's idle probes are off. */
    public boolean isNoProbeInterval() { return noProbeInterval; }

    /** The pipe's probe jitter; empty: the session's. */
    public OptionalDouble probeJitter() { return Double.isNaN(probeJitter) ? OptionalDouble.empty() : OptionalDouble.of(probeJitter); }

    /** Whether every setting follows the session's. */
    public boolean isInherit() { return equals(INHERIT); }

    /** other's settings over these: each one other writes replaces this one's. */
    PipeLiveness over(PipeLiveness other) {
        if (other.isInherit()) return this;
        Builder b = toBuilder();
        if (other.suspectFloor != null) b.suspectFloor = other.suspectFloor;
        if (other.deadTimeout != null || other.noDeadTimeout) { b.deadTimeout = other.deadTimeout; b.noDeadTimeout = other.noDeadTimeout; }
        if (other.probeInterval != null || other.noProbeInterval) { b.probeInterval = other.probeInterval; b.noProbeInterval = other.noProbeInterval; }
        if (!Double.isNaN(other.probeJitter)) b.probeJitter = other.probeJitter;
        return b.build();
    }

    /** The suspicion floor in nanoseconds; 0: the session's. */
    long suspectFloorNanos() { return suspectFloor == null ? 0 : ZmuxConfig.nanos(suspectFloor); }

    /** The dead timeout in nanoseconds (saturating); 0: the carrier's, then the session's; -1: off. */
    long deadTimeoutNanos() { return noDeadTimeout ? -1 : deadTimeout == null ? 0 : ZmuxConfig.nanos(deadTimeout); }

    /** The probe interval in nanoseconds; 0: off, -1: the session's. */
    long probeIntervalNanos() { return noProbeInterval ? 0 : probeInterval == null ? -1 : ZmuxConfig.nanos(probeInterval); }

    /** The jitter; NaN: the session's. */
    double probeJitterRaw() { return probeJitter; }

    @Override public boolean equals(Object o) {
        return o instanceof PipeLiveness p && Objects.equals(suspectFloor, p.suspectFloor) && Objects.equals(deadTimeout, p.deadTimeout) && noDeadTimeout == p.noDeadTimeout
                && Objects.equals(probeInterval, p.probeInterval) && noProbeInterval == p.noProbeInterval && Double.compare(probeJitter, p.probeJitter) == 0;
    }

    @Override public int hashCode() { return Objects.hash(suspectFloor, deadTimeout, noDeadTimeout, probeInterval, noProbeInterval, probeJitter); }

    @Override public String toString() {
        return "PipeLiveness[suspectFloor=" + suspectFloor() + ", deadTimeout=" + (noDeadTimeout ? "off" : deadTimeout()) + ", probeInterval="
                + (noProbeInterval ? "off" : probeInterval()) + ", probeJitter=" + probeJitter() + "]";
    }

    /** Builds a {@link PipeLiveness}; not calling a method follows the session's setting. */
    public static final class Builder {
        private Duration suspectFloor, deadTimeout, probeInterval;
        private boolean noProbeInterval, noDeadTimeout;
        private double probeJitter = Double.NaN;

        private Builder() {}

        /** The pipe's suspicion floor, at least 50 ms. */
        public Builder suspectFloor(Duration d) { suspectFloor = Liveness.atLeast(d, Duration.ofMillis(50), "suspectFloor"); return this; }

        /**
         * The pipe's dead timeout, at least 1 s (any larger value is safe); it overrides the carrier's default and the
         * session's.
         */
        public Builder deadTimeout(Duration d) { deadTimeout = Liveness.atLeast(d, Duration.ofSeconds(1), "deadTimeout"); noDeadTimeout = false; return this; }

        /**
         * The pipe's stall-based failure off, as {@link Liveness.Builder#noDeadTimeout()}, whatever the carrier and the
         * session say.
         */
        public Builder noDeadTimeout() { deadTimeout = null; noDeadTimeout = true; return this; }

        /**
         * The probe interval k of the pipe while it is idle, at least 1 s. Without an outbound
         * direction at the peer at most 15 s takes effect.
         */
        public Builder probeInterval(Duration d) {
            probeInterval = Liveness.atLeast(d, Duration.ofSeconds(1), "probeInterval");
            noProbeInterval = false;
            return this;
        }

        /** The pipe's idle probes off, except without an outbound direction at the peer, where they cannot be. */
        public Builder noProbeInterval() { probeInterval = null; noProbeInterval = true; return this; }

        /** The probe jitter j of the pipe, {@code 0 <= j < 1}: each probe waits a uniform draw from [k(1 - j), k]. */
        public Builder probeJitter(double j) { probeJitter = Liveness.jitter(j, "probeJitter"); return this; }

        public PipeLiveness build() { return new PipeLiveness(this); }
    }
}
