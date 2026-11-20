package io.zmux;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * The settings that judge whether a session and its pipes are alive, the one place of the
 * session's keep-alive and probe settings ({@link ZmuxConfig.Builder#liveness(Liveness)}); a pipe's own are
 * a {@link PipeLiveness}. {@link #defaults()} or {@link #builder()}: not calling a builder method
 * keeps the default; a setting that is off has a {@code noX()} method of its own. Replaced while
 * the session runs with {@link ZmuxSession#setLiveness}, which takes the whole object: a setting
 * left at its default is the default, not the value the session started with.
 * {@link ZmuxSession#liveness()} returns the settings in effect, so that passing them back
 * changes nothing. Immutable.
 */
public final class Liveness {
    /** The default keep-alive period. */
    public static final Duration DEFAULT_KEEP_ALIVE_PERIOD = Duration.ofSeconds(15);
    /** The default floor of failure suspicion. */
    public static final Duration DEFAULT_SUSPECT_FLOOR = Duration.ofMillis(250);
    /** The default probe interval of standby pipes while the session is busy. */
    public static final Duration DEFAULT_STANDBY_PROBE = Duration.ofSeconds(1);
    /** The default idle probe interval of pipe pairs. */
    public static final Duration DEFAULT_PROBE_INTERVAL = Duration.ofSeconds(15);
    /** The default probe jitter. */
    public static final double DEFAULT_PROBE_JITTER = 0.25;

    private static final Liveness DEFAULTS = builder().build();

    private final Duration keepAlivePeriod, suspectFloor, deadTimeout, standbyProbe, probeInterval; // null: off, deadTimeout derived or off
    private final boolean noDeadTimeout;
    private final double probeJitter;

    private Liveness(Builder b) {
        keepAlivePeriod = b.keepAlivePeriod;
        suspectFloor = b.suspectFloor;
        deadTimeout = b.deadTimeout;
        noDeadTimeout = b.noDeadTimeout;
        standbyProbe = b.standbyProbe;
        probeInterval = b.probeInterval;
        probeJitter = b.probeJitter;
    }

    /** Every setting at its default. */
    public static Liveness defaults() { return DEFAULTS; }

    public static Builder builder() { return new Builder(); }

    /** A builder holding these settings. */
    public Builder toBuilder() {
        Builder b = new Builder();
        b.keepAlivePeriod = keepAlivePeriod; b.suspectFloor = suspectFloor; b.deadTimeout = deadTimeout; b.noDeadTimeout = noDeadTimeout;
        b.standbyProbe = standbyProbe; b.probeInterval = probeInterval; b.probeJitter = probeJitter;
        return b;
    }

    /** The keep-alive period; empty: off. */
    public Optional<Duration> keepAlivePeriod() { return Optional.ofNullable(keepAlivePeriod); }

    /** The floor of failure suspicion. */
    public Duration suspectFloor() { return suspectFloor; }

    /** How long a suspect pipe stays suspect before it fails; empty: derived (the default), or off when {@link #isNoDeadTimeout()}. */
    public Optional<Duration> deadTimeout() { return Optional.ofNullable(deadTimeout); }

    /** Whether stall-based pipe failure is off ({@link Builder#noDeadTimeout()}). */
    public boolean isNoDeadTimeout() { return noDeadTimeout; }

    /** The dead timeout for the core: 0 derived, -1 off, else nanoseconds (saturating). */
    long deadTimeoutNanos() { return noDeadTimeout ? -1 : deadTimeout == null ? 0 : ZmuxConfig.nanos(deadTimeout); }

    /** The busy-session probe interval of standby and less-preferred pipes; empty: off. */
    public Optional<Duration> standbyProbe() { return Optional.ofNullable(standbyProbe); }

    /** The idle probe interval of pipe pairs; empty: off. */
    public Optional<Duration> probeInterval() { return Optional.ofNullable(probeInterval); }

    /** The probe jitter, from 0 (none) to below 1. */
    public double probeJitter() { return probeJitter; }

    @Override public boolean equals(Object o) {
        return o instanceof Liveness l && Objects.equals(keepAlivePeriod, l.keepAlivePeriod) && suspectFloor.equals(l.suspectFloor)
                && Objects.equals(deadTimeout, l.deadTimeout) && noDeadTimeout == l.noDeadTimeout && Objects.equals(standbyProbe, l.standbyProbe)
                && Objects.equals(probeInterval, l.probeInterval) && Double.compare(probeJitter, l.probeJitter) == 0;
    }

    @Override public int hashCode() { return Objects.hash(keepAlivePeriod, suspectFloor, deadTimeout, noDeadTimeout, standbyProbe, probeInterval, probeJitter); }

    @Override public String toString() {
        return "Liveness[keepAlivePeriod=" + keepAlivePeriod() + ", suspectFloor=" + suspectFloor + ", deadTimeout=" + (noDeadTimeout ? "off" : deadTimeout())
                + ", standbyProbe=" + standbyProbe() + ", probeInterval=" + probeInterval() + ", probeJitter=" + probeJitter + "]";
    }

    /** Builds a {@link Liveness}; each method checks its value. */
    public static final class Builder {
        private Duration keepAlivePeriod = DEFAULT_KEEP_ALIVE_PERIOD, suspectFloor = DEFAULT_SUSPECT_FLOOR, deadTimeout,
                standbyProbe = DEFAULT_STANDBY_PROBE, probeInterval = DEFAULT_PROBE_INTERVAL;
        private boolean noDeadTimeout;
        private double probeJitter = DEFAULT_PROBE_JITTER;

        private Builder() {}

        /**
         * The keep-alive period (QX_PING when idle), default 15 s, at least 1 s. The period in effect
         * is capped at half the effective idle timeout, and each wait is drawn from [k x (1 - j), k]
         * with j the probe jitter.
         */
        public Builder keepAlivePeriod(Duration d) { keepAlivePeriod = atLeast(d, Duration.ofSeconds(1), "keepAlivePeriod"); return this; }

        /** Keep-alive off, which makes the idle timeout a limit on application inactivity. */
        public Builder noKeepAlivePeriod() { keepAlivePeriod = null; return this; }

        /** The floor of failure suspicion, default 250 ms, at least 50 ms. */
        public Builder suspectFloor(Duration d) { suspectFloor = atLeast(d, Duration.ofMillis(50), "suspectFloor"); return this; }

        /**
         * How long a suspect pipe stays suspect before it fails, at least 1 s; not calling it: derived from the RTT and
         * the idle timeout. Any larger value is safe (deadlines saturate). A pipe whose carrier reports its own failure
         * ({@link io.zmux.carrier.PipeIo#withReportsFailure}) has stall-based failure off unless its own
         * {@link PipeLiveness.Builder#deadTimeout} is set.
         */
        public Builder deadTimeout(Duration d) { deadTimeout = atLeast(d, Duration.ofSeconds(1), "deadTimeout"); noDeadTimeout = false; return this; }

        /**
         * Stall-based pipe failure off: a stalled pipe still turns suspect, is avoided by the scheduler, has its data
         * reinjected and is probed, but only its carrier's error or end, its drain deadline, its removal or the inbound
         * silence of a receive-only pipe fail it. A pipe's own {@link PipeLiveness.Builder#deadTimeout} overrides it.
         */
        public Builder noDeadTimeout() { deadTimeout = null; noDeadTimeout = true; return this; }

        /** The busy-session probe interval of standby and less-preferred pipes, default 1 s, at least 1 s. */
        public Builder standbyProbe(Duration d) { standbyProbe = atLeast(d, Duration.ofSeconds(1), "standbyProbe"); return this; }

        /** Standby probes off: such pipes are probed at the idle probe interval only. */
        public Builder noStandbyProbe() { standbyProbe = null; return this; }

        /** The idle probe interval of pipe pairs, measured to the carrier, default 15 s, at least 1 s. */
        public Builder probeInterval(Duration d) { probeInterval = atLeast(d, Duration.ofSeconds(1), "probeInterval"); return this; }

        /**
         * The idle probes of pipe pairs off; answers to the peer's probes, standby probes and the
         * probes of attachments without an outbound direction at the peer stay.
         */
        public Builder noProbeInterval() { probeInterval = null; return this; }

        /**
         * The probe jitter j, at least 0 and below 1 (default 0.25): each probe waits a uniform draw
         * from [k x (1 - j), k]; 0 gives a fixed schedule. It also spreads the keep-alive waits.
         */
        public Builder probeJitter(double j) { probeJitter = jitter(j, "probeJitter"); return this; }

        public Liveness build() { return new Liveness(this); }
    }

    static Duration atLeast(Duration d, Duration min, String what) {
        if (Objects.requireNonNull(d, what).compareTo(min) < 0) throw new IllegalArgumentException(what + " below " + min);
        return d;
    }

    static double jitter(double j, String what) {
        if (!(j >= 0 && j < 1)) throw new IllegalArgumentException(what + " outside [0, 1)");
        return j;
    }
}
