package io.zmux;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;

/**
 * A session's liveness settings as a whole, for {@link ZmuxSession#setLiveness}, with the meanings,
 * defaults and bounds of the configuration ({@link ZmuxConfig.Builder}). A setting
 * not written is its default, not the value the session started with; a setting that is off has a
 * method of its own. {@link ZmuxSession#liveness()} returns the settings in effect, so that setting
 * them changes nothing. Immutable; each {@code with} or {@code disable} method returns a copy.
 */
public final class Liveness {
    private static final Duration DEFAULT_SUSPECT_FLOOR = Duration.ofMillis(250);
    private static final Duration DEFAULT_STANDBY_PROBE = Duration.ofSeconds(1);
    private static final Duration DEFAULT_PROBE_INTERVAL = Duration.ofSeconds(15);
    private static final Liveness DEFAULTS = new Liveness(ZmuxConfig.DEFAULT_KEEP_ALIVE_PERIOD, DEFAULT_SUSPECT_FLOOR, null,
            DEFAULT_STANDBY_PROBE, DEFAULT_PROBE_INTERVAL, ZmuxConfig.DEFAULT_PROBE_JITTER);

    private final Duration keepAlive, suspectFloor, deadTimeout, standbyProbe, probeInterval;
    private final double probeJitter;

    private Liveness(Duration keepAlive, Duration suspectFloor, Duration deadTimeout, Duration standbyProbe, Duration probeInterval, double probeJitter) {
        this.keepAlive = keepAlive;
        this.suspectFloor = suspectFloor;
        this.deadTimeout = deadTimeout;
        this.standbyProbe = standbyProbe;
        this.probeInterval = probeInterval;
        this.probeJitter = probeJitter;
    }

    /** Every setting at its default. */
    public static Liveness defaults() { return DEFAULTS; }

    /** The keep-alive period; empty: off. Default 15 s. */
    public Optional<Duration> keepAlive() { return Optional.ofNullable(keepAlive); }

    /** The floor of failure suspicion, default 250 ms. */
    public Duration suspectFloor() { return suspectFloor; }

    /** When a silent pipe is declared dead; empty: derived (the default). */
    public Optional<Duration> deadTimeout() { return Optional.ofNullable(deadTimeout); }

    /** The busy-session probe interval; empty: standby probes off. Default 1 s. */
    public Optional<Duration> standbyProbe() { return Optional.ofNullable(standbyProbe); }

    /** The idle probe interval; empty: idle probes off. Default 15 s. */
    public Optional<Duration> probeInterval() { return Optional.ofNullable(probeInterval); }

    /** The probe jitter, from 0 (none) to below 1, default 0.25. */
    public double probeJitter() { return probeJitter; }

    /** The keep-alive period, at least 1 s. */
    public Liveness withKeepAlive(Duration d) {
        return new Liveness(ZmuxConfig.atLeast(d, Duration.ofSeconds(1), "keepAlive"), suspectFloor, deadTimeout, standbyProbe, probeInterval, probeJitter);
    }

    /** Keep-alives off. */
    public Liveness disableKeepAlive() { return new Liveness(null, suspectFloor, deadTimeout, standbyProbe, probeInterval, probeJitter); }

    /** The floor of failure suspicion, at least 50 ms. */
    public Liveness withSuspectFloor(Duration d) {
        return new Liveness(keepAlive, ZmuxConfig.atLeast(d, Duration.ofMillis(50), "suspectFloor"), deadTimeout, standbyProbe, probeInterval, probeJitter);
    }

    /** The dead timeout, at least 1 s. */
    public Liveness withDeadTimeout(Duration d) {
        return new Liveness(keepAlive, suspectFloor, ZmuxConfig.atLeast(d, Duration.ofSeconds(1), "deadTimeout"), standbyProbe, probeInterval, probeJitter);
    }

    /** The busy-session probe interval, at least 1 s. */
    public Liveness withStandbyProbe(Duration d) {
        return new Liveness(keepAlive, suspectFloor, deadTimeout, ZmuxConfig.probeIntervalOf(d, "standbyProbe"), probeInterval, probeJitter);
    }

    /** Standby probes off. */
    public Liveness disableStandbyProbes() { return new Liveness(keepAlive, suspectFloor, deadTimeout, null, probeInterval, probeJitter); }

    /** The idle probe interval, at least 1 s. */
    public Liveness withProbeInterval(Duration d) {
        return new Liveness(keepAlive, suspectFloor, deadTimeout, standbyProbe, ZmuxConfig.probeIntervalOf(d, "probeInterval"), probeJitter);
    }

    /** Idle probes off. */
    public Liveness disableIdleProbes() { return new Liveness(keepAlive, suspectFloor, deadTimeout, standbyProbe, null, probeJitter); }

    /** The probe jitter, at least 0 and below 1; 0 is none. */
    public Liveness withProbeJitter(double j) {
        return new Liveness(keepAlive, suspectFloor, deadTimeout, standbyProbe, probeInterval, ZmuxConfig.jitter(j, "probeJitter"));
    }

    static Liveness of(Duration keepAlive, Duration suspectFloor, Duration deadTimeout, Duration standbyProbe, Duration probeInterval, double probeJitter) {
        return new Liveness(keepAlive, suspectFloor, deadTimeout, standbyProbe, probeInterval, probeJitter);
    }

    @Override public boolean equals(Object o) {
        return o instanceof Liveness l && Objects.equals(keepAlive, l.keepAlive) && suspectFloor.equals(l.suspectFloor)
                && Objects.equals(deadTimeout, l.deadTimeout) && Objects.equals(standbyProbe, l.standbyProbe)
                && Objects.equals(probeInterval, l.probeInterval) && Double.compare(probeJitter, l.probeJitter) == 0;
    }

    @Override public int hashCode() { return Objects.hash(keepAlive, suspectFloor, deadTimeout, standbyProbe, probeInterval, probeJitter); }

    @Override public String toString() {
        return "Liveness[keepAlive=" + keepAlive() + ", suspectFloor=" + suspectFloor + ", deadTimeout=" + deadTimeout()
                + ", standbyProbe=" + standbyProbe() + ", probeInterval=" + probeInterval() + ", probeJitter=" + probeJitter + "]";
    }

    /**
     * A pipe's own liveness settings, for {@link ZmuxPipe#setLiveness}, with the meanings of
     * {@link PipeOptions}: a setting not written follows the session's. Immutable.
     */
    public static final class Pipe {
        private static final Pipe FOLLOW = new Pipe(null, null, null, false, Double.NaN);

        private final Duration suspectFloor, deadTimeout, probeInterval;
        private final boolean idleProbesOff;
        private final double probeJitter; // NaN: the session's

        private Pipe(Duration suspectFloor, Duration deadTimeout, Duration probeInterval, boolean idleProbesOff, double probeJitter) {
            this.suspectFloor = suspectFloor;
            this.deadTimeout = deadTimeout;
            this.probeInterval = probeInterval;
            this.idleProbesOff = idleProbesOff;
            this.probeJitter = probeJitter;
        }

        /** Every setting follows the session's. */
        public static Pipe follow() { return FOLLOW; }

        /** The pipe's suspicion floor; empty: the session's. */
        public Optional<Duration> suspectFloor() { return Optional.ofNullable(suspectFloor); }

        /** The pipe's dead timeout; empty: the session's. */
        public Optional<Duration> deadTimeout() { return Optional.ofNullable(deadTimeout); }

        /** The pipe's idle probe interval; empty: the session's, or off when {@link #idleProbesOff()}. */
        public Optional<Duration> probeInterval() { return Optional.ofNullable(probeInterval); }

        /** Whether the pipe's idle probes are off. */
        public boolean idleProbesOff() { return idleProbesOff; }

        /** The pipe's probe jitter; empty: the session's. */
        public OptionalDouble probeJitter() { return Double.isNaN(probeJitter) ? OptionalDouble.empty() : OptionalDouble.of(probeJitter); }

        /** The suspicion floor, at least 50 ms. */
        public Pipe withSuspectFloor(Duration d) {
            return new Pipe(ZmuxConfig.atLeast(d, Duration.ofMillis(50), "suspectFloor"), deadTimeout, probeInterval, idleProbesOff, probeJitter);
        }

        /** The dead timeout, at least 1 s. */
        public Pipe withDeadTimeout(Duration d) {
            return new Pipe(suspectFloor, ZmuxConfig.atLeast(d, Duration.ofSeconds(1), "deadTimeout"), probeInterval, idleProbesOff, probeJitter);
        }

        /** The idle probe interval, at least 1 s. */
        public Pipe withProbeInterval(Duration d) {
            return new Pipe(suspectFloor, deadTimeout, ZmuxConfig.probeIntervalOf(d, "probeInterval"), false, probeJitter);
        }

        /** The pipe's idle probes off, except without an outbound direction at the peer, where they cannot be. */
        public Pipe disableIdleProbes() { return new Pipe(suspectFloor, deadTimeout, null, true, probeJitter); }

        /** The probe jitter, at least 0 and below 1; 0 is none. */
        public Pipe withProbeJitter(double j) {
            return new Pipe(suspectFloor, deadTimeout, probeInterval, idleProbesOff, ZmuxConfig.jitter(j, "probeJitter"));
        }

        @Override public boolean equals(Object o) {
            return o instanceof Pipe p && Objects.equals(suspectFloor, p.suspectFloor) && Objects.equals(deadTimeout, p.deadTimeout)
                    && Objects.equals(probeInterval, p.probeInterval) && idleProbesOff == p.idleProbesOff && Double.compare(probeJitter, p.probeJitter) == 0;
        }

        @Override public int hashCode() { return Objects.hash(suspectFloor, deadTimeout, probeInterval, idleProbesOff, probeJitter); }

        @Override public String toString() {
            return "Liveness.Pipe[suspectFloor=" + suspectFloor() + ", deadTimeout=" + deadTimeout() + ", probeInterval="
                    + (idleProbesOff ? "off" : probeInterval()) + ", probeJitter=" + probeJitter() + "]";
        }
    }
}
