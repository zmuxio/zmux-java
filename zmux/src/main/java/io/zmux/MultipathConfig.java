package io.zmux;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Multipath and resume, immutable. A session offers multipath when maxPipes exceeds 1 or resume is on;
 * either needs {@code auth}, checked by {@link ZmuxConfig.Builder#build()}. {@link #defaults()} holds the
 * defaults; each {@code with} method sets a value and each {@code disable} method turns probes off.
 *
 * <p>Settings:
 * <ul>
 * <li>maxPipes the attachments a session aims for, at least 1
 * <li>resumeTimeout the resume grace period (RESUME.md); zero turns resume off
 * <li>auth the secret sources of joins; null only without multipath
 * <li>scheduler the scheduling policy
 * <li>suspectFloor the floor of failure suspicion, at least 50 ms; unset: 250 ms
 * <li>deadTimeout when a silent pipe is declared dead, at least 1 s; unset: derived
 * <li>probation the base of a new pipe's probation; zero disables it; unset: derived
 * <li>spreadChunk the placement quantum in bytes, at least 1
 * <li>overflowDelay saturation must persist this long before spilling into a lower-preference tier
 * <li>standbyProbe busy-session probe interval for standby and less-preferred pipes; at least 1 s, default 1 s;
 *     {@link #disableStandbyProbes()} turns these probes off (such pipes then probe at probeInterval only)
 * <li>probeInterval idle pipe probe interval, measured to the carrier, at least 1 s, default 15 s;
 *     {@link #disableIdleProbes()} turns off the idle probes of pipe pairs only (answers to the peer's probes,
 *     standby probes and unpaired attachments keep probing)
 * <li>probeJitter the probe jitter j, 0 <= j < 1, default 0.25: each probe waits a uniform draw from
 *     [k_eff x (1 - j), k_eff] with k_eff = max(1 s, k - the pipe's longest record hold); the first wait of a
 *     bootstrap pipe from [0, k_eff] when j > 0; 0 gives a fixed schedule
 * <li>redundancy urgent and first-after-idle redundant copies
 * </ul>
 */
public final class MultipathConfig {
    private final int maxPipes;
    private final Duration resumeTimeout;
    private final PipeAuth auth;
    private final Scheduler scheduler;
    private final Optional<Duration> suspectFloor, deadTimeout, probation;
    private final int spreadChunk;
    private final Duration overflowDelay;
    // null: off.
    private final Duration standbyProbe, probeInterval;
    private final double probeJitter;
    private final boolean redundancy;

    /** The default probe jitter. */
    public static final double DEFAULT_PROBE_JITTER = 0.25;
    private static final MultipathConfig DEFAULTS = new MultipathConfig(1, Duration.ZERO, null, Scheduler.AFFINITY,
            Optional.empty(), Optional.empty(), Optional.empty(), io.zmux.internal.core.CoreConfig.DEFAULT_SPREAD_CHUNK,
            Duration.ofNanos(io.zmux.internal.core.CoreConfig.DEFAULT_OVERFLOW_DELAY),
            Duration.ofNanos(io.zmux.internal.core.CoreConfig.DEFAULT_STANDBY_PROBE),
            Duration.ofNanos(io.zmux.internal.core.CoreConfig.DEFAULT_PROBE_INTERVAL), DEFAULT_PROBE_JITTER, true);

    private MultipathConfig(int maxPipes, Duration resumeTimeout, PipeAuth auth, Scheduler scheduler,
                            Optional<Duration> suspectFloor, Optional<Duration> deadTimeout, Optional<Duration> probation, int spreadChunk,
                            Duration overflowDelay, Duration standbyProbe, Duration probeInterval, double probeJitter, boolean redundancy) {
        if (maxPipes < 1) throw new IllegalArgumentException("maxPipes below 1");
        Timeouts.nonNegative(resumeTimeout, "resumeTimeout");
        Objects.requireNonNull(scheduler, "scheduler");
        Objects.requireNonNull(suspectFloor, "suspectFloor").ifPresent(d -> atLeast(d, Duration.ofMillis(50), "suspectFloor"));
        Objects.requireNonNull(deadTimeout, "deadTimeout").ifPresent(d -> atLeast(d, Duration.ofSeconds(1), "deadTimeout"));
        Objects.requireNonNull(probation, "probation").ifPresent(d -> Timeouts.nonNegative(d, "probation"));
        if (spreadChunk < 1) throw new IllegalArgumentException("spreadChunk below 1");
        Timeouts.nonNegative(overflowDelay, "overflowDelay");
        if (standbyProbe != null) probeInterval(standbyProbe, "standbyProbe");
        if (probeInterval != null) probeInterval(probeInterval, "probeInterval");
        jitter(probeJitter, "probeJitter");
        this.maxPipes = maxPipes;
        this.resumeTimeout = resumeTimeout;
        this.auth = auth;
        this.scheduler = scheduler;
        this.suspectFloor = suspectFloor;
        this.deadTimeout = deadTimeout;
        this.probation = probation;
        this.spreadChunk = spreadChunk;
        this.overflowDelay = overflowDelay;
        this.standbyProbe = standbyProbe;
        this.probeInterval = probeInterval;
        this.probeJitter = probeJitter;
        this.redundancy = redundancy;
    }

    public int maxPipes() { return maxPipes; }
    public Duration resumeTimeout() { return resumeTimeout; }
    public PipeAuth auth() { return auth; }
    public Scheduler scheduler() { return scheduler; }
    public Optional<Duration> suspectFloor() { return suspectFloor; }
    public Optional<Duration> deadTimeout() { return deadTimeout; }
    public Optional<Duration> probation() { return probation; }
    public int spreadChunk() { return spreadChunk; }
    public Duration overflowDelay() { return overflowDelay; }
    /** The standby probe interval; empty: off. */
    public Optional<Duration> standbyProbe() { return Optional.ofNullable(standbyProbe); }
    /** The idle probe interval of pipe pairs; empty: off. */
    public Optional<Duration> probeInterval() { return Optional.ofNullable(probeInterval); }
    public double probeJitter() { return probeJitter; }
    public boolean redundancy() { return redundancy; }

    /** Checks a duration of at least min. */
    static Duration atLeast(Duration d, Duration min, String what) { return Timeouts.atLeast(d, min, what); }

    /** Checks a probe interval: at least 1 s (off is not a duration). */
    static Duration probeInterval(Duration d, String what) {
        if (Timeouts.nonNegative(d, what).compareTo(Duration.ofSeconds(1)) < 0) throw new IllegalArgumentException(what + " below 1s");
        return d;
    }

    /** Checks a probe jitter: at least 0 and below 1. */
    static double jitter(double j, String what) {
        if (!(j >= 0 && j < 1)) throw new IllegalArgumentException(what + " outside [0, 1)");
        return j;
    }

    private static boolean enabled(int maxPipes, Duration resume) { return maxPipes > 1 || !resume.isZero(); }

    /** Single-path defaults: one pipe, no resume. */
    public static MultipathConfig defaults() { return DEFAULTS; }

    /** The defaults with maxPipes and auth. */
    public static MultipathConfig of(int maxPipes, PipeAuth auth) { return DEFAULTS.withAuth(auth).withMaxPipes(maxPipes); }

    /** Whether sessions offer multipath (more than one pipe, or resume). */
    public boolean enabled() { return enabled(maxPipes, resumeTimeout); }

    public MultipathConfig withMaxPipes(int n) { return new MultipathConfig(n, resumeTimeout, auth, scheduler, suspectFloor, deadTimeout, probation, spreadChunk, overflowDelay, standbyProbe, probeInterval, probeJitter, redundancy); }
    public MultipathConfig withResumeTimeout(Duration d) { return new MultipathConfig(maxPipes, d, auth, scheduler, suspectFloor, deadTimeout, probation, spreadChunk, overflowDelay, standbyProbe, probeInterval, probeJitter, redundancy); }
    public MultipathConfig withAuth(PipeAuth a) { return new MultipathConfig(maxPipes, resumeTimeout, a, scheduler, suspectFloor, deadTimeout, probation, spreadChunk, overflowDelay, standbyProbe, probeInterval, probeJitter, redundancy); }
    public MultipathConfig withScheduler(Scheduler s) { return new MultipathConfig(maxPipes, resumeTimeout, auth, s, suspectFloor, deadTimeout, probation, spreadChunk, overflowDelay, standbyProbe, probeInterval, probeJitter, redundancy); }
    public MultipathConfig withSuspectFloor(Duration d) { return new MultipathConfig(maxPipes, resumeTimeout, auth, scheduler, Optional.of(d), deadTimeout, probation, spreadChunk, overflowDelay, standbyProbe, probeInterval, probeJitter, redundancy); }
    public MultipathConfig withDeadTimeout(Duration d) { return new MultipathConfig(maxPipes, resumeTimeout, auth, scheduler, suspectFloor, Optional.of(d), probation, spreadChunk, overflowDelay, standbyProbe, probeInterval, probeJitter, redundancy); }
    public MultipathConfig withProbation(Duration d) { return new MultipathConfig(maxPipes, resumeTimeout, auth, scheduler, suspectFloor, deadTimeout, Optional.of(d), spreadChunk, overflowDelay, standbyProbe, probeInterval, probeJitter, redundancy); }
    public MultipathConfig withSpreadChunk(int n) { return new MultipathConfig(maxPipes, resumeTimeout, auth, scheduler, suspectFloor, deadTimeout, probation, n, overflowDelay, standbyProbe, probeInterval, probeJitter, redundancy); }
    public MultipathConfig withOverflowDelay(Duration d) { return new MultipathConfig(maxPipes, resumeTimeout, auth, scheduler, suspectFloor, deadTimeout, probation, spreadChunk, d, standbyProbe, probeInterval, probeJitter, redundancy); }
    public MultipathConfig withStandbyProbe(Duration d) { return new MultipathConfig(maxPipes, resumeTimeout, auth, scheduler, suspectFloor, deadTimeout, probation, spreadChunk, overflowDelay, d, probeInterval, probeJitter, redundancy); }
    /** Standby probes off. */
    public MultipathConfig disableStandbyProbes() { return new MultipathConfig(maxPipes, resumeTimeout, auth, scheduler, suspectFloor, deadTimeout, probation, spreadChunk, overflowDelay, null, probeInterval, probeJitter, redundancy); }
    public MultipathConfig withProbeInterval(Duration d) { return new MultipathConfig(maxPipes, resumeTimeout, auth, scheduler, suspectFloor, deadTimeout, probation, spreadChunk, overflowDelay, standbyProbe, d, probeJitter, redundancy); }
    /** The idle probes of pipe pairs off. */
    public MultipathConfig disableIdleProbes() { return new MultipathConfig(maxPipes, resumeTimeout, auth, scheduler, suspectFloor, deadTimeout, probation, spreadChunk, overflowDelay, standbyProbe, null, probeJitter, redundancy); }
    public MultipathConfig withProbeJitter(double j) { return new MultipathConfig(maxPipes, resumeTimeout, auth, scheduler, suspectFloor, deadTimeout, probation, spreadChunk, overflowDelay, standbyProbe, probeInterval, j, redundancy); }
    public MultipathConfig withRedundancy(boolean on) { return new MultipathConfig(maxPipes, resumeTimeout, auth, scheduler, suspectFloor, deadTimeout, probation, spreadChunk, overflowDelay, standbyProbe, probeInterval, probeJitter, on); }

    @Override public boolean equals(Object o) {
        return o instanceof MultipathConfig m && maxPipes == m.maxPipes && resumeTimeout.equals(m.resumeTimeout)
                && Objects.equals(auth, m.auth) && scheduler.equals(m.scheduler) && suspectFloor.equals(m.suspectFloor)
                && deadTimeout.equals(m.deadTimeout) && probation.equals(m.probation) && spreadChunk == m.spreadChunk
                && overflowDelay.equals(m.overflowDelay) && Objects.equals(standbyProbe, m.standbyProbe)
                && Objects.equals(probeInterval, m.probeInterval) && Double.compare(probeJitter, m.probeJitter) == 0
                && redundancy == m.redundancy;
    }
    @Override public int hashCode() {
        return Objects.hash(maxPipes, resumeTimeout, auth, scheduler, suspectFloor, deadTimeout, probation, spreadChunk,
                overflowDelay, standbyProbe, probeInterval, probeJitter, redundancy);
    }
    @Override public String toString() {
        return "MultipathConfig[maxPipes=" + maxPipes + ", resumeTimeout=" + resumeTimeout + ", scheduler=" + scheduler
                + ", suspectFloor=" + suspectFloor + ", deadTimeout=" + deadTimeout + ", probation=" + probation
                + ", spreadChunk=" + spreadChunk + ", overflowDelay=" + overflowDelay + ", standbyProbe=" + standbyProbe()
                + ", probeInterval=" + probeInterval() + ", probeJitter=" + probeJitter + ", redundancy=" + redundancy + "]";
    }
}
