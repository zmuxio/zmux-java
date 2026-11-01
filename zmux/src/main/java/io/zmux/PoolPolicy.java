package io.zmux;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Set;

/**
 * Decides the members a pool keeps and when it dials: the pipes of a {@link PipePool}, asked on
 * every pass of the maintenance run, or the connections of a connection pool, asked when a pipe
 * is taken or released, and again no later than a decision's {@link Decision#recheck} after it.
 * {@link #decide} must not block; an exception from it applies
 * {@link Decision#KEEP}, and the run bounds the values it returns.
 */
@FunctionalInterface
public interface PoolPolicy {
    Decision decide(View view);

    /**
     * The state of the pool at one pass.
     *
     * @param members the pool's members, replacements and retiring connections excluded
     * @param failures consecutive failed dials
     * @param sinceLastDial time since the last dial started; null: none yet
     */
    record View(List<Member> members, int failures, Duration sinceLastDial) {
        public View { members = List.copyOf(members); }
    }

    /**
     * A pipe of the pool, or a connection of a connection pool.
     *
     * @param id the pipe ID, or the connection's fate group
     * @param age time since the pool added it
     * @param idle for a pipe its {@link PipeStats#sinceLastTraffic()}; for a connection zero
     *     while a pipe uses it, else the time since its last pipe ended, or since it was
     *     established if it never carried one
     * @param pipes 1 for a pipe; the pipes on a connection
     * @param bytesSent 0 for a connection
     * @param bytesReceived 0 for a connection
     * @param usable it serves the pool's directions; a connection takes new pipes
     * @param replacing a replacement of it is dialled or joined
     */
    record Member(long id, Duration age, Duration idle, int pipes, long bytesSent, long bytesReceived, boolean usable,
                  boolean replacing) {}

    /**
     * The answer to {@link #decide}; each item left unset keeps the pool's own behaviour. Start
     * from {@link #KEEP} and set items with the {@code with} methods.
     *
     * @param count pipes to keep, at least 1; unset: {@link PipePool#count()}. Pipes beyond it
     *     close, those in {@code retire} first, then the longest idle
     * @param dialAfter the wait from the start of the last dial to the next one, zero or more; it
     *     replaces the pool's backoff and is bounded only by the backoff maximum; unset: the
     *     pool's backoff
     * @param retire pipes to replace now, make before break
     * @param recheck the latest time after this decision at which the pool asks again, even
     *     without events, raised to {@link #MIN_RECHECK} (zero: as soon as that allows); unset: none
     *     (the pool's own triggers)
     * @param probeInterval the {@link PipeOptions.Builder#probeInterval probe interval} of the
     *     pipes the pool creates after this decision, at least 1 s; unset: the pool's options
     *     (or off with idleProbesOff). A pool that dials no pipes ignores it
     * @param idleProbesOff those pipes have their {@link PipeOptions.Builder#disableIdleProbes idle
     *     probes off} ({@link #disableIdleProbes()}); only without probeInterval
     * @param probeJitter the {@link PipeOptions.Builder#probeJitter probe jitter} of those
     *     pipes; unset: the pool's options
     */
    record Decision(OptionalInt count, Optional<Duration> dialAfter, Set<Long> retire, Optional<Duration> recheck,
                    Optional<Duration> probeInterval, boolean idleProbesOff, OptionalDouble probeJitter) {
        public static final Decision KEEP = new Decision(OptionalInt.empty(), Optional.empty(), Set.of(), Optional.empty(),
                Optional.empty(), false, OptionalDouble.empty());
        /** The least recheck a pool honours, against busy rechecking. */
        public static final Duration MIN_RECHECK = Duration.ofMillis(10);

        public Decision {
            Objects.requireNonNull(count);
            if (count.isPresent() && count.getAsInt() < 1) throw new IllegalArgumentException("count below 1");
            Objects.requireNonNull(dialAfter);
            if (dialAfter.isPresent() && dialAfter.get().isNegative()) throw new IllegalArgumentException("negative dialAfter");
            retire = Set.copyOf(retire);
            Objects.requireNonNull(recheck);
            if (recheck.isPresent() && recheck.get().isNegative()) throw new IllegalArgumentException("negative recheck");
            Objects.requireNonNull(probeInterval);
            probeInterval.ifPresent(d -> MultipathConfig.probeInterval(d, "probeInterval"));
            if (idleProbesOff && probeInterval.isPresent()) throw new IllegalArgumentException("probeInterval with idle probes off");
            Objects.requireNonNull(probeJitter);
            if (probeJitter.isPresent()) MultipathConfig.jitter(probeJitter.getAsDouble(), "probeJitter");
        }

        public Decision withCount(int n) { return new Decision(OptionalInt.of(n), dialAfter, retire, recheck, probeInterval, idleProbesOff, probeJitter); }
        public Decision withDialAfter(Duration d) { return new Decision(count, Optional.of(d), retire, recheck, probeInterval, idleProbesOff, probeJitter); }
        public Decision withRetire(Set<Long> ids) { return new Decision(count, dialAfter, ids, recheck, probeInterval, idleProbesOff, probeJitter); }
        public Decision withRecheck(Duration d) { return new Decision(count, dialAfter, retire, Optional.of(d), probeInterval, idleProbesOff, probeJitter); }
        public Decision withProbeInterval(Duration d) { return new Decision(count, dialAfter, retire, recheck, Optional.of(d), false, probeJitter); }
        /** The idle probes of the pipes created after this decision off. */
        public Decision disableIdleProbes() { return new Decision(count, dialAfter, retire, recheck, Optional.empty(), true, probeJitter); }
        public Decision withProbeJitter(double j) { return new Decision(count, dialAfter, retire, recheck, probeInterval, idleProbesOff, OptionalDouble.of(j)); }

        /** The recheck delay a pool waits in nanoseconds, the floor applied; 0: none. */
        public long recheckNanos() {
            if (recheck.isEmpty()) return 0;
            long n;
            try { n = recheck.get().toNanos(); } catch (ArithmeticException tooLong) { n = Long.MAX_VALUE; }
            return Math.max(n, MIN_RECHECK.toNanos());
        }
    }
}
