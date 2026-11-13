package io.zmux;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
     * @param pipeId the pipe of a pipe pool's member; null for a connection
     * @param fateGroup the fate group of a connection pool's connection; 0 for a pipe
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
    record Member(PipeId pipeId, long fateGroup, Duration age, Duration idle, int pipes, long bytesSent, long bytesReceived,
                  boolean usable, boolean replacing) {}

    /**
     * The answer to {@link #decide}; each item left unset keeps the pool's own behaviour. Start
     * from {@link #KEEP} and set items with the {@code with} methods.
     *
     * @param count members to keep, at least 1; unset: {@link PipePool#count()}. Members beyond it
     *     close, those retired first, then the longest idle
     * @param dialAfter the wait from the start of the last dial to the next one, zero or more; it
     *     replaces the pool's backoff and is bounded only by the backoff maximum; unset: the
     *     pool's backoff
     * @param retirePipes the pipes of a pipe pool to replace now, make before break
     * @param retireConnections the connections of a connection pool to retire, by fate group
     * @param recheck the latest time after this decision at which the pool asks again, even
     *     without events, raised to {@link #MIN_RECHECK} (zero: as soon as that allows); unset: none
     *     (the pool's own triggers)
     * @param liveness set on the pipes the pool dials after this decision, over its options'
     *     ({@link PipeOptions.Builder#liveness}): a setting it leaves unwritten keeps the pool's,
     *     else the session's. A pool that dials no pipes ignores it
     */
    record Decision(OptionalInt count, Optional<Duration> dialAfter, Set<PipeId> retirePipes, Set<Long> retireConnections,
                    Optional<Duration> recheck, PipeLiveness liveness) {
        public static final Decision KEEP = new Decision(OptionalInt.empty(), Optional.empty(), Set.of(), Set.of(), Optional.empty(),
                PipeLiveness.inherit());
        /** The least recheck a pool honours, against busy rechecking. */
        public static final Duration MIN_RECHECK = Duration.ofMillis(10);

        public Decision {
            Objects.requireNonNull(count);
            if (count.isPresent() && count.getAsInt() < 1) throw new IllegalArgumentException("count below 1");
            Objects.requireNonNull(dialAfter);
            if (dialAfter.isPresent() && dialAfter.get().isNegative()) throw new IllegalArgumentException("negative dialAfter");
            retirePipes = Set.copyOf(retirePipes);
            retireConnections = Set.copyOf(retireConnections);
            Objects.requireNonNull(recheck);
            if (recheck.isPresent() && recheck.get().isNegative()) throw new IllegalArgumentException("negative recheck");
            Objects.requireNonNull(liveness);
        }

        public Decision withCount(int n) { return new Decision(OptionalInt.of(n), dialAfter, retirePipes, retireConnections, recheck, liveness); }
        public Decision withDialAfter(Duration d) { return new Decision(count, Optional.of(d), retirePipes, retireConnections, recheck, liveness); }
        public Decision withRetirePipes(Set<PipeId> ids) { return new Decision(count, dialAfter, ids, retireConnections, recheck, liveness); }
        public Decision withRetireConnections(Set<Long> fateGroups) { return new Decision(count, dialAfter, retirePipes, fateGroups, recheck, liveness); }
        public Decision withRecheck(Duration d) { return new Decision(count, dialAfter, retirePipes, retireConnections, Optional.of(d), liveness); }
        public Decision withLiveness(PipeLiveness l) { return new Decision(count, dialAfter, retirePipes, retireConnections, recheck, l); }

        /** The recheck delay a pool waits, the floor applied; empty: none. */
        public Optional<Duration> recheckDelay() {
            return recheck.map(d -> d.compareTo(MIN_RECHECK) < 0 ? MIN_RECHECK : d);
        }
    }
}
