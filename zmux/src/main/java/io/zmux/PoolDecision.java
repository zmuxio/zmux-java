package io.zmux;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/**
 * The answer of a {@link PoolPolicy}. Every item left unset keeps the pool's own behaviour, so {@link #KEEP}, where a
 * decision starts, changes nothing; the {@code with} methods set items and throw {@link IllegalArgumentException} for
 * a value out of range. Durations are durations, not points in time, so a policy reads no clock of its own.
 *
 * @param count the members to keep, at least 1; members above it close gracefully, those in retirePipes (or
 *     retireConnections) first, then the longest idle. Unset: the pool's configured count
 * @param dialAfter a minimum wait from the start of the last dial attempt (the start of the pool before the first)
 *     to the next dial, redial or replacement. The pool also honours the sampled backoff
 * @param retirePipes members of a pipe pool to replace now, make before break, by pipe ID; a member that is not usable
 *     or already being replaced is skipped
 * @param retireConnections connections of a connection pool to retire, by fate group
 * @param recheck look again no later than this after the decision, even when nothing happens; positive, its effect
 *     bounded below by {@link #MIN_RECHECK} and not above. Unset: no extra look
 * @param liveness set on the pipes the pool dials after this decision; a setting it leaves to inherit keeps the
 *     pool's options', else the session's. A pool that dials no pipes ignores it
 */
public record PoolDecision(OptionalInt count, Optional<Duration> dialAfter, Set<PipeId> retirePipes, Set<Long> retireConnections,
                           Optional<Duration> recheck, PipeLiveness liveness) {
    /** The decision that changes nothing. */
    public static final PoolDecision KEEP = new PoolDecision(OptionalInt.empty(), Optional.empty(), Set.of(), Set.of(), Optional.empty(),
            PipeLiveness.inherit());
    /** The shortest recheck a pool honours, so that a policy cannot make it spin. */
    public static final Duration MIN_RECHECK = Duration.ofMillis(10);

    public PoolDecision {
        Objects.requireNonNull(count);
        if (count.isPresent() && count.getAsInt() < 1) throw new IllegalArgumentException("count below 1");
        Objects.requireNonNull(dialAfter);
        if (dialAfter.isPresent() && dialAfter.get().isNegative()) throw new IllegalArgumentException("negative dialAfter");
        retirePipes = Set.copyOf(retirePipes);
        retireConnections = Set.copyOf(retireConnections);
        Objects.requireNonNull(recheck);
        if (recheck.isPresent() && (recheck.get().isNegative() || recheck.get().isZero()))
            throw new IllegalArgumentException("recheck must be positive");
        Objects.requireNonNull(liveness);
    }

    /** The members to keep, at least 1. */
    public PoolDecision withCount(int n) {
        return new PoolDecision(OptionalInt.of(n), dialAfter, retirePipes, retireConnections, recheck, liveness);
    }

    /** The minimum wait from the start of the last dial attempt to the next, zero or more. */
    public PoolDecision withDialAfter(Duration d) {
        return new PoolDecision(count, Optional.of(Objects.requireNonNull(d, "dialAfter")), retirePipes, retireConnections, recheck, liveness);
    }

    public PoolDecision withRetirePipes(Set<PipeId> ids) {
        return new PoolDecision(count, dialAfter, ids, retireConnections, recheck, liveness);
    }

    public PoolDecision withRetireConnections(Set<Long> fateGroups) {
        return new PoolDecision(count, dialAfter, retirePipes, fateGroups, recheck, liveness);
    }

    /** Look again no later than d after this decision, a positive duration. */
    public PoolDecision withRecheck(Duration d) {
        return new PoolDecision(count, dialAfter, retirePipes, retireConnections, Optional.of(Objects.requireNonNull(d, "recheck")), liveness);
    }

    public PoolDecision withLiveness(PipeLiveness l) {
        return new PoolDecision(count, dialAfter, retirePipes, retireConnections, recheck, l);
    }

    /** The recheck in nanoseconds with {@link #MIN_RECHECK} applied, saturated; 0: none. */
    public long recheckNanos() {
        return recheck.map(d -> Math.max(io.zmux.internal.core.Time.nanos(d), MIN_RECHECK.toNanos())).orElse(0L);
    }

    /** The policy's minimum dial wait in nanoseconds, saturated. */
    public long dialAfterNanos() {
        return dialAfter.map(io.zmux.internal.core.Time::nanos).orElse(0L);
    }

}
