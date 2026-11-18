package io.zmux;

/**
 * Application code a pool asks, at each of its looks, for a {@link PoolDecision} on a {@link PoolView} of its
 * members: the pipes of a {@link PipePool}, or the connections of a connection pool. A pool looks at once when a pipe
 * of the session changes state or the session is suspended or resumed, after each dial attempt ends, no later than
 * the {@link PoolDecision#recheck() recheck} of the last decision, and every 100 ms (the maintenance interval).
 *
 * <p>A policy is an object, so it may keep state between looks. {@link #decide} runs on the pool's task, must return
 * promptly and must not block or call into the session. A call that throws anything but a {@link VirtualMachineError} (among them the
 * {@link IllegalArgumentException} of building an invalid decision, or an {@link Error}) or returns null is rejected as a whole: the pool
 * applies {@link PoolDecision#KEEP} for that look, hands the error to its rejection callback
 * ({@link PipePool#withDecisionRejected}) or, without one, logs it, and keeps maintaining its members.
 */
@FunctionalInterface
public interface PoolPolicy {
    /** The decision for this look, starting from {@link PoolDecision#KEEP}. */
    PoolDecision decide(PoolView view);
}
