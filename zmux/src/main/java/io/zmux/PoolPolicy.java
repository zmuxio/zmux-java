package io.zmux;

/**
 * Policy for a physical connection pool. Each decision uses the current pool view and
 * applies atomically to the pool owner.
 */
@FunctionalInterface
public interface PoolPolicy {
    /** The decision for this look, starting from {@link PoolDecision#KEEP}. */
    PoolDecision decide(PoolView view);
}
