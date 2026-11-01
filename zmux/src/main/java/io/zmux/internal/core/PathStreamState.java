package io.zmux.internal.core;

/**
 * The path-side state of a stream direction: the scheduler's part (own policy, affinity binding, round-robin values,
 * custom policy answer) and the retention part (acknowledged and lost ranges). Allocated on first use (L1); each part is
 * written only by its component.
 */
final class PathStreamState {
    final PathScheduler.StreamSlot sched = new PathScheduler.StreamSlot();
    final Retention.StreamSlot ret = new Retention.StreamSlot();
}
