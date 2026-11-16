package io.zmux.internal.core;

/**
 * The path-side state of a stream direction: the scheduler's part (own policy, affinity binding, round-robin values,
 * custom policy answer) and the retention part (acknowledged and lost ranges). Allocated on first use (L1); each part is
 * written only by its component.
 */
final class PathStreamState {
    final PathScheduler.StreamSlot sched = new PathScheduler.StreamSlot();
    final Retention.StreamSlot ret = new Retention.StreamSlot();
    /** The pipe that carried the stream's opening frame: its datagrams follow it there until the opening is acknowledged. */
    private Pipe openPipe;

    /** The opening frame went out on p. */
    void openedOn(Pipe p) {
        openPipe = p;
    }

    /** The pipe that carried the opening frame while it is unacknowledged (send queue base past 0: acknowledged), else null. */
    Pipe openingPipe(long sendBase) {
        if (openPipe != null && ((ret.acked().size() > 0 && ret.acked().lo(0) == 0) || sendBase > 0 || ret.finAcked())) openPipe = null;
        return openPipe;
    }
}
