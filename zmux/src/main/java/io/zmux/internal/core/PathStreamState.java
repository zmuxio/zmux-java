package io.zmux.internal.core;

/**
 * The path-side state of a stream direction: the scheduler's part (own policy, affinity binding, round-robin values,
 * custom policy answer) and the retention part (acknowledged and lost ranges). Allocated on first use (L1); each part is
 * written only by its component.
 */
final class PathStreamState {
    final PathScheduler.StreamSlot sched = new PathScheduler.StreamSlot();
    final Retention.StreamSlot ret;

    PathStreamState(SendHistory history, Stream stream) {
        ret = new Retention.StreamSlot(history, stream);
    }
    /** The pipe of the opening frame or of its latest reinjected copy: the stream's datagrams follow it (MULTIPATH.md S7). */
    private Pipe openPipe;
    /** The opening frame (empty or not) was acknowledged. */
    private boolean openAcked;

    /** The opening frame, or a reinjected copy of it, went out on p. */
    void openedOn(Pipe p) {
        openPipe = p;
    }

    /** A STREAM frame at offset 0, the opening or a copy of it, was acknowledged. */
    void openingAcked() {
        openAcked = true;
        openPipe = null;
    }

    boolean openingConfirmed() { return openAcked; }

    /** The pipe of the unacknowledged opening frame, or null. */
    Pipe openingPipe() {
        return openAcked ? null : openPipe;
    }
}
