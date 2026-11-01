package io.zmux.internal.core;

/** One entry of an outbound lane's retention log (MULTIPATH.md Section 7.4). */
final class Retained {
    /** Lane position after the record carrying the frame. */
    final long end;
    final FrameDesc d;
    private boolean reinjected;
    boolean reinjected() { return reinjected; }

    /** Marks the entry reinjected; false when it already was (each entry is sent again once). */
    boolean reinject() {
        if (reinjected) return false;
        reinjected = true;
        return true;
    }
    /** When the batch was handed to the carrier. */
    final long sent;
    /** E: the acknowledgement time expected at sending (MULTIPATH.md 7.5.1). */
    final long due;

    Retained(long end, FrameDesc d, long sent, long due) {
        this.end = end;
        this.d = d;
        this.sent = sent;
        this.due = due;
    }
}
