package io.zmux.internal.core;

/** One entry of an outbound lane's retention log. */
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
    private boolean penalized;

    /** Marks the entry re-sent for a penalty of its pipe; false when it already was or was reinjected. */
    boolean penalize() {
        if (reinjected || penalized) return false;
        penalized = true;
        return true;
    }
    /** When the batch was handed to the carrier. */
    final long sent;
    /** E: the acknowledgement time expected at sending. */
    final long due;

    Retained(long end, FrameDesc d, long sent, long due) {
        this.end = end;
        this.d = d;
        this.sent = sent;
        this.due = due;
    }
}
