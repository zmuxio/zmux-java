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
    /** When the batch was handed to the carrier. */
    final long sent;
    final Retention.Confirmation confirmation;
    /** Earliest confirmation deadline observed for this retained range. */
    private long repairDue = Time.NEVER;
    private Pipe source, repairTarget;
    private long repairEnd;
    private OwnerQueue.Link<Retained> streamLink, repairAccount;

    Pipe source() { return source; }
    Pipe repairTarget() { return repairTarget; }
    long repairDue() { return repairDue; }
    long repairEnd() { return repairEnd; }

    void index(Pipe pipe, OwnerQueue<Retained> stream, OwnerQueue<Retained> accounting) {
        source = pipe;
        streamLink = new OwnerQueue.Link<>(this);
        stream.add(streamLink);
        if (accounting != null) {
            repairAccount = new OwnerQueue.Link<>(this);
            accounting.add(repairAccount);
        }
    }

    void targetRepair(Pipe target) { repairTarget = target; }

    void repairedThrough(long offset) {
        repairTarget = null;
        repairEnd = offset;
    }

    void boundRepair(long due) { repairDue = Math.min(repairDue, due); }

    /** Releases the stream index and any in-flight copy accounting with this log entry. */
    void forgetPrefix() {
        if (streamLink != null) {
            streamLink.remove();
            streamLink = null;
        }
        if (repairAccount != null) {
            repairAccount.remove();
            repairAccount = null;
        }
        source = repairTarget = null;
    }

    Retained(long end, FrameDesc d, long sent, Retention.Confirmation confirmation) {
        this.end = end;
        this.d = d;
        this.sent = sent;
        this.confirmation = confirmation;
    }
}
