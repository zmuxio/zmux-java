package io.zmux.internal.core;

/**
 * The CoDel state of one datagram queue (RFC 8289, Section 5): the sojourn time of a datagram leaving the queue
 * decides, a condemned datagram is dropped instead of delivered, and the drop rate of the dropping state grows with
 * the square root of its drops. Times are session-clock nanoseconds; owned by the session's loop.
 */
final class CoDel {
    /** The interval: a queue whose sojourns stay above the target this long starts dropping. */
    static final long INTERVAL = 100 * Time.MILLISECOND;

    /** When a sojourn above the target first counts as standing; 0: below it. */
    private long firstAbove;
    private long dropNext;
    private int count;
    private int lastCount;
    private boolean dropping;

    // The state, for tests.
    int count() { return count; }
    boolean dropping() { return dropping; }
    long dropNext() { return dropNext; }

    /**
     * Decides on a datagram queued at {@code at} that leaves its queue at {@code now}: true drops it. A target of 0
     * or less turns CoDel off; alone, the datagram is the only one queued, which is never condemned.
     */
    boolean drop(long at, long now, long target, boolean alone) {
        if (target <= 0) return false;
        boolean ok = okToDrop(at, now, target, alone);
        if (dropping) {
            if (!ok) {
                dropping = false;
                return false;
            }
            if (now < dropNext) return false;
            count++;
            dropNext = controlLaw(dropNext, count);
            return true;
        }
        if (!ok) return false;
        dropping = true;
        // Back to dropping soon after leaving it: resume near the last rate.
        int delta = count - lastCount;
        count = delta > 1 && Time.sub(now, dropNext) < 16 * INTERVAL ? delta : 1;
        dropNext = controlLaw(now, count);
        lastCount = count;
        return true;
    }

    /** The queue emptied: the standing-queue state ends. */
    void idle() {
        firstAbove = 0;
        dropping = false;
    }

    /** Whether sojourns have stood above the target for an interval. */
    private boolean okToDrop(long at, long now, long target, boolean alone) {
        if (alone || Time.sub(now, at) < target) {
            firstAbove = 0;
            return false;
        }
        if (firstAbove == 0) {
            firstAbove = Time.add(now, INTERVAL);
            return false;
        }
        return now >= firstAbove;
    }

    /** When the next drop of the dropping state falls: interval / sqrt(count) after t. */
    private static long controlLaw(long t, int count) {
        return Time.add(t, (long) (INTERVAL / Math.sqrt(Math.max(count, 1))));
    }
}
