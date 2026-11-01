package io.zmux.internal.core;

/**
 * The probation history of a path (SCHEDULER.md Section 11): a pipe of fate group 0, or a
 * non-zero fate group of a joiner in one direction.
 */
final class ProbHistory {
    static final int MAX_K = 6;
    static final int RECENT = 10;

    private int k;
    int k() { return k; }
    private long endAt;
    private long endBase;
    private long uMax;
    long uMax() { return uMax; }
    private boolean ended;
    private boolean pending;
    boolean pending() { return pending; }
    private boolean trouble;
    boolean trouble() { return trouble; }
    private boolean hasUMax;
    boolean hasUMax() { return hasUMax; }

    /** Probation ended at {@code at} with backoff base {@code base}. */
    void end(long at, long base) {
        ended = true;
        endAt = at;
        endBase = base;
    }

    boolean recent(long at) {
        return ended && at <= Time.add(endAt, Time.mul(RECENT, endBase));
    }

    void troubleAt(boolean inProb, boolean upCounted, long upSince, long at) {
        if (upCounted) {
            long up = Time.sub(at, upSince);
            if (!hasUMax || up > uMax) {
                uMax = up;
                hasUMax = true;
            }
        }
        trouble = true;
        if (inProb || recent(at)) pending = true;
    }

    int nextK() {
        if (pending) {
            pending = false;
            k = Math.min(k + 1, MAX_K);
        }
        return k;
    }

    void decay(boolean quiet, long now) {
        if (quiet && !pending && (k > 0 || hasUMax) && ended && !recent(now)) {
            k = 0;
            hasUMax = false;
        }
    }
}
