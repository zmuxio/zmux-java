package io.zmux.internal.core;

/** A fixed-memory byte budget for optional copies of reliable and unreliable data. */
final class CopyBudget {
    private static final int WINDOW = 10;
    private static final int BUCKETS = WINDOW + 1;
    private final long[] seconds = new long[BUCKETS];
    private final long[] originals = new long[BUCKETS];
    private final long[] copies = new long[BUCKETS];
    private final int percent;
    private final long origin;
    private long burst = -1;

    CopyBudget(int percent, long origin) {
        this.percent = percent;
        this.origin = origin;
        java.util.Arrays.fill(seconds, -1);
    }

    void negotiateBurst(long bytes) {
        if (burst < 0) burst = bytes;
    }

    boolean enabled() { return percent != 0; }

    private long second(long now) { return Time.sub(now, origin) / Time.SECOND; }

    private int bucket(long now) {
        long second = second(now);
        int index = (int) (second % BUCKETS);
        if (seconds[index] != second) {
            seconds[index] = second;
            originals[index] = copies[index] = 0;
        }
        return index;
    }

    void original(long now, long bytes) {
        int index = bucket(now);
        originals[index] = Time.add(originals[index], bytes);
    }

    long remaining(long now) {
        if (!enabled()) return 0;
        long second = second(now), original = 0, copied = 0;
        for (int i = 0; i < BUCKETS; i++) {
            if (seconds[i] < 0) continue;
            long age = second - seconds[i];
            if (age < WINDOW) original = Time.add(original, originals[i]);
            if (age <= WINDOW) copied = Time.add(copied, copies[i]);
        }
        long allowance = original / 100 * percent + original % 100 * percent / 100;
        return Math.max(0, Time.add(allowance - copied, Math.max(0, burst)));
    }

    /** The next instant at which an outstanding copy leaves the rolling budget. */
    long nextRefill(long now) {
        long second = second(now);
        long next = Time.NEVER;
        for (int i = 0; i < BUCKETS; i++) {
            if (copies[i] == 0 || second - seconds[i] > WINDOW) continue;
            next = Math.min(next, Time.add(origin, Time.mul(Time.add(seconds[i], WINDOW + 1), Time.SECOND)));
        }
        return next;
    }

    void copy(long now, long bytes) {
        int index = bucket(now);
        copies[index] = Time.add(copies[index], bytes);
    }
}
