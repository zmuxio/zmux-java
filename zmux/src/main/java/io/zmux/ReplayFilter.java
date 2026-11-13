package io.zmux;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;

/**
 * Remembers keys for a window of time so that each is accepted once (single-use nonces, tokens,
 * request identifiers): {@link #check} accepts a key stamped t when t is within the window of
 * the clock and the key was not seen, and records it until t leaves the window, at most twice
 * the window. A full filter accepts nothing new; expiry costs little per check and no key is
 * scanned. Safe for concurrent use.
 *
 * <p>The filter trusts its inputs: authenticate the key and its stamp together and check only
 * what verified. An unauthenticated stamp lets a replay pick a fresh one, and unauthenticated
 * keys let anyone fill the filter.
 */
public final class ReplayFilter {
    private final long window, width, notBefore;
    private final int max;
    private final Clock clock;
    private final Bucket[] buckets;
    private int size;

    private static final class Bucket {
        long idx;
        HashSet<ByteBuffer> keys;
    }

    /**
     * @param window how far a stamp may be from the clock, at least 1 ms
     * @param max the keys held, at least 1
     * @param notBefore stamps before it are {@link ReplayVerdict#OUTSIDE}, such as the process start, so
     *     that what was sent to an earlier process is not accepted again; null: none
     * @param clock the filter's clock; null: the system's
     */
    public ReplayFilter(Duration window, int max, Instant notBefore, Clock clock) {
        this.window = Objects.requireNonNull(window, "window").toMillis();
        if (this.window <= 0) throw new IllegalArgumentException("window below 1 ms");
        if (max <= 0) throw new IllegalArgumentException("max below 1");
        this.max = max;
        this.notBefore = notBefore == null ? Long.MIN_VALUE : notBefore.toEpochMilli();
        this.clock = clock == null ? Clock.systemUTC() : clock;
        width = Math.max(this.window / 4, 1);
        // A key lives in the bucket of t + window: those from now to now + 2 window, plus one.
        int n = Math.toIntExact((2 * this.window + width - 1) / width + 2);
        buckets = new Bucket[n];
        for (int i = 0; i < n; i++) buckets[i] = new Bucket();
    }

    /** A filter of the system clock without a lower bound. */
    public ReplayFilter(Duration window, int max) {
        this(window, max, null, null);
    }

    /** Checks key stamped tMillis (Unix milliseconds) and records a fresh key; both must have been authenticated together. */
    public synchronized ReplayVerdict check(byte[] key, long tMillis) {
        Objects.requireNonNull(key, "key");
        long now = clock.millis();
        if (tMillis < now - window || tMillis > now + window || tMillis < notBefore) return ReplayVerdict.OUTSIDE;
        long low = Math.floorDiv(now, width), idx = Math.floorDiv(tMillis + window, width);
        for (Bucket b : buckets) {
            if (b.keys != null && b.idx < low) {
                size -= b.keys.size();
                b.keys = null;
            }
        }
        // A key with the same stamp sits in one bucket, but a replay may carry another stamp.
        ByteBuffer k = ByteBuffer.wrap(key);
        for (Bucket b : buckets) if (b.keys != null && b.keys.contains(k)) return ReplayVerdict.SEEN;
        if (size >= max) return ReplayVerdict.FULL;
        Bucket b = buckets[(int) Math.floorMod(idx, (long) buckets.length)];
        if (b.keys == null) {
            b.idx = idx;
            b.keys = new HashSet<>();
        }
        b.keys.add(ByteBuffer.wrap(key.clone()));
        size++;
        return ReplayVerdict.FRESH;
    }

    /** The keys held. */
    public synchronized int size() { return size; }
}
