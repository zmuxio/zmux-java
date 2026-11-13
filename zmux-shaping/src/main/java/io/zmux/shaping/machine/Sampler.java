package io.zmux.shaping.machine;

import java.security.SecureRandom;
import java.util.random.RandomGenerator;
import java.util.random.RandomGeneratorFactory;

/**
 * Integer draws from a random source: a value from a table of segments, a probability in parts per million, a jitter.
 * Each uses the high 64 bits of a 128-bit product, so that all implementations draw alike and spend the same words.
 * It is as thread-safe as its source.
 */
public final class Sampler {
    private static final RandomGeneratorFactory<RandomGenerator> DEFAULT = RandomGeneratorFactory.of("L64X256MixRandom");
    /** Bytes covering the whole L64X256MixRandom state: a, s and the 256-bit xoshiro part. */
    private static final int SEED_BYTES = (64 + 64 + 256) / 8;
    private static final SecureRandom SEEDS = new SecureRandom();
    private static final long MILLION = 1_000_000;
    /** floor(2^64 / 10^6). */
    private static final long JITTER_SCALE = 18446744073709L;

    private final RandomGenerator source;

    public Sampler(RandomGenerator source) { this.source = source; }

    /** A sampler of a generator whose full state is seeded from {@link SecureRandom}. */
    public static Sampler seeded() {
        byte[] seed = new byte[SEED_BYTES];
        SEEDS.nextBytes(seed);
        return new Sampler(DEFAULT.create(seed));
    }

    /** The options' source, or {@link #seeded}. */
    static Sampler of(MachineOptions o) {
        RandomGenerator r = o.newSource() == null ? null : o.newSource().get();
        return r != null ? new Sampler(r) : seeded();
    }

    /** One word of the source, unsigned. */
    public long next() { return source.nextLong(); }

    /**
     * A value of a table, flat as weight, lo, hi of each segment. One segment takes no word to choose, and a segment of
     * one value none to draw from.
     */
    public long draw(long[] segments) {
        int at = 0;
        if (segments.length > 3) {
            long total = 0;
            for (int i = 0; i < segments.length; i += 3) total += segments[i];
            long c = Math.unsignedMultiplyHigh(next(), total), sum = 0;
            for (int i = 0; i < segments.length; i += 3) {
                sum += segments[i];
                if (c < sum) { at = i; break; }
            }
        }
        long lo = segments[at + 1], hi = segments[at + 2];
        return lo == hi ? lo : lo + Math.unsignedMultiplyHigh(next(), hi - lo + 1);
    }

    /** Whether an event of probability p parts per million happens; 0 and 10^6 take no word. */
    public boolean prob(long p) {
        if (p == 0) return false;
        if (p >= MILLION) return true;
        return Math.unsignedMultiplyHigh(next(), MILLION) < p;
    }

    /** v lowered by a random part of up to j parts per million, j below 10^6; 0 takes no word. */
    public long jitter(long v, long j) {
        return j == 0 ? v : jitter(v, j, next());
    }

    /** {@link #jitter(long, long)} with the word u. */
    static long jitter(long v, long j, long u) {
        if (j == 0) return v;
        long m = Math.unsignedMultiplyHigh(v, j * JITTER_SCALE);
        return v - Math.unsignedMultiplyHigh(u, m + 1);
    }
}
