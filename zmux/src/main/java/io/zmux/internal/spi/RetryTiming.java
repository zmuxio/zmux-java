package io.zmux.internal.spi;

import java.util.random.RandomGenerator;

/** Shared backoff arithmetic for session and adapter retry owners. */
public final class RetryTiming {
    private RetryTiming() {}

    public static long advance(long nominal, long ceiling) {
        return nominal > ceiling / 2 ? ceiling : 2 * nominal;
    }

    public static long sample(long nominal, double down, double up, RandomGenerator random) {
        if (down == 0 && up == 0) return nominal;
        return sample(nominal, down, up, random.nextDouble());
    }

    public static long sample(long nominal, double down, double up, double u) {
        double upper = 1 + up;
        double factor = u == 1 ? upper : Math.min(upper, 1 - down + (down + up) * u);
        return factor == 1 ? nominal : (long) (nominal * factor);
    }
}
