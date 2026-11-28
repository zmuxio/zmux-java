package io.zmux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

class BackoffTest {
    @Test void includesBothJitterEndpoints() {
        Duration nominal = Duration.ofNanos(100);
        var backoff = new Backoff(nominal, nominal, 1, 1);
        assertEquals(0, backoff.sample(100, sample(0)));
        assertEquals(100, backoff.sample(100, sample(0.5)));
        assertEquals(200, backoff.sample(100, sample(1)));
    }

    @Test void explicitZeroDisablesJitterAndFractionalNanosecondsRoundDown() {
        long exact = (1L << 53) + 1;
        Duration nominal = Duration.ofNanos(exact);
        assertEquals(0.2, new Backoff(nominal, nominal).jitter());
        assertEquals(exact, new Backoff(nominal, nominal, 0).sample(exact,
                () -> { throw new AssertionError("no draw for disabled jitter"); }));
        assertEquals(exact, new Backoff(nominal, nominal, .5, .5).sample(exact, sample(.5)));
        assertEquals(1, new Backoff(nominal, nominal, .5).sample(3, sample(0)));
    }

    @Test void rejectsAnUnrepresentableMaximumWait() {
        Duration max = Duration.ofNanos(Long.MAX_VALUE / 2 + 1);
        assertThrows(IllegalArgumentException.class, () -> new Backoff(max, max, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new Backoff(max, max, Double.NaN, 0));
    }

    private static RandomGenerator sample(double value) {
        return new RandomGenerator() {
            @Override public double nextDouble() { return value; }
            @Override public long nextLong() { throw new AssertionError("expected one double sample"); }
        };
    }
}
