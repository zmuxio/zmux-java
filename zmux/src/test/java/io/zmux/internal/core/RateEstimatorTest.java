package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** The windowed maximum of R_p, with explicit times and rounds. */
class RateEstimatorTest {
    private static final double FAST = 5e6, SLOW = 3e4;
    private static final long S = Time.SECOND;

    /** A bottleneck (queue-proven) sample. */
    private static double proven(RateEstimator e, double rate, long now) {
        return e.sample(rate, false, e.blockEpoch(), true, now);
    }

    @Test void aPipeThatSlowsDownLosesItsOldRateWhenTheWindowPasses() {
        var e = new RateEstimator();
        proven(e, FAST, S);
        proven(e, SLOW, 2 * S);
        assertEquals(FAST, e.value(), "the fast sample is in the window");
        proven(e, SLOW, 10 * S);
        assertEquals(FAST, e.value(), "10 s after the fast sample it is still in");
        proven(e, SLOW, 12 * S);
        assertEquals(SLOW, e.value(), "only the slow samples are left");
    }

    @Test void theRoundCountAgesTheMaximumToo() {
        var e = new RateEstimator();
        proven(e, FAST, S);
        for (long cum = 1; cum <= 11; cum++) assertTrue(e.advance(cum, cum));
        proven(e, SLOW, 2 * S);
        assertEquals(SLOW, e.value(), "ten rounds later the fast sample is out");
    }

    @Test void withoutAnyAcceptedSampleTheLastValueIsKept() {
        var e = new RateEstimator();
        proven(e, FAST, S);
        // Application-limited samples below R_p do not enter, however long the window has passed.
        assertEquals(0, e.sample(SLOW, true, e.blockEpoch(), false, 60 * S));
        assertEquals(FAST, e.value());
    }

    @Test void anApplicationLimitedSampleEntersOnlyAboveTheCurrentRate() {
        var e = new RateEstimator();
        proven(e, SLOW, S);
        assertEquals(0, e.sample(FAST, true, e.blockEpoch(), false, 2 * S));
        assertEquals(FAST, e.value(), "raised, not reported as a bottleneck sample");
    }
}
