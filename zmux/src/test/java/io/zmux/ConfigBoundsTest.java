package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.ShapingLimits;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/** Shaping limits and probe intervals reject what they cannot honour; the exported defaults are the ones in use. */
class ConfigBoundsTest {
    @Test void paddingRatioIsAtMostOne() {
        assertDoesNotThrow(() -> ShapingLimits.none().withMaxPaddingRatio(1));
        for (double r : new double[] {1.5, -0.1, Double.NaN, Double.POSITIVE_INFINITY}) {
            assertThrows(IllegalArgumentException.class, () -> ShapingLimits.none().withMaxPaddingRatio(r), "ratio " + r);
        }
        assertThrows(IllegalArgumentException.class, () -> ShapingLimits.none().withMaxPaddingBytes(-1));
        assertThrows(IllegalArgumentException.class, () -> ShapingLimits.none().withMaxDelay(Duration.ofNanos(-1)));
    }

    @Test void sessionProbeIntervalBelowOneSecondIsInvalid() {
        MultipathConfig m = MultipathConfig.defaults();
        assertThrows(IllegalArgumentException.class, () -> m.withProbeInterval(Duration.ofMillis(500)));
        assertThrows(IllegalArgumentException.class, () -> m.withProbeInterval(Duration.ZERO));
        assertDoesNotThrow(() -> m.disableIdleProbes());
        assertDoesNotThrow(() -> m.withProbeInterval(Duration.ofSeconds(1)));
    }

    @Test void exportedDefaults() {
        assertEquals(Duration.ofSeconds(15), Timeouts.DEFAULT_KEEP_ALIVE);
        assertEquals(Duration.ofSeconds(30), Timeouts.DEFAULT_IDLE);
        assertEquals(java.util.Optional.of(Timeouts.DEFAULT_KEEP_ALIVE), Timeouts.defaults().keepAlive());
        assertEquals(java.util.Optional.of(Timeouts.DEFAULT_IDLE), Timeouts.defaults().idle());
        assertEquals(16382, RecordPlan.MAX_RECORD_LENGTH);
        assertEquals(64, RecordPlan.MIN_MAX_LENGTH);
        assertEquals(0.25, MultipathConfig.DEFAULT_PROBE_JITTER);
        assertEquals(MultipathConfig.DEFAULT_PROBE_JITTER, MultipathConfig.defaults().probeJitter());
        assertEquals(Duration.ofSeconds(60), PipePool.Backoff.DEFAULT_MAXIMUM);
        assertEquals(PipePool.Backoff.DEFAULT_MAXIMUM, PipePool.Backoff.defaults().maximum());
    }
}
