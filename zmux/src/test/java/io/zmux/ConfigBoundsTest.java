package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.WireAccount;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/** Shaping limits and probe intervals reject what they cannot honour; the exported defaults are the ones in use. */
class ConfigBoundsTest {
    @Test void anAccountFieldAboveItsMaximumIsInvalid() {
        assertDoesNotThrow(() -> new WireAccount(WireAccount.MAX_INITIAL, WireAccount.MAX_RATE, WireAccount.MAX_RATE));
        assertThrows(IllegalArgumentException.class, () -> new WireAccount(WireAccount.MAX_INITIAL + 1, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new WireAccount(0, WireAccount.MAX_RATE + 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new WireAccount(0, 0, WireAccount.MAX_RATE + 1));
        assertThrows(IllegalArgumentException.class, () -> new WireAccount(-1, 0, 0));
    }

    @Test void sessionProbeIntervalBelowOneSecondIsInvalid() {
        var m = ZmuxConfig.builder();
        assertThrows(IllegalArgumentException.class, () -> m.probeInterval(Duration.ofMillis(500)));
        assertThrows(IllegalArgumentException.class, () -> m.probeInterval(Duration.ZERO));
        assertDoesNotThrow(() -> m.disableIdleProbes());
        assertDoesNotThrow(() -> m.probeInterval(Duration.ofSeconds(1)));
    }

    @Test void exportedDefaults() {
        assertEquals(Duration.ofSeconds(15), ZmuxConfig.DEFAULT_KEEP_ALIVE_PERIOD);
        assertEquals(Duration.ofSeconds(30), ZmuxConfig.DEFAULT_MAX_IDLE_TIMEOUT);
        assertEquals(java.util.Optional.of(ZmuxConfig.DEFAULT_KEEP_ALIVE_PERIOD), ZmuxConfig.defaults().keepAlivePeriod());
        assertEquals(java.util.Optional.of(ZmuxConfig.DEFAULT_MAX_IDLE_TIMEOUT), ZmuxConfig.defaults().maxIdleTimeout());
        assertEquals(16382, RecordPlan.MAX_RECORD_LENGTH);
        assertEquals(64, RecordPlan.MIN_MAX_LENGTH);
        assertEquals(0.25, ZmuxConfig.DEFAULT_PROBE_JITTER);
        assertEquals(ZmuxConfig.DEFAULT_PROBE_JITTER, ZmuxConfig.defaults().probeJitter());
        assertEquals(Duration.ofSeconds(10), ZmuxConfig.DEFAULT_JOIN_TIMEOUT);
        assertEquals(ZmuxConfig.DEFAULT_JOIN_TIMEOUT, ZmuxConfig.defaults().joinTimeout());
        assertEquals(Duration.ofSeconds(1), ZmuxConfig.DEFAULT_MIN_BACKOFF);
        assertEquals(Duration.ofSeconds(60), ZmuxConfig.DEFAULT_MAX_BACKOFF);
        assertEquals(ZmuxConfig.DEFAULT_MIN_BACKOFF, ZmuxConfig.defaults().redialMinBackoff());
        assertEquals(ZmuxConfig.DEFAULT_MAX_BACKOFF, ZmuxConfig.defaults().redialMaxBackoff());
        assertEquals(ZmuxConfig.DEFAULT_MIN_BACKOFF, PipePool.Backoff.defaults().minimum());
        assertEquals(ZmuxConfig.DEFAULT_MAX_BACKOFF, PipePool.Backoff.defaults().maximum());
    }
}
