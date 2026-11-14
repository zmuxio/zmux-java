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
        var m = Liveness.builder();
        assertThrows(IllegalArgumentException.class, () -> m.probeInterval(Duration.ofMillis(500)));
        assertThrows(IllegalArgumentException.class, () -> m.probeInterval(Duration.ZERO));
        assertDoesNotThrow(() -> m.noProbeInterval());
        assertDoesNotThrow(() -> m.probeInterval(Duration.ofSeconds(1)));
    }

    @Test void exportedDefaults() {
        assertEquals(Duration.ofSeconds(15), Liveness.DEFAULT_KEEP_ALIVE_PERIOD);
        assertEquals(Duration.ofSeconds(30), ZmuxConfig.DEFAULT_MAX_IDLE_TIMEOUT);
        assertEquals(java.util.Optional.of(Liveness.DEFAULT_KEEP_ALIVE_PERIOD), ZmuxConfig.defaults().liveness().keepAlivePeriod());
        assertEquals(java.util.Optional.of(ZmuxConfig.DEFAULT_MAX_IDLE_TIMEOUT), ZmuxConfig.defaults().maxIdleTimeout());
        assertEquals(16382, RecordPlan.MAX_RECORD_LENGTH);
        assertEquals(64, RecordPlan.MIN_MAX_LENGTH);
        assertEquals(0.25, Liveness.DEFAULT_PROBE_JITTER);
        assertEquals(Liveness.DEFAULT_PROBE_JITTER, ZmuxConfig.defaults().liveness().probeJitter());
        assertEquals(Duration.ofSeconds(10), ZmuxConfig.DEFAULT_JOIN_TIMEOUT);
        assertEquals(ZmuxConfig.DEFAULT_JOIN_TIMEOUT, ZmuxConfig.defaults().joinTimeout());
        assertEquals(Duration.ofSeconds(1), Backoff.defaults().min());
        assertEquals(Duration.ofSeconds(60), Backoff.defaults().max());
        assertEquals(Backoff.defaults(), ZmuxConfig.defaults().redialBackoff());
    }
}
