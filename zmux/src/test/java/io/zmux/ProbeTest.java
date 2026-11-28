package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/** Public validation of per-pipe probe settings. */
class ProbeTest {
    @Test void probeSettingsAreValidated() {
        var b = PipeLiveness.builder();
        assertThrows(IllegalArgumentException.class, () -> b.probeInterval(Duration.ofMillis(999)));
        b.probeInterval(Duration.ofSeconds(1));
        assertThrows(IllegalArgumentException.class, () -> b.probeJitter(-0.01));
        assertThrows(IllegalArgumentException.class, () -> b.probeJitter(1));
        assertThrows(IllegalArgumentException.class, () -> b.probeJitter(Double.NaN));
        b.probeJitter(0).probeJitter(0.99);
        assertThrows(IllegalArgumentException.class, () -> Liveness.builder().probeJitter(1));
        assertThrows(IllegalArgumentException.class, () -> Liveness.builder().probeJitter(-1));
        assertEquals(0.5, Liveness.builder().probeJitter(0.5).build().probeJitter());
    }

}
