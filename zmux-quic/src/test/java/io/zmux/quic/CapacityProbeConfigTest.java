package io.zmux.quic;

import io.zmux.*;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class CapacityProbeConfigTest {
    private static Properties link(String windows) {
        var p = new Properties();
        p.setProperty("zmux.probe.rate", "31250000");
        p.setProperty("zmux.probe.delayMillis", "100");
        p.setProperty("zmux.probe.windows", windows);
        return p;
    }

    @Test void independentLinksScaleTheSessionBudgetButNotEachNativeWindow() {
        var p = link("bdp"); p.setProperty("zmux.probe.listenerBudget", Long.toString(4L << 30));
        var config = CapacityProbeConfig.read(p);
        var single = config.plan(1); var aggregate = config.plan(4);
        assertEquals(6_250_000, single.nominalBdp());
        assertEquals(12_500_000, single.nativeWindow());
        assertEquals(12_500_000, single.connectionWindow());
        assertEquals(25_000_000, aggregate.nominalBdp());
        assertEquals(single.nativeWindow(), aggregate.nativeWindow());
        assertEquals(50_000_000, aggregate.connectionWindow());
        config.checkBudget(5, 4);
    }

    @Test void carrierOnlyControlDoesNotChangeZmuxWindowConfiguration() {
        var defaults = CapacityProbeConfig.read(link("default")).plan(1);
        var carrier = CapacityProbeConfig.read(link("carrier")).plan(1);
        assertEquals(1_250_000, defaults.nativeWindow());
        assertEquals(12_500_000, carrier.nativeWindow());
        assertEquals(0, defaults.connectionWindow()); assertEquals(0, carrier.connectionWindow());
        assertEquals(0, defaults.streamWindow()); assertEquals(0, carrier.streamWindow());
        assertEquals(0, defaults.sendBuffer()); assertEquals(0, carrier.sendBuffer());
        assertEquals(1L << 30, CapacityProbeConfig.read(new Properties()).listenerBudget());
    }

    @Test void invalidOrUnfundedExperimentsFailBeforeOpeningConnections() {
        assertThrows(IllegalArgumentException.class, () -> CapacityProbeConfig.read(link("bdp")).checkBudget(5, 4));
        var noDelay = link("bdp"); noDelay.setProperty("zmux.probe.delayMillis", "0");
        assertThrows(IllegalArgumentException.class, () -> CapacityProbeConfig.read(noDelay));
        var empty = link("default"); empty.setProperty("zmux.probe.mode", "0"); empty.setProperty("zmux.probe.crossSession", "true");
        assertThrows(IllegalArgumentException.class, () -> CapacityProbeConfig.read(empty));
        var huge = link("bdp"); huge.setProperty("zmux.probe.rate", Long.toString(Long.MAX_VALUE));
        huge.setProperty("zmux.probe.delayMillis", "1000");
        assertThrows(ArithmeticException.class, () -> CapacityProbeConfig.read(huge).plan(4));
    }
}
