package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.shaping.ShapingLimits;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

/** Unset is the default, a set value is taken as it is (zero included), an invalid one is rejected. */
class OptionalValuesTest {
    @Test void poolDecisionItemsAreUnsetOrSet() {
        var keep = PoolPolicy.Decision.KEEP;
        assertEquals(OptionalInt.empty(), keep.count());
        assertEquals(Optional.empty(), keep.dialAfter());
        assertTrue(keep.liveness().isInherit());
        var l = PipeLiveness.builder();
        assertThrows(IllegalArgumentException.class, () -> l.probeInterval(Duration.ZERO), "0 is not off");
        assertTrue(PipeLiveness.builder().noProbeInterval().build().isNoProbeInterval());
        assertFalse(PipeLiveness.builder().noProbeInterval().probeInterval(Duration.ofSeconds(1)).build().isNoProbeInterval());
        assertEquals(Optional.of(Duration.ZERO), keep.withDialAfter(Duration.ZERO).dialAfter(), "0: dial at once");
        assertEquals(OptionalDouble.of(0), PipeLiveness.builder().probeJitter(0).build().probeJitter());
        assertThrows(IllegalArgumentException.class, () -> l.probeInterval(Duration.ofMillis(500)));
        assertThrows(IllegalArgumentException.class, () -> l.probeInterval(Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> keep.withDialAfter(Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> keep.withCount(0));
        assertThrows(IllegalArgumentException.class, () -> l.probeJitter(1));
    }

    @Test void pipeOptionsTurnIdleProbesOffExplicitly() {
        var off = PipeOptions.builder().liveness(PipeLiveness.builder().noProbeInterval().build()).build();
        assertEquals(0, off.join().probeInterval());
        assertEquals(-1, PipeOptions.defaults().join().probeInterval(), "unset: the session's");
        var inherit = PipeLiveness.inherit();
        assertEquals(0, off.withLiveness(inherit).join().probeInterval());
        assertEquals(-1, PipeOptions.defaults().withLiveness(inherit).join().probeInterval());
        assertEquals(0, PipeOptions.defaults().withLiveness(PipeLiveness.builder().noProbeInterval().build()).join().probeInterval());
        assertEquals(1_000_000_000, off.withLiveness(PipeLiveness.builder().probeInterval(Duration.ofSeconds(1)).build()).join().probeInterval());
        assertThrows(IllegalArgumentException.class, () -> PipeLiveness.builder().probeInterval(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> PipeLiveness.builder().probeInterval(Duration.ofMillis(999)));
    }

    @Test void offSettingsAreNotValues() {
        var d = ZmuxConfig.defaults();
        assertEquals(Optional.of(ZmuxConfig.DEFAULT_HANDSHAKE_TIMEOUT), d.handshakeTimeout());
        assertEquals(Optional.empty(), ZmuxConfig.builder().noHandshakeTimeout().build().handshakeTimeout());
        assertEquals(Optional.empty(), ZmuxConfig.builder().noIdleTimeout().build().maxIdleTimeout());
        assertEquals(Optional.empty(), ZmuxConfig.builder().liveness(Liveness.builder().noKeepAlive().build()).build().liveness().keepAlivePeriod());
        var t = ZmuxConfig.builder();
        var lt = Liveness.builder();
        assertThrows(IllegalArgumentException.class, () -> lt.keepAlivePeriod(Duration.ZERO), "0 has no meaning");
        assertThrows(IllegalArgumentException.class, () -> lt.keepAlivePeriod(Duration.ofMillis(999)));
        assertEquals(Optional.of(Duration.ofSeconds(1)), Liveness.builder().keepAlivePeriod(Duration.ofSeconds(1)).build().keepAlivePeriod());
        assertEquals(Optional.of(Duration.ZERO), ZmuxConfig.builder().handshakeTimeout(Duration.ZERO).build().handshakeTimeout());
        assertThrows(IllegalArgumentException.class, () -> t.maxIdleTimeout(Duration.ZERO), "below the 1 ms unit");
        assertThrows(IllegalArgumentException.class, () -> t.maxIdleTimeout(Duration.ofNanos(999_999)));
        assertThrows(IllegalArgumentException.class, () -> lt.keepAlivePeriod(Duration.ofNanos(-1)));
        var cfg = ZmuxConfig.builder().noHandshakeTimeout().noIdleTimeout().liveness(Liveness.builder().noKeepAlive().build())
                .noMemoryBudget().build();
        assertEquals(java.util.OptionalLong.empty(), cfg.memoryBudget());
        var core = cfg.toCore(io.zmux.internal.core.CoreConfig.Role.INITIATOR, io.zmux.internal.core.CoreConfig.Role.INITIATOR);
        assertEquals(io.zmux.internal.core.Time.NEVER, core.handshakeTimeout());
        assertEquals(0, core.local().maxIdleTimeoutMillis, "none announced");
        assertEquals(io.zmux.internal.core.CoreConfig.OFF, core.keepAlive());
        assertEquals(io.zmux.internal.core.CoreConfig.UNLIMITED, core.memoryBudget());
        var zero = ZmuxConfig.builder()
                .memoryBudget(0).build()
                .toCore(io.zmux.internal.core.CoreConfig.Role.INITIATOR, io.zmux.internal.core.CoreConfig.Role.INITIATOR);
        assertEquals(0, zero.memoryBudget());
        assertEquals(java.util.OptionalLong.empty(), ListenerConfig.builder().noMemoryBudget().build().memoryBudget());
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().memoryBudget(-1));
    }

    @Test void pipeOptionsRejectZeroLivenessBounds() {
        assertThrows(IllegalArgumentException.class, () -> PipeLiveness.builder().suspectFloor(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> PipeLiveness.builder().deadTimeout(Duration.ZERO));
        assertEquals(0, PipeOptions.defaults().join().suspectFloor(), "unset: the session's");
        assertEquals(50_000_000, PipeOptions.builder().liveness(PipeLiveness.builder().suspectFloor(Duration.ofMillis(50)).build()).build().join().suspectFloor());
    }

    @Test void multipathLivenessBoundsAreUnsetUntilSet() {
        var m = ZmuxConfig.defaults();
        assertEquals(Liveness.DEFAULT_SUSPECT_FLOOR, m.liveness().suspectFloor());
        assertEquals(Optional.empty(), m.liveness().deadTimeout());
        assertEquals(Optional.empty(), m.probation());
        assertThrows(IllegalArgumentException.class, () -> Liveness.builder().suspectFloor(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> Liveness.builder().deadTimeout(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().probation(Duration.ZERO), "0 is not off");
        assertEquals(Optional.of(Duration.ZERO), ZmuxConfig.builder().noProbation().build().probation());
    }

    @Test void standbyProbeIsOneSecondUnlessSet() {
        var m = Liveness.builder();
        assertEquals(Optional.of(Duration.ofSeconds(1)), Liveness.defaults().standbyProbe());
        assertEquals(Optional.empty(), Liveness.builder().noStandbyProbe().build().standbyProbe(), "off");
        assertEquals(Optional.empty(), Liveness.builder().noProbeInterval().build().probeInterval(), "off");
        assertThrows(IllegalArgumentException.class, () -> m.standbyProbe(Duration.ZERO), "0 is not off");
        assertThrows(IllegalArgumentException.class, () -> m.probeInterval(Duration.ZERO), "0 is not off");
        assertThrows(IllegalArgumentException.class, () -> m.standbyProbe(Duration.ofMillis(500)));
        assertThrows(IllegalArgumentException.class, () -> m.standbyProbe(Duration.ofSeconds(-1)));
    }

    @Test void qualityBandMinIntervalIsOneSecondUnlessSet() {
        assertEquals(QualityBand.DEFAULT_MIN_INTERVAL, new QualityBand(Duration.ZERO, 1.5).minInterval());
        assertEquals(Duration.ZERO, new QualityBand(Duration.ZERO, 1.5, Duration.ZERO).minInterval(), "0: no floor");
        assertThrows(IllegalArgumentException.class, () -> new QualityBand(Duration.ZERO, 1.5, Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> new QualityBand(Duration.ZERO, Double.NaN));
    }

    @Test void modePolicyRejectsANanTimerFactor() {
        assertThrows(IllegalArgumentException.class, () -> CarrierChoicePolicy.defaults().withTimer(Double.NaN, null, null));
    }

    @Test void oneWaitIsBoundedByDefault() {
        var d = ShapingLimits.defaults();
        assertFalse(d.wire().isSet());
        assertEquals(Duration.ofSeconds(1), d.under(io.zmux.shaping.ShapingBudget.NONE).maxDelay());
        assertNull(d.withNoMaxDelay().under(io.zmux.shaping.ShapingBudget.NONE).maxDelay(), "no bound");
        assertThrows(IllegalArgumentException.class, () -> d.withMaxDelay(Duration.ZERO));
    }

    @Test void recheckZeroIsAsSoonAsTheFloorAllows() {
        assertEquals(Optional.empty(), PoolPolicy.Decision.KEEP.recheckDelay(), "unset: none");
        assertEquals(Optional.of(PoolPolicy.Decision.MIN_RECHECK), PoolPolicy.Decision.KEEP.withRecheck(Duration.ZERO).recheckDelay());
        assertThrows(IllegalArgumentException.class, () -> PoolPolicy.Decision.KEEP.withRecheck(Duration.ofNanos(-1)));
    }

    @Test void carrierCacheTakesZeroAndRejectsNegatives() {
        var c = new CarrierCache(Duration.ZERO, Duration.ZERO, Duration.ZERO);
        assertEquals(Duration.ZERO, c.successTtl());
        assertEquals(Duration.ZERO, c.backoffMin());
        assertThrows(IllegalArgumentException.class, () -> new CarrierCache(Duration.ofSeconds(-1), Duration.ZERO, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new CarrierCache(Duration.ZERO, Duration.ofSeconds(2), Duration.ofSeconds(1)));
    }

    @Test void modePolicyTakesZeroAndRejectsNegatives() {
        var p = CarrierChoicePolicy.defaults().withTimer(0, Duration.ZERO, Duration.ZERO).withSettle(Duration.ZERO).withDropWindow(Duration.ZERO);
        assertEquals(Duration.ZERO, p.timer(Duration.ofMillis(100)));
        assertEquals(Duration.ZERO, p.settle());
        assertEquals(Duration.ofSeconds(1), CarrierChoicePolicy.defaults().settle());
        assertThrows(IllegalArgumentException.class, () -> CarrierChoicePolicy.defaults().withSettle(Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> CarrierChoicePolicy.defaults().withTimer(-1, Duration.ZERO, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> CarrierChoicePolicy.defaults().withTimer(3, Duration.ofSeconds(2), Duration.ofSeconds(1)));
    }
}
