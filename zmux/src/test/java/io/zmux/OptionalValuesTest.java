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
        assertEquals(Optional.empty(), keep.probeInterval());
        assertEquals(OptionalDouble.empty(), keep.probeJitter());
        assertThrows(IllegalArgumentException.class, () -> keep.withProbeInterval(Duration.ZERO), "0 is not off");
        assertTrue(keep.disableIdleProbes().idleProbesOff());
        assertFalse(keep.disableIdleProbes().withProbeInterval(Duration.ofSeconds(1)).idleProbesOff());
        assertEquals(Optional.of(Duration.ZERO), keep.withDialAfter(Duration.ZERO).dialAfter(), "0: dial at once");
        assertEquals(OptionalDouble.of(0), keep.withProbeJitter(0).probeJitter());
        assertThrows(IllegalArgumentException.class, () -> keep.withProbeInterval(Duration.ofMillis(500)));
        assertThrows(IllegalArgumentException.class, () -> keep.withProbeInterval(Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> keep.withDialAfter(Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> keep.withCount(0));
        assertThrows(IllegalArgumentException.class, () -> keep.withProbeJitter(1));
    }

    @Test void pipeOptionsTurnIdleProbesOffExplicitly() {
        var off = PipeOptions.builder().disableIdleProbes().build();
        assertEquals(0, off.join().probeInterval());
        assertEquals(-1, PipeOptions.defaults().join().probeInterval(), "unset: the session's");
        var keep = PoolPolicy.Decision.KEEP;
        assertEquals(0, off.withProbe(keep).join().probeInterval());
        assertEquals(-1, PipeOptions.defaults().withProbe(keep).join().probeInterval());
        assertEquals(0, PipeOptions.defaults().withProbe(keep.disableIdleProbes()).join().probeInterval());
        assertEquals(1_000_000_000, off.withProbe(keep.withProbeInterval(Duration.ofSeconds(1))).join().probeInterval());
        assertThrows(IllegalArgumentException.class, () -> PipeOptions.builder().probeInterval(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> PipeOptions.builder().probeInterval(Duration.ofMillis(999)));
    }

    @Test void offSettingsAreNotValues() {
        var t = Timeouts.defaults();
        assertEquals(Optional.of(Timeouts.DEFAULT_HANDSHAKE), t.handshake());
        assertEquals(Optional.empty(), t.disableHandshakeTimeout().handshake());
        assertEquals(Optional.empty(), t.disableIdleTimeout().idle());
        assertEquals(Optional.empty(), t.disableKeepAlive().keepAlive());
        assertThrows(IllegalArgumentException.class, () -> t.withKeepAlive(Duration.ZERO), "0 has no meaning");
        assertThrows(IllegalArgumentException.class, () -> t.withKeepAlive(Duration.ofMillis(999)));
        assertEquals(Optional.of(Duration.ofSeconds(1)), t.withKeepAlive(Duration.ofSeconds(1)).keepAlive());
        assertEquals(Optional.of(Duration.ZERO), t.withHandshake(Duration.ZERO).handshake());
        assertThrows(IllegalArgumentException.class, () -> t.withIdle(Duration.ZERO), "below the 1 ms unit");
        assertThrows(IllegalArgumentException.class, () -> t.withIdle(Duration.ofNanos(999_999)));
        assertThrows(IllegalArgumentException.class, () -> t.withKeepAlive(Duration.ofNanos(-1)));
        var cfg = ZmuxConfig.builder().timeouts(t.disableHandshakeTimeout().disableIdleTimeout().disableKeepAlive())
                .flowControl(FlowControl.defaults().withoutMemoryBudget()).build();
        var core = cfg.toCore(io.zmux.internal.core.CoreConfig.Role.INITIATOR, io.zmux.internal.core.CoreConfig.Role.INITIATOR);
        assertEquals(io.zmux.internal.core.Time.NEVER, core.handshakeTimeout());
        assertEquals(0, core.local().maxIdleTimeoutMillis, "none announced");
        assertEquals(io.zmux.internal.core.CoreConfig.OFF, core.keepAlive());
        assertEquals(io.zmux.internal.core.CoreConfig.UNLIMITED, core.memoryBudget());
        var zero = ZmuxConfig.builder()
                .flowControl(FlowControl.defaults().withMemoryBudget(0)).build()
                .toCore(io.zmux.internal.core.CoreConfig.Role.INITIATOR, io.zmux.internal.core.CoreConfig.Role.INITIATOR);
        assertEquals(0, zero.memoryBudget());
        assertEquals(java.util.OptionalLong.empty(), ListenerConfig.builder().noMemoryBudget().build().memoryBudget());
        assertThrows(IllegalArgumentException.class, () -> FlowControl.defaults().withMemoryBudget(-1));
    }

    @Test void pipeOptionsRejectZeroLivenessBounds() {
        assertThrows(IllegalArgumentException.class, () -> PipeOptions.builder().suspectFloor(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> PipeOptions.builder().deadTimeout(Duration.ZERO));
        assertEquals(0, PipeOptions.defaults().join().suspectFloor(), "unset: the session's");
        assertEquals(50_000_000, PipeOptions.builder().suspectFloor(Duration.ofMillis(50)).build().join().suspectFloor());
    }

    @Test void multipathLivenessBoundsAreUnsetUntilSet() {
        var m = MultipathConfig.defaults();
        assertEquals(Optional.empty(), m.suspectFloor());
        assertEquals(Optional.empty(), m.deadTimeout());
        assertEquals(Optional.empty(), m.probation());
        assertThrows(IllegalArgumentException.class, () -> m.withSuspectFloor(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> m.withDeadTimeout(Duration.ZERO));
        assertEquals(Optional.of(Duration.ZERO), m.withProbation(Duration.ZERO).probation());
    }

    @Test void standbyProbeIsOneSecondUnlessSet() {
        var m = MultipathConfig.defaults();
        assertEquals(Optional.of(Duration.ofSeconds(1)), m.standbyProbe());
        assertEquals(Optional.empty(), m.disableStandbyProbes().standbyProbe(), "off");
        assertEquals(Optional.empty(), m.disableIdleProbes().probeInterval(), "off");
        assertThrows(IllegalArgumentException.class, () -> m.withStandbyProbe(Duration.ZERO), "0 is not off");
        assertThrows(IllegalArgumentException.class, () -> m.withProbeInterval(Duration.ZERO), "0 is not off");
        assertThrows(IllegalArgumentException.class, () -> m.withStandbyProbe(Duration.ofMillis(500)));
        assertThrows(IllegalArgumentException.class, () -> m.withStandbyProbe(Duration.ofSeconds(-1)));
    }

    @Test void qualityBandMinIntervalIsOneSecondUnlessSet() {
        assertEquals(QualityBand.DEFAULT_MIN_INTERVAL, new QualityBand(Duration.ZERO, 1.5).minInterval());
        assertEquals(Duration.ZERO, new QualityBand(Duration.ZERO, 1.5, Duration.ZERO).minInterval(), "0: no floor");
        assertThrows(IllegalArgumentException.class, () -> new QualityBand(Duration.ZERO, 1.5, Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> new QualityBand(Duration.ZERO, Double.NaN));
    }

    @Test void modePolicyRejectsANanTimerFactor() {
        assertThrows(IllegalArgumentException.class, () -> ModePolicy.defaults().withTimer(Double.NaN, null, null));
    }

    @Test void shapingLimitsAreUnboundedUnlessSet() {
        var none = ShapingLimits.none();
        assertTrue(none.maxPaddingBytes().isEmpty() && none.maxPaddingRatio().isEmpty() && none.maxInjected().isEmpty()
                && none.maxPauseTotal().isEmpty() && none.maxDelay().isEmpty());
        assertEquals(0, none.withMaxInjected(0).maxInjected().getAsLong(), "0 allows none");
        assertEquals(Duration.ZERO, none.withMaxDelay(Duration.ZERO).maxDelay().orElseThrow());
        assertThrows(IllegalArgumentException.class, () -> none.withMaxInjected(-1));
        assertThrows(IllegalArgumentException.class, () -> none.withMaxPauseTotal(Duration.ofNanos(-1)));
    }

    @Test void recheckZeroIsAsSoonAsTheFloorAllows() {
        assertEquals(0, PoolPolicy.Decision.KEEP.recheckNanos(), "unset: none");
        assertEquals(PoolPolicy.Decision.MIN_RECHECK.toNanos(), PoolPolicy.Decision.KEEP.withRecheck(Duration.ZERO).recheckNanos());
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
        var p = ModePolicy.defaults().withTimer(0, Duration.ZERO, Duration.ZERO).withSettle(Duration.ZERO).withDropWindow(Duration.ZERO);
        assertEquals(Duration.ZERO, p.timer(Duration.ofMillis(100)));
        assertEquals(Duration.ZERO, p.settle());
        assertEquals(Duration.ofSeconds(1), ModePolicy.defaults().settle());
        assertThrows(IllegalArgumentException.class, () -> ModePolicy.defaults().withSettle(Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> ModePolicy.defaults().withTimer(-1, Duration.ZERO, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> ModePolicy.defaults().withTimer(3, Duration.ofSeconds(2), Duration.ofSeconds(1)));
    }
}
