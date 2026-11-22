package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.Time;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.VarInt;
import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.WireAccount;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/** Shaping limits and probe intervals reject what they cannot honour; the exported defaults are the ones in use. */
@org.junit.jupiter.api.Timeout(60)
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

    @Test void wireDeadlinesRoundUpAndPreserveLocalPrecision() throws Exception {
        var d = Duration.ofMillis(1).plusNanos(1);
        assertDeadlines(d, 2, d.toNanos());
        var subMillisecond = Duration.ofNanos(999_999);
        var builder = ZmuxConfig.builder();
        assertThrows(IllegalArgumentException.class, () -> builder.maxIdleTimeout(subMillisecond));
        assertThrows(IllegalArgumentException.class, () -> builder.resumeTimeout(subMillisecond));
    }

    @Test void wireDeadlineSaturationAndLocalClockOverflowAreIndependent() throws Exception {
        assertDeadlines(Duration.ofMillis(VarInt.MAX - 1).plusNanos(1), VarInt.MAX, Time.NEVER);
        assertDeadlines(Duration.ofMillis(VarInt.MAX).plusNanos(1), VarInt.MAX, Time.NEVER);
        assertDeadlines(Duration.ofSeconds(Long.MAX_VALUE), VarInt.MAX, Time.NEVER);
        // This value fits on the wire but the configured duration alone already exceeds the clock.
        var aboveClock = Duration.ofNanos(Long.MAX_VALUE).plusNanos(1);
        assertDeadlines(aboveClock, Long.MAX_VALUE / Time.MILLISECOND + 1, Time.NEVER);
        assertEquals(Time.NEVER, Time.add(5, Time.NEVER));
        assertEquals(Time.NEVER, Time.add(5, Long.MAX_VALUE - 4));
    }

    private static void assertDeadlines(Duration d, long wireMillis, long localNanos) throws Exception {
        var config = ZmuxConfig.builder().maxIdleTimeout(d).resumeTimeout(d).pipeAuth(PipeAuth.psk(new byte[32])).build();
        var core = config.toCore(CoreConfig.Role.INITIATOR, CoreConfig.Role.INITIATOR).toBuilder().build();
        var wire = ByteBuffer.allocate(4096);
        core.local().encode(wire);
        var decoded = TransportParams.decode(wire, 0, wire.position());
        assertEquals(wireMillis, decoded.maxIdleTimeoutMillis);
        assertEquals(wireMillis, decoded.resumeGraceMillis);
        assertEquals(localNanos, core.localIdleTimeout());
        assertEquals(localNanos, core.resume());
        assertEquals(d, config.maxIdleTimeout().orElseThrow());
        assertEquals(d, config.resumeTimeout().orElseThrow());
    }

    @Test void spreadChunkAcceptsTheFullPositiveIntRange() {
        var builder = ZmuxConfig.builder();
        assertEquals(1, builder.spreadChunk(1).build().spreadChunk());
        assertEquals(Integer.MAX_VALUE, builder.spreadChunk(Integer.MAX_VALUE).build().spreadChunk());
        assertThrows(IllegalArgumentException.class, () -> builder.spreadChunk(0));
        assertThrows(IllegalArgumentException.class, () -> builder.spreadChunk(Integer.MIN_VALUE));
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
