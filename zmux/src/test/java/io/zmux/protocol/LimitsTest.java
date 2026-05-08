package io.zmux.protocol;

import io.zmux.Settings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class LimitsTest {
    @Test
    void normalizeUsesDefaultsOnlyForZeroValues() {
        Limits normalized = new Limits(0L, 128L, 0L).normalize();

        assertEquals(Settings.defaults().maxFramePayload(), normalized.maxFramePayload());
        assertEquals(128L, normalized.maxControlPayloadBytes());
        assertEquals(Settings.defaults().maxExtensionPayloadBytes(), normalized.maxExtensionPayloadBytes());
    }

    @Test
    void normalizeReusesExplicitNonZeroInstanceAndCachesResolvedZeroSentinel() {
        Limits explicit = new Limits(128L, 256L, 512L);
        assertSame(explicit, explicit.normalize());

        Limits partial = new Limits(0L, 128L, 0L);
        Limits first = partial.normalize();
        Limits second = partial.normalize();

        assertSame(first, second);
        assertEquals(Settings.defaults().maxFramePayload(), first.maxFramePayload());
        assertEquals(128L, first.maxControlPayloadBytes());
        assertEquals(Settings.defaults().maxExtensionPayloadBytes(), first.maxExtensionPayloadBytes());
    }

    @Test
    void constructorRejectsNegativeAndOversizedValues() {
        IllegalArgumentException negative = assertThrows(
                IllegalArgumentException.class,
                () -> new Limits(-1L, 0L, 0L)
        );
        assertEquals("zmux limits maxFramePayload must be >= 0", negative.getMessage());

        IllegalArgumentException oversized = assertThrows(
                IllegalArgumentException.class,
                () -> new Limits(0L, Protocol.MAX_VARINT62 + 1L, 0L)
        );
        assertEquals("zmux limits maxControlPayloadBytes must be within varint62 range", oversized.getMessage());
    }
}
