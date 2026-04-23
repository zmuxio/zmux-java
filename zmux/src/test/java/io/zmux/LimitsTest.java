package io.zmux;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class LimitsTest {
    @Test
    void normalizeUsesDefaultsOnlyForZeroValues() {
        Limits normalized = new Limits(0L, 128L, 0L).normalize();

        assertEquals(Settings.defaults().maxFramePayload(), normalized.maxFramePayload());
        assertEquals(128L, normalized.maxControlPayloadBytes());
        assertEquals(Settings.defaults().maxExtensionPayloadBytes(), normalized.maxExtensionPayloadBytes());
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
