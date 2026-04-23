package io.zmux;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SettingsTest {
    @Test
    void builderRejectsNegativeValues() {
        IllegalArgumentException initialWindow = assertThrows(
                IllegalArgumentException.class,
                () -> Settings.builder().initialMaxData(-1L).build()
        );
        assertEquals("zmux settings initialMaxData must be >= 0", initialWindow.getMessage());

        IllegalArgumentException payloadLimit = assertThrows(
                IllegalArgumentException.class,
                () -> Settings.builder().maxFramePayload(-1L).build()
        );
        assertEquals("zmux settings maxFramePayload must be >= 0", payloadLimit.getMessage());

        IllegalArgumentException oversizedValue = assertThrows(
                IllegalArgumentException.class,
                () -> Settings.builder().maxControlPayloadBytes(Protocol.MAX_VARINT62 + 1L).build()
        );
        assertEquals("zmux settings maxControlPayloadBytes must be within varint62 range", oversizedValue.getMessage());
    }

    @Test
    void nullSchedulerHintFallsBackToBalanced() {
        Settings settings = Settings.builder()
                .schedulerHints(null)
                .build();

        assertEquals(SchedulerHint.UNSPECIFIED_OR_BALANCED, settings.schedulerHints());
    }
}
