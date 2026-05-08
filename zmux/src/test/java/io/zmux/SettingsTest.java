package io.zmux;

import io.zmux.protocol.Protocol;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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

    @Test
    void pingPaddingKeyRoundTripsThroughBuilderAndConstructor() {
        Settings settings = Settings.defaults().toBuilder()
                .pingPaddingKey(123_456L)
                .build();

        assertEquals(123_456L, settings.pingPaddingKey());
        assertEquals(settings, settings.toBuilder().build());

        Settings constructed = new Settings(
                settings.initialMaxStreamDataBidiLocallyOpened(),
                settings.initialMaxStreamDataBidiPeerOpened(),
                settings.initialMaxStreamDataUni(),
                settings.initialMaxData(),
                settings.maxIncomingStreamsBidi(),
                settings.maxIncomingStreamsUni(),
                settings.maxFramePayload(),
                settings.maxControlPayloadBytes(),
                settings.maxExtensionPayloadBytes(),
                settings.schedulerHints(),
                settings.pingPaddingKey()
        );
        assertEquals(settings, constructed);
    }

    @Test
    void limitsViewIsCached() {
        Settings settings = Settings.defaults();

        assertSame(settings.limits(), settings.limits());
    }
}
