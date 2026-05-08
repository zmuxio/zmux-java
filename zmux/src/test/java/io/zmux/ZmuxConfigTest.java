package io.zmux;

import io.zmux.protocol.Negotiated;
import io.zmux.protocol.Preface;
import io.zmux.protocol.Protocol;
import io.zmux.protocol.ZmuxCodec;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

final class ZmuxConfigTest {
    private static ZmuxConfig sampleConfig() {
        Settings settings = Settings.defaults().toBuilder()
                .initialMaxData(777_777L)
                .maxIncomingStreamsBidi(17L)
                .maxIncomingStreamsUni(19L)
                .maxFramePayload(32_768L)
                .maxControlPayloadBytes(8_192L)
                .maxExtensionPayloadBytes(8_192L)
                .schedulerHints(SchedulerHint.LATENCY)
                .pingPaddingKey(123L)
                .build();
        ZmuxEventHandler handler = event -> {
        };
        return ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .tieBreakerNonce(123_456_789L)
                .minProto(1L)
                .maxProto(2L)
                .capabilities(0x55AAL)
                .settings(settings)
                .prefacePadding(true)
                .prefacePaddingMinBytes(3L)
                .prefacePaddingMaxBytes(9L)
                .keepaliveInterval(Duration.ofSeconds(3))
                .keepaliveMaxPingInterval(Duration.ofSeconds(11))
                .keepaliveTimeout(Duration.ofSeconds(5))
                .pingPadding(true)
                .pingPaddingMinBytes(5L)
                .pingPaddingMaxBytes(13L)
                .sessionMemoryCap(9_999L)
                .perStreamQueuedDataHwm(1_111L)
                .sessionQueuedDataHwm(2_222L)
                .urgentQueuedBytesCap(3_333L)
                .pendingControlBytesBudget(4_444L)
                .pendingPriorityBytesBudget(5_555L)
                .abuseWindow(Duration.ofSeconds(7))
                .gracefulCloseDrainTimeout(Duration.ofMillis(333))
                .stopSendingGracefulDrainWindow(Duration.ofMillis(444))
                .stopSendingGracefulTailCap(6_666L)
                .hiddenAbortChurnWindow(Duration.ofMillis(555))
                .hiddenAbortChurnThreshold(13)
                .visibleTerminalChurnWindow(Duration.ofMillis(666))
                .visibleTerminalChurnThreshold(17)
                .inboundControlFrameBudget(23)
                .inboundControlBytesBudget(7_777L)
                .inboundExtFrameBudget(29)
                .inboundExtBytesBudget(8_888L)
                .inboundMixedFrameBudget(31)
                .inboundMixedBytesBudget(9_999L)
                .noOpControlFloodThreshold(37)
                .noOpMaxDataFloodThreshold(41)
                .noOpBlockedFloodThreshold(43)
                .noOpZeroDataFloodThreshold(47)
                .noOpPriorityUpdateFloodThreshold(53)
                .groupRebucketChurnThreshold(59)
                .inboundPingFloodThreshold(61)
                .acceptBacklogLimit(67)
                .acceptBacklogBytesLimit(10_101L)
                .tombstoneLimit(71)
                .markerOnlyUsedStreamLimit(73)
                .retainedOpenInfoBytesBudget(11_111L)
                .retainedPeerReasonBytesBudget(12_121L)
                .aggregateLateDataCap(13_131L)
                .eventHandler(handler)
                .build();
    }

    private static void assertConfigComponentsEqual(
            ZmuxConfig expected,
            ZmuxConfig actual,
            String ignoredComponentA,
            String ignoredComponentB
    ) {
        ZmuxConfig.Builder normalized = expected.toBuilder();
        if ("role".equals(ignoredComponentA) || "role".equals(ignoredComponentB)) {
            normalized.role(actual.role());
        }
        if ("tieBreakerNonce".equals(ignoredComponentA) || "tieBreakerNonce".equals(ignoredComponentB)) {
            normalized.tieBreakerNonce(actual.tieBreakerNonce());
        }
        assertEquals(normalized.build(), actual);
    }

    @Test
    void defaultsEnableRepositoryKeepaliveTemplate() {
        ZmuxConfig defaults = ZmuxConfig.defaults();

        assertEquals(Duration.ofMinutes(1), defaults.keepaliveInterval());
        assertEquals(Duration.ofMinutes(5), defaults.keepaliveMaxPingInterval());
        assertEquals(Duration.ZERO, defaults.gracefulCloseDrainTimeout());
        assertTrue(defaults.prefacePadding());
        assertTrue(defaults.pingPadding());
        assertEquals(ZmuxConfig.DEFAULT_CAPABILITIES, defaults.capabilities());
        assertTrue(Protocol.canCarryOpenInfo(defaults.capabilities()));
        assertTrue(Protocol.canCarryPriorityOnOpen(defaults.capabilities()));
        assertTrue(Protocol.canCarryGroupOnOpen(defaults.capabilities()));
        assertTrue(Protocol.canCarryPriorityInUpdate(defaults.capabilities()));
        assertTrue(Protocol.canCarryGroupInUpdate(defaults.capabilities()));
    }

    @Test
    void zeroCapabilitiesUseDefaultCapabilitySet() {
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .capabilities(0L)
                .build();

        assertFalse(config.disableCapabilities());
        assertEquals(ZmuxConfig.DEFAULT_CAPABILITIES, config.capabilities());
        assertEquals(ZmuxConfig.DEFAULT_CAPABILITIES, config.localPreface().capabilities());
    }

    @Test
    void disableCapabilitiesOverridesDefaultCapabilitySet() {
        ZmuxConfig config = ZmuxConfig.builder()
                .capabilities(Protocol.CAPABILITY_OPEN_METADATA)
                .disableCapabilities()
                .build();

        assertTrue(config.disableCapabilities());
        assertEquals(0L, config.capabilities());
        assertEquals(0L, config.localPreface().capabilities());
    }

    @Test
    void defaultCapabilitiesNegotiateWithDefaultPeer() throws Exception {
        ZmuxConfig localConfig = ZmuxConfig.defaults().withRole(Role.INITIATOR);
        ZmuxConfig peerConfig = ZmuxConfig.defaults().withRole(Role.RESPONDER);

        Negotiated negotiated = ZmuxCodec.negotiatePrefaces(
                localConfig.localPreface(),
                peerConfig.localPreface()
        );

        assertEquals(ZmuxConfig.DEFAULT_CAPABILITIES, negotiated.capabilities());
    }

    @Test
    void toBuilderRoundTripsAllComponents() {
        ZmuxConfig config = sampleConfig();

        assertConfigComponentsEqual(config, config.toBuilder().build(), null, null);
    }

    @Test
    void withExplicitRoleClearsNonceAndPreservesOtherComponents() {
        ZmuxConfig original = sampleConfig();
        ZmuxConfig adjusted = original.withRole(Role.INITIATOR);

        assertEquals(Role.INITIATOR, adjusted.role());
        assertEquals(0L, adjusted.tieBreakerNonce());
        assertConfigComponentsEqual(original, adjusted, "role", "tieBreakerNonce");
    }

    @Test
    void withAutoRolePreservesNonceAndOtherComponents() {
        ZmuxConfig original = sampleConfig();
        ZmuxConfig adjusted = original.withRole(Role.AUTO);

        assertEquals(Role.AUTO, adjusted.role());
        assertEquals(original.tieBreakerNonce(), adjusted.tieBreakerNonce());
        assertConfigComponentsEqual(original, adjusted, "role", null);
    }

    @Test
    void builderRejectsNegativeBudgetsAndDurations() {
        IllegalArgumentException negativeBudget = assertThrows(
                IllegalArgumentException.class,
                () -> ZmuxConfig.builder().sessionMemoryCap(-1L).build()
        );
        assertEquals("zmux config sessionMemoryCap must be >= 0", negativeBudget.getMessage());

        IllegalArgumentException negativeDuration = assertThrows(
                IllegalArgumentException.class,
                () -> ZmuxConfig.builder().keepaliveInterval(Duration.ofMillis(-1)).build()
        );
        assertEquals("zmux config keepaliveInterval must be >= 0", negativeDuration.getMessage());
    }

    @Test
    void builderRejectsInvalidProtocolRangeAndVarint62Fields() {
        ZmuxConfig zeroProtocolBounds = ZmuxConfig.builder()
                .minProto(0L)
                .maxProto(0L)
                .build();
        assertEquals(Protocol.PROTO_VERSION, zeroProtocolBounds.minProto());
        assertEquals(Protocol.PROTO_VERSION, zeroProtocolBounds.maxProto());

        IllegalArgumentException negativeMinProto = assertThrows(
                IllegalArgumentException.class,
                () -> ZmuxConfig.builder().minProto(-1L).build()
        );
        assertEquals("zmux config minProto must be > 0", negativeMinProto.getMessage());

        IllegalArgumentException invertedRange = assertThrows(
                IllegalArgumentException.class,
                () -> ZmuxConfig.builder().minProto(2L).maxProto(1L).build()
        );
        assertEquals("zmux config minProto must be <= maxProto", invertedRange.getMessage());

        IllegalArgumentException invalidNonce = assertThrows(
                IllegalArgumentException.class,
                () -> ZmuxConfig.builder().tieBreakerNonce(Protocol.MAX_VARINT62 + 1L).build()
        );
        assertEquals("zmux config tieBreakerNonce must be within varint62 range", invalidNonce.getMessage());

        IllegalArgumentException invalidMaxProto = assertThrows(
                IllegalArgumentException.class,
                () -> ZmuxConfig.builder().maxProto(Protocol.MAX_VARINT62 + 1L).build()
        );
        assertEquals("zmux config maxProto must be within varint62 range", invalidMaxProto.getMessage());
    }

    @Test
    void configSettingsNormalizeZeroPayloadLimitsToDefaults() {
        Settings settings = Settings.defaults().toBuilder()
                .initialMaxData(123L)
                .maxFramePayload(0L)
                .maxControlPayloadBytes(0L)
                .maxExtensionPayloadBytes(0L)
                .build();

        ZmuxConfig config = ZmuxConfig.builder()
                .settings(settings)
                .build();

        assertEquals(123L, config.settings().initialMaxData());
        assertEquals(Settings.defaults().maxFramePayload(), config.settings().maxFramePayload());
        assertEquals(Settings.defaults().maxControlPayloadBytes(), config.settings().maxControlPayloadBytes());
        assertEquals(Settings.defaults().maxExtensionPayloadBytes(), config.settings().maxExtensionPayloadBytes());
        Settings prefaceSettings = config.localPreface().settings();
        assertNotEquals(0L, prefaceSettings.pingPaddingKey());
        assertEquals(config.settings(), prefaceSettings.toBuilder().pingPaddingKey(0L).build());
    }

    @Test
    void localPrefaceAdvertisesPingPaddingKeyOnlyWhenEnabled() {
        Settings keyed = Settings.defaults().toBuilder()
                .pingPaddingKey(77L)
                .build();

        ZmuxConfig disabled = ZmuxConfig.builder()
                .role(Role.INITIATOR)
                .settings(keyed)
                .pingPadding(false)
                .build();
        assertEquals(0L, disabled.localPreface().settings().pingPaddingKey());

        ZmuxConfig configuredKey = disabled.toBuilder()
                .pingPadding(true)
                .build();
        assertEquals(77L, configuredKey.localPreface().settings().pingPaddingKey());

        ZmuxConfig generatedKey = ZmuxConfig.builder()
                .role(Role.INITIATOR)
                .pingPadding(true)
                .build();
        assertNotEquals(0L, generatedKey.localPreface().settings().pingPaddingKey());
    }

    @Test
    void configureDefaultConfigDoesNotRetainPerSessionRandomFields() {
        try {
            ZmuxConfig.configureDefaultConfig(builder -> builder
                    .tieBreakerNonce(99L)
                    .pingPadding(true)
                    .settings(Settings.defaults().toBuilder().pingPaddingKey(123L).build()));

            ZmuxConfig defaults = ZmuxConfig.defaults();

            assertEquals(0L, defaults.tieBreakerNonce());
            assertTrue(defaults.pingPadding());
            assertEquals(0L, defaults.settings().pingPaddingKey());
        } finally {
            ZmuxConfig.resetDefaultConfig();
        }
    }

    @Test
    void prefaceAndNegotiatedExposeCapabilityHelpers() {
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA
                | Protocol.CAPABILITY_PRIORITY_UPDATE
                | Protocol.CAPABILITY_PRIORITY_HINTS;
        Preface preface = new Preface(
                Protocol.PREFACE_VERSION,
                Role.INITIATOR,
                0L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                capabilities,
                Settings.defaults()
        );
        Negotiated negotiated = new Negotiated(
                Protocol.PROTO_VERSION,
                capabilities,
                Role.INITIATOR,
                Role.RESPONDER,
                Settings.defaults()
        );

        assertTrue(preface.hasCapability(Protocol.CAPABILITY_OPEN_METADATA));
        assertTrue(preface.supportsOpenMetadata());
        assertTrue(preface.supportsPriorityUpdate());
        assertTrue(preface.canCarryOpenInfo());
        assertTrue(preface.canCarryPriorityOnOpen());
        assertFalse(preface.canCarryGroupOnOpen());
        assertTrue(preface.canCarryPriorityInUpdate());
        assertFalse(preface.canCarryGroupInUpdate());
        assertTrue(preface.hasPeerVisiblePrioritySemantics());
        assertFalse(preface.hasPeerVisibleGroupSemantics());

        assertTrue(negotiated.hasCapability(Protocol.CAPABILITY_OPEN_METADATA));
        assertTrue(negotiated.supportsOpenMetadata());
        assertTrue(negotiated.supportsPriorityUpdate());
        assertTrue(negotiated.canCarryOpenInfo());
        assertTrue(negotiated.canCarryPriorityOnOpen());
        assertFalse(negotiated.canCarryGroupOnOpen());
        assertTrue(negotiated.canCarryPriorityInUpdate());
        assertFalse(negotiated.canCarryGroupInUpdate());
        assertTrue(negotiated.hasPeerVisiblePrioritySemantics());
        assertFalse(negotiated.hasPeerVisibleGroupSemantics());
    }
}
