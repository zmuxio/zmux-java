package io.zmux.protocol;

import io.zmux.ErrorCode;
import io.zmux.Role;
import io.zmux.Settings;
import io.zmux.ZmuxConfig;
import io.zmux.ZmuxException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

final class PrefaceCodecValidationTest {
    @Test
    void writePrefaceRejectsZeroMinProto() {
        Preface preface = new Preface(
                Protocol.PREFACE_VERSION,
                Role.INITIATOR,
                0L,
                0L,
                Protocol.PROTO_VERSION,
                0L,
                Settings.defaults()
        );

        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                assertThrows(IOException.class, () -> FrameCodec.writePreface(new ByteArrayOutputStream(), preface)),
                "zero minProto should be rejected before marshal"
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "zero minProto error code mismatch");
        assertEquals("protocol version bounds must be non-zero", error.getMessage(), "zero minProto error mismatch");
    }

    @Test
    void writePrefaceRejectsZeroMaxProto() {
        Preface preface = new Preface(
                Protocol.PREFACE_VERSION,
                Role.RESPONDER,
                0L,
                Protocol.PROTO_VERSION,
                0L,
                0L,
                Settings.defaults()
        );

        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                assertThrows(IOException.class, () -> FrameCodec.writePreface(new ByteArrayOutputStream(), preface)),
                "zero maxProto should be rejected before marshal"
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "zero maxProto error code mismatch");
        assertEquals("protocol version bounds must be non-zero", error.getMessage(), "zero maxProto error mismatch");
    }

    @Test
    void writePrefaceRejectsNullRoleAndSettings() {
        Preface nullRole = new Preface(
                Protocol.PREFACE_VERSION,
                null,
                0L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                0L,
                Settings.defaults()
        );
        ZmuxException roleError = assertInstanceOf(
                ZmuxException.class,
                assertThrows(IOException.class, () -> FrameCodec.writePreface(new ByteArrayOutputStream(), nullRole)),
                "null role should not leak NullPointerException"
        );
        assertEquals(ErrorCode.PROTOCOL.code(), roleError.code(), "null role error code mismatch");
        assertEquals("invalid role", roleError.getMessage(), "null role error mismatch");

        Preface nullSettings = new Preface(
                Protocol.PREFACE_VERSION,
                Role.INITIATOR,
                0L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                0L,
                null
        );
        ZmuxException settingsError = assertInstanceOf(
                ZmuxException.class,
                assertThrows(IOException.class, () -> FrameCodec.writePreface(new ByteArrayOutputStream(), nullSettings)),
                "null settings should not leak NullPointerException"
        );
        assertEquals(ErrorCode.PROTOCOL.code(), settingsError.code(), "null settings error code mismatch");
        assertEquals("settings are required", settingsError.getMessage(), "null settings error mismatch");
    }

    @Test
    void negotiatePrefacesRejectsNullRoleAndSettingsAsStructuredProtocolErrors() {
        Preface valid = new Preface(
                Protocol.PREFACE_VERSION,
                Role.INITIATOR,
                0L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                0L,
                Settings.defaults()
        );
        Preface nullRole = new Preface(
                Protocol.PREFACE_VERSION,
                null,
                0L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                0L,
                Settings.defaults()
        );
        ZmuxException roleError = assertInstanceOf(
                ZmuxException.class,
                assertThrows(IOException.class, () -> FrameCodec.negotiate(nullRole, valid)),
                "null role should not leak NullPointerException from negotiate"
        );
        assertEquals(ErrorCode.PROTOCOL.code(), roleError.code(), "null role negotiate error code mismatch");
        assertEquals("negotiate prefaces", roleError.operation(), "null role negotiate operation mismatch");
        assertEquals("invalid role", roleError.getMessage(), "null role negotiate error mismatch");

        Preface nullSettings = new Preface(
                Protocol.PREFACE_VERSION,
                Role.RESPONDER,
                0L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                0L,
                null
        );
        ZmuxException settingsError = assertInstanceOf(
                ZmuxException.class,
                assertThrows(IOException.class, () -> FrameCodec.negotiate(valid, nullSettings)),
                "null settings should not leak NullPointerException from negotiate"
        );
        assertEquals(ErrorCode.PROTOCOL.code(), settingsError.code(), "null settings negotiate error code mismatch");
        assertEquals("negotiate prefaces", settingsError.operation(), "null settings negotiate operation mismatch");
        assertEquals("settings are required", settingsError.getMessage(), "null settings negotiate error mismatch");
    }

    @Test
    void parseSettingsSkipsUnknownSettingPayloadWithoutInterpretingValue() throws IOException {
        byte[] raw = ZmuxCodec.appendTlv(null, 99L, new byte[]{(byte) 0xff, 0x00, 0x01});

        Settings parsed = PrefaceCodec.parseSettings(raw);

        assertEquals(Settings.defaults(), parsed, "unknown setting payload should be skipped as opaque TLV value");
    }

    @Test
    void parseSettingsReadsPingPaddingKeyAndSkipsPrefacePaddingPayload() throws IOException {
        byte[] raw = ZmuxCodec.appendTlv(
                null,
                Protocol.SETTING_PING_PADDING_KEY,
                ZmuxCodec.encodeVarint(456L)
        );
        raw = ZmuxCodec.appendTlv(raw, Protocol.SETTING_PREFACE_PADDING, new byte[]{1, 2, 3, 4});

        Settings parsed = PrefaceCodec.parseSettings(raw);

        assertEquals(456L, parsed.pingPaddingKey());
    }

    @Test
    void parseSettingsRejectsDuplicatePrefacePaddingSetting() throws IOException {
        byte[] raw = ZmuxCodec.appendTlv(null, Protocol.SETTING_PREFACE_PADDING, new byte[]{1});
        raw = ZmuxCodec.appendTlv(raw, Protocol.SETTING_PREFACE_PADDING, new byte[]{2});
        byte[] encoded = raw;

        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                assertThrows(IOException.class, () -> PrefaceCodec.parseSettings(encoded))
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code());
        assertEquals("duplicate setting id " + Protocol.SETTING_PREFACE_PADDING, error.getMessage());
    }

    @Test
    void parseSettingsBoundsKnownSettingValueVarintToTlvValue() throws IOException {
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        Varint62.write(raw, Protocol.SETTING_INITIAL_MAX_DATA);
        Varint62.write(raw, 1L);
        raw.write(0x40);
        FrameCodec.appendTlv(raw, 99L, new byte[]{0x01});

        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                assertThrows(IOException.class, () -> PrefaceCodec.parseSettings(raw.toByteArray()))
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code());
        assertEquals("parse settings", error.operation());
        assertEquals("truncated varint62", error.getMessage());
        assertNotNull(error.getCause(), "bounded setting value parse should retain the varint cause");
        assertEquals("truncated varint62", error.getCause().getMessage());
    }

    @Test
    void parseSettingsRejectsEmptyKnownSettingValueAsTruncatedVarint() throws IOException {
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        Varint62.write(raw, Protocol.SETTING_INITIAL_MAX_DATA);
        Varint62.write(raw, 0L);
        FrameCodec.appendTlv(raw, 99L, new byte[]{0x01});

        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                assertThrows(IOException.class, () -> PrefaceCodec.parseSettings(raw.toByteArray()))
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code());
        assertEquals("parse settings", error.operation());
        assertEquals("truncated varint62", error.getMessage());
    }

    @Test
    void parseSettingsPreservesNonCanonicalKnownSettingValue() throws IOException {
        byte[] raw = ZmuxCodec.appendTlv(
                null,
                Protocol.SETTING_INITIAL_MAX_DATA,
                new byte[]{0x40, 0x01}
        );

        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                assertThrows(IOException.class, () -> PrefaceCodec.parseSettings(raw))
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code());
        assertEquals("parse settings", error.operation());
        assertEquals("non-canonical varint62", error.getMessage());
    }

    @Test
    void writePrefaceWithConfigAddsIgnoredSettingsPadding() throws IOException {
        Preface preface = ZmuxConfig.builder()
                .role(Role.INITIATOR)
                .build()
                .localPreface();
        ZmuxConfig paddingConfig = ZmuxConfig.builder()
                .prefacePadding(true)
                .prefacePaddingMinBytes(16L)
                .prefacePaddingMaxBytes(16L)
                .build();
        ByteArrayOutputStream plain = new ByteArrayOutputStream();
        ByteArrayOutputStream padded = new ByteArrayOutputStream();

        FrameCodec.writePreface(plain, preface);
        FrameCodec.writePreface(padded, preface, paddingConfig);

        assertTrue(padded.size() > plain.size(), "preface padding should increase encoded settings length");
        assertEquals(preface, FrameCodec.readPreface(new ByteArrayInputStream(padded.toByteArray())));
    }
}
