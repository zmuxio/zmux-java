package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

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
}
