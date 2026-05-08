package io.zmux.protocol;

import io.zmux.ErrorCode;
import io.zmux.ZmuxException;
import java.io.ByteArrayOutputStream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class FrameCodecDiagTest {
    private static void appendTlv(ByteArrayOutputStream output, long type, byte[] value) throws Exception {
        Varint62.write(output, type);
        Varint62.write(output, value.length);
        output.write(value);
    }

    @Test
    void parseErrorPayloadDuplicateDebugTextDropsReason() throws Exception {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        Varint62.write(payload, ErrorCode.PROTOCOL.code());
        appendTlv(payload, Protocol.DIAG_DEBUG_TEXT, "first".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        appendTlv(payload, Protocol.DIAG_DEBUG_TEXT, "second".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        FrameCodec.ErrorPayload parsed = FrameCodec.parseErrorPayload(payload.toByteArray());
        assertEquals(ErrorCode.PROTOCOL.code(), parsed.code(), "error code mismatch");
        assertEquals("", parsed.reason(), "duplicate singleton DIAG should invalidate the reason text");
    }

    @Test
    void parseGoAwayPayloadDuplicateRetryAfterDropsReason() throws Exception {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        Varint62.write(payload, 8L);
        Varint62.write(payload, 12L);
        Varint62.write(payload, ErrorCode.INTERNAL.code());
        appendTlv(payload, Protocol.DIAG_RETRY_AFTER_MILLIS, Varint62.encode(1L));
        appendTlv(payload, Protocol.DIAG_RETRY_AFTER_MILLIS, Varint62.encode(2L));
        appendTlv(payload, Protocol.DIAG_DEBUG_TEXT, "maintenance".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        FrameCodec.GoAwayPayload parsed = FrameCodec.parseGoAwayPayload(payload.toByteArray());
        assertEquals(8L, parsed.lastAcceptedBidi(), "bidi watermark mismatch");
        assertEquals(12L, parsed.lastAcceptedUni(), "uni watermark mismatch");
        assertEquals(ErrorCode.INTERNAL.code(), parsed.code(), "code mismatch");
        assertEquals("", parsed.reason(), "duplicate singleton DIAG should invalidate the reason text");
    }

    @Test
    void parseErrorPayloadInvalidUtf8DropsReason() throws Exception {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        Varint62.write(payload, ErrorCode.PROTOCOL.code());
        appendTlv(payload, Protocol.DIAG_DEBUG_TEXT, new byte[]{(byte) 0xe2, (byte) 0x82});

        FrameCodec.ErrorPayload parsed = FrameCodec.parseErrorPayload(payload.toByteArray());
        assertEquals(ErrorCode.PROTOCOL.code(), parsed.code(), "error code mismatch");
        assertEquals("", parsed.reason(), "invalid UTF-8 should not surface a replacement-character reason");
    }

    @Test
    void parseGoAwayPayloadInvalidUtf8DropsReason() throws Exception {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        Varint62.write(payload, 8L);
        Varint62.write(payload, 12L);
        Varint62.write(payload, ErrorCode.INTERNAL.code());
        appendTlv(payload, Protocol.DIAG_DEBUG_TEXT, new byte[]{(byte) 0xe2, (byte) 0x82});

        FrameCodec.GoAwayPayload parsed = FrameCodec.parseGoAwayPayload(payload.toByteArray());
        assertEquals(8L, parsed.lastAcceptedBidi(), "bidi watermark mismatch");
        assertEquals(12L, parsed.lastAcceptedUni(), "uni watermark mismatch");
        assertEquals(ErrorCode.INTERNAL.code(), parsed.code(), "goaway code mismatch");
        assertEquals("", parsed.reason(), "invalid UTF-8 should not surface a replacement-character GOAWAY reason");
    }

    @Test
    void parseErrorPayloadOverlongUtf8DropsReason() throws Exception {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        Varint62.write(payload, ErrorCode.PROTOCOL.code());
        appendTlv(payload, Protocol.DIAG_DEBUG_TEXT, new byte[]{(byte) 0xc0, (byte) 0xaf});

        FrameCodec.ErrorPayload parsed = FrameCodec.parseErrorPayload(payload.toByteArray());
        assertEquals(ErrorCode.PROTOCOL.code(), parsed.code(), "error code mismatch");
        assertEquals("", parsed.reason(), "overlong UTF-8 should not surface a replacement-character reason");
    }

    @Test
    void parseErrorPayloadSurrogateUtf8DropsReason() throws Exception {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        Varint62.write(payload, ErrorCode.PROTOCOL.code());
        appendTlv(payload, Protocol.DIAG_DEBUG_TEXT, new byte[]{(byte) 0xed, (byte) 0xa0, (byte) 0x80});

        FrameCodec.ErrorPayload parsed = FrameCodec.parseErrorPayload(payload.toByteArray());
        assertEquals(ErrorCode.PROTOCOL.code(), parsed.code(), "error code mismatch");
        assertEquals("", parsed.reason(), "surrogate UTF-8 should not surface a replacement-character reason");
    }

    @Test
    void parseDiagReasonTruncatedVarintReturnsTruncatedTlv() {
        io.zmux.ZmuxException error = assertThrows(
                io.zmux.ZmuxException.class,
                () -> FrameCodec.parseDiagReason(new byte[]{0x40})
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "truncated DIAG TLV should be a protocol error");
        assertEquals("truncated tlv", error.getMessage(), "truncated DIAG TLV error mismatch");
    }

    @Test
    void buildGoAwayPayloadOmitsUnencodableReason() throws Exception {
        String invalidReason = new String(new char[]{'\uD83D'});

        byte[] payload = FrameCodec.buildGoAwayPayload(0L, 0L, ErrorCode.INTERNAL.code(), invalidReason, 16L);
        FrameCodec.GoAwayPayload parsed = FrameCodec.parseGoAwayPayload(payload);

        assertEquals(ErrorCode.INTERNAL.code(), parsed.code(), "goaway code mismatch");
        assertEquals("", parsed.reason(), "unencodable reason should be omitted on the wire");
        assertEquals(3, payload.length, "payload should contain only the three varints when the reason is omitted");
    }

    @Test
    void buildErrorPayloadTruncatesReasonAtUtf8CodePointBoundary() throws Exception {
        byte[] payload = FrameCodec.buildErrorPayload(ErrorCode.INTERNAL.code(), "€€", 7L);
        FrameCodec.ErrorPayload parsed = FrameCodec.parseErrorPayload(payload);

        assertEquals(ErrorCode.INTERNAL.code(), parsed.code(), "error code mismatch");
        assertEquals("€", parsed.reason(), "reason should be truncated to the longest UTF-8-safe prefix");
    }

    @Test
    void buildErrorPayloadAccountsForDebugTextLengthVarintGrowth() throws Exception {
        String reason = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()";
        byte[] payload = FrameCodec.buildErrorPayload(ErrorCode.INTERNAL.code(), reason, 66L);
        FrameCodec.ErrorPayload parsed = FrameCodec.parseErrorPayload(payload);

        assertEquals(ErrorCode.INTERNAL.code(), parsed.code(), "error code mismatch");
        assertEquals(reason.substring(0, 63), parsed.reason(),
                "reason cap should reserve room for the enlarged DIAG length varint");
        assertEquals(66, payload.length, "payload should fit exactly within the advertised cap");
    }

    @Test
    void buildErrorPayloadSkipsReasonWhenNoDebugTextTlvRoom() throws Exception {
        byte[] payload = FrameCodec.buildErrorPayload(ErrorCode.INTERNAL.code(), "x", 2L);
        FrameCodec.ErrorPayload parsed = FrameCodec.parseErrorPayload(payload);

        assertEquals(ErrorCode.INTERNAL.code(), parsed.code(), "error code mismatch");
        assertEquals("", parsed.reason(), "reason should be omitted when the cap cannot fit a DIAG TLV");
        assertEquals(1, payload.length, "payload should contain only the error code");
    }
}
