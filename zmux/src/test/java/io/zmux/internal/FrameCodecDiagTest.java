package io.zmux.internal;

import io.zmux.ErrorCode;
import io.zmux.Protocol;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
