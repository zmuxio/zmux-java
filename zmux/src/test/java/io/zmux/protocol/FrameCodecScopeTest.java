package io.zmux.protocol;

import io.zmux.ErrorCode;
import io.zmux.Settings;
import io.zmux.ZmuxException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class FrameCodecScopeTest {
    private static byte[] encodeFrame(FrameType type, int flags, long streamId, byte[] payload) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        long frameLength = 1L + Varint62.length(streamId) + payload.length;
        Varint62.write(output, frameLength);
        output.write(type.code() | flags);
        Varint62.write(output, streamId);
        output.write(payload);
        return output.toByteArray();
    }

    @Test
    void readFrameRejectsPingOnNonZeroStreamId() throws Exception {
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(
                        new ByteArrayInputStream(encodeFrame(FrameType.PING, 0, 4L, new byte[8])),
                        Settings.defaults().limits()
                )
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "PING with non-zero stream_id must fail with PROTOCOL");
    }

    @Test
    void readFrameRejectsAbortOnStreamZero() throws Exception {
        byte[] payload = FrameCodec.buildErrorPayload(ErrorCode.CANCELLED.code(), "", Settings.defaults().maxControlPayloadBytes());
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(
                        new ByteArrayInputStream(encodeFrame(FrameType.ABORT, 0, 0L, payload)),
                        Settings.defaults().limits()
                )
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "ABORT on stream_id 0 must fail with PROTOCOL");
    }

    @Test
    void readFrameRejectsPriorityUpdateOnStreamZero() throws Exception {
        byte[] payload = FrameCodec.buildPriorityUpdatePayload(
                Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS,
                7L,
                null,
                Settings.defaults().maxExtensionPayloadBytes()
        );
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(
                        new ByteArrayInputStream(encodeFrame(FrameType.EXT, 0, 0L, payload)),
                        Settings.defaults().limits()
                )
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "PRIORITY_UPDATE on stream_id 0 must fail with PROTOCOL");
    }

    @Test
    void readFrameRejectsEmptyExtPayloadWithProtocol() throws Exception {
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(
                        new ByteArrayInputStream(encodeFrame(FrameType.EXT, 0, 4L, new byte[0])),
                        Settings.defaults().limits()
                )
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "empty EXT payload must fail with PROTOCOL (SPEC 6.11)");
    }

    @Test
    void readFrameRejectsTruncatedExtSubtypeWithProtocol() throws Exception {
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(
                        new ByteArrayInputStream(encodeFrame(FrameType.EXT, 0, 4L, new byte[]{0x40})),
                        Settings.defaults().limits()
                )
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "truncated EXT subtype must fail with PROTOCOL (SPEC 6.11)");
    }

    @Test
    void readFrameRejectsNonCanonicalExtSubtypeWithProtocol() throws Exception {
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(
                        new ByteArrayInputStream(encodeFrame(FrameType.EXT, 0, 4L, new byte[]{0x40, 0x01})),
                        Settings.defaults().limits()
                )
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "non-canonical EXT subtype must fail with PROTOCOL (SPEC 6.11)");
    }

    @Test
    void readFrameKeepsFrameSizeForTruncatedPriorityUpdateTlv() throws Exception {
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(
                        new ByteArrayInputStream(encodeFrame(FrameType.EXT, 0, 4L, new byte[]{0x01, 0x01})),
                        Settings.defaults().limits()
                )
        );
        assertEquals(ErrorCode.FRAME_SIZE.code(), error.code(), "truncated PRIORITY_UPDATE TLV must fail with FRAME_SIZE");
    }

    @Test
    void sessionFrameReadDefersPriorityUpdateSubtypeValidation() throws Exception {
        byte[] streamZero = encodeFrame(FrameType.EXT, 0, 0L, new byte[]{0x01, 0x01, 0x01, 0x02});
        byte[] truncatedTlv = encodeFrame(FrameType.EXT, 0, 4L, new byte[]{0x01, 0x01});
        for (byte[] encoded : new byte[][]{streamZero, truncatedTlv}) {
            FrameEnvelopeCodec.InboundFrame frame = FrameEnvelopeCodec.readInboundSessionFrame(
                    FrameCodec.decoder(new ByteArrayInputStream(encoded)),
                    Settings.defaults().limits(),
                    null
            );
            assertEquals(FrameType.EXT, frame.type(), "session read should return the EXT frame for the session to judge");
            assertThrows(
                    ZmuxException.class,
                    () -> FrameCodec.readFrame(new ByteArrayInputStream(encoded), Settings.defaults().limits()),
                    "the public codec read stays strict"
            );
        }

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameEnvelopeCodec.readInboundSessionFrame(
                        FrameCodec.decoder(new ByteArrayInputStream(encodeFrame(FrameType.EXT, 0, 4L, new byte[0]))),
                        Settings.defaults().limits(),
                        null
                )
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "session read still rejects an EXT payload without ext_type");
    }

    @Test
    void readFrameRejectsCloseOnNonZeroStreamId() throws Exception {
        byte[] payload = FrameCodec.buildErrorPayload(ErrorCode.INTERNAL.code(), "close", Settings.defaults().maxControlPayloadBytes());
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(
                        new ByteArrayInputStream(encodeFrame(FrameType.CLOSE, 0, 4L, payload)),
                        Settings.defaults().limits()
                )
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "CLOSE with non-zero stream_id must fail with PROTOCOL");
    }
}
