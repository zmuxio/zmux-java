package io.zmux.internal;

import io.zmux.*;
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
