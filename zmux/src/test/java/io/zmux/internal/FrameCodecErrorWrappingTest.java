package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class FrameCodecErrorWrappingTest {
    @Test
    void readFrameWrapsInvalidFrameTypeAsProtocolError() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Varint62.write(output, 2L);
        output.write(0);
        Varint62.write(output, 0L);

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(new ByteArrayInputStream(output.toByteArray()), Settings.defaults().limits())
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "invalid frame types must surface as PROTOCOL errors");
        assertEquals(ZmuxErrorScope.SESSION, error.scope());
        assertEquals(ZmuxErrorSource.REMOTE, error.source());
        assertEquals(ZmuxErrorDirection.READ, error.direction());
    }

    @Test
    void readFrameWrapsTruncatedFramePayloadAsProtocolError() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Varint62.write(output, 3L);
        output.write(FrameType.PING.code());
        Varint62.write(output, 0L);

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(new ByteArrayInputStream(output.toByteArray()), Settings.defaults().limits())
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "truncated frames must surface as PROTOCOL errors");
        assertEquals(ZmuxErrorScope.SESSION, error.scope());
        assertEquals(ZmuxErrorSource.REMOTE, error.source());
        assertEquals(ZmuxErrorDirection.READ, error.direction());
    }

    @Test
    void readFrameRejectsOversizedFrameLengthBeforeBodyRead() throws Exception {
        Limits tinyLimits = new Limits(8L, 8L, 8L);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Varint62.write(output, 18L);

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(new ByteArrayInputStream(output.toByteArray()), tinyLimits)
        );
        assertEquals(ErrorCode.FRAME_SIZE.code(), error.code(), "oversized frame length should fail before reading a partial body");
        assertEquals(ZmuxErrorSource.REMOTE, error.source());
        assertEquals(ZmuxErrorDirection.READ, error.direction());
    }

    @Test
    void readFrameRejectsShortStreamIdBeforeReadingNextFrameBytes() {
        byte[] bytes = new byte[]{2, (byte) FrameType.DATA.code(), (byte) 0xc0, 99, 98, 97};
        CountingInputStream input = new CountingInputStream(bytes);

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(input, Settings.defaults().limits())
        );

        assertEquals(ErrorCode.FRAME_SIZE.code(), error.code(), "short stream_id must fail as frame-size");
        assertEquals(3, input.reads(), "reader must stop after the first stream_id byte");
    }

    @Test
    void readFrameWrapsMissingOpenMetadataLengthAsFrameSize() throws Exception {
        byte[] bytes = frame(FrameType.DATA, Protocol.FRAME_FLAG_OPEN_METADATA, 4L, new byte[0]);

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(new ByteArrayInputStream(bytes), Settings.defaults().limits())
        );

        assertEquals(ErrorCode.FRAME_SIZE.code(), error.code(), "missing OPEN_METADATA length should fail as frame-size");
        assertEquals(ZmuxErrorSource.REMOTE, error.source());
        assertEquals(ZmuxErrorDirection.READ, error.direction());
    }

    @Test
    void readFrameWrapsMalformedOpenMetadataTlvAsFrameSize() throws Exception {
        byte[] payload = new byte[]{
                2,
                (byte) Protocol.METADATA_STREAM_PRIORITY
        };
        byte[] bytes = frame(FrameType.DATA, Protocol.FRAME_FLAG_OPEN_METADATA, 4L, payload);

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(new ByteArrayInputStream(bytes), Settings.defaults().limits())
        );

        assertEquals(ErrorCode.FRAME_SIZE.code(), error.code(), "malformed OPEN_METADATA TLV should fail as frame-size");
    }

    @Test
    void readFrameWrapsMissingControlPayloadVarintAsFrameSize() throws Exception {
        byte[] bytes = frame(FrameType.RESET, 0, 4L, new byte[0]);

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(new ByteArrayInputStream(bytes), Settings.defaults().limits())
        );

        assertEquals(ErrorCode.FRAME_SIZE.code(), error.code(), "missing RESET error code should fail as frame-size");
    }

    @Test
    void readFrameWrapsMissingGoAwayPayloadVarintsAsFrameSize() throws Exception {
        byte[] bytes = frame(FrameType.GOAWAY, 0, 0L, new byte[0]);

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readFrame(new ByteArrayInputStream(bytes), Settings.defaults().limits())
        );

        assertEquals(ErrorCode.FRAME_SIZE.code(), error.code(), "missing GOAWAY fields should fail as frame-size");
    }

    @Test
    void readPrefaceWrapsInvalidRoleAsProtocolError() throws Exception {
        byte[] preface = new byte[]{
                'Z', 'M', 'U', 'X',
                Protocol.PREFACE_VERSION,
                (byte) 99
        };

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readPreface(new ByteArrayInputStream(preface))
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "invalid roles must surface as PROTOCOL errors");
        assertEquals(ZmuxErrorScope.SESSION, error.scope());
        assertEquals(ZmuxErrorSource.REMOTE, error.source());
        assertEquals(ZmuxErrorDirection.READ, error.direction());
    }

    @Test
    void readPrefaceWrapsTruncationAsProtocolError() {
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.readPreface(new ByteArrayInputStream(new byte[]{'Z', 'M', 'U'}))
        );
        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "truncated prefaces must surface as PROTOCOL errors");
        assertEquals(ZmuxErrorScope.SESSION, error.scope());
        assertEquals(ZmuxErrorSource.REMOTE, error.source());
        assertEquals(ZmuxErrorDirection.READ, error.direction());
    }

    @Test
    void buildPriorityUpdateWrapsLocalCapabilityFailureAsStructuredError() {
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.buildPriorityUpdatePayload(Protocol.CAPABILITY_PRIORITY_UPDATE, 7L, null, Settings.defaults().maxExtensionPayloadBytes())
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code());
        assertEquals(ZmuxErrorScope.SESSION, error.scope());
        assertEquals(ZmuxErrorSource.LOCAL, error.source());
        assertEquals(ZmuxErrorDirection.WRITE, error.direction());
    }

    @Test
    void gatheredFrameWriteStallSurfacesStructuredInternalError() {
        GatheringByteChannel stalled = new GatheringByteChannel() {
            @Override
            public int write(ByteBuffer src) {
                return 0;
            }

            @Override
            public long write(ByteBuffer[] srcs, int offset, int length) {
                return 0L;
            }

            @Override
            public long write(ByteBuffer[] srcs) {
                return 0L;
            }

            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public void close() {
            }
        };

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.writeFrame(
                        stalled,
                        new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[8]),
                        Settings.defaults().limits()
                )
        );

        assertEquals(ErrorCode.INTERNAL.code(), error.code());
        assertEquals(ZmuxErrorScope.SESSION, error.scope());
        assertEquals(ZmuxErrorSource.LOCAL, error.source());
        assertEquals(ZmuxErrorDirection.WRITE, error.direction());
    }

    private static byte[] frame(FrameType type, int flags, long streamId, byte[] payload) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        long frameLength = 1L + Varint62.length(streamId) + payload.length;
        Varint62.write(output, frameLength);
        output.write(type.code() | flags);
        Varint62.write(output, streamId);
        output.write(payload);
        return output.toByteArray();
    }

    private static final class CountingInputStream extends ByteArrayInputStream {
        private int reads;

        private CountingInputStream(byte[] bytes) {
            super(bytes);
        }

        @Override
        public synchronized int read() {
            int value = super.read();
            if (value >= 0) {
                reads++;
            }
            return value;
        }

        private int reads() {
            return reads;
        }
    }
}
