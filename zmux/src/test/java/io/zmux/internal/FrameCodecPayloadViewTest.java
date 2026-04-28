package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

final class FrameCodecPayloadViewTest {
    @Test
    void parseDataPayloadRetainsOpenInfoAfterSourceMutation() throws Exception {
        byte[] payload = FrameCodec.buildOpenMetadataPrefix(
                Protocol.CAPABILITY_OPEN_METADATA,
                null,
                null,
                "ssh".getBytes(StandardCharsets.UTF_8),
                1024
        );

        FrameCodec.DataPayload parsed = FrameCodec.parseDataPayload(payload, Protocol.FRAME_FLAG_OPEN_METADATA);
        Arrays.fill(payload, (byte) 'x');

        assertArrayEquals("ssh".getBytes(StandardCharsets.UTF_8), parsed.openInfo(), "open_info should be retained independently from the source payload");

        byte[] exposed = parsed.openInfo();
        exposed[0] = 'x';
        assertArrayEquals("ssh".getBytes(StandardCharsets.UTF_8), parsed.openInfo(), "retained open_info accessor should remain defensive");
    }

    @Test
    void parseDataPayloadViewAliasesAppDataSource() throws Exception {
        byte[] prefix = FrameCodec.buildOpenMetadataPrefix(
                Protocol.CAPABILITY_OPEN_METADATA,
                null,
                null,
                "ssh".getBytes(StandardCharsets.UTF_8),
                1024
        );
        byte[] payload = Arrays.copyOf(prefix, prefix.length + 3);
        payload[prefix.length] = 'a';
        payload[prefix.length + 1] = 'b';
        payload[prefix.length + 2] = 'c';

        FrameCodec.DataPayload parsed = FrameCodec.parseDataPayloadView(payload, Protocol.FRAME_FLAG_OPEN_METADATA);
        payload[prefix.length + 2] = 'z';

        assertEquals("abz", new String(parsed.appData(), StandardCharsets.UTF_8), "view parsing should expose the current app-data slice without a second copy");
    }

    @Test
    void parseDataPayloadViewDefersOpenInfoCopyButKeepsAccessorDefensive() throws Exception {
        byte[] prefix = FrameCodec.buildOpenMetadataPrefix(
                Protocol.CAPABILITY_OPEN_METADATA,
                null,
                null,
                "ssh".getBytes(StandardCharsets.UTF_8),
                1024
        );
        byte[] payload = Arrays.copyOf(prefix, prefix.length + 1);
        payload[prefix.length] = 'x';

        FrameCodec.DataPayload parsed = FrameCodec.parseDataPayloadView(payload, Protocol.FRAME_FLAG_OPEN_METADATA);
        payload[prefix.length - 1] = '!';

        assertArrayEquals("ss!".getBytes(StandardCharsets.UTF_8), parsed.openInfo(), "view parsing should defer open_info copying until accessor use");

        byte[] exposed = parsed.openInfo();
        exposed[0] = 'x';
        assertArrayEquals("ss!".getBytes(StandardCharsets.UTF_8), parsed.openInfo(), "open_info accessor should remain defensive");
    }

    @Test
    void parseStreamMetadataViewDefersOpenInfoCopy() throws Exception {
        byte[] prefix = FrameCodec.buildOpenMetadataPrefix(
                Protocol.CAPABILITY_OPEN_METADATA | Protocol.CAPABILITY_PRIORITY_HINTS | Protocol.CAPABILITY_STREAM_GROUPS,
                3L,
                7L,
                "ssh".getBytes(StandardCharsets.UTF_8),
                1024
        );
        Varint62.Decoded metadataLength = Varint62.decode(prefix, 0);
        int metadataOffset = metadataLength.length();
        int metadataLengthInt = (int) metadataLength.value();
        byte[] metadataBytes = Arrays.copyOfRange(prefix, metadataOffset, metadataOffset + metadataLengthInt);

        FrameCodec.ParsedMetadata parsed = FrameCodec.parseStreamMetadataView(metadataBytes);
        metadataBytes[metadataBytes.length - 1] = '!';

        assertEquals(3L, parsed.priority(), "priority metadata mismatch");
        assertEquals(Long.valueOf(7L), parsed.group(), "group metadata mismatch");
        assertArrayEquals("ss!".getBytes(StandardCharsets.UTF_8), parsed.openInfo(), "view parsing should defer open_info copying until accessor use");

        byte[] exposed = parsed.openInfo();
        exposed[0] = 'x';
        assertArrayEquals("ss!".getBytes(StandardCharsets.UTF_8), parsed.openInfo(), "parsed metadata accessor should remain defensive");
    }

    @Test
    void parseTlvsReturnsDefensiveValueObjects() throws Exception {
        byte[] metadata = "ssh".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        FrameCodec.appendTlv(output, Protocol.METADATA_OPEN_INFO, metadata);

        java.util.List<Tlv> tlvs = FrameCodec.parseTlvs(output.toByteArray());
        byte[] exposed = tlvs.get(0).value();
        exposed[0] = 'x';

        assertArrayEquals("ssh".getBytes(StandardCharsets.UTF_8), tlvs.get(0).value(), "TLV values should remain defensive");
    }

    @Test
    void parseTlvsRejectsLengthPastContainingPayload() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Varint62.write(output, Protocol.METADATA_OPEN_INFO);
        Varint62.write(output, Protocol.MAX_VARINT62);

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.parseTlvs(output.toByteArray())
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "oversized TLV length should be a protocol error");
        assertEquals("tlv value overruns containing payload", error.getMessage(), "oversized TLV length error mismatch");
    }

    @Test
    void parseTlvsWrapsTruncatedTypeVarintAsTlvError() {
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.parseTlvs(new byte[]{0x40})
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "truncated TLV type should be a protocol error");
        assertEquals("truncated tlv", error.getMessage(), "truncated TLV type error mismatch");
        assertNotNull(error.getCause(), "truncated TLV should retain the varint cause");
        assertEquals("truncated varint62", error.getCause().getMessage(), "truncated TLV cause mismatch");
    }

    @Test
    void parseTlvsWrapsTruncatedLengthVarintAsTlvError() {
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.parseTlvs(new byte[]{0x01, 0x40})
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "truncated TLV length should be a protocol error");
        assertEquals("truncated tlv", error.getMessage(), "truncated TLV length error mismatch");
        assertNotNull(error.getCause(), "truncated TLV should retain the varint cause");
        assertEquals("truncated varint62", error.getCause().getMessage(), "truncated TLV cause mismatch");
    }

    @Test
    void parseTlvsPreservesNonCanonicalVarintError() {
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.parseTlvs(new byte[]{0x40, 0x01, 0x00})
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "non-canonical TLV varint should be a protocol error");
        assertEquals("non-canonical varint62", error.getMessage(), "non-canonical varint error should be preserved");
    }

    @Test
    void parseDataPayloadBoundsMetadataTlvTypeToMetadataLength() {
        byte[] payload = {
                1,
                0x40,
                0
        };

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.parseDataPayloadView(payload, Protocol.FRAME_FLAG_OPEN_METADATA)
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "truncated metadata TLV type should be a protocol error");
        assertEquals("truncated tlv", error.getMessage(), "metadata TLV type must not read into app data");
    }

    @Test
    void parseDataPayloadBoundsMetadataTlvLengthToMetadataLength() {
        byte[] payload = {
                2,
                (byte) Protocol.METADATA_OPEN_INFO,
                0x40,
                0
        };

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.parseDataPayloadView(payload, Protocol.FRAME_FLAG_OPEN_METADATA)
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "truncated metadata TLV length should be a protocol error");
        assertEquals("truncated tlv", error.getMessage(), "metadata TLV length must not read into app data");
    }

    @Test
    void parseDataPayloadRejectsOpenMetadataLengthPastPayload() throws Exception {
        byte[] payload = Varint62.encode(Protocol.MAX_VARINT62);

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.parseDataPayload(payload, Protocol.FRAME_FLAG_OPEN_METADATA)
        );

        assertEquals(ErrorCode.FRAME_SIZE.code(), error.code(), "oversized opening metadata should be a frame-size error");
        assertEquals("open metadata overruns data payload", error.getMessage(), "oversized opening metadata error mismatch");
    }

    @Test
    void parseStreamMetadataBoundsVarintsToTlvValue() {
        byte[] metadata = {
                (byte) Protocol.METADATA_STREAM_PRIORITY,
                1,
                0x40,
                (byte) Protocol.METADATA_OPEN_INFO,
                1,
                'x'
        };

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.parseStreamMetadataView(metadata)
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "truncated metadata varint should be a protocol parse error");
        assertEquals("truncated varint62", error.getMessage(), "metadata varint must not read into the following TLV");
    }

    @Test
    void parseStreamMetadataRejectsTrailingMetadataVarintBytesAsTlvOverrun() {
        byte[] metadata = {
                (byte) Protocol.METADATA_STREAM_PRIORITY,
                2,
                7,
                'x'
        };

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.parseStreamMetadataView(metadata)
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "trailing metadata varint bytes should be a protocol parse error");
        assertEquals("tlv value overruns containing payload", error.getMessage(), "metadata varint must consume the full TLV value");
    }

    @Test
    void parsePriorityUpdateBoundsVarintsToTlvValue() {
        byte[] payload = {
                (byte) Protocol.EXT_PRIORITY_UPDATE,
                (byte) Protocol.METADATA_STREAM_PRIORITY,
                1,
                0x40,
                (byte) Protocol.METADATA_OPEN_INFO,
                1,
                'x'
        };

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> FrameCodec.parsePriorityUpdatePayload(payload)
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code(), "truncated priority-update varint should be a protocol parse error");
        assertEquals("truncated varint62", error.getMessage(), "priority-update varint must not read into the following TLV");
    }

    @Test
    void writeFrameSupportsOpeningPrefixPlusBodySlice() throws Exception {
        byte[] prefix = FrameCodec.buildOpenMetadataPrefix(
                Protocol.CAPABILITY_OPEN_METADATA | Protocol.CAPABILITY_PRIORITY_HINTS | Protocol.CAPABILITY_STREAM_GROUPS,
                3L,
                7L,
                "ssh".getBytes(StandardCharsets.UTF_8),
                1024
        );
        byte[] body = "body".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        FrameCodec.writeFrame(
                output,
                new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_OPEN_METADATA, 9L, prefix),
                prefix,
                body,
                0,
                body.length,
                Settings.defaults().limits()
        );

        FrameCodec.Frame written = FrameCodec.readFrame(new ByteArrayInputStream(output.toByteArray()), Settings.defaults().limits());
        FrameCodec.DataPayload parsed = FrameCodec.parseDataPayload(written.payload(), written.flags());
        assertEquals(9L, written.streamId(), "stream id mismatch");
        assertEquals(3L, parsed.priority(), "priority metadata mismatch");
        assertEquals(Long.valueOf(7L), parsed.group(), "group metadata mismatch");
        assertArrayEquals("ssh".getBytes(StandardCharsets.UTF_8), parsed.openInfo(), "open_info mismatch");
        assertEquals("body", new String(parsed.appData(), StandardCharsets.UTF_8), "body payload mismatch");
    }

    @Test
    void writeFrameSupportsOpeningPrefixPlusMultipartBody() throws Exception {
        byte[] prefix = FrameCodec.buildOpenMetadataPrefix(
                Protocol.CAPABILITY_OPEN_METADATA | Protocol.CAPABILITY_PRIORITY_HINTS | Protocol.CAPABILITY_STREAM_GROUPS,
                3L,
                7L,
                "ssh".getBytes(StandardCharsets.UTF_8),
                1024
        );
        byte[][] bodyParts = {
                "bo".getBytes(StandardCharsets.UTF_8),
                "dy".getBytes(StandardCharsets.UTF_8)
        };
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        FrameCodec.writeFrame(
                output,
                new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_OPEN_METADATA, 9L, prefix),
                prefix,
                bodyParts,
                0,
                0,
                4,
                Settings.defaults().limits()
        );

        FrameCodec.Frame written = FrameCodec.readFrame(new ByteArrayInputStream(output.toByteArray()), Settings.defaults().limits());
        FrameCodec.DataPayload parsed = FrameCodec.parseDataPayload(written.payload(), written.flags());
        assertEquals(9L, written.streamId(), "stream id mismatch");
        assertEquals(3L, parsed.priority(), "priority metadata mismatch");
        assertEquals(Long.valueOf(7L), parsed.group(), "group metadata mismatch");
        assertArrayEquals("ssh".getBytes(StandardCharsets.UTF_8), parsed.openInfo(), "open_info mismatch");
        assertEquals("body", new String(parsed.appData(), StandardCharsets.UTF_8), "body payload mismatch");
    }

    @Test
    void writeFrameSupportsGatheringMultipartBodyWithoutFlattening() throws Exception {
        byte[] prefix = FrameCodec.buildOpenMetadataPrefix(
                Protocol.CAPABILITY_OPEN_METADATA | Protocol.CAPABILITY_PRIORITY_HINTS | Protocol.CAPABILITY_STREAM_GROUPS,
                3L,
                7L,
                "ssh".getBytes(StandardCharsets.UTF_8),
                1024
        );
        byte[][] bodyParts = {
                "bo".getBytes(StandardCharsets.UTF_8),
                "dy".getBytes(StandardCharsets.UTF_8)
        };
        RecordingGatheringChannel output = new RecordingGatheringChannel();

        FrameCodec.writeFrame(
                output,
                new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_OPEN_METADATA, 9L, prefix),
                prefix,
                bodyParts,
                0,
                0,
                4,
                Settings.defaults().limits()
        );

        FrameCodec.Frame written = FrameCodec.readFrame(new ByteArrayInputStream(output.bytes()), Settings.defaults().limits());
        FrameCodec.DataPayload parsed = FrameCodec.parseDataPayload(written.payload(), written.flags());
        assertEquals(9L, written.streamId(), "stream id mismatch");
        assertEquals(3L, parsed.priority(), "priority metadata mismatch");
        assertEquals(Long.valueOf(7L), parsed.group(), "group metadata mismatch");
        assertArrayEquals("ssh".getBytes(StandardCharsets.UTF_8), parsed.openInfo(), "open_info mismatch");
        assertEquals("body", new String(parsed.appData(), StandardCharsets.UTF_8), "body payload mismatch");
        assertEquals(3, output.lastBufferCount(), "gathering write should inline small metadata prefix with the encoded header while preserving body part boundaries");
    }

    @Test
    void writeFrameRejectsInvalidMultipartStartOffsetBeforeWriting() {
        byte[][] bodyParts = {
                "ab".getBytes(StandardCharsets.UTF_8),
                "cd".getBytes(StandardCharsets.UTF_8)
        };
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        assertThrows(
                IndexOutOfBoundsException.class,
                () -> FrameCodec.writeFrame(
                        output,
                        new FrameCodec.Frame(FrameType.DATA, 0, 9L, new byte[0]),
                        null,
                        bodyParts,
                        0,
                        -1,
                        1,
                        Settings.defaults().limits()
                )
        );

        assertEquals(0, output.size(), "invalid multipart offset must be rejected before writing a partial frame");
    }

    @Test
    void writeFrameAllowsMultipartOffsetAtPartEndAndContinuesWithNextPart() throws Exception {
        byte[][] bodyParts = {
                "ab".getBytes(StandardCharsets.UTF_8),
                "cd".getBytes(StandardCharsets.UTF_8)
        };
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        FrameCodec.writeFrame(
                output,
                new FrameCodec.Frame(FrameType.DATA, 0, 9L, new byte[0]),
                null,
                bodyParts,
                0,
                bodyParts[0].length,
                2,
                Settings.defaults().limits()
        );

        FrameCodec.Frame written = FrameCodec.readFrame(new ByteArrayInputStream(output.toByteArray()), Settings.defaults().limits());
        assertEquals("cd", new String(written.payload(), StandardCharsets.UTF_8), "offset at part end should skip to the next part");
    }

    @Test
    void outboundLengthAccountingUsesLongArithmetic() throws Exception {
        long encodedLength = FrameEnvelopeCodec.encodedPayloadLength(Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertEquals(4_294_967_294L, encodedLength);

        assertEquals(1L + Varint62.length(9L) + encodedLength, FrameEnvelopeCodec.frameLength(9L, encodedLength));
        assertThrows(
                IOException.class,
                () -> FrameEnvelopeCodec.frameLength(9L, Protocol.MAX_VARINT62)
        );
    }

    private static final class RecordingGatheringChannel implements GatheringByteChannel {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private boolean open = true;
        private int lastBufferCount;

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) {
            long written = 0L;
            int count = 0;
            for (int i = 0; i < length; ++i) {
                ByteBuffer src = srcs[offset + i];
                if (src == null || !src.hasRemaining()) {
                    continue;
                }
                count++;
                int remaining = src.remaining();
                byte[] bytes = new byte[remaining];
                src.get(bytes);
                output.write(bytes, 0, bytes.length);
                written += remaining;
            }
            lastBufferCount = count;
            return written;
        }

        @Override
        public long write(ByteBuffer[] srcs) {
            return write(srcs, 0, srcs.length);
        }

        @Override
        public int write(ByteBuffer src) {
            int remaining = src.remaining();
            byte[] bytes = new byte[remaining];
            src.get(bytes);
            output.write(bytes, 0, bytes.length);
            lastBufferCount = remaining > 0 ? 1 : 0;
            return remaining;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() throws IOException {
            open = false;
            output.close();
        }

        private byte[] bytes() {
            return output.toByteArray();
        }

        private int lastBufferCount() {
            return lastBufferCount;
        }
    }
}
