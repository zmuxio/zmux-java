package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

final class SessionWriterTransportTest {
    private static final int RETAINED_ENCODED_BATCH_LIMIT = 32 * (int) Settings.defaults().maxFramePayload();
    private static final Settings LARGE_FRAME_SETTINGS = Settings.defaults()
            .toBuilder()
            .maxFramePayload(32 * 1024L)
            .build();
    private static final Settings OVERSIZED_FRAME_SETTINGS = Settings.defaults()
            .toBuilder()
            .maxFramePayload(RETAINED_ENCODED_BATCH_LIMIT + 4096L)
            .build();

    @Test
    void writeBatchEncodesLargeMultipartPayloadOnMergedOutputPath() throws Exception {
        RecordingOutputStream output = new RecordingOutputStream();
        SessionWriterTransport transport = new SessionWriterTransport(new TestOwner(output, LARGE_FRAME_SETTINGS.limits()));
        byte[][] parts = {
                repeated('a', 8 * 1024),
                repeated('b', 8 * 1024)
        };
        byte[] prefix = FrameCodec.buildOpenMetadataPrefix(
                Protocol.CAPABILITY_OPEN_METADATA,
                null,
                null,
                "meta".getBytes(StandardCharsets.UTF_8),
                LARGE_FRAME_SETTINGS.maxFramePayload()
        );
        SessionRuntime.OutboundFrame outbound = new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(
                        FrameType.DATA,
                        Protocol.FRAME_FLAG_OPEN_METADATA | Protocol.FRAME_FLAG_FIN,
                        1L,
                        prefix
                ),
                null,
                parts[0].length + parts[1].length,
                true,
                false,
                prefix,
                null,
                0,
                parts[0].length + parts[1].length,
                parts,
                0,
                0
        );

        long written = transport.writeBatch(Collections.singletonList(outbound));

        assertEquals(output.size(), written, "reported batch byte count should match encoded bytes");
        assertEquals(1, output.flushes(), "transport should flush once per batch");

        FrameCodec.Frame decoded = FrameCodec.readFrame(
                new ByteArrayInputStream(output.bytes()),
                LARGE_FRAME_SETTINGS.limits()
        );
        assertEquals(FrameType.DATA, decoded.type(), "decoded frame type mismatch");
        assertEquals(Protocol.FRAME_FLAG_OPEN_METADATA | Protocol.FRAME_FLAG_FIN, decoded.flags(), "decoded flags mismatch");
        FrameCodec.DataPayload decodedPayload = FrameCodec.parseDataPayload(decoded.payload(), decoded.flags());
        assertArrayEquals("meta".getBytes(StandardCharsets.UTF_8), decodedPayload.openInfo(), "open info mismatch");
        assertEquals(parts[0].length + parts[1].length, decodedPayload.appData().length, "application payload length mismatch");
        assertEquals((byte) 'a', decodedPayload.appData()[0], "first application payload byte mismatch");
        assertEquals((byte) 'b', decodedPayload.appData()[decodedPayload.appData().length - 1], "last application payload byte mismatch");
    }

    @Test
    void writeBatchUsesGatheringOutputForLargeDensePayload() throws Exception {
        RecordingOutputStream output = new RecordingOutputStream();
        RecordingGatheringByteChannel gathering = new RecordingGatheringByteChannel();
        SessionWriterTransport transport = new SessionWriterTransport(new TestOwner(output, LARGE_FRAME_SETTINGS.limits(), gathering));
        byte[] payload = repeated('g', 24 * 1024);
        SessionRuntime.OutboundFrame outbound = new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, 0, 1L, payload),
                null,
                payload.length,
                false,
                false
        );

        long written = transport.writeBatch(Collections.singletonList(outbound));

        assertEquals(gathering.size(), written, "reported batch byte count should match gathered bytes");
        assertEquals(1, gathering.writeCalls(), "large dense batch should use one gathering write");
        assertEquals(0, output.size(), "gather path must not copy the batch through OutputStream.write");
        assertEquals(1, output.flushes(), "gather path should preserve batch flush semantics");
        FrameCodec.Frame decoded = FrameCodec.readFrame(
                new ByteArrayInputStream(gathering.bytes()),
                LARGE_FRAME_SETTINGS.limits()
        );
        assertEquals(FrameType.DATA, decoded.type(), "decoded frame type mismatch");
        assertArrayEquals(payload, decoded.payload(), "decoded gathered payload mismatch");
    }

    @Test
    void writeBatchKeepsTinyPayloadOnMergedPathEvenWithGatheringOutput() throws Exception {
        RecordingOutputStream output = new RecordingOutputStream();
        RecordingGatheringByteChannel gathering = new RecordingGatheringByteChannel();
        SessionWriterTransport transport = new SessionWriterTransport(new TestOwner(output, Settings.defaults().limits(), gathering));
        byte[] payload = "small".getBytes(StandardCharsets.UTF_8);
        SessionRuntime.OutboundFrame outbound = new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, 0, 1L, payload),
                null,
                payload.length,
                false,
                false
        );

        long written = transport.writeBatch(Collections.singletonList(outbound));

        assertEquals(output.size(), written, "reported batch byte count should match merged bytes");
        assertEquals(0, gathering.writeCalls(), "tiny batches should avoid gathering overhead");
        assertEquals(1, output.flushes(), "merged path should flush once per batch");
    }

    @Test
    void writeBatchEncodesTinyPayloadOnMergedOutputPath() throws Exception {
        RecordingOutputStream output = new RecordingOutputStream();
        SessionWriterTransport transport = new SessionWriterTransport(new TestOwner(output));
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        SessionRuntime.OutboundFrame outbound = new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 1L, payload),
                null,
                payload.length,
                false,
                false
        );

        long written = transport.writeBatch(Collections.singletonList(outbound));

        assertEquals(output.size(), written, "reported batch byte count should match output bytes");
        assertEquals(1, output.flushes(), "fallback path should flush once per batch");
        FrameCodec.Frame decoded = FrameCodec.readFrame(
                new ByteArrayInputStream(output.bytes()),
                Settings.defaults().limits()
        );
        assertEquals(FrameType.DATA, decoded.type(), "decoded frame type mismatch");
        assertEquals("hello", new String(decoded.payload(), StandardCharsets.UTF_8), "decoded payload mismatch");
    }

    @Test
    void writeBatchDropsOversizedMergedBufferRetention() throws Exception {
        RecordingOutputStream output = new RecordingOutputStream();
        SessionWriterTransport transport = new SessionWriterTransport(new TestOwner(output, OVERSIZED_FRAME_SETTINGS.limits()));
        byte[] payload = repeated('x', RETAINED_ENCODED_BATCH_LIMIT);
        SessionRuntime.OutboundFrame outbound = new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, 0, 1L, payload),
                null,
                payload.length,
                false,
                false
        );

        long written = transport.writeBatch(Collections.singletonList(outbound));

        assertTrue(written > RETAINED_ENCODED_BATCH_LIMIT, "encoded frame should exceed the retained scratch cap");
        assertEquals(0, encodedBatchScratchLength(transport), "oversized merged write buffer should be dropped");
    }

    private static int encodedBatchScratchLength(SessionWriterTransport transport) throws Exception {
        Field field = SessionWriterTransport.class.getDeclaredField("encodedBatchScratch");
        field.setAccessible(true);
        return ((byte[]) field.get(transport)).length;
    }

    private static byte[] repeated(char value, int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) value;
        }
        return bytes;
    }

    private static final class TestOwner implements SessionWriterTransport.Owner {
        private final RecordingOutputStream output;
        private final Limits limits;
        private final GatheringByteChannel gatheringOutput;

        private TestOwner(RecordingOutputStream output) {
            this(output, Settings.defaults().limits());
        }

        private TestOwner(RecordingOutputStream output, Limits limits) {
            this(output, limits, null);
        }

        private TestOwner(RecordingOutputStream output, Limits limits, GatheringByteChannel gatheringOutput) {
            this.output = output;
            this.limits = limits;
            this.gatheringOutput = gatheringOutput;
        }

        @Override
        public OutputStream output() {
            return output;
        }

        @Override
        public GatheringByteChannel gatheringOutput() {
            return gatheringOutput;
        }

        @Override
        public Limits limits() {
            return limits;
        }
    }

    private static final class RecordingOutputStream extends OutputStream {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private int flushes;

        @Override
        public void write(int value) {
            bytes.write(value);
        }

        @Override
        public void write(byte[] buffer, int offset, int length) {
            bytes.write(buffer, offset, length);
        }

        @Override
        public void flush() {
            flushes++;
        }

        int size() {
            return bytes.size();
        }

        int flushes() {
            return flushes;
        }

        byte[] bytes() {
            return bytes.toByteArray();
        }
    }

    private static final class RecordingGatheringByteChannel implements GatheringByteChannel {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private boolean open = true;
        private int writeCalls;

        @Override
        public int write(ByteBuffer src) throws java.io.IOException {
            long written = write(new ByteBuffer[]{src}, 0, 1);
            return (int) written;
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws java.io.IOException {
            if (!open) {
                throw new ClosedChannelException();
            }
            writeCalls++;
            long written = 0L;
            for (int i = offset; i < offset + length; i++) {
                ByteBuffer src = srcs[i];
                int remaining = src.remaining();
                if (remaining == 0) {
                    continue;
                }
                byte[] copy = new byte[remaining];
                src.get(copy);
                bytes.write(copy, 0, copy.length);
                written += remaining;
            }
            return written;
        }

        @Override
        public long write(ByteBuffer[] srcs) throws java.io.IOException {
            return write(srcs, 0, srcs.length);
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }

        int size() {
            return bytes.size();
        }

        int writeCalls() {
            return writeCalls;
        }

        byte[] bytes() {
            return bytes.toByteArray();
        }
    }
}
