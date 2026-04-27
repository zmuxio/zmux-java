package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

final class SessionWriterTransportTest {
    @Test
    void writeBatchUsesGatheringOutputForLargePayloadWhenAvailable() throws Exception {
        RecordingOutputStream output = new RecordingOutputStream();
        RecordingGatheringChannel gathering = new RecordingGatheringChannel();
        SessionWriterTransport transport = new SessionWriterTransport(new TestOwner(output, gathering));

        byte[][] parts = {
                repeated('a', 8 * 1024),
                repeated('b', 8 * 1024)
        };
        byte[] prefix = FrameCodec.buildOpenMetadataPrefix(
                Protocol.CAPABILITY_OPEN_METADATA,
                null,
                null,
                "meta".getBytes(StandardCharsets.UTF_8),
                Settings.defaults().maxFramePayload()
        );
        FrameCodec.Frame frame = new FrameCodec.Frame(
                FrameType.DATA,
                Protocol.FRAME_FLAG_OPEN_METADATA | Protocol.FRAME_FLAG_FIN,
                1L,
                prefix
        );
        SessionRuntime.OutboundFrame outbound = new SessionRuntime.OutboundFrame(
                frame,
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

        assertTrue(gathering.gatherWrites() > 0, "gathering output should receive the batch");
        assertEquals(0, output.size(), "OutputStream path should not encode a duplicate batch");
        assertEquals(1, output.flushes(), "transport should preserve batch flush semantics");
        assertEquals(gathering.bytes().length, written, "reported batch byte count should match encoded bytes");

        FrameCodec.Frame decoded = FrameCodec.readFrame(
                new ByteArrayInputStream(gathering.bytes()),
                Settings.defaults().limits()
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
    void writeBatchKeepsTinyPayloadOnEncodedOutputPath() throws Exception {
        RecordingOutputStream output = new RecordingOutputStream();
        RecordingGatheringChannel gathering = new RecordingGatheringChannel();
        SessionWriterTransport transport = new SessionWriterTransport(new TestOwner(output, gathering));
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        SessionRuntime.OutboundFrame outbound = new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 1L, payload),
                null,
                payload.length,
                false,
                false
        );

        long written = transport.writeBatch(Collections.singletonList(outbound));

        assertEquals(0, gathering.gatherWrites(), "tiny payloads should avoid gathering overhead");
        assertEquals(output.size(), written, "reported fallback batch byte count should match output bytes");
        assertEquals(1, output.flushes(), "fallback path should flush once per batch");
    }

    @Test
    void writeBatchFallsBackToOutputStreamWithoutGatheringOutput() throws Exception {
        RecordingOutputStream output = new RecordingOutputStream();
        SessionWriterTransport transport = new SessionWriterTransport(new TestOwner(output, null));
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        SessionRuntime.OutboundFrame outbound = new SessionRuntime.OutboundFrame(
                new FrameCodec.Frame(FrameType.DATA, Protocol.FRAME_FLAG_FIN, 1L, payload),
                null,
                payload.length,
                false,
                false
        );

        long written = transport.writeBatch(Collections.singletonList(outbound));

        assertEquals(output.size(), written, "reported fallback batch byte count should match output bytes");
        assertEquals(1, output.flushes(), "fallback path should flush once per batch");
        FrameCodec.Frame decoded = FrameCodec.readFrame(
                new ByteArrayInputStream(output.bytes()),
                Settings.defaults().limits()
        );
        assertEquals(FrameType.DATA, decoded.type(), "decoded frame type mismatch");
        assertEquals("hello", new String(decoded.payload(), StandardCharsets.UTF_8), "decoded payload mismatch");
    }

    private static final class TestOwner implements SessionWriterTransport.Owner {
        private final RecordingOutputStream output;
        private final GatheringByteChannel gatheringOutput;

        private TestOwner(RecordingOutputStream output, GatheringByteChannel gatheringOutput) {
            this.output = output;
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
            return Settings.defaults().limits();
        }
    }

    private static byte[] repeated(char value, int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) value;
        }
        return bytes;
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

    private static final class RecordingGatheringChannel implements GatheringByteChannel {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private boolean open = true;
        private int gatherWrites;

        @Override
        public int write(ByteBuffer src) {
            int remaining = src.remaining();
            byte[] chunk = new byte[remaining];
            src.get(chunk);
            bytes.write(chunk, 0, chunk.length);
            return remaining;
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) {
            long total = 0L;
            gatherWrites++;
            for (int i = offset; i < offset + length; i++) {
                total += write(srcs[i]);
            }
            return total;
        }

        @Override
        public long write(ByteBuffer[] srcs) {
            return write(srcs, 0, srcs.length);
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() throws IOException {
            open = false;
        }

        int gatherWrites() {
            return gatherWrites;
        }

        byte[] bytes() {
            return bytes.toByteArray();
        }
    }
}
