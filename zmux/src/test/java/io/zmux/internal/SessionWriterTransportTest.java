package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

final class SessionWriterTransportTest {
    private static final Settings LARGE_FRAME_SETTINGS = Settings.defaults()
            .toBuilder()
            .maxFramePayload(32 * 1024L)
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

        private TestOwner(RecordingOutputStream output) {
            this(output, Settings.defaults().limits());
        }

        private TestOwner(RecordingOutputStream output, Limits limits) {
            this.output = output;
            this.limits = limits;
        }

        @Override
        public OutputStream output() {
            return output;
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
}
