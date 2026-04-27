package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class FrameCodecDecoderTest {
    @Test
    void decoderReusesBufferedBytesAcrossPrefaceAndFrameReads() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Preface preface = new Preface(
                Protocol.PREFACE_VERSION,
                Role.INITIATOR,
                0L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                0L,
                Settings.defaults()
        );
        FrameCodec.writePreface(bytes, preface);
        FrameCodec.writeFrame(
                bytes,
                new FrameCodec.Frame(FrameType.PING, 0, 0L, new byte[]{0, 0, 0, 0, 0, 0, 0, 1}),
                Settings.defaults().limits()
        );

        CountingBurstInputStream input = new CountingBurstInputStream(bytes.toByteArray());
        FrameCodec.Decoder decoder = FrameCodec.decoder(input);

        Preface decodedPreface = decoder.readPreface();
        FrameCodec.Frame frame = decoder.readFrame(Settings.defaults().limits());

        assertEquals(Role.INITIATOR, decodedPreface.role(), "decoded preface role mismatch");
        assertEquals(FrameType.PING, frame.type(), "decoded frame type mismatch");
        assertEquals(8, frame.payload().length, "decoded frame payload length mismatch");
        assertEquals(0, input.singleByteReads(), "stateful decoder should not fall back to raw single-byte reads on a normal bulk-capable stream");
        assertEquals(1, input.bulkReads(), "decoder should preserve prefetched bytes across preface/frame boundaries");
    }

    @Test
    void directInputReadRejectsInvalidProgress() {
        int[] reads = {-2, 4};
        for (int read : reads) {
            assertThrows(
                    IOException.class,
                    () -> FrameCodec.readInputBytes(new InvalidProgressInputStream(read), 3),
                    "direct input reads should reject invalid progress " + read
            );
        }
    }

    @Test
    void decoderReadRejectsInvalidProgress() {
        int[] reads = {-2, 8193};
        for (int read : reads) {
            FrameCodec.Decoder decoder = FrameCodec.decoder(new InvalidProgressInputStream(read));
            assertThrows(
                    IOException.class,
                    () -> decoder.readFrame(Settings.defaults().limits()),
                    "buffered decoder should reject invalid progress " + read
            );
        }
    }

    @Test
    void decoderRejectsShortStreamIdBeforeConsumingNextFrameBytes() {
        byte[] bytes = new byte[]{2, (byte) FrameType.DATA.code(), (byte) 0xc0, 99, 98, 97};
        OneByteInputStream input = new OneByteInputStream(bytes);
        FrameCodec.Decoder decoder = FrameCodec.decoder(input);

        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> decoder.readFrame(Settings.defaults().limits())
        );

        assertEquals(ErrorCode.FRAME_SIZE.code(), error.code(), "short stream_id must fail as frame-size");
        assertEquals(3, input.reads(), "decoder must stop after the first stream_id byte");
    }

    private static final class CountingBurstInputStream extends InputStream {
        private final byte[] bytes;
        private int position;
        private int singleByteReads;
        private int bulkReads;

        private CountingBurstInputStream(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public int read() {
            singleByteReads++;
            if (position >= bytes.length) {
                return -1;
            }
            return bytes[position++] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            bulkReads++;
            if (position >= bytes.length) {
                return -1;
            }
            int copy = Math.min(length, bytes.length - position);
            System.arraycopy(bytes, position, buffer, offset, copy);
            position += copy;
            return copy;
        }

        private int singleByteReads() {
            return singleByteReads;
        }

        private int bulkReads() {
            return bulkReads;
        }
    }

    private static final class OneByteInputStream extends InputStream {
        private final byte[] bytes;
        private int position;
        private int reads;

        private OneByteInputStream(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public int read() {
            if (position >= bytes.length) {
                return -1;
            }
            reads++;
            return bytes[position++] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) {
            if (length == 0) {
                return 0;
            }
            if (position >= bytes.length) {
                return -1;
            }
            buffer[offset] = bytes[position++];
            reads++;
            return 1;
        }

        private int reads() {
            return reads;
        }
    }

    private static final class InvalidProgressInputStream extends InputStream {
        private final int read;

        private InvalidProgressInputStream(int read) {
            this.read = read;
        }

        @Override
        public int read() {
            return -1;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) {
            return read;
        }
    }
}
