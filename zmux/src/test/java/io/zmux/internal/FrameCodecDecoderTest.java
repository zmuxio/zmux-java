package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
