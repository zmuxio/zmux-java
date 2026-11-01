package io.zmux.internal.wire;

import io.zmux.carrier.VarInt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.zmux.testutil.SpecFixtures;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

/**
 * Fuzz-style tests: seeded random and mutated inputs never throw anything but
 * {@link WireException}, never read outside their range, and what decodes
 * re-encodes stably.
 */
class FuzzTest {
    private static final long SEED = Long.getLong("zmux.fuzz.seed", 0x5eed_2026L);
    private static final int ROUNDS = Integer.getInteger("zmux.fuzz.rounds", 200_000);

    static List<byte[]> seeds() throws Exception {
        List<byte[]> out = new ArrayList<>();
        for (String n : List.of("wire/valid.ndjson", "wire/invalid.ndjson")) {
            for (var e : SpecFixtures.ndjson(n)) if (e.get("hex") instanceof String h) out.add(SpecFixtures.hex(h));
        }
        return out;
    }

    static byte[] mutate(byte[] in, SplittableRandom r) {
        byte[] b = in.clone();
        switch (r.nextInt(5)) {
            case 0 -> { if (b.length > 0) b[r.nextInt(b.length)] ^= (byte) (1 << r.nextInt(8)); }
            case 1 -> { if (b.length > 0) b[r.nextInt(b.length)] = (byte) r.nextInt(256); }
            case 2 -> b = java.util.Arrays.copyOf(b, r.nextInt(b.length + 1));
            case 3 -> {
                byte[] x = new byte[b.length + 1 + r.nextInt(8)];
                System.arraycopy(b, 0, x, 0, b.length);
                for (int i = b.length; i < x.length; i++) x[i] = (byte) r.nextInt(256);
                b = x;
            }
            default -> {
                b = new byte[r.nextInt(64)];
                for (int i = 0; i < b.length; i++) b[i] = (byte) r.nextInt(256);
            }
        }
        return b;
    }

    /** Embeds the input in a larger buffer so that a read past the range would see guard bytes. */
    static void exercise(byte[] in) {
        byte[] padded = new byte[in.length + 32];
        System.arraycopy(in, 0, padded, 16, in.length);
        ByteBuffer b = ByteBuffer.wrap(padded);
        int pos = 16, end = 16 + in.length;
        Frame f = new Frame();
        FrameDecoder d = new FrameDecoder();
        try {
            int n = d.decode(b, pos, end, f);
            assertTrue(n > 0 && n <= in.length);
            assertTrue(f.dataPos == 0 || (f.dataPos >= pos && f.dataPos + f.dataLen <= end));
            byte[] exact = new byte[n];
            System.arraycopy(in, 0, exact, 0, n);
            CorpusTest.checkRoundTrip("fuzz", exact, false);
            if (f.kind == FrameKind.TRANSPORT_PARAMETERS) {
                TransportParams p = TransportParams.decode(b, f.dataPos, f.dataLen);
                byte[] enc = p.toBytes();
                TransportParams q = TransportParams.decode(ByteBuffer.wrap(enc), 0, enc.length);
                assertArrayEquals(enc, q.toBytes());
            }
        } catch (WireException expected) {
            // rejected input
        }
        try {
            Preamble.parse(b, pos, in.length, 1 << 16);
        } catch (WireException expected) {
            // rejected input
        }
        try {
            TransportParams.decode(b, pos, in.length);
        } catch (WireException expected) {
            // rejected input
        }
        try {
            Records.size(b, pos, end, 65535);
        } catch (WireException expected) {
            // rejected input
        }
        new Records.FirstRecordCheck(65535, true, true).feed(b, pos, end);
    }

    @Test
    void mutatedCorpus() throws Exception {
        List<byte[]> seeds = seeds();
        SplittableRandom r = new SplittableRandom(SEED);
        for (int i = 0; i < ROUNDS; i++) {
            byte[] in = seeds.get(r.nextInt(seeds.size()));
            for (int k = r.nextInt(3); k >= 0; k--) in = mutate(in, r);
            exercise(in);
        }
    }

    @Test
    void parameterBlocks() {
        SplittableRandom r = new SplittableRandom(SEED + 1);
        long[] ids = {0x01, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x1d, 0x20, TransportParams.ID_MAX_RECORD_SIZE,
                TransportParams.ID_ROLE, TransportParams.ID_GOAWAY, TransportParams.ID_PREAMBLE,
                TransportParams.ID_MULTIPATH, TransportParams.ID_PIPE_ACK, TransportParams.ID_MAX_LANES,
                TransportParams.ID_RESUME, TransportParams.ID_APP_PROTOCOLS, 27, 58};
        for (int i = 0; i < ROUNDS / 4; i++) {
            ByteBuffer b = ByteBuffer.allocate(512);
            for (int k = r.nextInt(6); k > 0; k--) {
                VarInt.put(b, ids[r.nextInt(ids.length)]);
                int l = r.nextInt(20);
                VarInt.put(b, l);
                for (int j = 0; j < l; j++) b.put((byte) r.nextInt(256));
            }
            byte[] in = java.util.Arrays.copyOf(b.array(), b.position());
            exercise(in);
            try {
                TransportParams p = TransportParams.decode(ByteBuffer.wrap(in), 0, in.length);
                byte[] enc = p.toBytes();
                assertArrayEquals(enc, TransportParams.decode(ByteBuffer.wrap(enc), 0, enc.length).toBytes());
            } catch (WireException expected) {
                // rejected input
            }
        }
    }
}
