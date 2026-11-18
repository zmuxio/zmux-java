package io.zmux.internal.wire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Property tests of the wire parsers: what the encoders write the decoders read back, whatever the field values and
 * the way the bytes are split, and arbitrary bytes are rejected with a {@link WireException} or decoded within their
 * range, never anything else. Each test runs {@link #ITERATIONS} cases from a {@link SplittableRandom} for each fixed
 * seed, so a run is the same every time; a failure names the seed and the case number, and the case is reproduced by
 * running that seed.
 */
class WirePropertyTest {
    private static final long MAX_60 = 1L << 60;
    private static final int ITERATIONS = 200;
    private static final String SEEDS = "seed {0}";

    /** One generated case of a property. */
    private interface Case { void run(SplittableRandom rnd) throws Exception; }

    private static void forAll(long seed, Case c) {
        var rnd = new SplittableRandom(seed);
        for (int i = 0; i < ITERATIONS; i++) {
            try {
                c.run(rnd);
            } catch (Exception | AssertionError e) {
                throw new AssertionError("seed " + seed + ", case " + i + ": " + e, e);
            }
        }
    }

    // ---- generators ----

    private static final long[] BOUNDARIES = {0L, 63L, 64L, 16383L, 16384L, (1L << 30) - 1, 1L << 30, (1L << 62) - 1};

    /** Varint values, the encoding boundaries over-represented. */
    private static long varint(SplittableRandom rnd) {
        return switch (rnd.nextInt(3)) {
            case 0 -> rnd.nextLong(VarInt.MAX + 1);
            case 1 -> BOUNDARIES[rnd.nextInt(BOUNDARIES.length)];
            default -> rnd.nextLong((1L << 14) + 1);
        };
    }

    private static long id60(SplittableRandom rnd) {
        return rnd.nextLong(MAX_60 + 1);
    }

    private static byte[] bytes(SplittableRandom rnd, int maxSize) {
        byte[] b = new byte[rnd.nextInt(maxSize + 1)];
        if (rnd.nextInt(4) == 0) {
            // Bytes that start varints and frames: the parsers go deeper than on uniform bytes.
            byte[] pool = {0, 1, 2, 3, 0x3f, 0x40, 0x41, (byte) 0x80, (byte) 0xc0, (byte) 0xff};
            for (int i = 0; i < b.length; i++) b[i] = pool[rnd.nextInt(pool.length)];
        } else {
            for (int i = 0; i < b.length; i++) b[i] = (byte) rnd.nextInt(256);
        }
        return b;
    }

    private static ByteBuffer buffer(int n) {
        return ByteBuffer.allocate(n);
    }

    // ---- varint ----

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void varintRoundTrip(long seed) {
        forAll(seed, rnd -> {
            long v = varint(rnd);
            var b = buffer(8);
            VarInt.put(b, v);
            b.flip();
            assertEquals(VarInt.length(v), b.remaining());
            assertEquals(v, VarInt.get(b, 0, b.limit()));
            assertEquals(b.remaining(), VarInt.lengthOf(b.get(0)));
        });
    }

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void varintNonMinimal(long seed) {
        forAll(seed, rnd -> {
            long v = varint(rnd);
            int extra = rnd.nextInt(4);
            int n = Math.min(8, Math.max(VarInt.length(v), 1 << extra));
            var b = buffer(8);
            VarInt.putN(b, v, n);
            b.flip();
            assertEquals(n, b.remaining());
            assertEquals(n, VarInt.lengthOf(b.get(0)));
            assertEquals(v, VarInt.get(b, 0, n));
        });
    }

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void varintTruncated(long seed) {
        forAll(seed, rnd -> {
            long v = varint(rnd);
            var b = buffer(8);
            VarInt.put(b, v);
            int n = b.position();
            b.flip();
            for (int limit = 0; limit < n; limit++) assertEquals(-1, VarInt.get(b, 0, limit));
        });
    }

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void varintOfArbitraryBytes(long seed) {
        forAll(seed, rnd -> {
            byte[] in = bytes(rnd, 96);
            long v = VarInt.get(ByteBuffer.wrap(in), 0, in.length);
            assertTrue(v >= -1 && v <= VarInt.MAX);
            if (v >= 0) assertTrue(VarInt.lengthOf(in[0]) <= in.length);
        });
    }

    // ---- frames ----

    /** One frame the encoder writes and what the decoder must read of it. */
    interface Check { void verify(Frame f); }
    record Encoded(byte[] bytes, Check check) {}

    private interface Writer { void write(ByteBuffer b); }

    private static Encoded encode(int capacity, Writer w, Check c) {
        var b = buffer(capacity);
        w.write(b);
        b.flip();
        byte[] out = new byte[b.remaining()];
        b.get(out);
        return new Encoded(out, c);
    }

    /** A frame of a drawn form, with three drawn varints for its fields. */
    private static Encoded frame(SplittableRandom rnd) {
        long x = varint(rnd), y = varint(rnd), z = varint(rnd);
        return switch (rnd.nextInt(10)) {
            case 0 -> encode(64, b -> FrameEncoder.resetStream(b, x, y, z), f -> {
                assertEquals(FrameKind.RESET_STREAM, f.kind);
                assertEquals(List.of(x, y, z), List.of(f.streamId, f.code, f.finalSize));
            });
            case 1 -> encode(64, b -> FrameEncoder.resetStreamAt(b, x, y, z, z / 2), f -> {
                assertEquals(FrameKind.RESET_STREAM_AT, f.kind);
                assertEquals(List.of(x, y, z, z / 2), List.of(f.streamId, f.code, f.finalSize, f.reliableSize));
            });
            case 2 -> encode(64, b -> FrameEncoder.stopSending(b, x, y), f -> {
                assertEquals(FrameKind.STOP_SENDING, f.kind);
                assertEquals(List.of(x, y), List.of(f.streamId, f.code));
            });
            case 3 -> encode(64, b -> FrameEncoder.maxData(b, x), f -> {
                assertEquals(FrameKind.MAX_DATA, f.kind);
                assertEquals(x, f.value);
            });
            case 4 -> encode(64, b -> FrameEncoder.maxStreamData(b, x, y), f -> {
                assertEquals(FrameKind.MAX_STREAM_DATA, f.kind);
                assertEquals(List.of(x, y), List.of(f.streamId, f.value));
            });
            case 5 -> encode(64, b -> FrameEncoder.streamDataBlocked(b, x, y), f -> {
                assertEquals(FrameKind.STREAM_DATA_BLOCKED, f.kind);
                assertEquals(List.of(x, y), List.of(f.streamId, f.value));
            });
            case 6 -> encode(64, b -> FrameEncoder.goAway(b, x % MAX_60, y % MAX_60, z), f -> {
                assertEquals(FrameKind.GOAWAY, f.kind);
                assertEquals(List.of(x % MAX_60, y % MAX_60, z), List.of(f.maxBidi, f.maxUni, f.code));
            });
            case 7 -> encode(64, b -> FrameEncoder.priorityUpdate(b, x, y, (int) (z % 8), (z & 8) != 0), f -> {
                assertEquals(FrameKind.PRIORITY_UPDATE, f.kind);
                assertEquals(List.of(x, y, z % 8), List.of(f.streamId, f.seq, (long) f.urgency));
                assertEquals((z & 8) != 0, f.incremental);
            });
            case 8 -> encode(64, b -> FrameEncoder.ping(b, (x & 1) != 0, x), f -> {
                assertEquals(FrameKind.PING, f.kind);
                assertEquals(x, f.value);
                assertEquals((x & 1) != 0, f.response);
            });
            default -> {
                byte[] reason = new byte[(int) (z % 40)];
                boolean app = (y & 1) != 0;
                yield encode(FrameEncoder.closeLength(app, x, y, reason.length), b -> FrameEncoder.close(b, app, x, y, reason), f -> {
                    assertEquals(FrameKind.CONNECTION_CLOSE, f.kind);
                    assertEquals(app, f.application);
                    assertEquals(x, f.code);
                    assertEquals(reason.length, f.dataLen);
                    if (!app) assertEquals(y, f.errorFrameType);
                });
            }
        };
    }

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void frameRoundTrip(long seed) {
        forAll(seed, rnd -> {
            Encoded e = frame(rnd);
            var f = new Frame();
            int n = new FrameDecoder().decode(ByteBuffer.wrap(e.bytes()), 0, e.bytes().length, f);
            assertEquals(e.bytes().length, n);
            e.check().verify(f);
        });
    }

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void frameTruncated(long seed) {
        forAll(seed, rnd -> {
            Encoded e = frame(rnd);
            var in = ByteBuffer.wrap(e.bytes());
            for (int limit = 0; limit < e.bytes().length; limit++) {
                int cut = limit;
                assertThrows(WireException.class, () -> new FrameDecoder().decode(in, 0, cut, new Frame()), "cut at " + cut);
            }
        });
    }

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void streamFrameRoundTrip(long seed) {
        forAll(seed, rnd -> {
            long id = id60(rnd);
            int n = rnd.nextInt(4001);
            long off = varint(rnd) % (1L << 40);
            boolean fin = rnd.nextBoolean(), withLength = rnd.nextBoolean();
            var b = buffer(32 + n);
            FrameEncoder.streamHeader(b, id, off, n, fin, withLength);
            int header = b.position();
            b.put(new byte[n]);
            b.flip();
            var f = new Frame();
            assertEquals(header + n, new FrameDecoder().decode(b, 0, b.limit(), f));
            assertEquals(FrameKind.STREAM, f.kind);
            assertEquals(List.of(id, off, (long) n), List.of(f.streamId, f.offset, (long) f.dataLen));
            assertEquals(fin, f.fin);
            assertEquals(header, f.dataPos);
        });
    }

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void arbitraryFramesAreRejectedOrWithinRange(long seed) {
        forAll(seed, rnd -> {
            byte[] in = bytes(rnd, 96);
            byte[] padded = new byte[in.length + 8];
            System.arraycopy(in, 0, padded, 4, in.length);
            var f = new Frame();
            try {
                int n = new FrameDecoder().decode(ByteBuffer.wrap(padded), 4, 4 + in.length, f);
                assertTrue(n > 0 && n <= in.length, "length " + n + " of " + in.length);
                assertTrue(f.dataLen == 0 || (f.dataPos >= 4 && f.dataPos + f.dataLen <= 4 + in.length));
            } catch (WireException rejected) {
                // rejected input
            }
        });
    }

    // ---- preamble ----

    private static Preamble preamble(SplittableRandom rnd) {
        byte[] openInfo = rnd.nextBoolean() ? null : bytes(rnd, 40);
        int urgency = rnd.nextInt(-1, 8);
        boolean incremental = rnd.nextBoolean(), carrier = rnd.nextBoolean();
        List<Preamble.Tlv> tlvs = new ArrayList<>();
        for (int n = rnd.nextInt(5); n > 0; n--) {
            long type;
            do {
                type = rnd.nextLong(Preamble.TLV_APP_FIRST, Preamble.TLV_APP_LAST + 1);
            } while (!Preamble.isApplication(type));
            tlvs.add(new Preamble.Tlv(type, bytes(rnd, 24)));
        }
        return new Preamble(0, openInfo, urgency, urgency >= 0 && incremental, carrier, tlvs, null);
    }

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void preambleRoundTrip(long seed) {
        forAll(seed, rnd -> {
            Preamble p = preamble(rnd);
            var b = buffer(p.encodedSize());
            p.encode(b);
            assertFalse(b.hasRemaining());
            b.flip();
            Preamble q = Preamble.parse(b, 0, b.limit(), 1 << 16);
            assertEquals(b.limit(), q.size());
            assertArrayEquals(p.openInfo(), q.openInfo());
            assertEquals(p.urgency(), q.urgency());
            assertEquals(p.incremental(), q.incremental());
            assertEquals(p.pipeCarrier(), q.pipeCarrier());
            assertEquals(p.applicationTlvs().size(), q.applicationTlvs().size());
            for (int i = 0; i < p.applicationTlvs().size(); i++) {
                assertEquals(p.applicationTlvs().get(i).type(), q.applicationTlvs().get(i).type());
                assertArrayEquals(p.applicationTlvs().get(i).value(), q.applicationTlvs().get(i).value());
            }
        });
    }

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void preambleTruncated(long seed) {
        forAll(seed, rnd -> {
            Preamble p = preamble(rnd);
            var b = buffer(p.encodedSize());
            p.encode(b);
            b.flip();
            for (int len = 0; len < b.limit(); len++) {
                int cut = len;
                assertThrows(WireException.class, () -> Preamble.parse(b, 0, cut, 1 << 16), "cut at " + cut);
            }
        });
    }

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void preambleAboveItsLimit(long seed) {
        forAll(seed, rnd -> {
            Preamble p = preamble(rnd);
            var b = buffer(p.encodedSize());
            p.encode(b);
            b.flip();
            assertThrows(WireException.class, () -> Preamble.parse(b, 0, b.limit(), b.limit() - 1L));
        });
    }

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void arbitraryPreamblesAreRejectedOrWithinRange(long seed) {
        forAll(seed, rnd -> {
            byte[] in = bytes(rnd, 96);
            try {
                Preamble p = Preamble.parse(ByteBuffer.wrap(in), 0, in.length, 1 << 16);
                assertTrue(p.size() > 0 && p.size() <= in.length);
            } catch (WireException rejected) {
                // rejected input
            }
        });
    }

    // ---- capsules ----

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void capsuleHeaderAcrossSplits(long seed) {
        forAll(seed, rnd -> {
            long type = varint(rnd), length = varint(rnd);
            int[] cuts = new int[rnd.nextInt(13)];
            for (int i = 0; i < cuts.length; i++) cuts[i] = rnd.nextInt(1, 6);
            var whole = buffer(16);
            VarInt.put(whole, type);
            VarInt.put(whole, length);
            whole.flip();
            byte[] bytes = new byte[whole.remaining()];
            whole.get(bytes);
            var header = new Capsule.Header();
            assertTrue(header.idle());
            int at = 0;
            boolean done = false;
            for (int i = 0; !done; i++) {
                int step = Math.min(bytes.length - at, cuts.length == 0 ? bytes.length : cuts[i % cuts.length]);
                var piece = ByteBuffer.wrap(bytes, at, step);
                at += step;
                done = header.feed(piece);
                assertFalse(piece.hasRemaining());
                if (!done) assertTrue(at < bytes.length, "header incomplete after all its bytes");
            }
            assertEquals(bytes.length, at);
            assertEquals(type, header.type());
            assertEquals(length, header.length());
            assertTrue(header.idle());
        });
    }

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void datagramCapsuleHeader(long seed) {
        forAll(seed, rnd -> {
            long context = varint(rnd);
            int payload = rnd.nextInt(70_001);
            var b = buffer(Capsule.MAX_DATAGRAM_HEADER);
            Capsule.putDatagramHeader(b, context, payload);
            assertEquals(Capsule.datagramHeaderLength(context, payload), b.position());
            byte[] a = new byte[Capsule.MAX_DATAGRAM_HEADER];
            assertEquals(b.position(), Capsule.putDatagramHeader(a, 0, context, payload));
            b.flip();
            byte[] written = new byte[b.remaining()];
            b.duplicate().get(written);
            assertArrayEquals(Arrays.copyOf(a, written.length), written);
            var header = new Capsule.Header();
            assertTrue(header.feed(b));
            assertEquals(Capsule.DATAGRAM, header.type());
            var field = new Capsule.Field();
            assertTrue(field.feed(b));
            assertEquals(context, field.value());
            assertEquals(VarInt.length(context) + (long) payload, header.length());
            assertFalse(b.hasRemaining());
        });
    }

    @ParameterizedTest(name = SEEDS) @ValueSource(longs = {1, 2, 3, 4, 5})
    void capsuleFieldOfArbitraryBytes(long seed) {
        forAll(seed, rnd -> {
            byte[] in = bytes(rnd, 96);
            var field = new Capsule.Field();
            var b = ByteBuffer.wrap(in);
            if (field.feed(b)) {
                assertTrue(field.value() >= 0 && field.value() <= VarInt.MAX);
                assertEquals(field.length(), VarInt.lengthOf(in[0]));
            } else {
                assertFalse(b.hasRemaining());
            }
        });
    }
}
