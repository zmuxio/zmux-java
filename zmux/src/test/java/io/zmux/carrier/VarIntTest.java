package io.zmux.carrier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/** RFC 9000 Section 16 and Appendix A.1 encodings through the public helper. */
class VarIntTest {
    @Test
    void rfcExamples() {
        check(37, "25");
        check(15293, "7bbd");
        check(494878333, "9d7f3e7d");
        check(151288809941952652L, "c2197c5eff14e88c");
    }

    @Test
    void boundaries() {
        long[] v = {0, 63, 64, 16383, 16384, (1L << 30) - 1, 1L << 30, VarInt.MAX};
        int[] n = {1, 1, 2, 2, 4, 4, 8, 8};
        for (int i = 0; i < v.length; i++) {
            assertEquals(n[i], VarInt.length(v[i]));
            byte[] a = new byte[8];
            assertEquals(n[i], VarInt.put(a, 0, v[i]));
            assertEquals(n[i], VarInt.lengthOf(a[0]));
            assertEquals(v[i], VarInt.get(ByteBuffer.wrap(a), 0, n[i]));
            assertEquals(-1, VarInt.get(ByteBuffer.wrap(a), 0, n[i] - 1), "truncated");
        }
    }

    private static void check(long v, String hex) {
        byte[] want = java.util.HexFormat.of().parseHex(hex);
        ByteBuffer b = ByteBuffer.allocate(8);
        VarInt.put(b, v);
        assertArrayEquals(want, java.util.Arrays.copyOf(b.array(), b.position()));
        assertEquals(v, VarInt.get(ByteBuffer.wrap(want), 0, want.length));
    }
}
