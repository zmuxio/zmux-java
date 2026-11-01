package io.zmux.carrier;

import java.nio.ByteBuffer;

/** QUIC variable-length integers (RFC 9000 Section 16): the one encoder and decoder of the library. */
public final class VarInt {
    /** The largest encodable value, 2^62-1. */
    public static final long MAX = (1L << 62) - 1;

    private VarInt() {}

    /** Length of the shortest encoding of {@code v} ({@code 0 <= v <= MAX}). */
    public static int length(long v) {
        if (v < (1L << 6)) return 1;
        if (v < (1L << 14)) return 2;
        if (v < (1L << 30)) return 4;
        return 8;
    }

    /** Writes the shortest encoding at the buffer's position. */
    public static void put(ByteBuffer b, long v) {
        putN(b, v, length(v));
    }

    /** Writes {@code v} in exactly {@code n} bytes (1, 2, 4 or 8); v must fit. */
    public static void putN(ByteBuffer b, long v, int n) {
        switch (n) {
            case 1 -> b.put((byte) v);
            case 2 -> b.putShort((short) (v | 0x4000));
            case 4 -> b.putInt((int) (v | 0x8000_0000L));
            default -> b.putLong(v | 0xc000_0000_0000_0000L);
        }
    }

    /** Writes the shortest encoding into {@code a} at {@code i}; returns the next index. */
    public static int put(byte[] a, int i, long v) {
        int n = length(v);
        long x = n == 1 ? v : n == 2 ? v | 0x4000 : n == 4 ? v | 0x8000_0000L : v | 0xc000_0000_0000_0000L;
        for (int k = n - 1; k >= 0; k--) {
            a[i + k] = (byte) x;
            x >>>= 8;
        }
        return i + n;
    }

    /** Encoded length of the varint whose first byte is {@code first}. */
    public static int lengthOf(byte first) {
        return 1 << ((first & 0xff) >>> 6);
    }

    /**
     * Decodes the varint at absolute index {@code i} of {@code b}, which must hold
     * {@link #lengthOf} bytes before {@code limit}; returns -1 when truncated.
     */
    public static long get(ByteBuffer b, int i, int limit) {
        if (i >= limit) return -1;
        int first = b.get(i) & 0xff;
        int n = 1 << (first >>> 6);
        if (limit - i < n) return -1;
        return switch (n) {
            case 1 -> first;
            case 2 -> b.getShort(i) & 0x3fff;
            case 4 -> b.getInt(i) & 0x3fff_ffffL;
            default -> b.getLong(i) & 0x3fff_ffff_ffff_ffffL;
        };
    }
}
