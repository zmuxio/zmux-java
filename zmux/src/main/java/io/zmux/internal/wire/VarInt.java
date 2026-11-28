package io.zmux.internal.wire;

import io.zmux.internal.buffer.ReceiveSpans;
import java.nio.ByteBuffer;

/** QUIC variable-length integers (RFC 9000 Section 16): the one encoder and decoder of the library. */
public final class VarInt {
    /** The largest advertised cumulative stream count. */
    public static final long MAX_STREAM_COUNT = 1L << 60;
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
        long encoded = prefixed(v, n);
        switch (n) {
            case 1 -> b.put((byte) encoded);
            case 2 -> b.putShort((short) encoded);
            case 4 -> b.putInt((int) encoded);
            default -> b.putLong(encoded);
        }
    }

    /** Writes the shortest encoding into {@code a} at {@code i}; returns the next index. */
    public static int put(byte[] a, int i, long v) {
        int n = length(v);
        long x = prefixed(v, n);
        for (int k = n - 1; k >= 0; k--) {
            a[i + k] = (byte) x;
            x >>>= 8;
        }
        return i + n;
    }

    private static long prefixed(long value, int length) {
        return value | switch (length) {
            case 1 -> 0;
            case 2 -> 0x4000L;
            case 4 -> 0x8000_0000L;
            case 8 -> 0xc000_0000_0000_0000L;
            default -> throw new IllegalArgumentException("invalid QUIC varint width");
        };
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
        byte first = b.get(i);
        int n = lengthOf(first);
        if (limit - i < n) return -1;
        return switch (n) {
            case 1 -> first;
            case 2 -> b.getShort(i) & 0x3fff;
            case 4 -> b.getInt(i) & 0x3fff_ffffL;
            default -> b.getLong(i) & 0x3fff_ffff_ffff_ffffL;
        };
    }

    /** Reads a bounded varint from the contiguous input or the owned byte-carrier ranges. */
    public static long get(ByteBuffer b, ReceiveSpans spans, int i, int limit) {
        if (spans == null) return get(b, i, limit);
        if (i >= limit) return -1;
        ReceiveSpans.Span span = spans.span(i);
        int from = spans.position(span, i);
        byte first = span.data().get(from);
        int width = lengthOf(first);
        if (width > limit - i) return -1;
        if (width <= span.to() - from) return get(span.data(), from, from + width);
        long value = first & 0x3fL;
        for (int j = 1; j < width; j++) value = (value << 8) | (spans.get(i + j) & 0xffL);
        return value;
    }
    /** Decodes a QUIC varint after its width and availability have already been checked. */
    public static long getKnownWidth(byte[] bytes, int offset, int width) {
        long value = bytes[offset] & 0x3fL;
        for (int i = 1; i < width; i++) value = (value << 8) | (bytes[offset + i] & 0xffL);
        return value;
    }
}
