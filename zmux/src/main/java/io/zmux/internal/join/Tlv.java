package io.zmux.internal.join;

import io.zmux.carrier.SecureCarrierException;
import static io.zmux.carrier.SecureCarrierException.Code.TLV_NON_MINIMAL;
import static io.zmux.carrier.SecureCarrierException.Code.TLV_ORDER;
import static io.zmux.carrier.SecureCarrierException.Code.TLV_TRUNCATED;
import static io.zmux.carrier.SecureCarrierException.Code.TLV_UNKNOWN_CRITICAL;

import io.zmux.carrier.VarInt;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/** The item encoding of handshake payloads. */
public final class Tlv {
    /** Takes each item of a payload in turn. */
    @FunctionalInterface
    public interface Visitor {
        /** Validates the value of a known type and returns true; returns false for an unknown type. */
        boolean visit(long type, byte[] value) throws SecureCarrierException;
    }

    /** One item and the index after it. */
    public record Item(long type, byte[] value, int next) {}

    private Tlv() {}

    /** Reads the {@code n}-byte varint at {@code off}; the bytes must be there. */
    public static long readVarint(byte[] b, int off, int n) {
        long v = b[off] & 0x3fL;
        for (int i = 1; i < n; i++) {
            v = (v << 8) | (b[off + i] & 0xffL);
        }
        return v;
    }

    /** Reads a shortest varint at {@code off} of the range ending at {@code end}; the next index is {@code off + VarInt.length(v)}. */
    public static long minimalVarint(byte[] b, int off, int end) throws SecureCarrierException {
        if (off >= end) throw new SecureCarrierException(TLV_TRUNCATED);
        int n = VarInt.lengthOf(b[off]);
        if (end - off < n) throw new SecureCarrierException(TLV_TRUNCATED);
        long v = readVarint(b, off, n);
        if (VarInt.length(v) != n) throw new SecureCarrierException(TLV_NON_MINIMAL);
        return v;
    }

    /** Reads one type, length and value at {@code off} of the range ending at {@code end}. */
    public static Item item(byte[] b, int off, int end) throws SecureCarrierException {
        long type = minimalVarint(b, off, end);
        int i = off + VarInt.length(type);
        long len = minimalVarint(b, i, end);
        i += VarInt.length(len);
        if (len > end - i) throw new SecureCarrierException(TLV_TRUNCATED);
        int stop = i + (int) len;
        return new Item(type, Arrays.copyOfRange(b, i, stop), stop);
    }

    /**
     * Applies the payload rules in byte order, so the first violation met is the result: syntax,
     * then order, then the value, then whether the type is known.
     */
    public static void parse(byte[] b, Visitor visitor) throws SecureCarrierException {
        boolean first = true;
        long last = 0;
        int i = 0;
        while (i < b.length) {
            Item it = item(b, i, b.length);
            if (!first && it.type() <= last) throw new SecureCarrierException(TLV_ORDER);
            first = false;
            last = it.type();
            i = it.next();
            if (!visitor.visit(it.type(), it.value()) && (it.type() & 1) == 1) {
                throw new SecureCarrierException(TLV_UNKNOWN_CRITICAL);
            }
        }
    }

    /** Appends a shortest varint. */
    public static void putVarint(ByteArrayOutputStream out, long v) {
        byte[] a = new byte[VarInt.length(v)];
        VarInt.put(a, 0, v);
        out.writeBytes(a);
    }

    /** Appends a whole item. */
    public static void putItem(ByteArrayOutputStream out, long type, byte[] value) {
        putVarint(out, type);
        putVarint(out, value.length);
        out.writeBytes(value);
    }

    /** A payload's padding field for an item of value length v, as a plan gives it: none, an empty item, or v bytes. */
    public static int paddingField(int v) {
        return v < 0 ? 0 : v == 0 ? -1 : v;
    }
}
