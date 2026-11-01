package io.zmux.internal.wire;

import io.zmux.carrier.VarInt;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A stream preamble (EXTENSIONS.md Section 4): parsed from, or encoded into, the
 * STREAM frame at offset 0.
 *
 * @param size Length field plus Length; 0 for a preamble to encode
 * @param openInfo OPEN_INFO value, or null when absent
 * @param urgency PRIORITY urgency, or -1 when absent
 * @param applicationTlvs application TLVs in order
 * @param grease a GREASE TLV to append when encoding, or null
 */
public record Preamble(int size, byte[] openInfo, int urgency, boolean incremental, boolean pipeCarrier,
                       List<Tlv> applicationTlvs, Tlv grease) {

    public static final long TLV_OPEN_INFO = 0x01;
    public static final long TLV_PRIORITY = 0x02;
    public static final long TLV_PIPE_CARRIER = 0x05;
    public static final long TLV_APP_FIRST = 0x1000;
    public static final long TLV_APP_LAST = 0x3fff;

    /** One TLV. */
    public record Tlv(long type, byte[] value) {}

    public boolean hasPriority() {
        return urgency >= 0;
    }

    /** Whether {@code t} is a GREASE TLV type (0x1f * N + 0x21). */
    public static boolean isGrease(long t) {
        return t >= 0x21 && Long.remainderUnsigned(t - 0x21, 0x1f) == 0;
    }

    /** Whether {@code t} is an application TLV type. */
    public static boolean isApplication(long t) {
        return t >= TLV_APP_FIRST && t <= TLV_APP_LAST && !isGrease(t);
    }

    /** The PRIORITY byte (EXTENSIONS.md Section 5.2). */
    public static byte priorityByte(int urgency, boolean incremental) {
        return (byte) (urgency << 5 | (incremental ? 0x10 : 0));
    }

    /**
     * Parses a preamble from the data of the STREAM frame at offset 0 alone
     * (EXTENSIONS.md Section 4.4 rule 1). Every violation is a PROTOCOL_VIOLATION.
     */
    public static Preamble parse(ByteBuffer b, int pos, int len, long limit) throws WireException {
        int end = pos + len;
        long l = VarInt.get(b, pos, end);
        if (l < 0) throw WireException.protocol("preamble Length incomplete");
        int n = VarInt.lengthOf(b.get(pos));
        long size = n + l;
        if (size > len) throw WireException.protocol("preamble longer than its frame");
        if (size > limit) throw WireException.protocol("preamble above the limit");
        byte[] openInfo = null;
        int urgency = -1;
        boolean incremental = false, carrier = false;
        List<Tlv> app = List.of();
        int i = pos + n, stop = pos + (int) size;
        while (i < stop) {
            long t = VarInt.get(b, i, stop);
            if (t < 0) throw WireException.protocol("truncated preamble TLV");
            i += VarInt.lengthOf(b.get(i));
            long vl = VarInt.get(b, i, stop);
            if (vl < 0) throw WireException.protocol("truncated preamble TLV");
            i += VarInt.lengthOf(b.get(i));
            if (vl > stop - i) throw WireException.protocol("truncated preamble TLV");
            int vp = i;
            i += (int) vl;
            if (t == TLV_OPEN_INFO) {
                if (openInfo != null) throw WireException.protocol("second OPEN_INFO");
                openInfo = new byte[(int) vl];
                b.get(vp, openInfo);
            } else if (t == TLV_PRIORITY) {
                if (urgency >= 0) throw WireException.protocol("second PRIORITY");
                int p = vl == 1 ? b.get(vp) & 0xff : -1;
                if (p < 0 || (p & 0x0f) != 0) throw WireException.protocol("bad PRIORITY TLV");
                urgency = p >>> 5;
                incremental = (p & 0x10) != 0;
            } else if (t == TLV_PIPE_CARRIER) {
                if (carrier) throw WireException.protocol("second PIPE_CARRIER");
                if (vl != 0) throw WireException.protocol("bad PIPE_CARRIER TLV");
                carrier = true;
            } else if (isApplication(t)) {
                byte[] v = new byte[(int) vl];
                b.get(vp, v);
                if (app.isEmpty()) app = new ArrayList<>(2);
                app.add(new Tlv(t, v));
            }
        }
        return new Preamble((int) size, openInfo, urgency, incremental, carrier,
                app.isEmpty() ? List.of() : List.copyOf(app), null);
    }

    private static int tlvLength(long t, int l) {
        return VarInt.length(t) + VarInt.length(l) + l;
    }

    /** Length of the TLVs. */
    public int bodyLength() {
        int n = 0;
        if (openInfo != null) n += tlvLength(TLV_OPEN_INFO, openInfo.length);
        if (urgency >= 0) n += tlvLength(TLV_PRIORITY, 1);
        if (pipeCarrier) n += tlvLength(TLV_PIPE_CARRIER, 0);
        for (Tlv a : applicationTlvs) n += tlvLength(a.type(), a.value().length);
        if (grease != null) n += tlvLength(grease.type(), grease.value().length);
        return n;
    }

    /** Encoded size. */
    public int encodedSize() {
        int l = bodyLength();
        return VarInt.length(l) + l;
    }

    private static void tlv(ByteBuffer b, long t, byte[] v) {
        VarInt.put(b, t);
        VarInt.put(b, v.length);
        b.put(v);
    }

    /** Encodes the TLVs in increasing type order with the GREASE TLV last (TRACES.md Section 3.3). */
    public void encode(ByteBuffer b) {
        VarInt.put(b, bodyLength());
        if (openInfo != null) tlv(b, TLV_OPEN_INFO, openInfo);
        if (urgency >= 0) tlv(b, TLV_PRIORITY, new byte[] {priorityByte(urgency, incremental)});
        if (pipeCarrier) tlv(b, TLV_PIPE_CARRIER, new byte[0]);
        for (Tlv a : applicationTlvs) tlv(b, a.type(), a.value());
        if (grease != null) tlv(b, grease.type(), grease.value());
    }
}
