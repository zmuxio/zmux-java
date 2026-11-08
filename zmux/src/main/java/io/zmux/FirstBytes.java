package io.zmux;

import io.zmux.carrier.Framing;
import io.zmux.carrier.VarInt;
import io.zmux.internal.wire.FrameType;
import java.util.Objects;

/**
 * What the first bytes of a carrier say: the one authority for hosts and transforms that tell
 * zmux records from other content, at admission or when drawing a prefix that must not look
 * like one (redrawn until both framings give {@link Kind#OTHER}). It judges the syntax of a
 * prefix only, not the limits of a receiver. Bytes beginning 16 03, as TLS does, are always
 * {@link Kind#OTHER}.
 *
 * @param kind the outcome
 * @param need the total number of bytes needed with {@link Kind#NEED_MORE} (at most {@link #MAX}), else 0
 */
public record FirstBytes(Kind kind, int need) {
    /** The most bytes {@link #classify} needs. */
    public static final int MAX = 5;

    private static final long MAX_RECORD_SIZE = 65535;
    private static final FirstBytes RECORD = new FirstBytes(Kind.RECORD, 0), OTHER = new FirstBytes(Kind.OTHER, 0);

    /** The outcomes. */
    public enum Kind {
        /** More bytes are needed to tell. */
        NEED_MORE,
        /** They can begin a first record of this specification. */
        RECORD,
        /** They cannot. */
        OTHER
    }

    /**
     * Classifies b's first {@code remaining()} bytes, not moving its position: on a byte stream a
     * Size of 9 up to the largest record size, as a variable-length integer of 1, 2 or 4 bytes,
     * then a first frame type; in a message, the first frame type at once.
     */
    public static FirstBytes classify(java.nio.ByteBuffer b, Framing framing) {
        Objects.requireNonNull(framing, "framing");
        int pos = b.position(), have = b.remaining();
        if (have == 0) return new FirstBytes(Kind.NEED_MORE, 1);
        if (framing == Framing.MESSAGES) return firstLead(b.get(pos)) ? RECORD : OTHER;
        int first = b.get(pos) & 0xff, l = 1 << (first >> 6);
        if (l == 8 || l == 1 && first < 9 || l == 4 && have >= 2 && b.get(pos + 1) != 0) return OTHER;
        if (have < l) return new FirstBytes(Kind.NEED_MORE, l + 1);
        long size = first & 0x3f;
        for (int i = 1; i < l; i++) size = size << 8 | b.get(pos + i) & 0xff;
        if (size < 9 || size > MAX_RECORD_SIZE) return OTHER;
        if (have == l) return new FirstBytes(Kind.NEED_MORE, l + 1);
        return firstLead(b.get(pos + l)) ? RECORD : OTHER;
    }

    /** {@link #classify(java.nio.ByteBuffer, Framing)} of a byte array. */
    public static FirstBytes classify(byte[] b, Framing framing) {
        return classify(java.nio.ByteBuffer.wrap(b), framing);
    }

    private static boolean firstLead(byte b) {
        for (byte l : LEADS) if (l == b) return true;
        return false;
    }

    /** The first bytes of the encodings of the frame types that may begin a first record. */
    private static final byte[] LEADS = new byte[FrameType.FIRST_FRAME_TYPES.length];
    static {
        for (int i = 0; i < LEADS.length; i++) {
            var e = java.nio.ByteBuffer.allocate(8);
            VarInt.put(e, FrameType.FIRST_FRAME_TYPES[i]);
            LEADS[i] = e.get(0);
        }
    }
}
